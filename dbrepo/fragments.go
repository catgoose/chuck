// Package dbrepo provides composable SQL fragment helpers for building queries.
//
// Most helpers in this package use @Name placeholders (e.g., @ID, @Name) which
// rely on database/sql's sql.Named() for driver-level parameter translation.
// This is distinct from the chuck.Dialect.Placeholder() method which returns
// engine-specific positional syntax ($1, ?, @p1) for raw SQL composition.
//
// The @Name convention works on database/sql drivers that translate
// sql.NamedArg into native parameter syntax — mattn/go-sqlite3,
// microsoft/go-mssqldb, and jackc/pgx all do this. It does NOT work on
// lib/pq, which leaves @Name tokens verbatim, and it does not survive
// sqlx.Rebind, which only recognizes ? and :name. Callers on those stacks
// should use the positional-bind escape hatches:
//
//   - BulkInsertInto: dialect-positional placeholders for INSERT batches.
//   - UpdateBuilder.SetValues: opt-out from @Name on UPDATE statements,
//     emitting ? placeholders that survive sqlx.Rebind.
//
// For identifier quoting, use the Q-suffixed variants (ColumnsQ, SetClauseQ,
// InsertIntoQ) which accept a chuck.Dialect and quote table/column names via
// QuoteIdentifier.
package dbrepo

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/catgoose/chuck"
)

// Columns joins column names into a comma-separated list.
//
//	Columns("ID", "Name", "Email") => "ID, Name, Email"
func Columns(cols ...string) string {
	return strings.Join(cols, ", ")
}

// Placeholders returns named placeholders for the given columns.
//
//	Placeholders("ID", "Name", "Email") => "@ID, @Name, @Email"
func Placeholders(cols ...string) string {
	ps := make([]string, len(cols))
	for i, c := range cols {
		ps[i] = "@" + c
	}
	return strings.Join(ps, ", ")
}

// SetClause builds a SET fragment for UPDATE statements.
//
//	SetClause("Name", "Email") => "Name = @Name, Email = @Email"
func SetClause(cols ...string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = @%s", c, c)
	}
	return strings.Join(parts, ", ")
}

// InsertInto builds a full INSERT INTO … VALUES … statement.
//
//	InsertInto("Users", "Name", "Email") =>
//	  "INSERT INTO Users (Name, Email) VALUES (@Name, @Email)"
func InsertInto(table string, cols ...string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, Columns(cols...), Placeholders(cols...))
}

// ColumnsQ joins column names into a comma-separated list with dialect quoting.
//
//	ColumnsQ(d, "ID", "Name", "Email") => `"id", "name", "email"` (Postgres) or `"ID", "Name", "Email"` (SQLite)
func ColumnsQ(d chuck.Identifier, cols ...string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(d.NormalizeIdentifier(c))
	}
	return strings.Join(quoted, ", ")
}

// SetClauseQ builds a SET fragment for UPDATE statements with dialect quoting.
//
//	SetClauseQ(d, "Name", "Email") => `"name" = @Name, "email" = @Email` (Postgres) or `"Name" = @Name, "Email" = @Email` (SQLite)
func SetClauseQ(d chuck.Identifier, cols ...string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = @%s", d.QuoteIdentifier(d.NormalizeIdentifier(c)), c)
	}
	return strings.Join(parts, ", ")
}

// InsertIntoQ builds a full INSERT INTO … VALUES … statement with dialect quoting.
// Schema-qualified table names ("sg.SalesAgents") are rendered with each part
// quoted separately when d is a chuck.Dialect; SQLite drops the schema.
//
//	InsertIntoQ(d, "Users", "Name", "Email") =>
//	  `INSERT INTO "users" ("name", "email") VALUES (@Name, @Email)` (Postgres)
//	  `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email)` (SQLite)
//	InsertIntoQ(mssql, "sg.SalesAgents", "Name") =>
//	  `INSERT INTO [sg].[SalesAgents] ([Name]) VALUES (@Name)`
func InsertIntoQ(d chuck.Identifier, table string, cols ...string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteTableIdent(d, table), ColumnsQ(d, cols...), Placeholders(cols...))
}

// BulkInsertInto builds a multi-row INSERT INTO … VALUES … statement with
// dialect-specific positional placeholders and quoted identifiers.
//
// Unlike InsertInto/InsertIntoQ which use @Name placeholders, BulkInsertInto
// uses the dialect's Placeholder method to produce positional parameters
// ($1, $2 for Postgres; ? for SQLite; @p1, @p2 for MSSQL).
//
// The caller is responsible for ensuring the total parameter count
// (len(cols) * rowCount) stays within the database engine's limit:
//   - SQLite:    999 (default SQLITE_MAX_VARIABLE_NUMBER)
//   - MSSQL:    2100
//   - Postgres: 65535
//
// Example:
//
//	BulkInsertInto(pgDialect, "users", []string{"name", "email"}, 3)
//	// => INSERT INTO "users" ("name", "email") VALUES ($1, $2), ($3, $4), ($5, $6)
func BulkInsertInto(d chuck.Dialect, table string, cols []string, rowCount int) string {
	quotedTable := quoteTable(d, table)
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = d.QuoteIdentifier(d.NormalizeIdentifier(c))
	}
	colList := strings.Join(quotedCols, ", ")

	rows := make([]string, rowCount)
	n := 1
	for r := range rowCount {
		ph := make([]string, len(cols))
		for c := range cols {
			ph[c] = d.Placeholder(n)
			n++
		}
		rows[r] = "(" + strings.Join(ph, ", ") + ")"
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		quotedTable, colList, strings.Join(rows, ", "))
}

// UpsertInto builds a dialect-aware UPSERT statement (INSERT ... ON CONFLICT DO UPDATE
// or MERGE for MSSQL). conflictCols are the columns that determine uniqueness; all
// remaining columns in cols are updated on conflict.
//
//	UpsertInto(pgDialect, "Users", []string{"Email"}, "Email", "Name", "Age") =>
//	  INSERT INTO "Users" ("Email", "Name", "Age") VALUES (@Email, @Name, @Age)
//	  ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name", "Age" = EXCLUDED."Age"
func UpsertInto(d chuck.Dialect, table string, conflictCols []string, cols ...string) string {
	updateCols := nonConflictCols(cols, conflictCols)
	updateSet := upsertSetClause(d, updateCols)
	on := chuck.ParseObjectName(strings.TrimSpace(table))
	if on.Schema == "" {
		return d.Upsert(table, Columns(cols...), Placeholders(cols...), Columns(conflictCols...), updateSet)
	}
	if d.Engine() == chuck.SQLite {
		return d.Upsert(on.Name, Columns(cols...), Placeholders(cols...), Columns(conflictCols...), updateSet)
	}
	return renderQualifiedUpsert(d, on, Columns(cols...), Placeholders(cols...), Columns(conflictCols...), updateSet)
}

// UpsertIntoQ builds a dialect-aware UPSERT statement with identifier quoting.
// Schema-qualified table names ("sg.SalesAgents") are rendered with each part
// quoted separately; SQLite drops the schema component.
//
//	UpsertIntoQ(pgDialect, "Users", []string{"Email"}, "Email", "Name", "Age") =>
//	  INSERT INTO "Users" ("Email", "Name", "Age") VALUES (@Email, @Name, @Age)
//	  ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name", "Age" = EXCLUDED."Age"
func UpsertIntoQ(d chuck.Dialect, table string, conflictCols []string, cols ...string) string {
	updateCols := nonConflictCols(cols, conflictCols)
	updateSet := upsertSetClauseQ(d, updateCols)
	on := chuck.ParseObjectName(strings.TrimSpace(table))
	if on.Schema == "" {
		return d.Upsert(d.NormalizeIdentifier(table), ColumnsQ(d, cols...), Placeholders(cols...), ColumnsQ(d, conflictCols...), updateSet)
	}
	if d.Engine() == chuck.SQLite {
		return d.Upsert(d.NormalizeIdentifier(on.Name), ColumnsQ(d, cols...), Placeholders(cols...), ColumnsQ(d, conflictCols...), updateSet)
	}
	return renderQualifiedUpsert(d, on, ColumnsQ(d, cols...), Placeholders(cols...), ColumnsQ(d, conflictCols...), updateSet)
}

// renderQualifiedUpsert builds an UPSERT statement for a schema-qualified
// target. Reproduces the dialect's own Upsert template using a pre-rendered
// schema.table token so qualified names render with each part quoted
// separately (e.g. [sg].[Agents] on MSSQL).
func renderQualifiedUpsert(d chuck.Dialect, target chuck.ObjectName, columns, values, conflictColumns, updateSet string) string {
	qt := chuck.QualifyTable(d, target)
	switch d.Engine() {
	case chuck.Postgres:
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			qt, columns, values, conflictColumns, updateSet)
	case chuck.MSSQL:
		parts := strings.Split(conflictColumns, ", ")
		onParts := make([]string, len(parts))
		for i, col := range parts {
			col = strings.TrimSpace(col)
			onParts[i] = fmt.Sprintf("Target.%s = Source.%s", col, col)
		}
		onClause := strings.Join(onParts, " AND ")
		return fmt.Sprintf(
			"MERGE %s AS Target USING (VALUES (%s)) AS Source (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
			qt, values, columns, onClause, updateSet, columns, values,
		)
	default:
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			qt, columns, values, conflictColumns, updateSet)
	}
}

// quoteTableIdent applies dialect identifier quoting to a possibly
// schema-qualified table name. Accepts chuck.Identifier so it works with the
// existing InsertInto* signatures; if the identifier is also a Dialect the
// engine-aware path (SQLite schema drop) kicks in.
func quoteTableIdent(d chuck.Identifier, table string) string {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return table
	}
	if strings.ContainsAny(trimmed, " \t()") {
		return table
	}
	on := chuck.ParseObjectName(trimmed)
	if dialect, ok := d.(chuck.Dialect); ok {
		return chuck.QualifyTable(dialect, on)
	}
	// Non-Dialect Identifier: fall back to per-part quoting without engine-
	// aware schema handling. SQLite passes through as a Dialect, so we only
	// reach this branch from synthetic Identifier-only callers/tests.
	if on.Schema == "" {
		return d.QuoteIdentifier(d.NormalizeIdentifier(on.Name))
	}
	return d.QuoteIdentifier(d.NormalizeIdentifier(on.Schema)) + "." + d.QuoteIdentifier(d.NormalizeIdentifier(on.Name))
}

// nonConflictCols returns columns from cols that are not in conflictCols.
func nonConflictCols(cols, conflictCols []string) []string {
	conflict := make(map[string]bool, len(conflictCols))
	for _, c := range conflictCols {
		conflict[c] = true
	}
	var result []string
	for _, c := range cols {
		if !conflict[c] {
			result = append(result, c)
		}
	}
	return result
}

// upsertSetClause builds the SET fragment for an upsert's update portion.
// For Postgres/SQLite it uses EXCLUDED.col, for MSSQL it uses Source.col.
func upsertSetClause(d chuck.Dialect, cols []string) string {
	ref := excludedRef(d)
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = %s.%s", c, ref, c)
	}
	return strings.Join(parts, ", ")
}

// upsertSetClauseQ builds the SET fragment with identifier quoting.
func upsertSetClauseQ(d chuck.Dialect, cols []string) string {
	ref := excludedRef(d)
	parts := make([]string, len(cols))
	for i, c := range cols {
		qc := d.QuoteIdentifier(d.NormalizeIdentifier(c))
		parts[i] = fmt.Sprintf("%s = %s.%s", qc, ref, qc)
	}
	return strings.Join(parts, ", ")
}

// excludedRef returns the source-row reference keyword for upsert SET clauses.
func excludedRef(d chuck.Dialect) string {
	if d.Engine() == chuck.MSSQL {
		return "Source"
	}
	return "EXCLUDED"
}

// NamedArgs converts a map to a slice of sql.NamedArg values suitable for
// passing to database/sql query methods. Keys are sorted for deterministic output.
func NamedArgs(m map[string]any) []any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]any, 0, len(m))
	for _, k := range keys {
		args = append(args, sql.Named(k, m[k]))
	}
	return args
}
