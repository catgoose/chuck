// Package dbrepo provides composable SQL fragment helpers for building queries.
//
// Functions in this package use @Name placeholders (e.g., @ID, @Name) which rely on
// database/sql's sql.Named() for driver-level parameter translation. This is distinct
// from the fraggle.Dialect.Placeholder() method which returns engine-specific positional
// syntax ($1, ?, @p1) for raw SQL composition.
//
// The @Name convention works because database/sql drivers translate sql.NamedArg values
// into their native parameter syntax at execution time. This means dbrepo output is
// dialect-agnostic — the same query string works across all engines when paired with
// sql.Named() arguments.
//
// For identifier quoting, use the Q-suffixed variants (ColumnsQ, SetClauseQ, InsertIntoQ)
// which accept a fraggle.Dialect and quote table/column names via QuoteIdentifier.
package dbrepo

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/catgoose/fraggle"
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
//	ColumnsQ(d, "ID", "Name", "Email") => `"ID", "Name", "Email"` (Postgres/SQLite)
func ColumnsQ(d fraggle.Dialect, cols ...string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

// SetClauseQ builds a SET fragment for UPDATE statements with dialect quoting.
//
//	SetClauseQ(d, "Name", "Email") => `"Name" = @Name, "Email" = @Email` (Postgres/SQLite)
func SetClauseQ(d fraggle.Dialect, cols ...string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = @%s", d.QuoteIdentifier(c), c)
	}
	return strings.Join(parts, ", ")
}

// InsertIntoQ builds a full INSERT INTO … VALUES … statement with dialect quoting.
//
//	InsertIntoQ(d, "Users", "Name", "Email") =>
//	  `INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email)` (Postgres/SQLite)
func InsertIntoQ(d fraggle.Dialect, table string, cols ...string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.QuoteIdentifier(table), ColumnsQ(d, cols...), Placeholders(cols...))
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
