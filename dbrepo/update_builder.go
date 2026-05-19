package dbrepo

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// UpdateBuilder constructs composable UPDATE queries with SET, WHERE, and RETURNING clauses.
type UpdateBuilder struct {
	table      string
	cols       []string
	setValues  []any
	positional bool
	where      *WhereBuilder
	returning  []string
	dialect    chuck.Dialect
}

// NewUpdate creates a new UpdateBuilder for the given table and columns.
// The columns are used to generate the SET clause (e.g., "Name = @Name, Email = @Email").
func NewUpdate(table string, columns ...string) *UpdateBuilder {
	return &UpdateBuilder{
		table: table,
		cols:  columns,
		where: NewWhere(),
	}
}

// Where sets the WhereBuilder for filtering.
func (u *UpdateBuilder) Where(w *WhereBuilder) *UpdateBuilder {
	u.where = w
	return u
}

// WithDialect sets the dialect for identifier quoting.
func (u *UpdateBuilder) WithDialect(d chuck.Dialect) *UpdateBuilder {
	u.dialect = d
	return u
}

// Returning sets columns to return after the update (Postgres/SQLite RETURNING clause).
func (u *UpdateBuilder) Returning(cols ...string) *UpdateBuilder {
	u.returning = cols
	return u
}

// SetValues opts UpdateBuilder out of the default `SET col = @col` named
// placeholders and into positional `?` placeholders. values must be supplied
// in the same order as the columns passed to NewUpdate; Build will panic if
// len(values) != len(columns). The returned args slice from Build then begins
// with these SET values followed by the WHERE-clause args.
//
// This is the escape hatch for callers that route their query through
// sqlx.Rebind / lib/pq, where the default `@Name` token does not survive the
// driver layer. WHERE conditions are caller-supplied: use `?` placeholders
// inside WhereBuilder.And / Or fragments so they pass through Rebind cleanly
// alongside the SET clause. Dialect identifier quoting (WithDialect) still
// applies to the column identifiers; only the placeholder shape changes.
func (u *UpdateBuilder) SetValues(values ...any) *UpdateBuilder {
	u.setValues = values
	u.positional = true
	return u
}

// Build returns the complete SQL query string and the collected arguments.
//
// Default (named) mode: SET clause uses `@Name` placeholders and the returned
// args slice contains only the WHERE-clause args; callers supply SET values
// separately via sql.Named / NamedArgs.
//
// Positional mode (after SetValues): SET clause uses `?` placeholders and the
// returned args slice begins with the supplied SET values in column order,
// followed by the WHERE-clause args. Panics if the number of SetValues does
// not match the number of columns supplied to NewUpdate.
func (u *UpdateBuilder) Build() (query string, args []any) {
	var parts []string

	tableName := u.table
	var setClause string
	switch {
	case u.positional:
		if len(u.setValues) != len(u.cols) {
			panic(fmt.Sprintf("dbrepo: UpdateBuilder.SetValues count (%d) does not match column count (%d)", len(u.setValues), len(u.cols)))
		}
		if u.dialect != nil {
			tableName = quoteTable(u.dialect, u.table)
		}
		setClause = positionalSetClause(u.dialect, u.cols)
	case u.dialect != nil:
		tableName = quoteTable(u.dialect, u.table)
		setClause = SetClauseQ(u.dialect, u.cols...)
	default:
		setClause = SetClause(u.cols...)
	}

	parts = append(parts, fmt.Sprintf("UPDATE %s SET %s", tableName, setClause))

	if u.where.HasConditions() {
		parts = append(parts, u.where.String())
	}

	if u.positional {
		args = append(args, u.setValues...)
	}
	args = append(args, u.where.Args()...)

	if len(u.returning) > 0 && u.dialect != nil {
		rc := u.dialect.ReturningClause(Columns(u.returning...))
		if rc != "" {
			parts = append(parts, rc)
		}
	}

	return strings.Join(parts, " "), args
}

// positionalSetClause builds a SET fragment using `?` placeholders. Column
// identifiers are dialect-quoted when d is non-nil so callers retain quoting
// across the positional path; the placeholder is always `?` because the path
// targets sqlx.Rebind-style rewriting (which converts `?` to the engine's
// native positional syntax at execution time).
func positionalSetClause(d chuck.Dialect, cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		if d != nil {
			parts[i] = fmt.Sprintf("%s = ?", d.QuoteIdentifier(d.NormalizeIdentifier(c)))
		} else {
			parts[i] = c + " = ?"
		}
	}
	return strings.Join(parts, ", ")
}
