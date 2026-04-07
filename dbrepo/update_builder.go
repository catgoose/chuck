package dbrepo

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// UpdateBuilder constructs composable UPDATE queries with SET, WHERE, and RETURNING clauses.
type UpdateBuilder struct {
	table     string
	cols      []string
	where     *WhereBuilder
	returning []string
	dialect   chuck.Dialect
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

// Build returns the complete SQL query string and the collected arguments.
func (u *UpdateBuilder) Build() (query string, args []any) {
	var parts []string

	tableName := u.table
	var setClause string
	if u.dialect != nil {
		tableName = u.dialect.QuoteIdentifier(u.dialect.NormalizeIdentifier(u.table))
		setClause = SetClauseQ(u.dialect, u.cols...)
	} else {
		setClause = SetClause(u.cols...)
	}

	parts = append(parts, fmt.Sprintf("UPDATE %s SET %s", tableName, setClause))

	if u.where.HasConditions() {
		parts = append(parts, u.where.String())
	}

	args = u.where.Args()

	if len(u.returning) > 0 && u.dialect != nil {
		rc := u.dialect.ReturningClause(Columns(u.returning...))
		if rc != "" {
			parts = append(parts, rc)
		}
	}

	return strings.Join(parts, " "), args
}
