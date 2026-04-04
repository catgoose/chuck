package dbrepo

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// DeleteBuilder constructs composable DELETE queries with WHERE and RETURNING clauses.
type DeleteBuilder struct {
	table     string
	where     *WhereBuilder
	returning []string
	dialect   chuck.Dialect
}

// NewDelete creates a new DeleteBuilder for the given table.
func NewDelete(table string) *DeleteBuilder {
	return &DeleteBuilder{
		table: table,
		where: NewWhere(),
	}
}

// Where sets the WhereBuilder for filtering.
func (d *DeleteBuilder) Where(w *WhereBuilder) *DeleteBuilder {
	d.where = w
	return d
}

// WithDialect sets the dialect for identifier quoting.
func (d *DeleteBuilder) WithDialect(dial chuck.Dialect) *DeleteBuilder {
	d.dialect = dial
	return d
}

// Returning sets columns to return after the delete (Postgres/SQLite RETURNING clause).
func (d *DeleteBuilder) Returning(cols ...string) *DeleteBuilder {
	d.returning = cols
	return d
}

// Build returns the complete SQL query string and the collected arguments.
func (d *DeleteBuilder) Build() (query string, args []any) {
	var parts []string

	tableName := d.table
	if d.dialect != nil {
		tableName = d.dialect.QuoteIdentifier(d.table)
	}

	parts = append(parts, fmt.Sprintf("DELETE FROM %s", tableName))

	if d.where.HasConditions() {
		parts = append(parts, d.where.String())
	}

	args = d.where.Args()

	if len(d.returning) > 0 && d.dialect != nil {
		rc := d.dialect.ReturningClause(Columns(d.returning...))
		if rc != "" {
			parts = append(parts, rc)
		}
	}

	return strings.Join(parts, " "), args
}
