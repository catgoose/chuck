package dbrepo

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// joinClause represents a single JOIN in a SELECT query.
type joinClause struct {
	joinType  string // "JOIN" or "LEFT JOIN"
	table     string
	condition string
}

// SelectBuilder constructs composable SELECT queries with WHERE, ORDER BY, and pagination.
type SelectBuilder struct {
	table   string
	cols    string
	joins   []joinClause
	where   *WhereBuilder
	orderBy string
	limit   int
	offset  int
	dialect chuck.Dialect
}

// NewSelect creates a new SelectBuilder for the given table and columns.
func NewSelect(table string, cols ...string) *SelectBuilder {
	return &SelectBuilder{
		table: table,
		cols:  Columns(cols...),
		where: NewWhere(),
	}
}

// Where sets the WhereBuilder for filtering.
func (s *SelectBuilder) Where(w *WhereBuilder) *SelectBuilder {
	s.where = w
	return s
}

// OrderBy sets the ORDER BY clause (e.g., "Name ASC" or "CreatedAt DESC, Name ASC").
func (s *SelectBuilder) OrderBy(clause string) *SelectBuilder {
	s.orderBy = clause
	return s
}

// OrderByMap builds an ORDER BY clause from a sort string and column map, with a default fallback.
// The sortStr format is "column:direction" (e.g., "name:asc" or "created_at:desc").
// Multiple sorts can be separated by commas: "name:asc,created_at:desc".
func (s *SelectBuilder) OrderByMap(sortStr string, columnMap map[string]string, defaultSort string) *SelectBuilder {
	s.orderBy = BuildOrderByClause(sortStr, columnMap, defaultSort)
	return s
}

// Paginate sets LIMIT and OFFSET for pagination.
func (s *SelectBuilder) Paginate(limit, offset int) *SelectBuilder {
	s.limit = limit
	s.offset = offset
	return s
}

// WithDialect sets the dialect for pagination clause generation.
func (s *SelectBuilder) WithDialect(d chuck.Dialect) *SelectBuilder {
	s.dialect = d
	return s
}

// Join adds an INNER JOIN clause. The table name is dialect-quoted when a dialect
// is set; the condition is passed through as raw SQL.
func (s *SelectBuilder) Join(table, condition string) *SelectBuilder {
	s.joins = append(s.joins, joinClause{joinType: "JOIN", table: table, condition: condition})
	return s
}

// LeftJoin adds a LEFT JOIN clause. The table name is dialect-quoted when a dialect
// is set; the condition is passed through as raw SQL.
func (s *SelectBuilder) LeftJoin(table, condition string) *SelectBuilder {
	s.joins = append(s.joins, joinClause{joinType: "LEFT JOIN", table: table, condition: condition})
	return s
}

// Build returns the complete SQL query string and the collected arguments.
func (s *SelectBuilder) Build() (query string, args []any) {
	var parts []string
	tableName := s.table
	cols := s.cols
	if s.dialect != nil {
		tableName = s.dialect.QuoteIdentifier(s.table)
		cols = quoteDotQualifiedColumns(s.dialect, s.cols)
	}
	parts = append(parts, fmt.Sprintf("SELECT %s FROM %s", cols, tableName))

	for _, j := range s.joins {
		jt := j.table
		if s.dialect != nil {
			jt = s.dialect.QuoteIdentifier(j.table)
		}
		parts = append(parts, fmt.Sprintf("%s %s ON %s", j.joinType, jt, j.condition))
	}

	if s.where.HasConditions() {
		parts = append(parts, s.where.String())
	}

	if s.orderBy != "" {
		if strings.HasPrefix(strings.ToUpper(s.orderBy), "ORDER BY") {
			parts = append(parts, s.orderBy)
		} else {
			parts = append(parts, "ORDER BY "+s.orderBy)
		}
	}

	args = s.where.Args()

	if s.limit > 0 {
		if s.dialect != nil {
			parts = append(parts, s.dialect.Pagination())
		} else {
			parts = append(parts, "LIMIT @Limit OFFSET @Offset")
		}
		args = append(args, sql.Named("Offset", s.offset), sql.Named("Limit", s.limit))
	}

	return strings.Join(parts, " "), args
}

// CountQuery returns a COUNT(*) query using the same FROM, JOIN, and WHERE clauses.
func (s *SelectBuilder) CountQuery() (query string, args []any) {
	var parts []string
	tableName := s.table
	if s.dialect != nil {
		tableName = s.dialect.QuoteIdentifier(s.table)
	}
	parts = append(parts, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))

	for _, j := range s.joins {
		jt := j.table
		if s.dialect != nil {
			jt = s.dialect.QuoteIdentifier(j.table)
		}
		parts = append(parts, fmt.Sprintf("%s %s ON %s", j.joinType, jt, j.condition))
	}

	if s.where.HasConditions() {
		parts = append(parts, s.where.String())
	}

	return strings.Join(parts, " "), s.where.Args()
}

// quoteDotQualifiedColumns takes a comma-separated column list and quotes only
// dot-qualified names (Table.Column) by quoting each part separately.
// Simple column names are left as-is to preserve backward compatibility.
func quoteDotQualifiedColumns(d chuck.Identifier, cols string) string {
	parts := strings.Split(cols, ", ")
	result := make([]string, len(parts))
	for i, col := range parts {
		col = strings.TrimSpace(col)
		if dotIdx := strings.Index(col, "."); dotIdx >= 0 {
			table := col[:dotIdx]
			column := col[dotIdx+1:]
			result[i] = d.QuoteIdentifier(table) + "." + d.QuoteIdentifier(column)
		} else {
			result[i] = col
		}
	}
	return strings.Join(result, ", ")
}
