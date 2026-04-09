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
		tableName = s.dialect.QuoteIdentifier(s.dialect.NormalizeIdentifier(s.table))
		cols = quoteDotQualifiedColumns(s.dialect, s.cols)
	}
	parts = append(parts, fmt.Sprintf("SELECT %s FROM %s", cols, tableName))

	for _, j := range s.joins {
		jt := j.table
		if s.dialect != nil {
			jt = quoteJoinTarget(s.dialect, j.table)
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
		tableName = s.dialect.QuoteIdentifier(s.dialect.NormalizeIdentifier(s.table))
	}
	parts = append(parts, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))

	for _, j := range s.joins {
		jt := j.table
		if s.dialect != nil {
			jt = quoteJoinTarget(s.dialect, j.table)
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
//
// The input is split on "," (not ", ") and each token is whitespace-trimmed so
// that variations like "u.ID,v.Name", "u.ID,  v.Name", and "u.ID , v.Name" all
// parse the same way. Output is always rejoined with ", " for consistency.
func quoteDotQualifiedColumns(d chuck.Identifier, cols string) string {
	parts := strings.Split(cols, ",")
	result := make([]string, len(parts))
	for i, col := range parts {
		col = strings.TrimSpace(col)
		if dotIdx := strings.Index(col, "."); dotIdx >= 0 {
			table := col[:dotIdx]
			column := col[dotIdx+1:]
			result[i] = d.QuoteIdentifier(d.NormalizeIdentifier(table)) + "." + d.QuoteIdentifier(d.NormalizeIdentifier(column))
		} else {
			result[i] = col
		}
	}
	return strings.Join(result, ", ")
}

// parseJoinTarget splits a JOIN target expression into its base table
// identifier and an optional alias.
//
// Supported shapes:
//   - "Users"        -> base="Users", alias="",  asForm=false
//   - "Users u"      -> base="Users", alias="u", asForm=false
//   - "Orders AS o"  -> base="Orders", alias="o", asForm=true (AS preserved in
//     the original case from the input)
//   - "Orders as o"  -> base="Orders", alias="o", asForm=true
//
// Ambiguous or unsupported shapes (empty input, anything containing
// parentheses such as derived tables/subqueries, or expressions with more
// than three whitespace-separated tokens) return base="" with the full input
// preserved in alias so the caller can pass it through unquoted.
func parseJoinTarget(s string) (base, alias, asKeyword string) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return "", "", ""
	}
	// Derived tables / subqueries and anything exotic: preserve verbatim.
	if strings.ContainsAny(trimmed, "()") {
		return "", trimmed, ""
	}
	fields := strings.Fields(trimmed)
	switch len(fields) {
	case 1:
		return fields[0], "", ""
	case 2:
		return fields[0], fields[1], ""
	case 3:
		if strings.EqualFold(fields[1], "AS") {
			return fields[0], fields[2], fields[1]
		}
		// Three tokens without AS is ambiguous — preserve verbatim.
		return "", trimmed, ""
	default:
		return "", trimmed, ""
	}
}

// quoteJoinTarget applies dialect quoting to a JOIN target while preserving
// any alias tokens unchanged. Only the base table identifier is normalized
// and quoted; aliases (with or without the AS keyword) are emitted literally
// so callers can match them in ON/WHERE clauses without surprise casing.
//
// If the target shape is ambiguous (derived tables, more than three tokens,
// etc.) parseJoinTarget yields an empty base and the full original input is
// returned as-is to preserve historical behavior.
func quoteJoinTarget(d chuck.Dialect, target string) string {
	base, alias, asKeyword := parseJoinTarget(target)
	if base == "" {
		// Ambiguous shape: fall back to the raw input unchanged.
		return alias
	}
	quoted := d.QuoteIdentifier(d.NormalizeIdentifier(base))
	switch {
	case alias == "":
		return quoted
	case asKeyword != "":
		return quoted + " " + asKeyword + " " + alias
	default:
		return quoted + " " + alias
	}
}
