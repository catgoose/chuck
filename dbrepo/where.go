package dbrepo

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/catgoose/chuck"
)

// WhereBuilder constructs composable WHERE clauses with named parameters.
type WhereBuilder struct {
	clauses []string
	args    []any
	dialect chuck.Dialect
}

// NewWhere creates a new WhereBuilder.
func NewWhere() *WhereBuilder {
	return &WhereBuilder{}
}

// WithDialect sets the dialect for dialect-aware operations (e.g. ILIKE on Postgres).
func (w *WhereBuilder) WithDialect(d chuck.Dialect) *WhereBuilder {
	w.dialect = d
	return w
}

// And adds an AND condition with optional named args.
func (w *WhereBuilder) And(condition string, args ...any) *WhereBuilder {
	if len(w.clauses) == 0 {
		w.clauses = append(w.clauses, condition)
	} else {
		w.clauses = append(w.clauses, "AND "+condition)
	}
	w.args = append(w.args, args...)
	return w
}

// AndIf adds an AND condition only when ok is true.
func (w *WhereBuilder) AndIf(ok bool, condition string, args ...any) *WhereBuilder {
	if !ok {
		return w
	}
	return w.And(condition, args...)
}

// Or adds an OR branch to the previous condition.
func (w *WhereBuilder) Or(condition string, args ...any) *WhereBuilder {
	if len(w.clauses) == 0 {
		w.clauses = append(w.clauses, condition)
	} else {
		w.clauses = append(w.clauses, "OR "+condition)
	}
	w.args = append(w.args, args...)
	return w
}

// OrIf adds an OR condition only when ok is true.
func (w *WhereBuilder) OrIf(ok bool, condition string, args ...any) *WhereBuilder {
	if !ok {
		return w
	}
	return w.Or(condition, args...)
}

// validIdentifier matches safe SQL column names: letters, digits, underscores, and dots
// (for qualified names like "t.Name"). Must start with a letter or underscore.
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// Search adds a LIKE search condition across the given fields.
// When a dialect is set via WithDialect, Postgres uses ILIKE for case-insensitive matching.
// Field names are validated to prevent SQL injection — only alphanumeric characters,
// underscores, and dots (for qualified names) are allowed.
func (w *WhereBuilder) Search(search string, fields ...string) *WhereBuilder {
	if search == "" || len(fields) == 0 {
		return w
	}
	pattern := "%" + search + "%"
	op := "LIKE"
	if w.dialect != nil && w.dialect.Engine() == chuck.Postgres {
		op = "ILIKE"
	}
	var conditions []string
	for _, field := range fields {
		if !validIdentifier.MatchString(field) {
			continue
		}
		conditions = append(conditions, fmt.Sprintf("%s %s @SearchPattern", field, op))
	}
	if len(conditions) == 0 {
		return w
	}
	w.And("("+strings.Join(conditions, " OR ")+")",
		sql.Named("Search", search),
		sql.Named("SearchPattern", pattern),
	)
	return w
}

// String returns the full WHERE clause or an empty string when no conditions exist.
func (w *WhereBuilder) String() string {
	if len(w.clauses) == 0 {
		return ""
	}
	return fmt.Sprintf("WHERE %s", strings.Join(w.clauses, " "))
}

// Args returns the collected named arguments.
func (w *WhereBuilder) Args() []any {
	return w.args
}

// HasConditions reports whether any conditions have been added.
func (w *WhereBuilder) HasConditions() bool {
	return len(w.clauses) > 0
}

// colName returns the first non-empty override, or the default.
func colName(defaultName string, override []string) string {
	if len(override) > 0 && override[0] != "" {
		return override[0]
	}
	return defaultName
}

// NotDeleted adds a "DeletedAt IS NULL" condition for soft-delete filtering.
// Pass an optional column name to override the default "DeletedAt".
func (w *WhereBuilder) NotDeleted(col ...string) *WhereBuilder {
	return w.And(colName("DeletedAt", col) + " IS NULL")
}

// NotExpired adds a condition that filters out expired records.
// Pass an optional column name to override the default "ExpiresAt".
func (w *WhereBuilder) NotExpired(col ...string) *WhereBuilder {
	c := colName("ExpiresAt", col)
	return w.And(fmt.Sprintf("(%s IS NULL OR %s > CURRENT_TIMESTAMP)", c, c))
}

// HasStatus adds a "Status = @Status" condition.
// Pass an optional column name to override the default "Status".
func (w *WhereBuilder) HasStatus(status string, col ...string) *WhereBuilder {
	c := colName("Status", col)
	return w.And(c+" = @Status", sql.Named("Status", status))
}

// IsRoot adds a "ParentID IS NULL" condition for tree root nodes.
// Pass an optional column name to override the default "ParentID".
func (w *WhereBuilder) IsRoot(col ...string) *WhereBuilder {
	return w.And(colName("ParentID", col) + " IS NULL")
}

// HasParent adds a "ParentID = @ParentID" condition.
// Pass an optional column name to override the default "ParentID".
func (w *WhereBuilder) HasParent(parentID int64, col ...string) *WhereBuilder {
	c := colName("ParentID", col)
	return w.And(c+" = @ParentID", sql.Named("ParentID", parentID))
}

// NotReplaced adds a "ReplacedByID IS NULL" condition for current (non-replaced) records.
// Pass an optional column name to override the default "ReplacedByID".
func (w *WhereBuilder) NotReplaced(col ...string) *WhereBuilder {
	return w.And(colName("ReplacedByID", col) + " IS NULL")
}

// ReplacedBy adds a "ReplacedByID = @ReplacedByID" condition.
// Pass an optional column name to override the default "ReplacedByID".
func (w *WhereBuilder) ReplacedBy(id int64, col ...string) *WhereBuilder {
	c := colName("ReplacedByID", col)
	return w.And(c+" = @ReplacedByID", sql.Named("ReplacedByID", id))
}

// NotArchived adds an "ArchivedAt IS NULL" condition for archive filtering.
// Pass an optional column name to override the default "ArchivedAt".
func (w *WhereBuilder) NotArchived(col ...string) *WhereBuilder {
	return w.And(colName("ArchivedAt", col) + " IS NULL")
}

// NotArchivedBool adds a condition for boolean archived columns.
// Generates "NOT archived" for Postgres/SQLite, "archived = 0" for MSSQL.
// Pass an optional column name to override the default "archived".
func (w *WhereBuilder) NotArchivedBool(col ...string) *WhereBuilder {
	c := colName("archived", col)
	if w.dialect != nil && w.dialect.Engine() == chuck.MSSQL {
		return w.And(c + " = 0")
	}
	return w.And("NOT " + c)
}

// HasVersion adds a "Version = @Version" condition for optimistic locking.
// Pass an optional column name to override the default "Version".
func (w *WhereBuilder) HasVersion(version int, col ...string) *WhereBuilder {
	c := colName("Version", col)
	return w.And(c+" = @Version", sql.Named("Version", version))
}
