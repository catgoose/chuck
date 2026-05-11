package dbrepo

import (
	"strings"

	"github.com/catgoose/chuck"
)

// quoteTable renders a possibly schema-qualified table identifier through the
// dialect's normalization and quoting. The input may be unqualified ("Users")
// or qualified ("sg.SalesAgents"); whitespace or parentheses signal the input
// is something exotic (alias form or derived table) and are returned
// untouched.
//
// SQLite drops the schema component because it has no schema namespace.
func quoteTable(d chuck.Dialect, table string) string {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return table
	}
	if strings.ContainsAny(trimmed, " \t()") {
		return table
	}
	return chuck.QualifyTable(d, chuck.ParseObjectName(trimmed))
}

// quoteQualifiedColumn renders a possibly qualified column reference. The
// supported shapes are:
//
//   - "col"                  -> "col" (passthrough; preserves back-compat)
//   - "table.col"            -> quoted "table"."col"
//   - "schema.table.col"     -> quoted "schema"."table"."col"
//
// Tokens with whitespace or parentheses (expressions, function calls, derived
// references) are returned verbatim so callers can mix raw SQL into column
// lists without dbrepo trying to parse it.
func quoteQualifiedColumn(d chuck.Identifier, col string) string {
	col = strings.TrimSpace(col)
	if col == "" {
		return col
	}
	if strings.ContainsAny(col, " \t()") {
		return col
	}
	parts := strings.Split(col, ".")
	// Reject empty segments — leave the raw form for the caller.
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			return col
		}
	}
	switch len(parts) {
	case 1:
		// Bare column name: passthrough preserves prior unquoted behavior so
		// existing callers using "Name" or "Tasks.ID" do not regress.
		return col
	case 2, 3:
		out := make([]string, len(parts))
		for i, p := range parts {
			out[i] = d.QuoteIdentifier(d.NormalizeIdentifier(strings.TrimSpace(p)))
		}
		return strings.Join(out, ".")
	default:
		return col
	}
}
