package schema

import (
	"fmt"

	"github.com/catgoose/chuck"
)

// IndexDef defines a table index.
type IndexDef struct {
	name    string
	columns string
	unique  bool
	where   string
}

// Index creates a new index definition.
func Index(name, columns string) IndexDef {
	return IndexDef{name: name, columns: columns}
}

// UniqueIndex creates a unique index definition.
func UniqueIndex(name, columns string) IndexDef {
	return IndexDef{name: name, columns: columns, unique: true}
}

// PartialIndex creates a partial (filtered) index definition with a WHERE clause.
func PartialIndex(name, columns string) IndexDef {
	return IndexDef{name: name, columns: columns}
}

// UniquePartialIndex creates a unique partial (filtered) index definition.
func UniquePartialIndex(name, columns string) IndexDef {
	return IndexDef{name: name, columns: columns, unique: true}
}

// Where sets a filter condition on the index, making it a partial (filtered) index.
// This returns a new IndexDef with the WHERE clause applied.
func (idx IndexDef) Where(condition string) IndexDef {
	idx.where = condition
	return idx
}

// ddlIfNotExists renders the CREATE INDEX IF NOT EXISTS statement for the given
// dialect and table object.
func (idx IndexDef) ddlIfNotExists(d chuck.Dialect, table chuck.ObjectName) string {
	return idx.renderDDL(d, table, true)
}

// ddl renders the CREATE INDEX statement for the given dialect and table object.
func (idx IndexDef) ddl(d chuck.Dialect, table chuck.ObjectName) string {
	return idx.renderDDL(d, table, false)
}

func (idx IndexDef) renderDDL(d chuck.Dialect, table chuck.ObjectName, ifNotExists bool) string {
	// Unqualified plain index can route through the dialect helper to preserve
	// existing byte-identical output for callers that have snapshot expectations.
	if (table.Schema == "" || d.Engine() == chuck.SQLite) && !idx.unique && idx.where == "" {
		bare := d.NormalizeIdentifier(table.Name)
		if ifNotExists {
			return d.CreateIndexIfNotExists(idx.name, bare, idx.columns)
		}
		return idx.buildStandardIndex(d, table, false)
	}

	return idx.buildCreateIndex(d, table, ifNotExists)
}

// buildCreateIndex builds the full CREATE INDEX statement with support for
// UNIQUE and WHERE clauses across all dialects.
func (idx IndexDef) buildCreateIndex(d chuck.Dialect, table chuck.ObjectName, ifNotExists bool) string {
	switch d.Engine() {
	case chuck.MSSQL:
		return idx.buildMSSQLIndex(d, table, ifNotExists)
	default:
		return idx.buildStandardIndex(d, table, ifNotExists)
	}
}

// buildStandardIndex generates CREATE INDEX for Postgres and SQLite.
func (idx IndexDef) buildStandardIndex(d chuck.Dialect, table chuck.ObjectName, ifNotExists bool) string {
	s := "CREATE "
	if idx.unique {
		s += "UNIQUE "
	}
	s += "INDEX "
	if ifNotExists {
		s += "IF NOT EXISTS "
	}
	s += fmt.Sprintf("%s ON %s(%s)",
		d.QuoteIdentifier(idx.name),
		qualifyTable(d, table),
		chuck.QuoteColumns(d, idx.columns),
	)
	if idx.where != "" {
		s += " WHERE " + idx.where
	}
	return s
}

// buildMSSQLIndex generates CREATE INDEX for MSSQL, using the IF NOT EXISTS
// pattern with sys.indexes.
func (idx IndexDef) buildMSSQLIndex(d chuck.Dialect, table chuck.ObjectName, ifNotExists bool) string {
	qi := d.QuoteIdentifier(idx.name)
	qt := qualifyTable(d, table)

	create := "CREATE "
	if idx.unique {
		create += "UNIQUE "
	}
	create += fmt.Sprintf("INDEX %s ON %s(%s)", qi, qt, chuck.QuoteColumns(d, idx.columns))
	if idx.where != "" {
		create += " WHERE " + idx.where
	}

	if !ifNotExists {
		return create
	}

	// Wrap in IF NOT EXISTS check using sys.indexes; OBJECT_ID takes the
	// schema-qualified object literal so the right table is matched even
	// when the same bare name exists under multiple schemas.
	schema, name := normalizedObject(d, table)
	objArg := name
	if schema != "" {
		objArg = schema + "." + name
	}
	return fmt.Sprintf(
		"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'%s' AND object_id = OBJECT_ID(N'%s')) %s",
		escapeQuote(idx.name), escapeQuote(objArg), create,
	)
}

// escapeQuote doubles single quotes for safe embedding in SQL string literals.
func escapeQuote(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
