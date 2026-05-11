package schema

import (
	"fmt"

	"github.com/catgoose/chuck"
)

// qualifyTable returns the dialect-rendered, quoted, fully-qualified table
// identifier for the given ObjectName. Delegates to chuck.QualifyTable so DDL,
// seed SQL, snapshot/inspection, and dbrepo all share one rendering path.
func qualifyTable(d chuck.Dialect, o chuck.ObjectName) string {
	return chuck.QualifyTable(d, o)
}

// normalizedObject returns the dialect-normalized schema and name for the
// given ObjectName. The returned schema is empty when the engine does not
// support schemas (SQLite) or when the ObjectName has no schema set.
func normalizedObject(d chuck.Dialect, o chuck.ObjectName) (schema, name string) {
	name = d.NormalizeIdentifier(o.Name)
	if o.Schema == "" || d.Engine() == chuck.SQLite {
		return "", name
	}
	return d.NormalizeIdentifier(o.Schema), name
}

// objectKey returns a stable lookup key for an ObjectName. Used to key maps
// and detect duplicate identity across schema-qualified definitions.
func objectKey(o chuck.ObjectName) string {
	if o.Schema == "" {
		return o.Name
	}
	return o.Schema + "." + o.Name
}

// createTableSQL renders a CREATE TABLE statement for the given object.
func createTableSQL(d chuck.Dialect, o chuck.ObjectName, body string) string {
	return fmt.Sprintf("\n\t\tCREATE TABLE %s (\n%s\n\t\t)", qualifyTable(d, o), body)
}

// createTableIfNotExistsSQL renders a CREATE TABLE IF NOT EXISTS statement for
// the given object. For unqualified objects it delegates to the dialect so
// existing output remains byte-identical; for schema-qualified objects it
// emits an engine-appropriate template using qualifyTable.
func createTableIfNotExistsSQL(d chuck.Dialect, o chuck.ObjectName, body string) string {
	if o.Schema == "" || d.Engine() == chuck.SQLite {
		return d.CreateTableIfNotExists(d.NormalizeIdentifier(o.Name), body)
	}
	qt := qualifyTable(d, o)
	switch d.Engine() {
	case chuck.MSSQL:
		schema := d.NormalizeIdentifier(o.Schema)
		name := d.NormalizeIdentifier(o.Name)
		objArg := fmt.Sprintf("[%s].[%s]", schema, name)
		return fmt.Sprintf(
			"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN CREATE TABLE %s (\n%s\n\t\t) END",
			escapeQuote(objArg), qt, body,
		)
	default:
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", qt, body)
	}
}

// dropTableIfExistsSQL renders a DROP TABLE IF EXISTS statement for the given
// object. Unqualified objects delegate to the dialect helper.
func dropTableIfExistsSQL(d chuck.Dialect, o chuck.ObjectName) string {
	if o.Schema == "" || d.Engine() == chuck.SQLite {
		return d.DropTableIfExists(d.NormalizeIdentifier(o.Name))
	}
	qt := qualifyTable(d, o)
	switch d.Engine() {
	case chuck.MSSQL:
		schema := d.NormalizeIdentifier(o.Schema)
		name := d.NormalizeIdentifier(o.Name)
		objArg := fmt.Sprintf("[%s].[%s]", schema, name)
		return fmt.Sprintf(
			"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN DROP TABLE %s; END",
			escapeQuote(objArg), qt,
		)
	default:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", qt)
	}
}

// insertOrIgnoreSQL renders an INSERT OR IGNORE / INSERT ... ON CONFLICT DO
// NOTHING / TRY-CATCH-INSERT statement for the given object. Unqualified
// objects delegate to the dialect helper to preserve existing behavior.
func insertOrIgnoreSQL(d chuck.Dialect, o chuck.ObjectName, columns, values string) string {
	if o.Schema == "" || d.Engine() == chuck.SQLite {
		return d.InsertOrIgnore(d.NormalizeIdentifier(o.Name), columns, values)
	}
	qt := qualifyTable(d, o)
	switch d.Engine() {
	case chuck.Postgres:
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", qt, columns, values)
	case chuck.MSSQL:
		return fmt.Sprintf(
			"BEGIN TRY INSERT INTO %s (%s) VALUES (%s) END TRY BEGIN CATCH IF ERROR_NUMBER() <> 2627 AND ERROR_NUMBER() <> 2601 THROW END CATCH",
			qt, columns, values,
		)
	default:
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qt, columns, values)
	}
}

// renderFKReference renders a REFERENCES clause body for the given FK target.
// Returns "schema.table(col)" with both pieces properly quoted.
func renderFKReference(d chuck.Dialect, target chuck.ObjectName, column string) string {
	return fmt.Sprintf("%s(%s)", qualifyTable(d, target), d.QuoteIdentifier(d.NormalizeIdentifier(column)))
}

// resolveSchemaForInspection returns the schema name that should be used when
// querying the live database for the given object. Defaults are applied per
// engine ("public" for Postgres, "dbo" for MSSQL) when the ObjectName has no
// explicit schema; SQLite returns an empty string.
func resolveSchemaForInspection(d chuck.Dialect, o chuck.ObjectName) string {
	schema, _ := normalizedObject(d, o)
	if schema != "" {
		return schema
	}
	switch d.Engine() {
	case chuck.Postgres:
		return "public"
	case chuck.MSSQL:
		return "dbo"
	default:
		return ""
	}
}
