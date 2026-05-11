// Package chuck provides database engine abstractions for multi-dialect SQL DDL.
// It allows switching between database engines (e.g., MSSQL for production, SQLite for development)
// while keeping SQL visible and explicit.
package chuck

import (
	"fmt"
	"strings"
)

// ObjectName is a structured database object identity composed of an optional
// schema and a required name. It is used internally to model schema-qualified
// tables and foreign-key targets so that dialects can render correctly
// qualified identifiers without resorting to dotted-string parsing.
type ObjectName struct {
	Schema string
	Name   string
}

// IsZero reports whether the ObjectName has neither schema nor name set.
func (o ObjectName) IsZero() bool { return o.Schema == "" && o.Name == "" }

// String returns "schema.name" when a schema is set, otherwise just "name".
// The output is for logging/diagnostics and is not quoted or normalized.
func (o ObjectName) String() string {
	if o.Schema == "" {
		return o.Name
	}
	return o.Schema + "." + o.Name
}

// Equal reports whether two ObjectNames have the same schema and name.
func (o ObjectName) Equal(other ObjectName) bool {
	return o.Schema == other.Schema && o.Name == other.Name
}

// ParseObjectName splits a "schema.table" string into a structured
// ObjectName. Inputs without a dot (or those that contain whitespace or
// parentheses, which signal aliases or derived tables) are treated as
// unqualified. Leading and trailing whitespace are trimmed.
//
// Examples:
//
//	ParseObjectName("Users")           -> {Name: "Users"}
//	ParseObjectName("sg.SalesAgents")  -> {Schema: "sg", Name: "SalesAgents"}
//	ParseObjectName(" sg . agents ")   -> {Schema: "sg", Name: "agents"}
//	ParseObjectName("(SELECT 1)")      -> {Name: "(SELECT 1)"} // passthrough
func ParseObjectName(s string) ObjectName {
	s = strings.TrimSpace(s)
	if s == "" {
		return ObjectName{}
	}
	// Anything with whitespace or parens is not a plain "schema.table" — leave
	// for the caller to pass through unchanged.
	if strings.ContainsAny(s, " \t()") {
		return ObjectName{Name: s}
	}
	idx := strings.Index(s, ".")
	if idx <= 0 || idx == len(s)-1 {
		return ObjectName{Name: s}
	}
	return ObjectName{
		Schema: strings.TrimSpace(s[:idx]),
		Name:   strings.TrimSpace(s[idx+1:]),
	}
}

// QualifyTable returns the dialect-rendered, quoted, fully-qualified table
// identifier for the given ObjectName. Both schema and name components are
// normalized via the dialect before being quoted. SQLite drops any schema
// component because it has no first-class schema namespace.
//
// This is the canonical helper shared by the schema DSL and dbrepo so that
// qualified-name rendering does not fork across the codebase.
func QualifyTable(d Dialect, o ObjectName) string {
	name := d.NormalizeIdentifier(o.Name)
	if o.Schema == "" || d.Engine() == SQLite {
		return d.QuoteIdentifier(name)
	}
	schema := d.NormalizeIdentifier(o.Schema)
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(name)
}

// Engine identifies a database engine.
type Engine string

const (
	MSSQL    Engine = "sqlserver"
	SQLite   Engine = "sqlite3"
	Postgres Engine = "postgres"
)

// ParseEngine converts a string to an Engine, returning an error for unknown values.
func ParseEngine(s string) (Engine, error) {
	switch s {
	case "sqlserver", "mssql":
		return MSSQL, nil
	case "sqlite3", "sqlite":
		return SQLite, nil
	case "postgres", "postgresql":
		return Postgres, nil
	default:
		return "", fmt.Errorf("unknown database engine: %q (expected sqlserver, mssql, sqlite3, sqlite, postgres, or postgresql)", s)
	}
}

// TypeMapper maps Go types to SQL column type strings.
type TypeMapper interface {
	IntType() string
	BigIntType() string
	TextType() string
	StringType(maxLen int) string
	VarcharType(maxLen int) string
	BoolType() string
	FloatType() string
	DecimalType(precision, scale int) string
	TimestampType() string
	UUIDType() string
	JSONType() string
	AutoIncrement() string
}

// DDLWriter generates DDL statements.
type DDLWriter interface {
	CreateTableIfNotExists(table, body string) string
	DropTableIfExists(table string) string
	CreateIndexIfNotExists(indexName, table, columns string) string
	InsertOrIgnore(table, columns, values string) string
	Upsert(table, columns, values, conflictColumns, updateSet string) string
	ReturningClause(columns string) string
}

// QueryWriter generates query fragments.
type QueryWriter interface {
	Placeholder(n int) string
	Pagination() string
	Now() string
	LastInsertIDQuery() string
	SupportsLastInsertID() bool
	// IsNull returns a dialect-specific expression that evaluates to fallback
	// when col is NULL (e.g. COALESCE, IFNULL, ISNULL).
	IsNull(col, fallback string) string
	// Concat returns a dialect-specific concatenation of the given parts.
	// Parts wrapped in single quotes (string literals) are passed through as-is;
	// bare identifiers are normalized and quoted.
	Concat(parts ...string) string
}

// Identifier handles SQL identifier formatting.
type Identifier interface {
	NormalizeIdentifier(name string) string
	QuoteIdentifier(name string) string
}

// Inspector provides schema introspection queries.
type Inspector interface {
	TableExistsQuery() string
	TableColumnsQuery() string
}

// Dialect provides engine-specific SQL fragments.
// It composes all sub-interfaces, so any value that implements Dialect
// also satisfies TypeMapper, DDLWriter, QueryWriter, Identifier, and Inspector.
// Implementations return raw SQL strings that callers compose into full queries.
type Dialect interface {
	TypeMapper
	DDLWriter
	QueryWriter
	Identifier
	Inspector
	// Engine returns the engine identifier (used as the driver name for sql.Open).
	Engine() Engine
}

// QuoteColumns splits a comma-separated column list, normalizes and quotes each identifier.
// Sort direction suffixes (ASC, DESC) are preserved and re-appended after quoting.
func QuoteColumns(d Identifier, columns string) string {
	parts := strings.Split(columns, ",")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		suffix := ""
		upper := strings.ToUpper(part)
		if strings.HasSuffix(upper, " DESC") {
			suffix = " DESC"
			part = strings.TrimSpace(part[:len(part)-5])
		} else if strings.HasSuffix(upper, " ASC") {
			suffix = " ASC"
			part = strings.TrimSpace(part[:len(part)-4])
		}
		quoted[i] = d.QuoteIdentifier(d.NormalizeIdentifier(part)) + suffix
	}
	return strings.Join(quoted, ", ")
}

// isStringLiteral reports whether s is a SQL string literal (wrapped in single quotes).
func isStringLiteral(s string) bool {
	return len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\''
}

// New returns a Dialect for the given engine.
func New(engine Engine) (Dialect, error) {
	switch engine {
	case MSSQL:
		return MSSQLDialect{}, nil
	case SQLite:
		return SQLiteDialect{}, nil
	case Postgres:
		return PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %q", engine)
	}
}
