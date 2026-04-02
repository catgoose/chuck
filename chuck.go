// Package chuck provides database engine abstractions for multi-dialect SQL DDL.
// It allows switching between database engines (e.g., MSSQL for production, SQLite for development)
// while keeping SQL visible and explicit.
package chuck

import (
	"fmt"
	"strings"
)

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
	ReturningClause(columns string) string
}

// QueryWriter generates query fragments.
type QueryWriter interface {
	Placeholder(n int) string
	Pagination() string
	Now() string
	LastInsertIDQuery() string
	SupportsLastInsertID() bool
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
