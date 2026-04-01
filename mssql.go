package fraggle

import (
	"fmt"
	"strings"
)

// Compile-time interface checks.
var (
	_ Dialect    = MSSQLDialect{}
	_ TypeMapper = MSSQLDialect{}
	_ DDLWriter  = MSSQLDialect{}
	_ QueryWriter = MSSQLDialect{}
	_ Identifier = MSSQLDialect{}
	_ Inspector  = MSSQLDialect{}
)

// MSSQLDialect implements Dialect for Microsoft SQL Server.
type MSSQLDialect struct{}

func (MSSQLDialect) Engine() Engine { return MSSQL }

func (MSSQLDialect) Pagination() string {
	return "OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY"
}

func (MSSQLDialect) AutoIncrement() string {
	return "INT PRIMARY KEY IDENTITY(1,1)"
}

func (MSSQLDialect) Now() string { return "GETDATE()" }

func (MSSQLDialect) TimestampType() string { return "DATETIME" }

func (MSSQLDialect) StringType(maxLen int) string {
	return fmt.Sprintf("NVARCHAR(%d)", maxLen)
}

func (MSSQLDialect) VarcharType(maxLen int) string {
	return fmt.Sprintf("VARCHAR(%d)", maxLen)
}

func (MSSQLDialect) IntType() string  { return "INT" }
func (MSSQLDialect) TextType() string { return "NVARCHAR(MAX)" }
func (MSSQLDialect) BoolType() string { return "BIT" }

func (MSSQLDialect) Placeholder(n int) string {
	return fmt.Sprintf("@p%d", n)
}

func (MSSQLDialect) ReturningClause(_ string) string { return "" }

func (MSSQLDialect) NormalizeIdentifier(name string) string { return name }

func (d MSSQLDialect) QuoteIdentifier(name string) string {
	// Escape any ] in the name by doubling it, then wrap in brackets
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (MSSQLDialect) BigIntType() string            { return "BIGINT" }
func (MSSQLDialect) FloatType() string             { return "FLOAT" }
func (MSSQLDialect) UUIDType() string              { return "UNIQUEIDENTIFIER" }
func (MSSQLDialect) JSONType() string              { return "NVARCHAR(MAX)" }

func (MSSQLDialect) DecimalType(precision, scale int) string {
	return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
}

func (d MSSQLDialect) CreateTableIfNotExists(table, body string) string {
	q := d.QuoteIdentifier(table)
	return fmt.Sprintf(
		"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN CREATE TABLE %s (\n%s\n\t\t) END",
		q, q, body,
	)
}

func (d MSSQLDialect) DropTableIfExists(table string) string {
	q := d.QuoteIdentifier(table)
	return fmt.Sprintf(
		"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN DROP TABLE %s; END",
		q, q,
	)
}

func (d MSSQLDialect) CreateIndexIfNotExists(indexName, table, columns string) string {
	qi := d.QuoteIdentifier(indexName)
	qt := d.QuoteIdentifier(table)
	return fmt.Sprintf(
		"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'%s' AND object_id = OBJECT_ID(N'%s')) CREATE INDEX %s ON %s(%s)",
		strings.ReplaceAll(indexName, "'", "''"), strings.ReplaceAll(table, "'", "''"), qi, qt, QuoteColumns(d, columns),
	)
}

func (MSSQLDialect) LastInsertIDQuery() string { return "SELECT SCOPE_IDENTITY() AS ID" }

func (MSSQLDialect) SupportsLastInsertID() bool { return false }

func (MSSQLDialect) TableExistsQuery() string {
	return "SELECT name FROM sys.objects WHERE object_id = OBJECT_ID(@p1) AND type = 'U'"
}

func (MSSQLDialect) TableColumnsQuery() string {
	return "SELECT COLUMN_NAME AS name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1"
}

func (d MSSQLDialect) InsertOrIgnore(table, columns, values string) string {
	return fmt.Sprintf(
		"BEGIN TRY INSERT INTO %s (%s) VALUES (%s) END TRY BEGIN CATCH IF ERROR_NUMBER() <> 2627 AND ERROR_NUMBER() <> 2601 THROW END CATCH",
		d.QuoteIdentifier(table), columns, values,
	)
}
