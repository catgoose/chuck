package fraggle

import "fmt"

// Compile-time interface checks.
var (
	_ Dialect    = PostgresDialect{}
	_ TypeMapper = PostgresDialect{}
	_ DDLWriter  = PostgresDialect{}
	_ QueryWriter = PostgresDialect{}
	_ Identifier = PostgresDialect{}
	_ Inspector  = PostgresDialect{}
)

// PostgresDialect implements Dialect for PostgreSQL.
type PostgresDialect struct{}

func (PostgresDialect) Engine() Engine    { return Postgres }
func (PostgresDialect) Pagination() string { return "LIMIT @Limit OFFSET @Offset" }
func (PostgresDialect) AutoIncrement() string {
	return "SERIAL PRIMARY KEY"
}
func (PostgresDialect) Now() string           { return "NOW()" }
func (PostgresDialect) TimestampType() string { return "TIMESTAMPTZ" }
func (PostgresDialect) StringType(_ int) string {
	return "TEXT"
}
func (PostgresDialect) VarcharType(maxLen int) string {
	return fmt.Sprintf("VARCHAR(%d)", maxLen)
}
func (PostgresDialect) IntType() string  { return "INTEGER" }
func (PostgresDialect) TextType() string { return "TEXT" }
func (PostgresDialect) BoolType() string { return "BOOLEAN" }

func (PostgresDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (PostgresDialect) ReturningClause(columns string) string {
	return fmt.Sprintf("RETURNING %s", columns)
}

func (PostgresDialect) NormalizeIdentifier(name string) string {
	return camelToSnake(name)
}

func (PostgresDialect) QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

func (PostgresDialect) BigIntType() string  { return "BIGINT" }
func (PostgresDialect) FloatType() string   { return "DOUBLE PRECISION" }
func (PostgresDialect) UUIDType() string    { return "UUID" }
func (PostgresDialect) JSONType() string    { return "JSONB" }

func (PostgresDialect) DecimalType(precision, scale int) string {
	return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
}

func (d PostgresDialect) CreateTableIfNotExists(table, body string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", d.QuoteIdentifier(table), body)
}

func (d PostgresDialect) DropTableIfExists(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", d.QuoteIdentifier(table))
}

func (d PostgresDialect) CreateIndexIfNotExists(indexName, table, columns string) string {
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)",
		d.QuoteIdentifier(indexName), d.QuoteIdentifier(table), QuoteColumns(d, columns))
}

func (PostgresDialect) LastInsertIDQuery() string { return "" }
func (PostgresDialect) SupportsLastInsertID() bool { return false }

func (PostgresDialect) TableExistsQuery() string {
	return "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1"
}

func (PostgresDialect) TableColumnsQuery() string {
	return "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position"
}

func (d PostgresDialect) InsertOrIgnore(table, columns, values string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", d.QuoteIdentifier(table), columns, values)
}
