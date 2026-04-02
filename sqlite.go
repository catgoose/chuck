package chuck

import "fmt"

// Compile-time interface checks.
var (
	_ Dialect    = SQLiteDialect{}
	_ TypeMapper = SQLiteDialect{}
	_ DDLWriter  = SQLiteDialect{}
	_ QueryWriter = SQLiteDialect{}
	_ Identifier = SQLiteDialect{}
	_ Inspector  = SQLiteDialect{}
)

// SQLiteDialect implements Dialect for SQLite.
type SQLiteDialect struct{}

func (SQLiteDialect) Engine() Engine { return SQLite }

func (SQLiteDialect) Pagination() string {
	return "LIMIT @Limit OFFSET @Offset"
}

func (SQLiteDialect) AutoIncrement() string {
	return "INTEGER PRIMARY KEY AUTOINCREMENT"
}

func (SQLiteDialect) Now() string { return "CURRENT_TIMESTAMP" }

func (SQLiteDialect) TimestampType() string { return "TIMESTAMP" }

func (SQLiteDialect) StringType(_ int) string { return "TEXT" }

func (SQLiteDialect) VarcharType(_ int) string { return "TEXT" }

func (SQLiteDialect) IntType() string  { return "INTEGER" }
func (SQLiteDialect) TextType() string { return "TEXT" }
func (SQLiteDialect) BoolType() string { return "INTEGER" }

func (SQLiteDialect) Placeholder(_ int) string { return "?" }

func (SQLiteDialect) ReturningClause(columns string) string {
	return fmt.Sprintf("RETURNING %s", columns)
}

func (SQLiteDialect) NormalizeIdentifier(name string) string { return name }

func (SQLiteDialect) QuoteIdentifier(name string) string {
	return `"` + name + `"`
}

func (SQLiteDialect) BigIntType() string  { return "INTEGER" }
func (SQLiteDialect) FloatType() string   { return "REAL" }
func (SQLiteDialect) UUIDType() string    { return "TEXT" }
func (SQLiteDialect) JSONType() string    { return "TEXT" }

func (SQLiteDialect) DecimalType(_, _ int) string { return "REAL" }

func (d SQLiteDialect) CreateTableIfNotExists(table, body string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", d.QuoteIdentifier(table), body)
}

func (d SQLiteDialect) DropTableIfExists(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", d.QuoteIdentifier(table))
}

func (d SQLiteDialect) CreateIndexIfNotExists(indexName, table, columns string) string {
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)",
		d.QuoteIdentifier(indexName), d.QuoteIdentifier(table), QuoteColumns(d, columns))
}

func (SQLiteDialect) LastInsertIDQuery() string { return "" }

func (SQLiteDialect) SupportsLastInsertID() bool { return true }

func (SQLiteDialect) TableExistsQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
}

func (SQLiteDialect) TableColumnsQuery() string {
	return "SELECT name FROM pragma_table_info(?)"
}

func (d SQLiteDialect) InsertOrIgnore(table, columns, values string) string {
	return fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (%s)", d.QuoteIdentifier(table), columns, values)
}
