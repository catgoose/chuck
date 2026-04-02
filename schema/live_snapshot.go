package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// LiveColumnSnapshot describes a column as it exists in a live database.
type LiveColumnSnapshot struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default,omitempty"`
}

// LiveIndexSnapshot describes an index as it exists in a live database.
type LiveIndexSnapshot struct {
	Name    string `json:"name"`
	Columns string `json:"columns"`
}

// LiveTableSnapshot describes a table's actual schema as read from a live database.
// Compare against TableSnapshot (from Snapshot()) to detect schema drift.
type LiveTableSnapshot struct {
	Name    string               `json:"name"`
	Columns []LiveColumnSnapshot `json:"columns"`
	Indexes []LiveIndexSnapshot  `json:"indexes,omitempty"`
}

// LiveSnapshot queries the database and returns the actual schema for a table.
// The result can be compared against a declared Snapshot() to detect drift.
func LiveSnapshot(ctx context.Context, db *sql.DB, d chuck.Dialect, tableName string) (LiveTableSnapshot, error) {
	snap := LiveTableSnapshot{Name: tableName}

	// Check table exists
	var exists interface{}
	if err := db.QueryRowContext(ctx, d.TableExistsQuery(), tableName).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return snap, fmt.Errorf("table %q does not exist", tableName)
		}
		return snap, fmt.Errorf("check table %q: %w", tableName, err)
	}

	// Query column details
	cols, err := queryColumns(ctx, db, d, tableName)
	if err != nil {
		return snap, fmt.Errorf("query columns for %q: %w", tableName, err)
	}
	snap.Columns = cols

	// Query indexes
	indexes, err := queryIndexes(ctx, db, d, tableName)
	if err != nil {
		return snap, fmt.Errorf("query indexes for %q: %w", tableName, err)
	}
	snap.Indexes = indexes

	return snap, nil
}

// LiveSchemaSnapshot queries the database for all listed tables and returns their live schemas.
func LiveSchemaSnapshot(ctx context.Context, db *sql.DB, d chuck.Dialect, tableNames ...string) ([]LiveTableSnapshot, error) {
	snaps := make([]LiveTableSnapshot, 0, len(tableNames))
	for _, name := range tableNames {
		snap, err := LiveSnapshot(ctx, db, d, name)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

func queryColumns(ctx context.Context, db *sql.DB, d chuck.Dialect, tableName string) ([]LiveColumnSnapshot, error) {
	var query string
	switch d.Engine() {
	case chuck.SQLite:
		query = `SELECT name, type, CASE WHEN "notnull" = 1 OR pk = 1 THEN 'NO' ELSE 'YES' END AS nullable, COALESCE(dflt_value, '') AS dflt FROM pragma_table_info(?)`
	case chuck.Postgres:
		query = `SELECT column_name, UPPER(data_type), is_nullable, COALESCE(column_default, '') FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`
	case chuck.MSSQL:
		query = `SELECT COLUMN_NAME, UPPER(DATA_TYPE), IS_NULLABLE, COALESCE(COLUMN_DEFAULT, '') FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1 ORDER BY ORDINAL_POSITION`
	default:
		return nil, fmt.Errorf("unsupported engine: %s", d.Engine())
	}

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []LiveColumnSnapshot
	for rows.Next() {
		var name, colType, nullable, dflt string
		if err := rows.Scan(&name, &colType, &nullable, &dflt); err != nil {
			return nil, err
		}
		cols = append(cols, LiveColumnSnapshot{
			Name:     name,
			Type:     strings.TrimSpace(colType),
			Nullable: strings.EqualFold(nullable, "YES"),
			Default:  strings.TrimSpace(dflt),
		})
	}
	return cols, rows.Err()
}

func queryIndexes(ctx context.Context, db *sql.DB, d chuck.Dialect, tableName string) ([]LiveIndexSnapshot, error) {
	var query string
	switch d.Engine() {
	case chuck.SQLite:
		query = `SELECT name, '' AS columns FROM pragma_index_list(?) WHERE origin != 'pk'`
	case chuck.Postgres:
		query = `SELECT i.relname, pg_get_indexdef(ix.indexrelid) FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid WHERE t.relname = $1 AND NOT ix.indisprimary`
	case chuck.MSSQL:
		query = `SELECT si.name, STUFF((SELECT ', ' + sc.name FROM sys.index_columns ic JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id WHERE ic.object_id = si.object_id AND ic.index_id = si.index_id FOR XML PATH('')), 1, 2, '') FROM sys.indexes si WHERE si.object_id = OBJECT_ID(@p1) AND si.is_primary_key = 0 AND si.name IS NOT NULL`
	default:
		return nil, fmt.Errorf("unsupported engine: %s", d.Engine())
	}

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []LiveIndexSnapshot
	for rows.Next() {
		var name, columns string
		if err := rows.Scan(&name, &columns); err != nil {
			return nil, err
		}
		indexes = append(indexes, LiveIndexSnapshot{
			Name:    name,
			Columns: columns,
		})
	}
	return indexes, rows.Err()
}

// LiveSnapshotString returns a human-readable representation of a live table schema,
// in the same format as SnapshotString for easy side-by-side comparison.
func (s LiveTableSnapshot) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "TABLE %s\n", s.Name)

	for _, c := range s.Columns {
		var parts []string
		parts = append(parts, c.Type)
		if !c.Nullable {
			parts = append(parts, "NOT NULL")
		}
		if c.Default != "" {
			parts = append(parts, "DEFAULT "+c.Default)
		}
		fmt.Fprintf(&b, "  %-20s %s\n", c.Name, strings.Join(parts, " "))
	}

	for _, idx := range s.Indexes {
		if idx.Columns != "" {
			fmt.Fprintf(&b, "  INDEX %s ON (%s)\n", idx.Name, idx.Columns)
		} else {
			fmt.Fprintf(&b, "  INDEX %s\n", idx.Name)
		}
	}

	return b.String()
}
