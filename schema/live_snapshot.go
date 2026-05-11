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
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
	Where   string   `json:"where,omitempty"`
}

// LiveTableSnapshot describes a table's actual schema as read from a live database.
// Compare against TableSnapshot (from Snapshot()) to detect schema drift.
type LiveTableSnapshot struct {
	Schema  string               `json:"schema,omitempty"`
	Name    string               `json:"name"`
	Columns []LiveColumnSnapshot `json:"columns"`
	Indexes []LiveIndexSnapshot  `json:"indexes,omitempty"`
}

// Object returns the structured ObjectName captured in the live snapshot.
func (s LiveTableSnapshot) Object() chuck.ObjectName {
	return chuck.ObjectName{Schema: s.Schema, Name: s.Name}
}

// LiveSnapshot queries the database and returns the actual schema for an
// unqualified table. Equivalent to LiveSnapshotObject with no schema; kept
// for back-compat.
func LiveSnapshot(ctx context.Context, db *sql.DB, d chuck.Dialect, tableName string) (LiveTableSnapshot, error) {
	return LiveSnapshotObject(ctx, db, d, chuck.ObjectName{Name: tableName})
}

// LiveSnapshotObject queries the database and returns the actual schema for a
// schema-qualified table. SQLite ignores any schema component; Postgres
// defaults to "public" and MSSQL defaults to "dbo" when target.Schema is "".
func LiveSnapshotObject(ctx context.Context, db *sql.DB, d chuck.Dialect, target chuck.ObjectName) (LiveTableSnapshot, error) {
	schema, name := normalizedObject(d, target)
	snap := LiveTableSnapshot{Schema: schema, Name: name}
	inspectSchema := resolveSchemaForInspection(d, target)

	if err := checkTableExists(ctx, db, d, inspectSchema, name); err != nil {
		return snap, err
	}

	cols, err := queryColumns(ctx, db, d, inspectSchema, name)
	if err != nil {
		return snap, fmt.Errorf("query columns for %q: %w", target.String(), err)
	}
	snap.Columns = cols

	indexes, err := queryIndexes(ctx, db, d, inspectSchema, name)
	if err != nil {
		return snap, fmt.Errorf("query indexes for %q: %w", target.String(), err)
	}
	snap.Indexes = indexes

	return snap, nil
}

// LiveSchemaSnapshot queries the database for all listed unqualified tables.
func LiveSchemaSnapshot(ctx context.Context, db *sql.DB, d chuck.Dialect, tableNames ...string) ([]LiveTableSnapshot, error) {
	snaps := make([]LiveTableSnapshot, 0, len(tableNames))
	for _, name := range tableNames {
		snap, err := LiveSnapshotObject(ctx, db, d, chuck.ObjectName{Name: name})
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// LiveSchemaSnapshotObjects queries the database for all listed schema-qualified
// tables. Use this when any table needs schema-aware introspection.
func LiveSchemaSnapshotObjects(ctx context.Context, db *sql.DB, d chuck.Dialect, targets ...chuck.ObjectName) ([]LiveTableSnapshot, error) {
	snaps := make([]LiveTableSnapshot, 0, len(targets))
	for _, t := range targets {
		snap, err := LiveSnapshotObject(ctx, db, d, t)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// checkTableExists runs the engine-appropriate existence probe for a (schema,
// name) pair. SQLite ignores schema.
func checkTableExists(ctx context.Context, db *sql.DB, d chuck.Dialect, schema, name string) error {
	var (
		query string
		args  []any
	)
	switch d.Engine() {
	case chuck.SQLite:
		query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
		args = []any{name}
	case chuck.Postgres:
		query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
		args = []any{schema, name}
	case chuck.MSSQL:
		objArg := name
		if schema != "" {
			objArg = schema + "." + name
		}
		query = "SELECT name FROM sys.objects WHERE object_id = OBJECT_ID(@p1) AND type = 'U'"
		args = []any{objArg}
	default:
		return fmt.Errorf("unsupported engine: %s", d.Engine())
	}

	var exists any
	display := name
	if schema != "" {
		display = schema + "." + name
	}
	if err := db.QueryRowContext(ctx, query, args...).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("table %q does not exist", display)
		}
		return fmt.Errorf("check table %q: %w", display, err)
	}
	return nil
}

func queryColumns(ctx context.Context, db *sql.DB, d chuck.Dialect, schema, name string) ([]LiveColumnSnapshot, error) {
	switch d.Engine() {
	case chuck.SQLite, chuck.Postgres:
		return queryColumnsSimple(ctx, db, d, schema, name)
	case chuck.MSSQL:
		return queryColumnsMSSQL(ctx, db, schema, name)
	default:
		return nil, fmt.Errorf("unsupported engine: %s", d.Engine())
	}
}

// queryColumnsSimple handles engines whose column queries return the shared
// 4-column shape (name, type, nullable, default) and need no per-column
// reconstruction work. Used by SQLite and Postgres.
func queryColumnsSimple(ctx context.Context, db *sql.DB, d chuck.Dialect, schema, name string) ([]LiveColumnSnapshot, error) {
	var (
		query string
		args  []any
	)
	switch d.Engine() {
	case chuck.SQLite:
		query = `SELECT name, type, CASE WHEN "notnull" = 1 OR pk = 1 THEN 'NO' ELSE 'YES' END AS nullable, COALESCE(dflt_value, '') AS dflt FROM pragma_table_info(?)`
		args = []any{name}
	case chuck.Postgres:
		query = postgresColumnQuery
		args = []any{schema, name}
	default:
		return nil, fmt.Errorf("unsupported engine: %s", d.Engine())
	}

	rows, err := db.QueryContext(ctx, query, args...)
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

// mssqlColumnQuery selects the metadata needed to reconstruct parameterized
// MSSQL column type strings. CHARACTER_MAXIMUM_LENGTH is in characters (not
// bytes) per the SQL Server INFORMATION_SCHEMA.COLUMNS contract, with -1
// indicating MAX. NUMERIC_PRECISION/NUMERIC_SCALE are populated for numeric
// types and NULL otherwise. All three are nullable, hence the NullInt64 scans.
var mssqlColumnQuery = `SELECT COLUMN_NAME, UPPER(DATA_TYPE), IS_NULLABLE, COALESCE(COLUMN_DEFAULT, ''), CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2 ORDER BY ORDINAL_POSITION`

// queryColumnsMSSQL queries MSSQL column metadata and rebuilds parameterized
// type strings (e.g. NVARCHAR(255), DECIMAL(10,2), VARCHAR(MAX)) so live
// snapshots match the declared types produced by Snapshot().
func queryColumnsMSSQL(ctx context.Context, db *sql.DB, schema, tableName string) ([]LiveColumnSnapshot, error) {
	rows, err := db.QueryContext(ctx, mssqlColumnQuery, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []LiveColumnSnapshot
	for rows.Next() {
		var (
			name, dataType, nullable, dflt string
			charMaxLength                  sql.NullInt64
			numericPrecision               sql.NullInt64
			numericScale                   sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &nullable, &dflt, &charMaxLength, &numericPrecision, &numericScale); err != nil {
			return nil, err
		}
		cols = append(cols, LiveColumnSnapshot{
			Name:     name,
			Type:     reconstructMSSQLType(strings.TrimSpace(dataType), charMaxLength, numericPrecision, numericScale),
			Nullable: strings.EqualFold(nullable, "YES"),
			Default:  strings.TrimSpace(dflt),
		})
	}
	return cols, rows.Err()
}

// reconstructMSSQLType rebuilds a parameterized type string from
// INFORMATION_SCHEMA.COLUMNS metadata. The base dataType is expected to be
// uppercase. Returns the bare base type when no parameters apply.
//
// CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA.COLUMNS is already
// expressed in characters (not bytes), so NVARCHAR/NCHAR do not need a
// byte-to-character conversion. A value of -1 means MAX.
func reconstructMSSQLType(dataType string, charMaxLength, numericPrecision, numericScale sql.NullInt64) string {
	switch dataType {
	case "VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "VARBINARY", "BINARY":
		if !charMaxLength.Valid {
			return dataType
		}
		if charMaxLength.Int64 == -1 {
			return fmt.Sprintf("%s(MAX)", dataType)
		}
		return fmt.Sprintf("%s(%d)", dataType, charMaxLength.Int64)
	case "DECIMAL", "NUMERIC":
		if !numericPrecision.Valid || !numericScale.Valid {
			return dataType
		}
		return fmt.Sprintf("%s(%d,%d)", dataType, numericPrecision.Int64, numericScale.Int64)
	default:
		return dataType
	}
}

func queryIndexes(ctx context.Context, db *sql.DB, d chuck.Dialect, schema, tableName string) ([]LiveIndexSnapshot, error) {
	switch d.Engine() {
	case chuck.SQLite:
		return queryIndexesSQLite(ctx, db, tableName)
	case chuck.Postgres:
		return queryIndexesPostgres(ctx, db, schema, tableName)
	case chuck.MSSQL:
		return queryIndexesMSSQL(ctx, db, schema, tableName)
	default:
		return nil, fmt.Errorf("unsupported engine: %s", d.Engine())
	}
}

func queryIndexesSQLite(ctx context.Context, db *sql.DB, tableName string) ([]LiveIndexSnapshot, error) {
	// pragma_index_list returns: seq, name, unique, origin, partial
	listQuery := `SELECT name, "unique", partial FROM pragma_index_list(?) WHERE origin = 'c'`
	rows, err := db.QueryContext(ctx, listQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type idxMeta struct {
		name    string
		unique  bool
		partial bool
	}
	var metas []idxMeta
	for rows.Next() {
		var name string
		var unique, partial int
		if err := rows.Scan(&name, &unique, &partial); err != nil {
			return nil, err
		}
		metas = append(metas, idxMeta{name: name, unique: unique == 1, partial: partial == 1})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var indexes []LiveIndexSnapshot
	for _, m := range metas {
		// Get columns via pragma_index_info
		colQuery := `SELECT name FROM pragma_index_info(?) ORDER BY seqno`
		colRows, err := db.QueryContext(ctx, colQuery, m.name)
		if err != nil {
			return nil, err
		}
		var cols []string
		for colRows.Next() {
			var col string
			if err := colRows.Scan(&col); err != nil {
				colRows.Close()
				return nil, err
			}
			cols = append(cols, col)
		}
		colRows.Close()
		if err := colRows.Err(); err != nil {
			return nil, err
		}

		idx := LiveIndexSnapshot{
			Name:    m.name,
			Columns: cols,
			Unique:  m.unique,
		}

		// SQLite stores the WHERE clause in the index SQL; extract it if partial.
		if m.partial {
			sqlQuery := `SELECT sql FROM sqlite_master WHERE type='index' AND name=?`
			var idxSQL sql.NullString
			if err := db.QueryRowContext(ctx, sqlQuery, m.name).Scan(&idxSQL); err == nil && idxSQL.Valid {
				if w := extractSQLiteWhere(idxSQL.String); w != "" {
					idx.Where = w
				}
			}
		}

		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// extractSQLiteWhere extracts the WHERE clause from a SQLite CREATE INDEX statement.
func extractSQLiteWhere(createSQL string) string {
	upper := strings.ToUpper(createSQL)
	idx := strings.LastIndex(upper, " WHERE ")
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(createSQL[idx+7:])
}

func queryIndexesPostgres(ctx context.Context, db *sql.DB, schema, tableName string) ([]LiveIndexSnapshot, error) {
	query := postgresIndexQuery

	rows, err := db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []LiveIndexSnapshot
	for rows.Next() {
		var name, predicate, columns string
		var unique bool
		if err := rows.Scan(&name, &unique, &predicate, &columns); err != nil {
			return nil, err
		}
		idx := LiveIndexSnapshot{
			Name:   name,
			Unique: unique,
			Where:  predicate,
		}
		if columns != "" {
			idx.Columns = splitAndTrim(columns, ",")
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// postgresColumnQuery retrieves column metadata for a (schema, table) pair.
// The schema and table name are passed as parameters $1 and $2 respectively
// so the same query works for any namespace, not just public.
// Exported at package level for testability.
var postgresColumnQuery = `SELECT c.column_name, UPPER(format_type(a.atttypid, a.atttypmod)), c.is_nullable, COALESCE(c.column_default, '') FROM information_schema.columns c JOIN pg_attribute a ON a.attname = c.column_name JOIN pg_class t ON t.relname = c.table_name AND t.oid = a.attrelid JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname = c.table_schema WHERE c.table_schema = $1 AND c.table_name = $2 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY c.ordinal_position`

// postgresIndexQuery retrieves index metadata for a (schema, table) pair.
// Exported at package level for testability.
var postgresIndexQuery = `
		SELECT
			i.relname AS index_name,
			ix.indisunique,
			COALESCE(pg_get_expr(ix.indpred, ix.indrelid), '') AS predicate,
			array_to_string(ARRAY(
				SELECT a.attname
				FROM unnest(ix.indkey) k
				JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k
			), ',') AS columns
		FROM pg_index ix
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace AND n.nspname = $1
		WHERE t.relname = $2 AND NOT ix.indisprimary`

// mssqlIndexQuery retrieves index metadata for a schema-qualified object via
// OBJECT_ID, which accepts the literal "[schema].[name]" pattern. Exported at
// package level for testability.
var mssqlIndexQuery = `
	SELECT
		si.name,
		si.is_unique,
		COALESCE(si.filter_definition, ''),
		STUFF((
			SELECT ', ' + sc.name
			FROM sys.index_columns ic
			JOIN sys.columns sc ON sc.object_id = ic.object_id AND sc.column_id = ic.column_id
			WHERE ic.object_id = si.object_id AND ic.index_id = si.index_id
			AND ic.is_included_column = 0
			ORDER BY ic.key_ordinal
			FOR XML PATH('')
		), 1, 2, '')
	FROM sys.indexes si
	WHERE si.object_id = OBJECT_ID(@p1)
	AND si.is_primary_key = 0
	AND si.name IS NOT NULL`

func queryIndexesMSSQL(ctx context.Context, db *sql.DB, schema, tableName string) ([]LiveIndexSnapshot, error) {
	objArg := tableName
	if schema != "" {
		objArg = schema + "." + tableName
	}
	rows, err := db.QueryContext(ctx, mssqlIndexQuery, objArg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []LiveIndexSnapshot
	for rows.Next() {
		var name, filter, columns string
		var unique bool
		if err := rows.Scan(&name, &unique, &filter, &columns); err != nil {
			return nil, err
		}
		idx := LiveIndexSnapshot{
			Name:   name,
			Unique: unique,
			Where:  filter,
		}
		if columns != "" {
			idx.Columns = splitAndTrim(columns, ",")
		}
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

// splitAndTrim splits a string by separator and trims whitespace from each element.
func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}

// LiveSnapshotString returns a human-readable representation of a live table schema,
// in the same format as SnapshotString for easy side-by-side comparison.
func (s LiveTableSnapshot) String() string {
	var b strings.Builder
	if s.Schema != "" {
		fmt.Fprintf(&b, "TABLE %s.%s\n", s.Schema, s.Name)
	} else {
		fmt.Fprintf(&b, "TABLE %s\n", s.Name)
	}

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
		prefix := "INDEX"
		if idx.Unique {
			prefix = "UNIQUE INDEX"
		}
		colStr := strings.Join(idx.Columns, ", ")
		if colStr != "" {
			if idx.Where != "" {
				fmt.Fprintf(&b, "  %s %s ON (%s) WHERE %s\n", prefix, idx.Name, colStr, idx.Where)
			} else {
				fmt.Fprintf(&b, "  %s %s ON (%s)\n", prefix, idx.Name, colStr)
			}
		} else {
			fmt.Fprintf(&b, "  %s %s\n", prefix, idx.Name)
		}
	}

	return b.String()
}
