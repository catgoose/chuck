package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/catgoose/chuck"
)

// SchemaDiff is a structured diff between a declared schema and a live database.
// It is JSON-serializable and designed for consumption by developers, agents, and CI tools.
type SchemaDiff struct {
	Table          string           `json:"table"`
	TableMissing   bool             `json:"table_missing,omitempty"`
	AddedColumns   []ColumnSnapshot `json:"added_columns,omitempty"`
	RemovedColumns []string         `json:"removed_columns,omitempty"`
	ChangedColumns []ColumnDiff     `json:"changed_columns,omitempty"`
	MissingIndexes []IndexSnapshot  `json:"missing_indexes,omitempty"`
	ExtraIndexes   []string         `json:"extra_indexes,omitempty"`
	ChangedIndexes []IndexDiff      `json:"changed_indexes,omitempty"`
	HasDrift       bool             `json:"has_drift"`
}

// IndexDiff describes an index that exists in both declared and live schemas
// but has mismatched properties (columns, uniqueness, or where clause).
type IndexDiff struct {
	Name            string `json:"name"`
	DeclaredColumns string `json:"declared_columns,omitempty"`
	LiveColumns     string `json:"live_columns,omitempty"`
	DeclaredUnique  bool   `json:"declared_unique"`
	LiveUnique      bool   `json:"live_unique"`
	DeclaredWhere   string `json:"declared_where,omitempty"`
	LiveWhere       string `json:"live_where,omitempty"`
}

// ColumnDiff describes a column that exists in both declared and live schemas
// but has mismatched properties.
type ColumnDiff struct {
	Name            string   `json:"name"`
	DeclaredNotNull bool     `json:"declared_not_null"`
	LiveNullable    bool     `json:"live_nullable"`
	DeclaredType    string   `json:"declared_type,omitempty"`
	LiveType        string   `json:"live_type,omitempty"`
	DeclaredDefault string   `json:"declared_default,omitempty"`
	LiveDefault     string   `json:"live_default,omitempty"`
	Mismatches      []string `json:"mismatches,omitempty"`
}

// DiffSchema compares a declared table against the live database and returns
// a structured diff. If the table doesn't exist, TableMissing is true.
//
// The comparison covers the following dimensions:
//   - Column presence (missing/extra), type, nullability, and default value
//   - Index presence (missing/extra), columns, uniqueness, and partial predicate (WHERE clause)
//
// It does not currently detect drift in column-level unique constraints,
// primary keys, foreign keys, or CHECK constraints.
func DiffSchema(ctx context.Context, db *sql.DB, d chuck.Dialect, td *TableDef) (*SchemaDiff, error) {
	tableName := d.NormalizeIdentifier(td.Name)
	diff := &SchemaDiff{Table: tableName}

	live, err := LiveSnapshot(ctx, db, d, tableName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			diff.TableMissing = true
			diff.HasDrift = true
			return diff, nil
		}
		return nil, err
	}

	declared := td.Snapshot(d)

	// Build lookup maps
	liveColMap := make(map[string]LiveColumnSnapshot, len(live.Columns))
	for _, lc := range live.Columns {
		liveColMap[lc.Name] = lc
	}

	declaredColMap := make(map[string]ColumnSnapshot, len(declared.Columns))
	for _, dc := range declared.Columns {
		declaredColMap[dc.Name] = dc
	}

	// Columns in declared but not in live (added)
	for _, dc := range declared.Columns {
		if _, ok := liveColMap[dc.Name]; !ok {
			diff.AddedColumns = append(diff.AddedColumns, dc)
		}
	}

	// Columns in live but not in declared (removed)
	for _, lc := range live.Columns {
		if _, ok := declaredColMap[lc.Name]; !ok {
			diff.RemovedColumns = append(diff.RemovedColumns, lc.Name)
		}
	}

	// Columns in both but with mismatches
	for _, dc := range declared.Columns {
		lc, ok := liveColMap[dc.Name]
		if !ok {
			continue
		}

		cd := ColumnDiff{
			Name:            dc.Name,
			DeclaredNotNull: dc.NotNull,
			LiveNullable:    lc.Nullable,
			DeclaredType:    dc.Type,
			LiveType:        lc.Type,
			DeclaredDefault: dc.Default,
			LiveDefault:     lc.Default,
		}

		if dc.NotNull != !lc.Nullable {
			cd.Mismatches = append(cd.Mismatches, "nullability")
		}
		if normalizeType(dc.Type) != normalizeType(lc.Type) {
			cd.Mismatches = append(cd.Mismatches, "type")
		}
		if normalizeDefault(dc.Default) != normalizeDefault(lc.Default) {
			cd.Mismatches = append(cd.Mismatches, "default")
		}

		if len(cd.Mismatches) > 0 {
			diff.ChangedColumns = append(diff.ChangedColumns, cd)
		}
	}

	// Indexes in declared but not in live
	liveIndexMap := make(map[string]LiveIndexSnapshot, len(live.Indexes))
	for _, idx := range live.Indexes {
		liveIndexMap[idx.Name] = idx
	}
	for _, idx := range declared.Indexes {
		liveIdx, ok := liveIndexMap[idx.Name]
		if !ok {
			diff.MissingIndexes = append(diff.MissingIndexes, idx)
			continue
		}
		// Compare index properties
		liveColStr := strings.Join(liveIdx.Columns, ", ")
		if idx.Columns != liveColStr || idx.Unique != liveIdx.Unique || idx.Where != liveIdx.Where {
			diff.ChangedIndexes = append(diff.ChangedIndexes, IndexDiff{
				Name:            idx.Name,
				DeclaredColumns: idx.Columns,
				LiveColumns:     liveColStr,
				DeclaredUnique:  idx.Unique,
				LiveUnique:      liveIdx.Unique,
				DeclaredWhere:   idx.Where,
				LiveWhere:       liveIdx.Where,
			})
		}
	}

	// Indexes in live but not in declared
	declaredIndexMap := make(map[string]bool, len(declared.Indexes))
	for _, idx := range declared.Indexes {
		declaredIndexMap[idx.Name] = true
	}
	for _, idx := range live.Indexes {
		if !declaredIndexMap[idx.Name] {
			diff.ExtraIndexes = append(diff.ExtraIndexes, idx.Name)
		}
	}

	diff.HasDrift = len(diff.AddedColumns) > 0 ||
		len(diff.RemovedColumns) > 0 ||
		len(diff.ChangedColumns) > 0 ||
		len(diff.MissingIndexes) > 0 ||
		len(diff.ExtraIndexes) > 0 ||
		len(diff.ChangedIndexes) > 0

	return diff, nil
}

// DiffAll diffs multiple tables.
func DiffAll(ctx context.Context, db *sql.DB, d chuck.Dialect, tables ...*TableDef) ([]*SchemaDiff, error) {
	diffs := make([]*SchemaDiff, 0, len(tables))
	for _, td := range tables {
		diff, err := DiffSchema(ctx, db, d, td)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, diff)
	}
	return diffs, nil
}

// WriteTo writes the diff as formatted JSON to an io.Writer.
func (d *SchemaDiff) WriteTo(w io.Writer) (int64, error) {
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')
	n, err := w.Write(data)
	return int64(n), err
}

// WriteJSON writes the diff as formatted JSON to a file path.
func (d *SchemaDiff) WriteJSON(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = d.WriteTo(f)
	return err
}

// WriteDiffsTo writes multiple diffs as a JSON array to an io.Writer.
func WriteDiffsTo(diffs []*SchemaDiff, w io.Writer) (int64, error) {
	data, err := json.MarshalIndent(diffs, "", "  ")
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')
	n, err := w.Write(data)
	return int64(n), err
}

// normalizeType strips constraint keywords from a declared type string so that
// it can be compared against the base data type reported by a live database.
// For example, "INTEGER PRIMARY KEY AUTOINCREMENT" becomes "INTEGER", and
// "SERIAL PRIMARY KEY" becomes "SERIAL".
//
// It also normalizes Postgres type aliases so that declared types like
// "VARCHAR(255)" match live types like "CHARACTER VARYING(255)", and
// "TIMESTAMPTZ" matches "TIMESTAMP WITH TIME ZONE".
func normalizeType(s string) string {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)

	// Remove known constraint suffixes that are embedded in compound type declarations.
	for _, kw := range []string{
		"PRIMARY KEY AUTOINCREMENT",
		"PRIMARY KEY IDENTITY(1,1)",
		"PRIMARY KEY",
		"AUTOINCREMENT",
		"NOT NULL",
	} {
		upper = strings.ReplaceAll(upper, kw, "")
	}
	upper = strings.TrimSpace(upper)

	// Normalize Postgres type aliases to canonical forms.
	// This ensures declared types match what format_type() returns from pg_attribute.
	upper = normalizePostgresAlias(upper)

	return upper
}

// postgresAliases maps Postgres type aliases to their canonical forms.
// Aliases without parameters (length/precision) are listed here.
// Parameterized aliases like VARCHAR(n) are handled separately.
var postgresAliases = map[string]string{
	"TIMESTAMPTZ":              "TIMESTAMP WITH TIME ZONE",
	"TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
	"TIMESTAMP":                "TIMESTAMP WITHOUT TIME ZONE",
	"TIMETZ":                   "TIME WITH TIME ZONE",
	"TIME WITH TIME ZONE":      "TIME WITH TIME ZONE",
	"TIME":                     "TIME WITHOUT TIME ZONE",
	"BOOL":                     "BOOLEAN",
	"INT":                      "INTEGER",
	"INT4":                     "INTEGER",
	"INT2":                     "SMALLINT",
	"INT8":                     "BIGINT",
	"FLOAT4":                   "REAL",
	"FLOAT8":                   "DOUBLE PRECISION",
	"SERIAL":                   "INTEGER",
	"BIGSERIAL":                "BIGINT",
	"SMALLSERIAL":              "SMALLINT",
}

// normalizePostgresAlias converts Postgres type aliases to canonical forms so that
// declared types (e.g., VARCHAR(255), TIMESTAMPTZ, NUMERIC(10,2)) match the
// strings returned by format_type() (e.g., CHARACTER VARYING(255), TIMESTAMP WITH TIME ZONE).
func normalizePostgresAlias(s string) string {
	// Extract base type and optional parenthesized params
	base, params := splitTypeParams(s)

	// Check for parameterized alias normalization
	switch base {
	case "VARCHAR", "CHARACTER VARYING":
		base = "CHARACTER VARYING"
	case "CHAR", "CHARACTER":
		base = "CHARACTER"
	case "NUMERIC", "DECIMAL":
		base = "NUMERIC"
	default:
		// Check non-parameterized aliases
		if canonical, ok := postgresAliases[base]; ok {
			base = canonical
		}
	}

	if params != "" {
		return base + "(" + params + ")"
	}
	return base
}

// splitTypeParams splits a type string like "VARCHAR(255)" into ("VARCHAR", "255")
// or "INTEGER" into ("INTEGER", "").
func splitTypeParams(s string) (base, params string) {
	idx := strings.IndexByte(s, '(')
	if idx < 0 {
		return s, ""
	}
	end := strings.LastIndexByte(s, ')')
	if end < idx {
		return s, ""
	}
	return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx+1 : end])
}

// normalizeDefault normalizes a default value for comparison. Different databases
// may wrap defaults differently (e.g., parentheses, quotes). This trims whitespace,
// removes outer parentheses, and lowercases SQL keywords/functions while preserving
// the case of string literal contents inside single quotes. Postgres sequence-backed
// defaults (nextval(...)) canonicalize to the empty string so a declared column with
// no explicit default matches a live column whose default is the implicit sequence
// created by SERIAL, BIGSERIAL, or INTEGER PRIMARY KEY.
func normalizeDefault(s string) string {
	s = strings.TrimSpace(s)
	// Strip outer parentheses (common in some databases, e.g., SQLite wraps defaults)
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		s = s[1 : len(s)-1]
	}
	// Split on single quotes to selectively lowercase only non-quoted segments.
	// Even-indexed parts are outside quotes; odd-indexed parts are inside quotes.
	parts := strings.Split(s, "'")
	for i := range parts {
		if i%2 == 0 {
			parts[i] = strings.ToLower(parts[i])
		}
	}
	result := strings.Join(parts, "'")
	// Postgres sequence-backed defaults canonicalize to the empty string
	// so a declared column with no explicit default matches a live column
	// whose default is the implicit sequence created by SERIAL, BIGSERIAL,
	// or INTEGER PRIMARY KEY. The lowercasing pass above already normalized
	// "NEXTVAL" / "Nextval" / etc. to lowercase.
	if strings.HasPrefix(result, "nextval(") && strings.HasSuffix(result, ")") {
		return ""
	}
	return result
}

// WriteDiffsJSON writes multiple diffs as a JSON array to a file path.
func WriteDiffsJSON(diffs []*SchemaDiff, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = WriteDiffsTo(diffs, f)
	return err
}
