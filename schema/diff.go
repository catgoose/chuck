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
	HasDrift       bool             `json:"has_drift"`
}

// ColumnDiff describes a column that exists in both declared and live schemas
// but has mismatched properties.
type ColumnDiff struct {
	Name            string `json:"name"`
	DeclaredNotNull bool   `json:"declared_not_null"`
	LiveNullable    bool   `json:"live_nullable"`
	DeclaredType    string `json:"declared_type,omitempty"`
	LiveType        string `json:"live_type,omitempty"`
}

// DiffSchema compares a declared table against the live database and returns
// a structured diff. If the table doesn't exist, TableMissing is true.
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
		if dc.NotNull != !lc.Nullable {
			diff.ChangedColumns = append(diff.ChangedColumns, ColumnDiff{
				Name:            dc.Name,
				DeclaredNotNull: dc.NotNull,
				LiveNullable:    lc.Nullable,
				DeclaredType:    dc.Type,
				LiveType:        lc.Type,
			})
		}
	}

	// Indexes in declared but not in live
	liveIndexMap := make(map[string]bool, len(live.Indexes))
	for _, idx := range live.Indexes {
		liveIndexMap[idx.Name] = true
	}
	for _, idx := range declared.Indexes {
		if !liveIndexMap[idx.Name] {
			diff.MissingIndexes = append(diff.MissingIndexes, idx)
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
		len(diff.ExtraIndexes) > 0

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
