package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// SchemaError describes a single schema validation mismatch.
type SchemaError struct {
	Table   string
	Column  string
	Message string
}

func (e SchemaError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("%s.%s: %s", e.Table, e.Column, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Table, e.Message)
}

// ValidateSchema compares a declared table definition against the live database
// and returns all mismatches found. Column names are normalized for the dialect
// before comparison — e.g. "CreatedAt" becomes "created_at" on Postgres.
//
// Returns nil if the live schema matches the declaration.
func ValidateSchema(ctx context.Context, db *sql.DB, d chuck.Dialect, td *TableDef) []SchemaError {
	tableName := d.NormalizeIdentifier(td.Name)

	live, err := LiveSnapshot(ctx, db, d, tableName)
	if err != nil {
		return []SchemaError{{Table: tableName, Message: err.Error()}}
	}

	declared := td.Snapshot(d)

	var errs []SchemaError

	// Build lookup of live columns by name
	liveColMap := make(map[string]LiveColumnSnapshot, len(live.Columns))
	for _, lc := range live.Columns {
		liveColMap[lc.Name] = lc
	}

	// Check column count
	if len(declared.Columns) != len(live.Columns) {
		errs = append(errs, SchemaError{
			Table:   tableName,
			Message: fmt.Sprintf("column count mismatch: declared %d, live %d", len(declared.Columns), len(live.Columns)),
		})
	}

	// Check each declared column exists and matches
	for _, dc := range declared.Columns {
		lc, ok := liveColMap[dc.Name]
		if !ok {
			errs = append(errs, SchemaError{
				Table:  tableName,
				Column: dc.Name,
				Message: "column missing",
			})
			continue
		}

		if dc.NotNull != !lc.Nullable {
			errs = append(errs, SchemaError{
				Table:  tableName,
				Column: dc.Name,
				Message: fmt.Sprintf("nullability mismatch: declared NOT NULL=%v, live nullable=%v", dc.NotNull, lc.Nullable),
			})
		}
	}

	// Check for extra columns in live that aren't declared
	declaredColMap := make(map[string]bool, len(declared.Columns))
	for _, dc := range declared.Columns {
		declaredColMap[dc.Name] = true
	}
	for _, lc := range live.Columns {
		if !declaredColMap[lc.Name] {
			errs = append(errs, SchemaError{
				Table:  tableName,
				Column: lc.Name,
				Message: "unexpected column (exists in database but not in declaration)",
			})
		}
	}

	// Check declared indexes exist and match properties
	liveIndexMap := make(map[string]LiveIndexSnapshot, len(live.Indexes))
	for _, idx := range live.Indexes {
		liveIndexMap[idx.Name] = idx
	}
	for _, idx := range declared.Indexes {
		liveIdx, ok := liveIndexMap[idx.Name]
		if !ok {
			errs = append(errs, SchemaError{
				Table:   tableName,
				Message: fmt.Sprintf("index %q missing", idx.Name),
			})
			continue
		}
		liveColStr := strings.Join(liveIdx.Columns, ", ")
		if idx.Columns != liveColStr {
			errs = append(errs, SchemaError{
				Table:   tableName,
				Message: fmt.Sprintf("index %q columns mismatch: declared %q, live %q", idx.Name, idx.Columns, liveColStr),
			})
		}
		if idx.Unique != liveIdx.Unique {
			errs = append(errs, SchemaError{
				Table:   tableName,
				Message: fmt.Sprintf("index %q uniqueness mismatch: declared unique=%v, live unique=%v", idx.Name, idx.Unique, liveIdx.Unique),
			})
		}
		if idx.Where != liveIdx.Where {
			errs = append(errs, SchemaError{
				Table:   tableName,
				Message: fmt.Sprintf("index %q where clause mismatch: declared %q, live %q", idx.Name, idx.Where, liveIdx.Where),
			})
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// ValidateAll validates multiple table definitions against the live database.
// Returns all mismatches across all tables, or nil if everything matches.
func ValidateAll(ctx context.Context, db *sql.DB, d chuck.Dialect, tables ...*TableDef) []SchemaError {
	var errs []SchemaError
	for _, td := range tables {
		if tableErrs := ValidateSchema(ctx, db, d, td); tableErrs != nil {
			errs = append(errs, tableErrs...)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}
