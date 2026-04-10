package schema

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
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
// before comparison -- e.g. "CreatedAt" becomes "created_at" on Postgres.
//
// The comparison covers the following dimensions:
//   - Column presence (missing/extra), type, nullability, and default value
//   - Index presence (missing/extra), columns, uniqueness, and partial predicate (WHERE clause)
//
// It does not currently detect drift in column-level unique constraints,
// primary keys, foreign keys, or CHECK constraints.
//
// Returns nil if the live schema matches the declaration.
func ValidateSchema(ctx context.Context, db *sql.DB, d chuck.Dialect, td *TableDef) []SchemaError {
	tableName := d.NormalizeIdentifier(td.Name)

	live, err := LiveSnapshot(ctx, db, d, tableName)
	if err != nil {
		return []SchemaError{{Table: tableName, Message: err.Error()}}
	}

	return validateAgainstLiveSnapshot(td, d, live, tableName)
}

// validateAgainstLiveSnapshot is the pure comparison core of ValidateSchema:
// it compares a declared TableDef against an already-fetched LiveTableSnapshot
// and returns any drift errors. Extracted so the comparator can be unit-tested
// against crafted live snapshots without requiring a real database.
func validateAgainstLiveSnapshot(td *TableDef, d chuck.Dialect, live LiveTableSnapshot, tableName string) []SchemaError {
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

		if normalizeType(dc.Type) != normalizeType(lc.Type) {
			errs = append(errs, SchemaError{
				Table:  tableName,
				Column: dc.Name,
				Message: fmt.Sprintf("type mismatch: declared %q, live %q", dc.Type, lc.Type),
			})
		}

		if normalizeDefault(dc.Default) != normalizeDefault(lc.Default) {
			errs = append(errs, SchemaError{
				Table:  tableName,
				Column: dc.Name,
				Message: fmt.Sprintf("default mismatch: declared %q, live %q", dc.Default, lc.Default),
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
		declaredCanonical := canonicalIndexColumns(d, strings.Split(idx.Columns, ","))
		liveCanonical := canonicalIndexColumns(d, liveIdx.Columns)
		if !slices.Equal(declaredCanonical, liveCanonical) {
			liveColStr := strings.Join(liveIdx.Columns, ", ")
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

	// Check for extra indexes in live that aren't declared
	declaredIndexMap := make(map[string]bool, len(declared.Indexes))
	for _, idx := range declared.Indexes {
		declaredIndexMap[idx.Name] = true
	}
	implicitUniques := declaredSingleColumnUniques(td)
	for _, idx := range live.Indexes {
		if declaredIndexMap[idx.Name] {
			continue
		}
		// A live single-column unique index may be the implicit index that
		// the database created to back a column-level UNIQUE constraint.
		// Match by column set rather than by name so engine-specific naming
		// (Postgres "<table>_<col>_key", MSSQL "UQ__<prefix>__<hash>") all work.
		if idx.Unique && len(idx.Columns) == 1 && idx.Where == "" {
			if implicitUniques[strings.ToLower(idx.Columns[0])] {
				continue
			}
		}
		errs = append(errs, SchemaError{
			Table:   tableName,
			Message: fmt.Sprintf("unexpected index %q (exists in database but not in declaration)", idx.Name),
		})
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// declaredSingleColumnUniques returns the set of column names (lowercased)
// that are declared unique via a single-column UNIQUE constraint, either on
// the ColumnDef itself (via .Unique()) or as a single-column entry in
// TableDef.UniqueColumns. Composite unique constraints (len > 1) are
// intentionally excluded — those are handled by the standard index
// declaration path.
//
// The returned map is used to suppress false-positive "unexpected index"
// errors for the implicit single-column unique indexes that databases
// automatically create to back column-level UNIQUE constraints (e.g.
// Postgres "<table>_<col>_key", MSSQL "UQ__<prefix>__<hash>").
func declaredSingleColumnUniques(td *TableDef) map[string]bool {
	out := make(map[string]bool)
	for _, c := range td.cols {
		if c.unique {
			out[strings.ToLower(c.name)] = true
		}
	}
	for _, uc := range td.uniqueConstraints {
		if len(uc.columns) == 1 {
			out[strings.ToLower(uc.columns[0])] = true
		}
	}
	return out
}

// canonicalIndexColumns returns the canonical comparable form of an index
// column list. Each token is trimmed and run through the dialect's
// NormalizeIdentifier so that engine-driven identifier transforms (e.g.
// Postgres lowercasing PascalCase to snake_case) do not produce false
// drift in index column comparison. Order is preserved because index
// column order is significant.
func canonicalIndexColumns(d chuck.Dialect, tokens []string) []string {
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		out = append(out, d.NormalizeIdentifier(t))
	}
	return out
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
