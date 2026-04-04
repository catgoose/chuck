package schema

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// SeedValues represents a row of seed data as column name to Go value pairs.
// Supported value types: string, int, int64, float64, bool, nil, and SQLExpr.
// Values are automatically converted to SQL literals per dialect.
type SeedValues map[string]interface{}

// sqlExpr is a raw SQL expression that bypasses quoting.
type sqlExpr struct {
	expr string
}

// SQLExpr wraps a raw SQL expression so it is emitted verbatim in seed data.
// Use this for database functions like CURRENT_TIMESTAMP or dialect-specific expressions.
func SQLExpr(expr string) sqlExpr {
	return sqlExpr{expr: expr}
}

// goValueToSQL converts a Go value to a SQL literal string for the given dialect.
func goValueToSQL(d chuck.Dialect, v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case sqlExpr:
		return val.expr
	case string:
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'"
	case bool:
		switch d.Engine() {
		case chuck.Postgres:
			if val {
				return "TRUE"
			}
			return "FALSE"
		default: // SQLite, MSSQL
			if val {
				return "1"
			}
			return "0"
		}
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// toSeedRow converts a SeedValues to a SeedRow using the given dialect for SQL literal conversion.
func (sv SeedValues) toSeedRow(d chuck.Dialect) SeedRow {
	row := make(SeedRow, len(sv))
	for k, v := range sv {
		row[k] = goValueToSQL(d, v)
	}
	return row
}

// WithSeedValues declares initial seed data using Go values instead of SQL literals.
// Values are automatically quoted per dialect when SeedSQL is called.
func (t *TableDef) WithSeedValues(rows ...SeedValues) *TableDef {
	t.seedValueRows = append(t.seedValueRows, rows...)
	return t
}

// SeedSQL returns idempotent INSERT statements for all seed rows (both SeedRow and SeedValues)
// using the dialect's InsertOrIgnore method.
// Only columns present in the seed row are included -- missing columns use their DB defaults.
func (t *TableDef) seedValuesSQL(d chuck.Dialect) []string {
	if len(t.seedValueRows) == 0 {
		return nil
	}

	insertCols := t.InsertColumns()
	var stmts []string
	for _, sv := range t.seedValueRows {
		row := sv.toSeedRow(d)
		var cols []string
		var vals []string
		for _, col := range insertCols {
			if v, ok := row[col]; ok {
				cols = append(cols, d.NormalizeIdentifier(col))
				vals = append(vals, v)
			}
		}
		if len(cols) == 0 {
			continue
		}
		stmts = append(stmts, d.InsertOrIgnore(
			d.NormalizeIdentifier(t.Name),
			strings.Join(cols, ", "),
			strings.Join(vals, ", "),
		))
	}
	return stmts
}
