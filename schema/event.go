package schema

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// NewEventTable creates an append-only event/log table. All columns are immutable (no updates).
// Includes an auto-increment ID, a timestamp defaulting to NOW(), and caller-defined columns
// for the event data (e.g., event type, actor, payload).
func NewEventTable(name string, cols ...ColumnDef) *TableDef {
	lower := strings.ToLower(name)
	allCols := []ColumnDef{
		AutoIncrCol("ID"),
	}
	for _, c := range cols {
		allCols = append(allCols, c.Immutable())
	}
	allCols = append(allCols,
		Col("CreatedAt", TypeTimestamp()).NotNull().
			DefaultFn(func(d chuck.Dialect) string { return d.Now() }).Immutable(),
	)
	return NewTable(name).
		Columns(allCols...).
		Indexes(
			Index(fmt.Sprintf("idx_%s_createdat", lower), "CreatedAt"),
		)
}
