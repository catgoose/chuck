package schema

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// NewQueueTable creates a job/outbox queue table with status tracking and retry support.
// Includes ID, a caller-defined payload column name, Status (default "pending"), RetryCount,
// ScheduledAt, ProcessedAt, and CreatedAt. Indexes on Status and ScheduledAt for efficient polling.
func NewQueueTable(name, payloadCol string) *TableDef {
	lower := strings.ToLower(name)
	return NewTable(name).
		Columns(
			AutoIncrCol("ID"),
			Col(payloadCol, TypeText()).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'pending'"),
			Col("RetryCount", TypeInt()).NotNull().Default("0"),
			Col("ScheduledAt", TypeTimestamp()),
			Col("ProcessedAt", TypeTimestamp()),
			Col("CreatedAt", TypeTimestamp()).NotNull().
				DefaultFn(func(d chuck.Dialect) string { return d.Now() }).Immutable(),
		).
		Indexes(
			Index(fmt.Sprintf("idx_%s_status", lower), "Status"),
			Index(fmt.Sprintf("idx_%s_scheduledat", lower), "ScheduledAt"),
			Index(fmt.Sprintf("idx_%s_status_scheduledat", lower), "Status, ScheduledAt"),
		)
}
