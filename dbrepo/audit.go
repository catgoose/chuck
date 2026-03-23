package dbrepo

import (
	"database/sql"
	"time"
)

// NowFunc is the function used to get the current time.
// Override this in tests to freeze or control time:
//
//	dbrepo.NowFunc = func() time.Time { return fixedTime }
var NowFunc = time.Now

// GetNow returns the current time via NowFunc.
func GetNow() time.Time {
	return NowFunc()
}

// SetCreateTimestamps sets CreatedAt and UpdatedAt to current time.
func SetCreateTimestamps(createdAt, updatedAt *time.Time) {
	now := GetNow()
	if createdAt != nil {
		*createdAt = now
	}
	if updatedAt != nil {
		*updatedAt = now
	}
}

// SetUpdateTimestamp sets UpdatedAt to current time.
func SetUpdateTimestamp(updatedAt *time.Time) {
	if updatedAt != nil {
		*updatedAt = GetNow()
	}
}

// SetSoftDelete sets DeletedAt to the current time for soft-delete.
func SetSoftDelete(deletedAt *time.Time) {
	if deletedAt != nil {
		*deletedAt = GetNow()
	}
}

// SetDeleteAudit sets DeletedAt and DeletedBy for a soft-delete with audit trail.
func SetDeleteAudit(deletedAt *time.Time, deletedBy *string, user string) {
	SetSoftDelete(deletedAt)
	if deletedBy != nil {
		*deletedBy = user
	}
}

// SetCreateAudit sets CreatedBy and UpdatedBy for a new record.
func SetCreateAudit(createdBy, updatedBy *string, user string) {
	if createdBy != nil {
		*createdBy = user
	}
	if updatedBy != nil {
		*updatedBy = user
	}
}

// SetUpdateAudit sets UpdatedBy for an updated record.
func SetUpdateAudit(updatedBy *string, user string) {
	if updatedBy != nil {
		*updatedBy = user
	}
}

// InitVersion sets Version to 1 for a new record.
func InitVersion(version *int) {
	if version != nil {
		*version = 1
	}
}

// IncrementVersion increments Version by 1.
func IncrementVersion(version *int) {
	if version != nil {
		*version++
	}
}

// SetSortOrder sets the SortOrder value.
func SetSortOrder(sortOrder *int, order int) {
	if sortOrder != nil {
		*sortOrder = order
	}
}

// SetStatus sets the Status value.
func SetStatus(status *string, value string) {
	if status != nil {
		*status = value
	}
}

// SetExpiry sets ExpiresAt to the given time.
func SetExpiry(expiresAt *time.Time, t time.Time) {
	if expiresAt != nil {
		*expiresAt = t
	}
}

// SetReplacement sets ReplacedByID to the given ID.
func SetReplacement(replacedByID *int64, id int64) {
	if replacedByID != nil {
		*replacedByID = id
	}
}

// ClearReplacement sets a sql.NullInt64 to NULL (Valid = false).
func ClearReplacement(replacedByID *sql.NullInt64) {
	if replacedByID != nil {
		replacedByID.Valid = false
		replacedByID.Int64 = 0
	}
}

// SetArchive sets ArchivedAt to the current time.
func SetArchive(archivedAt *time.Time) {
	if archivedAt != nil {
		*archivedAt = GetNow()
	}
}

// ClearArchive sets a sql.NullTime to NULL (Valid = false) to unarchive.
func ClearArchive(archivedAt *sql.NullTime) {
	if archivedAt != nil {
		archivedAt.Valid = false
		archivedAt.Time = time.Time{}
	}
}

// ClearExpiry sets a sql.NullTime to NULL (Valid = false) to remove expiry.
func ClearExpiry(expiresAt *sql.NullTime) {
	if expiresAt != nil {
		expiresAt.Valid = false
		expiresAt.Time = time.Time{}
	}
}
