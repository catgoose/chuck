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

// GetNow returns NowFunc's time as UTC. Every Chuck-managed timestamp is a UTC
// instant, so Go-written values share a scale with the dialect's generated
// current-time defaults.
func GetNow() time.Time {
	return NowFunc().UTC()
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

// SetDeleteActor sets DeletedAt to the current time and writes the actor
// into deletedBy for a soft-delete with delete-actor attribution. T is the
// caller's actor-identity type (string, int64, uuid.UUID, etc). Both
// pointers are nil-safe and skipped when nil. Pairs with the schema-side
// WithSoftDelete() + WithDeleteActor(...) trait composition.
func SetDeleteActor[T any](deletedAt *time.Time, deletedBy *T, actor T) {
	SetSoftDelete(deletedAt)
	if deletedBy != nil {
		*deletedBy = actor
	}
}

// SetCreateActor writes the actor into createdBy and updatedBy for a new
// record. T is the caller's actor-identity type (string, int64, uuid.UUID,
// etc). Each pointer is nil-safe and skipped when nil. Pairs with the
// schema-side WithAuditActors(...) trait.
func SetCreateActor[T any](createdBy, updatedBy *T, actor T) {
	if createdBy != nil {
		*createdBy = actor
	}
	if updatedBy != nil {
		*updatedBy = actor
	}
}

// SetUpdateActor writes the actor into updatedBy for an updated record.
// T is the caller's actor-identity type. The pointer is nil-safe and
// skipped when nil. Pairs with the schema-side WithAuditActors(...) trait.
func SetUpdateActor[T any](updatedBy *T, actor T) {
	if updatedBy != nil {
		*updatedBy = actor
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

// SetExpiry sets ExpiresAt to the given time, normalized to UTC.
func SetExpiry(expiresAt *time.Time, t time.Time) {
	if expiresAt != nil {
		*expiresAt = t.UTC()
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
