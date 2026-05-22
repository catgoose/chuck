package dbrepo

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetCreateTimestamps(t *testing.T) {
	var created, updated time.Time
	SetCreateTimestamps(&created, &updated)
	assert.False(t, created.IsZero())
	assert.False(t, updated.IsZero())
	assert.Equal(t, created, updated)
}

func TestSetUpdateTimestamp(t *testing.T) {
	var updated time.Time
	SetUpdateTimestamp(&updated)
	assert.False(t, updated.IsZero())
}

func TestSetSoftDelete(t *testing.T) {
	var deleted time.Time
	SetSoftDelete(&deleted)
	assert.False(t, deleted.IsZero())
}

func TestInitVersion(t *testing.T) {
	var v int
	InitVersion(&v)
	assert.Equal(t, 1, v)
}

func TestIncrementVersion(t *testing.T) {
	v := 1
	IncrementVersion(&v)
	assert.Equal(t, 2, v)
}

func TestSetStatus(t *testing.T) {
	var s string
	SetStatus(&s, "active")
	assert.Equal(t, "active", s)
}

func TestSetSortOrder(t *testing.T) {
	var o int
	SetSortOrder(&o, 5)
	assert.Equal(t, 5, o)
}

func TestActorHelpers_StringActor(t *testing.T) {
	var createdBy, updatedBy string
	SetCreateActor(&createdBy, &updatedBy, "admin")
	assert.Equal(t, "admin", createdBy)
	assert.Equal(t, "admin", updatedBy)

	SetUpdateActor(&updatedBy, "user1")
	assert.Equal(t, "user1", updatedBy)
}

func TestActorHelpers_TypedActor_Int64(t *testing.T) {
	var createdBy, updatedBy int64
	SetCreateActor(&createdBy, &updatedBy, int64(42))
	assert.Equal(t, int64(42), createdBy)
	assert.Equal(t, int64(42), updatedBy)

	SetUpdateActor(&updatedBy, int64(7))
	assert.Equal(t, int64(7), updatedBy)
}

func TestDeleteActor_StringActor(t *testing.T) {
	var deletedAt time.Time
	var deletedBy string
	SetDeleteActor(&deletedAt, &deletedBy, "admin")
	assert.False(t, deletedAt.IsZero())
	assert.Equal(t, "admin", deletedBy)
}

func TestDeleteActor_TypedActor_Int64(t *testing.T) {
	var deletedAt time.Time
	var deletedBy int64
	SetDeleteActor(&deletedAt, &deletedBy, int64(99))
	assert.False(t, deletedAt.IsZero())
	assert.Equal(t, int64(99), deletedBy)
}

func TestArchive(t *testing.T) {
	var archivedAt time.Time
	SetArchive(&archivedAt)
	assert.False(t, archivedAt.IsZero())

	nullTime := sql.NullTime{Time: archivedAt, Valid: true}
	ClearArchive(&nullTime)
	assert.False(t, nullTime.Valid)
	assert.True(t, nullTime.Time.IsZero())
}

func TestExpiry(t *testing.T) {
	var expiresAt time.Time
	future := time.Now().Add(24 * time.Hour)
	SetExpiry(&expiresAt, future)
	assert.Equal(t, future, expiresAt)

	nullTime := sql.NullTime{Time: expiresAt, Valid: true}
	ClearExpiry(&nullTime)
	assert.False(t, nullTime.Valid)
	assert.True(t, nullTime.Time.IsZero())
}

func TestReplacement(t *testing.T) {
	var replacedBy int64
	SetReplacement(&replacedBy, 42)
	assert.Equal(t, int64(42), replacedBy)

	nullInt := sql.NullInt64{Int64: replacedBy, Valid: true}
	ClearReplacement(&nullInt)
	assert.False(t, nullInt.Valid)
	assert.Equal(t, int64(0), nullInt.Int64)
}

func TestNilSafety(t *testing.T) {
	// All helpers should be safe to call with nil pointers
	SetCreateTimestamps(nil, nil)
	SetUpdateTimestamp(nil)
	SetSoftDelete(nil)
	InitVersion(nil)
	IncrementVersion(nil)
	SetSortOrder(nil, 0)
	SetStatus(nil, "")
	SetExpiry(nil, time.Time{})
	SetReplacement(nil, 0)
	ClearReplacement(nil)
	SetArchive(nil)
	ClearArchive(nil)
	ClearExpiry(nil)
	// Verify nil safety for sql.Null* variants
	var nilNullInt64 *sql.NullInt64
	ClearReplacement(nilNullInt64)
	var nilNullTime *sql.NullTime
	ClearArchive(nilNullTime)
	ClearExpiry(nilNullTime)
	var nilString *string
	SetCreateActor(nilString, nilString, "")
	SetUpdateActor(nilString, "")
	SetDeleteActor(nil, nilString, "")
	// Typed variants must be equally nil-safe.
	var nilInt64 *int64
	SetCreateActor(nilInt64, nilInt64, int64(0))
	SetUpdateActor(nilInt64, int64(0))
	SetDeleteActor(nil, nilInt64, int64(0))
}

func TestNowFuncOverride(t *testing.T) {
	fixed := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	original := NowFunc
	NowFunc = func() time.Time { return fixed }
	defer func() { NowFunc = original }()

	assert.Equal(t, fixed, GetNow())

	var created, updated time.Time
	SetCreateTimestamps(&created, &updated)
	assert.Equal(t, fixed, created)
	assert.Equal(t, fixed, updated)
}
