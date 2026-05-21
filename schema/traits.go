package schema

import "github.com/catgoose/chuck"

// TimestampColumnDefs returns CreatedAt and UpdatedAt column definitions.
func TimestampColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("CreatedAt", TypeTimestamp()).NotNull().
			DefaultFn(func(d chuck.Dialect) string { return d.Now() }).Immutable(),
		Col("UpdatedAt", TypeTimestamp()).NotNull().
			DefaultFn(func(d chuck.Dialect) string { return d.Now() }),
	}
}

// SoftDeleteColumnDefs returns a nullable DeletedAt timestamp column.
func SoftDeleteColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("DeletedAt", TypeTimestamp()),
	}
}

// AuditTrailSpec carries the explicit ColumnDefs the audit trail trait
// appends to a TableDef. Each field is the caller's own ColumnDef, including
// type, nullability, default, and mutability — chuck does not silently
// override any of them. Callers that want the historical string-based shape
// should use DefaultStringAuditTrail; integer/UUID/FK-style actor columns
// just pass a ColumnDef built with the caller's chosen type and constraints.
//
// The created-by column is intentionally not forced immutable here; the
// caller's ColumnDef wins. Presets that want the historical "created-by is
// frozen at insert time" behavior should call .Immutable() themselves
// (DefaultStringAuditTrail does).
type AuditTrailSpec struct {
	CreatedBy ColumnDef
	UpdatedBy ColumnDef
	DeletedBy ColumnDef
}

// AuditColumnDefs returns the CreatedBy / UpdatedBy / DeletedBy ColumnDefs
// from the spec, in the order the audit trait appends them to a TableDef.
func AuditColumnDefs(spec AuditTrailSpec) []ColumnDef {
	return []ColumnDef{spec.CreatedBy, spec.UpdatedBy, spec.DeletedBy}
}

// DefaultStringAuditTrail returns the historical chuck audit shape: three
// VARCHAR(255) actor columns named CreatedBy / UpdatedBy / DeletedBy, with
// CreatedBy marked Immutable() so it is frozen at insert time and UpdatedBy /
// DeletedBy left mutable. Use this when the caller stores actor identity as
// a free-form string (username, email, opaque id) and wants the original
// chuck defaults. Callers wanting typed actor identity (e.g. UserID INT
// referencing Users) should build their own AuditTrailSpec.
func DefaultStringAuditTrail() AuditTrailSpec {
	return AuditTrailSpec{
		CreatedBy: Col("CreatedBy", TypeString(255)).Immutable(),
		UpdatedBy: Col("UpdatedBy", TypeString(255)),
		DeletedBy: Col("DeletedBy", TypeString(255)),
	}
}

// VersionColumnDefs returns a Version column for optimistic concurrency control.
func VersionColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("Version", TypeInt()).NotNull().Default("1"),
	}
}

// SortOrderColumnDefs returns a SortOrder column for manual ordering.
func SortOrderColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("SortOrder", TypeInt()).NotNull().Default("0"),
	}
}

// StatusColumnDefs returns a Status column with a default value.
func StatusColumnDefs(defaultStatus string) []ColumnDef {
	return []ColumnDef{
		Col("Status", TypeVarchar(50)).NotNull().Default("'" + defaultStatus + "'"),
	}
}

// NotesColumnDefs returns a nullable Notes text column.
func NotesColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("Notes", TypeText()),
	}
}

// UUIDColumnDefs returns a UUID column (NOT NULL, UNIQUE).
func UUIDColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("UUID", TypeVarchar(36)).NotNull().Unique().Immutable(),
	}
}

// ParentColumnDefs returns a nullable ParentID column for tree structures.
func ParentColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("ParentID", TypeInt()),
	}
}

// ExpiryColumnDefs returns a nullable ExpiresAt timestamp column.
func ExpiryColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("ExpiresAt", TypeTimestamp()),
	}
}

// ReplacementColumnDefs returns a nullable ReplacedByID column for entity lineage tracking.
func ReplacementColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("ReplacedByID", TypeInt()),
	}
}

// ArchiveColumnDefs returns a nullable ArchivedAt timestamp column.
func ArchiveColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("ArchivedAt", TypeTimestamp()),
	}
}
