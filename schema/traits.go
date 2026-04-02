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

// AuditColumnDefs returns CreatedBy, UpdatedBy, and DeletedBy columns.
func AuditColumnDefs() []ColumnDef {
	return []ColumnDef{
		Col("CreatedBy", TypeString(255)).Immutable(),
		Col("UpdatedBy", TypeString(255)),
		Col("DeletedBy", TypeString(255)),
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
