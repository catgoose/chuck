package schema

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// ColumnSnapshot describes a single column's resolved schema for a given dialect.
type ColumnSnapshot struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"not_null,omitempty"`
	Unique     bool   `json:"unique,omitempty"`
	PrimaryKey bool   `json:"primary_key,omitempty"`
	AutoIncr   bool   `json:"auto_increment,omitempty"`
	Mutable    bool   `json:"mutable"`
	Default    string `json:"default,omitempty"`
	RefTable   string `json:"references_table,omitempty"`
	RefColumn  string `json:"references_column,omitempty"`
	OnDelete   string `json:"on_delete,omitempty"`
	OnUpdate   string `json:"on_update,omitempty"`
	Check      string `json:"check,omitempty"`
}

// IndexSnapshot describes a single index.
type IndexSnapshot struct {
	Name    string `json:"name"`
	Columns string `json:"columns"`
	Unique  bool   `json:"unique,omitempty"`
	Where   string `json:"where,omitempty"`
}

// TableSnapshot describes a table's full resolved schema for a given dialect.
type TableSnapshot struct {
	Name              string             `json:"name"`
	Columns           []ColumnSnapshot   `json:"columns"`
	Indexes           []IndexSnapshot    `json:"indexes,omitempty"`
	UniqueConstraints [][]string         `json:"unique_constraints,omitempty"`
	HasSoftDelete     bool               `json:"has_soft_delete,omitempty"`
	HasVersion        bool               `json:"has_version,omitempty"`
	HasExpiry         bool               `json:"has_expiry,omitempty"`
	HasArchive        bool               `json:"has_archive,omitempty"`
}

// Snapshot returns a structured, dialect-resolved representation of the table schema.
// The output is useful for diffing against a live database or serializing to JSON
// for CI validation.
func (t *TableDef) Snapshot(d chuck.Dialect) TableSnapshot {
	snap := TableSnapshot{
		Name:          d.NormalizeIdentifier(t.Name),
		HasSoftDelete: t.hasSoftDelete,
		HasVersion:    t.hasVersion,
		HasExpiry:     t.hasExpiry,
		HasArchive:    t.hasArchive,
	}

	for _, c := range t.cols {
		cs := ColumnSnapshot{
			Name:       d.NormalizeIdentifier(c.name),
			Type:       c.typeFn(d),
			NotNull:    c.notNull || c.pk,
			Unique:     c.unique,
			PrimaryKey: c.pk,
			AutoIncr:   c.autoIncr,
			Mutable:    c.mutable,
		}
		if c.defaultFn != nil {
			cs.Default = c.defaultFn(d)
		} else if c.defaultVal != "" {
			cs.Default = c.defaultVal
		}
		if c.refTable != "" {
			cs.RefTable = d.NormalizeIdentifier(c.refTable)
			cs.RefColumn = d.NormalizeIdentifier(c.refColumn)
			cs.OnDelete = c.onDelete
			cs.OnUpdate = c.onUpdate
		}
		cs.Check = c.checkExpr
		snap.Columns = append(snap.Columns, cs)
	}

	for _, idx := range t.indexes {
		snap.Indexes = append(snap.Indexes, IndexSnapshot{
			Name:    idx.name,
			Columns: idx.columns,
			Unique:  idx.unique,
			Where:   idx.where,
		})
	}

	for _, uc := range t.uniqueConstraints {
		norm := make([]string, len(uc.columns))
		for i, col := range uc.columns {
			norm[i] = d.NormalizeIdentifier(col)
		}
		snap.UniqueConstraints = append(snap.UniqueConstraints, norm)
	}

	return snap
}

// SnapshotString returns a human-readable, diff-friendly text representation
// of the table schema resolved for the given dialect. The format is designed
// for side-by-side comparison with a live database schema.
func (t *TableDef) SnapshotString(d chuck.Dialect) string {
	snap := t.Snapshot(d)
	var b strings.Builder

	fmt.Fprintf(&b, "TABLE %s\n", snap.Name)

	for _, c := range snap.Columns {
		var parts []string
		parts = append(parts, c.Type)
		upperType := strings.ToUpper(c.Type)
		if c.PrimaryKey && !strings.Contains(upperType, "PRIMARY KEY") {
			parts = append(parts, "PRIMARY KEY")
		}
		if c.AutoIncr && !strings.Contains(upperType, "AUTOINCREMENT") && !strings.Contains(upperType, "AUTO_INCREMENT") && !strings.Contains(upperType, "SERIAL") {
			parts = append(parts, "AUTO INCREMENT")
		}
		if c.NotNull {
			parts = append(parts, "NOT NULL")
		}
		if c.Unique {
			parts = append(parts, "UNIQUE")
		}
		if c.Default != "" {
			parts = append(parts, "DEFAULT "+c.Default)
		}
		if c.RefTable != "" {
			ref := fmt.Sprintf("REFERENCES %s(%s)", c.RefTable, c.RefColumn)
			if c.OnDelete != "" {
				ref += " ON DELETE " + c.OnDelete
			}
			if c.OnUpdate != "" {
				ref += " ON UPDATE " + c.OnUpdate
			}
			parts = append(parts, ref)
		}
		if c.Check != "" {
			parts = append(parts, fmt.Sprintf("CHECK (%s)", c.Check))
		}
		if !c.Mutable {
			parts = append(parts, "[immutable]")
		}
		fmt.Fprintf(&b, "  %-20s %s\n", c.Name, strings.Join(parts, " "))
	}

	for _, idx := range snap.Indexes {
		prefix := "INDEX"
		if idx.Unique {
			prefix = "UNIQUE INDEX"
		}
		if idx.Where != "" {
			fmt.Fprintf(&b, "  %s %s ON (%s) WHERE %s\n", prefix, idx.Name, idx.Columns, idx.Where)
		} else {
			fmt.Fprintf(&b, "  %s %s ON (%s)\n", prefix, idx.Name, idx.Columns)
		}
	}

	for _, uc := range snap.UniqueConstraints {
		fmt.Fprintf(&b, "  UNIQUE (%s)\n", strings.Join(uc, ", "))
	}

	return b.String()
}

// SchemaSnapshot returns snapshots for multiple tables, useful for
// dumping the entire declared schema for comparison or CI validation.
func SchemaSnapshot(d chuck.Dialect, tables ...*TableDef) []TableSnapshot {
	snaps := make([]TableSnapshot, len(tables))
	for i, t := range tables {
		snaps[i] = t.Snapshot(d)
	}
	return snaps
}

// SchemaSnapshotString returns a human-readable snapshot of multiple tables.
func SchemaSnapshotString(d chuck.Dialect, tables ...*TableDef) string {
	var b strings.Builder
	for i, t := range tables {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(t.SnapshotString(d))
	}
	return b.String()
}
