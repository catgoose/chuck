package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/catgoose/chuck"
)

// RetiredTableDef is a lightweight tombstone for a table that was previously
// managed but is no longer part of the current schema manifest. It carries a
// structured ObjectName identity so destructive-rebuild flows can drop the old
// table without temporarily re-registering it as a current *TableDef.
//
// RetiredTableDef intentionally has no columns, indexes, or traits: it is not
// a partial table definition. Its only contracts are dialect-aware DropSQL
// rendering and participation in DropRetiredTables, including the MSSQL
// inbound-FK detach path the destructive rebuild sequence needs.
type RetiredTableDef struct {
	name   string
	schema string
}

// RetiredTable declares a retired (unqualified) managed table for tombstone
// teardown. Use RetiredQualifiedTable when the original table lived in a
// non-default schema.
func RetiredTable(name string) *RetiredTableDef {
	return &RetiredTableDef{name: name}
}

// RetiredQualifiedTable declares a retired managed table with an explicit
// schema namespace. Equivalent to RetiredTable(name) with WithSchema applied.
func RetiredQualifiedTable(schema, name string) *RetiredTableDef {
	return &RetiredTableDef{schema: schema, name: name}
}

// Name returns the retired table's bare name.
func (r *RetiredTableDef) Name() string { return r.name }

// Schema returns the retired table's schema namespace, or "" if unqualified.
func (r *RetiredTableDef) Schema() string { return r.schema }

// Object returns the structured ObjectName for the retired table.
func (r *RetiredTableDef) Object() chuck.ObjectName {
	return chuck.ObjectName{Schema: r.schema, Name: r.name}
}

// QualifiedNameFor returns the dialect-rendered, quoted, schema-qualified
// identifier (e.g. [dbo].[GroupMembershipSources] on MSSQL,
// "public"."group_membership_sources" on Postgres). On SQLite the schema
// component is dropped because SQLite has no namespace.
func (r *RetiredTableDef) QualifiedNameFor(d chuck.Dialect) string {
	return qualifyTable(d, r.Object())
}

// DropSQL returns the dialect-rendered DROP TABLE IF EXISTS statement for the
// retired table. Same rendering path as TableDef.DropSQL, so MSSQL emits the
// `sys.objects` existence probe and Postgres/SQLite use their native
// `DROP TABLE IF EXISTS` forms.
func (r *RetiredTableDef) DropSQL(d chuck.Dialect) string {
	return dropTableIfExistsSQL(d, r.Object())
}

// DropRetiredTables drops each declared retired table tombstone in
// caller-supplied order. Duplicates (by structured object identity) are
// deduped so callers can compose tombstone lists from multiple feature
// modules without coordinating across them.
//
// On MSSQL, inbound foreign keys whose parent or referenced endpoint is in the
// retired set are detached first via ALTER TABLE ... DROP CONSTRAINT, so a
// retired table that is still referenced by a current managed table (the
// issue #111 case) can actually be dropped. The FK detach is scoped to the
// retired endpoints only — unmanaged FKs that touch no retired table are left
// alone, matching the issue's "no broad teardown of unmanaged tables"
// constraint.
//
// On SQLite and Postgres, DROP TABLE IF EXISTS is issued directly. Postgres
// callers needing CASCADE semantics should drop dependent objects themselves
// first; chuck does not synthesize CASCADE because the surprise blast radius
// would exceed this helper's explicit-list contract.
//
// Errors include the failing tombstone's qualified identity for traceability.
// Returns nil when the retired list is empty.
func DropRetiredTables(ctx context.Context, db *sql.DB, d chuck.Dialect, retired ...*RetiredTableDef) error {
	if len(retired) == 0 {
		return nil
	}
	ordered, err := dedupeRetired(retired)
	if err != nil {
		return err
	}
	if len(ordered) == 0 {
		return nil
	}
	if d.Engine() == chuck.MSSQL {
		if err := dropRetiredInboundForeignKeysMSSQL(ctx, db, d, ordered); err != nil {
			return err
		}
	}
	for _, r := range ordered {
		stmt := r.DropSQL(d)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			display := displayQualifiedName(d, r.Object())
			return fmt.Errorf("schema: drop retired table %s: %w", display, err)
		}
	}
	return nil
}

// dedupeRetired returns retired tombstones in caller-supplied order with
// duplicates removed by structured object identity. Empty names fail loud so
// misuse surfaces at declaration time rather than as a confusing live error.
func dedupeRetired(retired []*RetiredTableDef) ([]*RetiredTableDef, error) {
	seen := make(map[string]struct{}, len(retired))
	out := make([]*RetiredTableDef, 0, len(retired))
	for i, r := range retired {
		if r == nil {
			return nil, fmt.Errorf("schema: retired table at index %d is nil", i)
		}
		if r.name == "" {
			return nil, fmt.Errorf("schema: retired table at index %d has empty name", i)
		}
		k := objectKey(r.Object())
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}
	return out, nil
}

// dropRetiredInboundForeignKeysMSSQL detaches every live FK whose parent or
// referenced table is one of the supplied retired tombstones. Unqualified
// tombstones default to the `dbo` schema so they line up with what
// sys.foreign_keys reports for default-schema tables, matching the existing
// ownedTableKeySet convention.
func dropRetiredInboundForeignKeysMSSQL(ctx context.Context, db *sql.DB, d chuck.Dialect, retired []*RetiredTableDef) error {
	keys := retiredTableKeySet(d, retired, "dbo")
	if len(keys) == 0 {
		return nil
	}
	fks, err := foreignKeysMatchingKeySetMSSQL(ctx, db, keys)
	if err != nil {
		return err
	}
	for _, fk := range fks {
		stmt := DropForeignKeySQL(d, fk)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			parent := displayQualifiedName(d, chuck.ObjectName{Schema: fk.ParentSchema, Name: fk.ParentTable})
			return fmt.Errorf("schema: drop foreign key %q on %s before retired table drop: %w", fk.Name, parent, err)
		}
	}
	return nil
}

// retiredTableKeySet builds the "schema.name" key set for retired tombstones,
// mirroring ownedTableKeySet so the MSSQL FK filter logic stays uniform.
func retiredTableKeySet(d chuck.Dialect, retired []*RetiredTableDef, defaultSchema string) map[string]struct{} {
	keys := make(map[string]struct{}, len(retired))
	for _, r := range retired {
		schema := r.schema
		if schema == "" {
			schema = defaultSchema
		}
		keys[d.NormalizeIdentifier(schema)+"."+d.NormalizeIdentifier(r.name)] = struct{}{}
	}
	return keys
}
