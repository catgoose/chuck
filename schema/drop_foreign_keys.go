package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/catgoose/chuck"
)

// ForeignKeyRef identifies a single inbound foreign-key constraint discovered
// during destructive bootstrap. Parent is the table that defines the FK column
// (the child side); Referenced is the table the FK points at. Both endpoints
// carry their schema explicitly so callers do not need to re-resolve defaults.
type ForeignKeyRef struct {
	Name             string
	ParentSchema     string
	ParentTable      string
	ReferencedSchema string
	ReferencedTable  string
}

// InboundForeignKeys returns the foreign-key constraints currently defined in
// the live database whose parent (FK-defining) or referenced table belongs to
// the owned set of declared tables. The returned slice can be fed into
// DropForeignKeySQL to produce the ALTER TABLE ... DROP CONSTRAINT statements
// needed before a DropOrder-driven destructive teardown on engines where
// existing FKs would block DROP TABLE.
//
// Owned-set membership is derived from the supplied *TableDef set rather than
// a parallel handwritten table list: every declared schema-qualified name (or
// the engine default schema for unqualified declarations) participates.
//
// MSSQL is the primary target: inline FKs declared by chuck/schema emit
// auto-generated constraint names (e.g. FK__Goals__AgentID__1234ABCD) that
// must be dropped by name before DROP TABLE will succeed. SQLite has no
// schema namespace and lets DROP TABLE proceed regardless of inbound FKs;
// Postgres callers should generally prefer DROP TABLE ... CASCADE. For both
// non-MSSQL engines this helper returns (nil, nil) so it can be called
// unconditionally from dialect-agnostic bootstrap code.
func InboundForeignKeys(ctx context.Context, db *sql.DB, d chuck.Dialect, tables ...*TableDef) ([]ForeignKeyRef, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	if d.Engine() != chuck.MSSQL {
		return nil, nil
	}
	return inboundForeignKeysMSSQL(ctx, db, d, tables)
}

// DropForeignKeySQL returns the ALTER TABLE ... DROP CONSTRAINT statement that
// removes the given foreign key on its parent table. The parent identifier is
// rendered fully qualified and quoted via the dialect.
func DropForeignKeySQL(d chuck.Dialect, fk ForeignKeyRef) string {
	parent := chuck.QualifyTable(d, chuck.ObjectName{Schema: fk.ParentSchema, Name: fk.ParentTable})
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s",
		parent,
		d.QuoteIdentifier(d.NormalizeIdentifier(fk.Name)),
	)
}

// DropInboundForeignKeys executes DROP CONSTRAINT for every inbound FK
// returned by InboundForeignKeys, in the order they appear. It is intended to
// run immediately before DropOrder-driven table teardown on MSSQL. The
// returned slice records the constraints that were dropped (in execution
// order) so callers can log or diff against the next bootstrap pass.
func DropInboundForeignKeys(ctx context.Context, db *sql.DB, d chuck.Dialect, tables ...*TableDef) ([]ForeignKeyRef, error) {
	fks, err := InboundForeignKeys(ctx, db, d, tables...)
	if err != nil {
		return nil, err
	}
	for _, fk := range fks {
		stmt := DropForeignKeySQL(d, fk)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			parent := displayQualifiedName(d, chuck.ObjectName{Schema: fk.ParentSchema, Name: fk.ParentTable})
			return nil, fmt.Errorf("drop foreign key %q on %s: %w", fk.Name, parent, err)
		}
	}
	return fks, nil
}

func inboundForeignKeysMSSQL(ctx context.Context, db *sql.DB, d chuck.Dialect, tables []*TableDef) ([]ForeignKeyRef, error) {
	owned := ownedTableKeySet(d, tables, "dbo")
	if len(owned) == 0 {
		return nil, nil
	}

	const q = `SELECT fk.name AS constraint_name, ` +
		`SCHEMA_NAME(parent.schema_id) AS parent_schema, ` +
		`parent.name AS parent_table, ` +
		`SCHEMA_NAME(ref.schema_id) AS referenced_schema, ` +
		`ref.name AS referenced_table ` +
		`FROM sys.foreign_keys AS fk ` +
		`JOIN sys.objects AS parent ON parent.object_id = fk.parent_object_id ` +
		`JOIN sys.objects AS ref ON ref.object_id = fk.referenced_object_id ` +
		`ORDER BY parent_schema, parent.name, fk.name`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query sys.foreign_keys: %w", err)
	}
	defer rows.Close()

	var out []ForeignKeyRef
	for rows.Next() {
		var fk ForeignKeyRef
		if err := rows.Scan(&fk.Name, &fk.ParentSchema, &fk.ParentTable, &fk.ReferencedSchema, &fk.ReferencedTable); err != nil {
			return nil, fmt.Errorf("scan sys.foreign_keys row: %w", err)
		}
		if !ownedFK(owned, fk) {
			continue
		}
		out = append(out, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sys.foreign_keys: %w", err)
	}
	return out, nil
}

// ownedTableKeySet returns the set of "schema.name" keys for the owned tables.
// Unqualified declarations are assigned defaultSchema (the engine default,
// e.g. "dbo" on MSSQL) so they match what live introspection returns. Names
// are normalized through the dialect to align with the engine's stored form.
func ownedTableKeySet(d chuck.Dialect, tables []*TableDef, defaultSchema string) map[string]struct{} {
	owned := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		schema := t.schema
		if schema == "" {
			schema = defaultSchema
		}
		key := d.NormalizeIdentifier(schema) + "." + d.NormalizeIdentifier(t.Name)
		owned[key] = struct{}{}
	}
	return owned
}

// ownedFK reports whether either endpoint of fk lies within the owned set.
// A match on the parent side means the owned table carries the FK column;
// a match on the referenced side means an outside table points at our owned
// table and must be detached before our table can be dropped.
func ownedFK(owned map[string]struct{}, fk ForeignKeyRef) bool {
	if _, ok := owned[fk.ParentSchema+"."+fk.ParentTable]; ok {
		return true
	}
	_, ok := owned[fk.ReferencedSchema+"."+fk.ReferencedTable]
	return ok
}
