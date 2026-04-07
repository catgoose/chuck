package schema

import (
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// TypeFunc resolves a column type for a given dialect.
type TypeFunc func(chuck.Dialect) string

// TypeString returns a TypeFunc for a string column with the given max length.
func TypeString(n int) TypeFunc {
	return func(d chuck.Dialect) string { return d.StringType(n) }
}

// TypeVarchar returns a TypeFunc for a varchar column with the given max length.
func TypeVarchar(n int) TypeFunc {
	return func(d chuck.Dialect) string { return d.VarcharType(n) }
}

// TypeTimestamp returns a TypeFunc for a timestamp column.
func TypeTimestamp() TypeFunc {
	return func(d chuck.Dialect) string { return d.TimestampType() }
}

// TypeAutoIncrement returns a TypeFunc for an auto-incrementing primary key column.
func TypeAutoIncrement() TypeFunc {
	return func(d chuck.Dialect) string { return d.AutoIncrement() }
}

// TypeInt returns a TypeFunc for an integer column.
func TypeInt() TypeFunc {
	return func(d chuck.Dialect) string { return d.IntType() }
}

// TypeBigInt returns a TypeFunc for a 64-bit integer column.
func TypeBigInt() TypeFunc {
	return func(d chuck.Dialect) string { return d.BigIntType() }
}

// TypeFloat returns a TypeFunc for a floating-point column.
func TypeFloat() TypeFunc {
	return func(d chuck.Dialect) string { return d.FloatType() }
}

// TypeDecimal returns a TypeFunc for an exact numeric column with precision and scale.
func TypeDecimal(precision, scale int) TypeFunc {
	return func(d chuck.Dialect) string { return d.DecimalType(precision, scale) }
}

// TypeText returns a TypeFunc for an unlimited text column.
func TypeText() TypeFunc {
	return func(d chuck.Dialect) string { return d.TextType() }
}

// TypeBool returns a TypeFunc for a boolean column.
func TypeBool() TypeFunc {
	return func(d chuck.Dialect) string { return d.BoolType() }
}

// TypeUUID returns a TypeFunc for a UUID column.
func TypeUUID() TypeFunc {
	return func(d chuck.Dialect) string { return d.UUIDType() }
}

// TypeJSON returns a TypeFunc for a JSON column.
func TypeJSON() TypeFunc {
	return func(d chuck.Dialect) string { return d.JSONType() }
}

// TypeLiteral returns a TypeFunc that always returns the given literal string.
func TypeLiteral(s string) TypeFunc {
	return func(_ chuck.Dialect) string { return s }
}

// ColumnDef defines a single table column.
type ColumnDef struct {
	name       string
	typeFn     TypeFunc
	notNull    bool
	unique     bool
	pk         bool
	autoIncr   bool
	mutable    bool
	defaultVal string
	defaultFn  func(chuck.Dialect) string
	refTable   string
	refColumn  string
	onDelete   string
	onUpdate   string
	checkExpr  string
}

// Col creates a new column definition. By default columns are nullable and mutable.
func Col(name string, typeFn TypeFunc) ColumnDef {
	return ColumnDef{name: name, typeFn: typeFn, mutable: true}
}

// AutoIncrCol creates an auto-incrementing primary key column (immutable).
func AutoIncrCol(name string) ColumnDef {
	return ColumnDef{
		name:     name,
		typeFn:   TypeAutoIncrement(),
		pk:       true,
		autoIncr: true,
		mutable:  false,
	}
}

// TypeUUIDPK returns a TypeFunc for a UUID primary key column.
// This embeds PRIMARY KEY in the type definition, similar to TypeAutoIncrement.
func TypeUUIDPK() TypeFunc {
	return func(d chuck.Dialect) string {
		if d.Engine() == chuck.Postgres {
			return "UUID PRIMARY KEY DEFAULT gen_random_uuid()"
		}
		return d.UUIDType() + " PRIMARY KEY"
	}
}

// UUIDPKCol creates a UUID primary key column (immutable).
// On Postgres this generates: id UUID PRIMARY KEY DEFAULT gen_random_uuid()
// On other engines it uses the engine's UUID type with PRIMARY KEY.
func UUIDPKCol(name string) ColumnDef {
	return ColumnDef{
		name:    name,
		typeFn:  TypeUUIDPK(),
		pk:      true,
		mutable: false,
	}
}

// NotNull marks the column as NOT NULL.
func (c ColumnDef) NotNull() ColumnDef { c.notNull = true; return c }

// Unique marks the column with a UNIQUE constraint.
func (c ColumnDef) Unique() ColumnDef { c.unique = true; return c }

// PrimaryKey marks the column as a primary key, adding PRIMARY KEY to the generated DDL.
func (c ColumnDef) PrimaryKey() ColumnDef { c.pk = true; return c }

// Default sets a literal DEFAULT expression (e.g. "'active'").
func (c ColumnDef) Default(expr string) ColumnDef { c.defaultVal = expr; return c }

// DefaultFn sets a dialect-aware DEFAULT expression (e.g. d.Now()).
func (c ColumnDef) DefaultFn(fn func(chuck.Dialect) string) ColumnDef { c.defaultFn = fn; return c }

// Immutable marks the column as immutable (excluded from UPDATE column lists).
func (c ColumnDef) Immutable() ColumnDef { c.mutable = false; return c }

// Mutable marks the column as mutable (included in UPDATE column lists).
func (c ColumnDef) Mutable() ColumnDef { c.mutable = true; return c }

// References adds a foreign key reference to another table's column.
func (c ColumnDef) References(table, column string) ColumnDef {
	c.refTable = table
	c.refColumn = column
	return c
}

// OnDelete sets the referential action for DELETE (e.g. "CASCADE", "SET NULL").
func (c ColumnDef) OnDelete(action string) ColumnDef { c.onDelete = action; return c }

// OnUpdate sets the referential action for UPDATE (e.g. "CASCADE", "SET NULL").
func (c ColumnDef) OnUpdate(action string) ColumnDef { c.onUpdate = action; return c }

// Check adds a CHECK constraint with the given SQL expression (e.g. "age >= 0").
func (c ColumnDef) Check(expr string) ColumnDef { c.checkExpr = expr; return c }

// Name returns the column name.
func (c ColumnDef) Name() string { return c.name }

// ddl renders one DDL column line for the given dialect.
func (c ColumnDef) ddl(d chuck.Dialect) string {
	var parts []string

	parts = append(parts, d.QuoteIdentifier(d.NormalizeIdentifier(c.name)))

	typeStr := c.typeFn(d)
	parts = append(parts, typeStr)

	if c.pk && !strings.Contains(typeStr, "PRIMARY KEY") {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.notNull {
		parts = append(parts, "NOT NULL")
	}
	if c.unique {
		parts = append(parts, "UNIQUE")
	}

	if c.defaultFn != nil {
		if v := c.defaultFn(d); v != "" {
			parts = append(parts, fmt.Sprintf("DEFAULT %s", v))
		}
	} else if c.defaultVal != "" {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", c.defaultVal))
	}

	if c.refTable != "" && c.refColumn != "" {
		ref := fmt.Sprintf("REFERENCES %s(%s)",
			d.QuoteIdentifier(d.NormalizeIdentifier(c.refTable)),
			d.QuoteIdentifier(d.NormalizeIdentifier(c.refColumn)))
		if c.onDelete != "" {
			ref += " ON DELETE " + c.onDelete
		}
		if c.onUpdate != "" {
			ref += " ON UPDATE " + c.onUpdate
		}
		parts = append(parts, ref)
	}

	if c.checkExpr != "" {
		parts = append(parts, fmt.Sprintf("CHECK (%s)", c.checkExpr))
	}

	return strings.Join(parts, " ")
}
