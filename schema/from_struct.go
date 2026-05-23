package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// FromStruct derives a simple TableDef from a Go struct using `chuck` field
// tags. It is an intentionally bounded convenience helper for small,
// feature-owned tables (caches, lookup tables, session/settings rows); the
// explicit NewTable / Col DSL remains the primary, recommended schema path
// for real application schema with indexes, composite uniques, traits,
// seeds, or schema qualification.
//
// Supported tag tokens (comma-separated inside `chuck:"..."`):
//
//	-               skip the field
//	name=<col>      override the column name; a single bare unknown token
//	                is also accepted as the column name
//	pk              mark column as PRIMARY KEY
//	auto            mark column as auto-increment; requires pk and an
//	                integer-kind field
//	unique          add UNIQUE constraint (rejected together with pk)
//	notnull         force NOT NULL
//	null            force nullable
//	size=<n>        VARCHAR(n) on string fields
//	default=<expr>  literal DEFAULT expression (caller owns quoting)
//
// Inferred column types:
//
//	bool                              -> TypeBool
//	int / int8 / int16 / int32        -> TypeInt
//	uint8 / uint16                    -> TypeInt
//	int64 / uint / uint32 / uint64    -> TypeBigInt
//	float32 / float64                 -> TypeFloat
//	string (no size)                  -> TypeString(255)
//	string (size=N)                   -> TypeVarchar(N)
//	time.Time                         -> TypeTimestamp
//
// Pointer fields wrap the same inferred type and default to nullable.
// Non-pointer fields default to NOT NULL. `notnull` and `null` override
// either default. Unexported fields are skipped silently; anonymous /
// embedded fields are rejected.
//
// Unsupported field kinds, conflicting tag combinations, and malformed
// tokens fail loud via panic so misuse surfaces at table-declaration time
// (typically a package-level `var`). The returned *TableDef is a normal
// table definition; callers compose Indexes, WithSchema, traits, and the
// rest of the DSL on top of it as usual.
func FromStruct[T any](name string) *TableDef {
	rt := reflect.TypeFor[T]()
	def, err := tableDefFromType(rt, name)
	if err != nil {
		panic(err)
	}
	return def
}

func tableDefFromType(rt reflect.Type, name string) (*TableDef, error) {
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("schema.FromStruct[%s]: type must be a struct, got %s", rt.String(), rt.Kind())
	}
	if name == "" {
		return nil, fmt.Errorf("schema.FromStruct[%s]: table name must be non-empty", rt.String())
	}

	td := NewTable(name)
	for f := range rt.Fields() {
		if !f.IsExported() {
			continue
		}
		if f.Anonymous {
			return nil, fmt.Errorf("schema.FromStruct[%s]: anonymous/embedded field %q is not supported", rt.String(), f.Name)
		}
		tag, ok := f.Tag.Lookup("chuck")
		if ok && strings.TrimSpace(tag) == "-" {
			continue
		}
		col, err := columnFromField(f, tag)
		if err != nil {
			return nil, fmt.Errorf("schema.FromStruct[%s]: field %q: %w", rt.String(), f.Name, err)
		}
		td.cols = append(td.cols, col)
	}
	if len(td.cols) == 0 {
		return nil, fmt.Errorf("schema.FromStruct[%s]: no exported fields produced columns", rt.String())
	}
	return td, nil
}

type fieldTag struct {
	colName    string
	pk         bool
	auto       bool
	unique     bool
	notNull    bool
	nullable   bool
	size       int
	hasSize    bool
	defaultVal string
	hasDefault bool
}

func parseFieldTag(raw string) (fieldTag, error) {
	var ft fieldTag
	if strings.TrimSpace(raw) == "" {
		return ft, nil
	}
	nameFromBare := ""
	nameFromKV := ""
	for tok := range strings.SplitSeq(raw, ",") {
		t := strings.TrimSpace(tok)
		if t == "" {
			continue
		}
		if k, v, ok := strings.Cut(t, "="); ok {
			key := strings.TrimSpace(k)
			val := strings.TrimSpace(v)
			switch key {
			case "name":
				if val == "" {
					return ft, fmt.Errorf("tag name= requires a value")
				}
				if nameFromKV != "" {
					return ft, fmt.Errorf("tag name= specified more than once")
				}
				nameFromKV = val
			case "size":
				n, err := strconv.Atoi(val)
				if err != nil || n <= 0 {
					return ft, fmt.Errorf("tag size=%q must be a positive integer", val)
				}
				if ft.hasSize {
					return ft, fmt.Errorf("tag size= specified more than once")
				}
				ft.size = n
				ft.hasSize = true
			case "default":
				if val == "" {
					return ft, fmt.Errorf("tag default= requires a value")
				}
				if ft.hasDefault {
					return ft, fmt.Errorf("tag default= specified more than once")
				}
				ft.defaultVal = val
				ft.hasDefault = true
			default:
				return ft, fmt.Errorf("unknown tag key %q", key)
			}
			continue
		}
		switch t {
		case "pk":
			ft.pk = true
		case "auto":
			ft.auto = true
		case "unique":
			ft.unique = true
		case "notnull":
			ft.notNull = true
		case "null":
			ft.nullable = true
		default:
			if nameFromBare != "" {
				return ft, fmt.Errorf("multiple bare tokens %q and %q; use name= to override the column name", nameFromBare, t)
			}
			nameFromBare = t
		}
	}
	if nameFromKV != "" && nameFromBare != "" {
		return ft, fmt.Errorf("both name= and bare column-name token %q specified", nameFromBare)
	}
	if nameFromKV != "" {
		ft.colName = nameFromKV
	} else {
		ft.colName = nameFromBare
	}
	return ft, nil
}

var timeType = reflect.TypeFor[time.Time]()

func columnFromField(f reflect.StructField, tag string) (ColumnDef, error) {
	ft, err := parseFieldTag(tag)
	if err != nil {
		return ColumnDef{}, err
	}
	if ft.notNull && ft.nullable {
		return ColumnDef{}, fmt.Errorf("notnull and null are mutually exclusive")
	}
	if ft.auto && !ft.pk {
		return ColumnDef{}, fmt.Errorf("auto requires pk")
	}
	if ft.unique && ft.pk {
		return ColumnDef{}, fmt.Errorf("unique is redundant with pk; choose one")
	}
	if ft.pk && ft.nullable {
		return ColumnDef{}, fmt.Errorf("pk columns cannot be nullable")
	}

	colName := ft.colName
	if colName == "" {
		colName = f.Name
	}

	isPointer := f.Type.Kind() == reflect.Pointer
	elem := f.Type
	if isPointer {
		elem = f.Type.Elem()
	}

	if ft.hasSize && !(elem.Kind() == reflect.String) {
		return ColumnDef{}, fmt.Errorf("size= is only valid for string fields")
	}

	if ft.auto {
		if !isIntegerKind(elem.Kind()) {
			return ColumnDef{}, fmt.Errorf("auto requires an integer-kind field, got %s", elem.Kind())
		}
		if isPointer {
			return ColumnDef{}, fmt.Errorf("auto fields cannot be pointer-typed")
		}
		if ft.hasDefault {
			return ColumnDef{}, fmt.Errorf("auto fields cannot also declare default=")
		}
		return AutoIncrCol(colName), nil
	}

	typeFn, err := inferTypeFunc(elem, ft)
	if err != nil {
		return ColumnDef{}, err
	}

	col := Col(colName, typeFn)
	notNull := !isPointer
	if ft.notNull {
		notNull = true
	}
	if ft.nullable {
		notNull = false
	}
	if ft.pk {
		col = col.PrimaryKey()
		notNull = true
	}
	if notNull {
		col = col.NotNull()
	}
	if ft.unique {
		col = col.Unique()
	}
	if ft.hasDefault {
		col = col.Default(ft.defaultVal)
	}
	return col, nil
}

func inferTypeFunc(elem reflect.Type, ft fieldTag) (TypeFunc, error) {
	if elem == timeType {
		return TypeTimestamp(), nil
	}
	switch elem.Kind() {
	case reflect.Bool:
		return TypeBool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Uint8, reflect.Uint16:
		return TypeInt(), nil
	case reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
		return TypeBigInt(), nil
	case reflect.Float32, reflect.Float64:
		return TypeFloat(), nil
	case reflect.String:
		if ft.hasSize {
			return TypeVarchar(ft.size), nil
		}
		return TypeString(255), nil
	}
	return nil, fmt.Errorf("unsupported field type %s", elem.String())
}

func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}
