package schema_test

import (
	"strings"
	"testing"
	"time"

	"github.com/catgoose/chuck"
	"github.com/catgoose/chuck/schema"
)

type sessionSettingsRow struct {
	ID          int       `chuck:"pk,auto"`
	SessionUUID string    `chuck:"size=36,unique,notnull"`
	Theme       string    `chuck:"size=50,notnull,default='light'"`
	Layout      string    `chuck:"size=50,notnull,default='classic'"`
	CreatedAt   time.Time `chuck:"created_at"`
	UpdatedAt   time.Time `chuck:"name=updated_at"`
	Notes       *string
	private     string //nolint:unused // exercises unexported-field skip
}

func TestFromStruct_HappyPath(t *testing.T) {
	// MSSQL renders VARCHAR(n) verbatim, so it shows the size= tag honestly.
	d, _ := chuck.New(chuck.MSSQL)
	td := schema.FromStruct[sessionSettingsRow]("SessionSettings")

	if td.Name != "SessionSettings" {
		t.Fatalf("name = %q, want SessionSettings", td.Name)
	}

	got := td.SnapshotString(d)
	wantContains := []string{
		"TABLE SessionSettings",
		"ID                   INT PRIMARY KEY IDENTITY(1,1) AUTO INCREMENT NOT NULL [immutable]",
		"SessionUUID          VARCHAR(36) NOT NULL UNIQUE",
		"Theme                VARCHAR(50) NOT NULL DEFAULT 'light'",
		"Layout               VARCHAR(50) NOT NULL DEFAULT 'classic'",
		"created_at           DATETIME NOT NULL",
		"updated_at           DATETIME NOT NULL",
		"Notes                NVARCHAR(255)", // pointer string -> nullable, no size -> TypeString
	}
	for _, want := range wantContains {
		if !strings.Contains(got, want) {
			t.Errorf("snapshot missing %q\nsnapshot:\n%s", want, got)
		}
	}

	// Update columns drop the auto-increment column.
	updates := td.UpdateColumns()
	for _, c := range updates {
		if c == "ID" {
			t.Errorf("UpdateColumns should not include auto-increment ID, got %v", updates)
		}
	}
}

type intKindsRow struct {
	Small int32 `chuck:"notnull"`
	Big   int64 `chuck:"notnull"`
	UBig  uint64
	F32   float32 `chuck:"notnull"`
	F64   float64 `chuck:"notnull"`
	Flag  bool    `chuck:"notnull,default=0"`
}

func TestFromStruct_TypeInference(t *testing.T) {
	d, _ := chuck.New(chuck.MSSQL)
	td := schema.FromStruct[intKindsRow]("Kinds")
	snap := td.SnapshotString(d)

	checks := []struct{ col, typeStr string }{
		{"Small", "INT NOT NULL"},
		{"Big", "BIGINT NOT NULL"},
		{"UBig", "BIGINT"}, // non-pointer non-tagged uint64 defaults NotNull; but verify the type
		{"F32", "FLOAT NOT NULL"},
		{"F64", "FLOAT NOT NULL"},
		{"Flag", "BIT NOT NULL DEFAULT 0"},
	}
	for _, c := range checks {
		if !strings.Contains(snap, c.col) || !strings.Contains(snap, c.typeStr) {
			t.Errorf("snapshot missing %s with %s\n%s", c.col, c.typeStr, snap)
		}
	}
}

type nullableRow struct {
	ID      int        `chuck:"pk,auto"`
	Name    *string    `chuck:"size=100"`
	When    *time.Time `chuck:""`
	Force   string     `chuck:"null"`     // non-pointer forced nullable
	Forced  *string    `chuck:"notnull"`  // pointer forced NOT NULL
	NumOpt  *int64
}

func TestFromStruct_NullabilityRules(t *testing.T) {
	d, _ := chuck.New(chuck.MSSQL)
	td := schema.FromStruct[nullableRow]("Nullable")
	snap := td.SnapshotString(d)

	mustContain := []string{
		"Name                 VARCHAR(100)\n",  // pointer string, no NotNull, size=100
		"When                 DATETIME\n",      // pointer time, nullable
		"Force                NVARCHAR(255)\n", // null tag, no NotNull
		"Forced               NVARCHAR(255) NOT NULL", // pointer string + notnull
		"NumOpt               BIGINT\n",        // pointer int64, nullable
	}
	for _, want := range mustContain {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot missing %q\n%s", want, snap)
		}
	}
}

type compositionRow struct {
	ID   int    `chuck:"pk,auto"`
	Name string `chuck:"size=255,notnull,unique"`
}

func TestFromStruct_Composition(t *testing.T) {
	d, _ := chuck.New(chuck.SQLite)
	td := schema.FromStruct[compositionRow]("Items").
		WithSchema("ops").
		Indexes(schema.Index("idx_items_name", "Name")).
		WithTimestamps().
		WithSoftDelete()

	if td.Schema() != "ops" {
		t.Errorf("Schema() = %q, want ops", td.Schema())
	}
	if !td.HasSoftDelete() {
		t.Error("HasSoftDelete() = false; trait composition broken")
	}
	cols := td.SelectColumns()
	want := []string{"ID", "Name", "CreatedAt", "UpdatedAt", "DeletedAt"}
	if len(cols) != len(want) {
		t.Fatalf("SelectColumns = %v, want %v", cols, want)
	}
	for i, c := range cols {
		if c != want[i] {
			t.Errorf("col[%d] = %q, want %q", i, c, want[i])
		}
	}

	// CREATE SQL should include the index, qualified table, and trait columns.
	stmts := td.CreateSQL(d)
	joined := strings.Join(stmts, "\n")
	for _, want := range []string{`CREATE TABLE "Items"`, "DeletedAt", "idx_items_name"} {
		if !strings.Contains(joined, want) {
			t.Errorf("CREATE SQL missing %q\n%s", want, joined)
		}
	}
}

type skipRow struct {
	ID   int    `chuck:"pk,auto"`
	Skip string `chuck:"-"`
	Keep string `chuck:"notnull"`
}

func TestFromStruct_SkipTag(t *testing.T) {
	td := schema.FromStruct[skipRow]("Skip")
	for _, c := range td.SelectColumns() {
		if c == "Skip" {
			t.Errorf("Skip column should be excluded via chuck:\"-\" tag, got %v", td.SelectColumns())
		}
	}
}

func TestFromStruct_FailLoud(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
		want string
	}{
		{
			name: "non-struct generic",
			fn:   func() { schema.FromStruct[int]("X") },
			want: "must be a struct",
		},
		{
			name: "empty table name",
			fn:   func() { schema.FromStruct[sessionSettingsRow]("") },
			want: "non-empty",
		},
		{
			name: "no exported fields",
			fn:   func() { schema.FromStruct[struct{ private int }]("Empty") },
			want: "no exported fields",
		},
		{
			name: "embedded field",
			fn: func() {
				type Embedded struct{ X int }
				type Outer struct {
					Embedded
					Y int
				}
				schema.FromStruct[Outer]("Outer")
			},
			want: "anonymous/embedded",
		},
		{
			name: "unsupported field type",
			fn: func() {
				type bad struct {
					ID   int      `chuck:"pk,auto"`
					Tags []string `chuck:"notnull"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "unsupported field type",
		},
		{
			name: "notnull and null conflict",
			fn: func() {
				type bad struct {
					Name string `chuck:"notnull,null"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "mutually exclusive",
		},
		{
			name: "auto without pk",
			fn: func() {
				type bad struct {
					ID int `chuck:"auto"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "auto requires pk",
		},
		{
			name: "unique with pk",
			fn: func() {
				type bad struct {
					ID int `chuck:"pk,unique"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "unique is redundant with pk",
		},
		{
			name: "pk nullable",
			fn: func() {
				type bad struct {
					ID int `chuck:"pk,null"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "pk columns cannot be nullable",
		},
		{
			name: "size on non-string",
			fn: func() {
				type bad struct {
					N int `chuck:"size=10"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "size= is only valid for string fields",
		},
		{
			name: "size non-positive",
			fn: func() {
				type bad struct {
					S string `chuck:"size=0"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "size=",
		},
		{
			name: "auto on non-int",
			fn: func() {
				type bad struct {
					S string `chuck:"pk,auto"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "auto requires an integer-kind field",
		},
		{
			name: "auto on pointer int",
			fn: func() {
				type bad struct {
					ID *int `chuck:"pk,auto"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "auto fields cannot be pointer-typed",
		},
		{
			name: "auto with default",
			fn: func() {
				type bad struct {
					ID int `chuck:"pk,auto,default=1"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "auto fields cannot also declare default=",
		},
		{
			name: "name= empty value",
			fn: func() {
				type bad struct {
					X int `chuck:"name="`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "tag name= requires a value",
		},
		{
			name: "duplicate name=",
			fn: func() {
				type bad struct {
					X int `chuck:"name=a,name=b"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "name= specified more than once",
		},
		{
			name: "duplicate bare name token",
			fn: func() {
				type bad struct {
					X int `chuck:"foo,bar"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "multiple bare tokens",
		},
		{
			name: "both name= and bare name",
			fn: func() {
				type bad struct {
					X int `chuck:"foo,name=bar"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "both name= and bare column-name token",
		},
		{
			name: "unknown tag key",
			fn: func() {
				type bad struct {
					X int `chuck:"flux=2"`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: `unknown tag key "flux"`,
		},
		{
			name: "default= empty",
			fn: func() {
				type bad struct {
					X int `chuck:"default="`
				}
				schema.FromStruct[bad]("Bad")
			},
			want: "tag default= requires a value",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("expected panic containing %q, got none", tc.want)
				}
				msg, ok := r.(error)
				if !ok {
					if s, isStr := r.(string); isStr {
						if !strings.Contains(s, tc.want) {
							t.Fatalf("panic %q does not contain %q", s, tc.want)
						}
						return
					}
					t.Fatalf("panic was not error or string: %T %v", r, r)
				}
				if !strings.Contains(msg.Error(), tc.want) {
					t.Fatalf("panic %q does not contain %q", msg.Error(), tc.want)
				}
			}()
			tc.fn()
		})
	}
}

func TestFromStruct_PostgresNormalization(t *testing.T) {
	pg, _ := chuck.New(chuck.Postgres)
	type row struct {
		ID        int       `chuck:"pk,auto"`
		Email     string    `chuck:"size=255,notnull,unique"`
		CreatedAt time.Time `chuck:"notnull"`
	}
	td := schema.FromStruct[row]("UserCache")

	snap := td.SnapshotString(pg)
	for _, want := range []string{
		"TABLE user_cache",
		"id                   SERIAL PRIMARY KEY NOT NULL [immutable]",
		"email                VARCHAR(255) NOT NULL UNIQUE",
		"created_at           TIMESTAMPTZ NOT NULL",
	} {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot missing %q\n%s", want, snap)
		}
	}
}
