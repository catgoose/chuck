package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func TestValidateSchema(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	table := NewTable("Items").
		Columns(
			AutoIncrCol("ID"),
			Col("Name", TypeString(255)).NotNull(),
			Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
		).
		Indexes(
			Index("idx_items_name", "Name"),
		)

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	t.Run("valid_schema", func(t *testing.T) {
		errs := ValidateSchema(ctx, db, d, table)
		assert.Nil(t, errs)
	})

	t.Run("missing_column", func(t *testing.T) {
		// Declare a table with an extra column that doesn't exist in DB
		extra := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
				Col("Priority", TypeInt()),
			)

		errs := ValidateSchema(ctx, db, d, extra)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, "Items.Priority: column missing")
	})

	t.Run("table_not_exists", func(t *testing.T) {
		missing := NewTable("Nonexistent").
			Columns(AutoIncrCol("ID"))

		errs := ValidateSchema(ctx, db, d, missing)
		require.NotNil(t, errs)
		assert.Contains(t, errs[0].Error(), "does not exist")
	})

	t.Run("nullability_mismatch", func(t *testing.T) {
		// Declare Status as nullable, but it's NOT NULL in the DB
		mismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)),
			)

		errs := ValidateSchema(ctx, db, d, mismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Status" && strings.Contains(e.Message, "nullability") {
				found = true
			}
		}
		assert.True(t, found, "expected nullability mismatch for Status, got: %v", errs)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		// Declare Name as INTEGER, but it's TEXT in the DB
		typeMismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeInt()).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
			)

		errs := ValidateSchema(ctx, db, d, typeMismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Name" && strings.Contains(e.Message, "type mismatch") {
				found = true
			}
		}
		assert.True(t, found, "expected type mismatch for Name, got: %v", errs)
	})

	t.Run("default_mismatch", func(t *testing.T) {
		// Declare Status with a different default than in DB
		defaultMismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull().Default("'pending'"),
			)

		errs := ValidateSchema(ctx, db, d, defaultMismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Status" && strings.Contains(e.Message, "default mismatch") {
				found = true
			}
		}
		assert.True(t, found, "expected default mismatch for Status, got: %v", errs)
	})

	t.Run("autoincr_no_false_positive", func(t *testing.T) {
		// Auto-increment columns should not cause a false type mismatch
		errs := ValidateSchema(ctx, db, d, table)
		assert.Nil(t, errs, "expected no errors for matching schema, got: %v", errs)
	})

	t.Run("extra_live_column", func(t *testing.T) {
		// Declare fewer columns than exist in DB
		fewer := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
			)

		errs := ValidateSchema(ctx, db, d, fewer)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, "Items.Status: unexpected column (exists in database but not in declaration)")
	})

	t.Run("missing_index", func(t *testing.T) {
		withExtraIndex := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			).
			Indexes(
				Index("idx_items_name", "Name"),
				Index("idx_items_status", "Status"),
			)

		errs := ValidateSchema(ctx, db, d, withExtraIndex)
		require.NotNil(t, errs)

		var messages []string
		for _, e := range errs {
			messages = append(messages, e.Error())
		}
		assert.Contains(t, messages, `Items: index "idx_items_status" missing`)
	})

	t.Run("index_uniqueness_mismatch", func(t *testing.T) {
		// idx_items_name is non-unique in DB, declare as unique
		mismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			).
			Indexes(
				UniqueIndex("idx_items_name", "Name"),
			)

		errs := ValidateSchema(ctx, db, d, mismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if strings.Contains(e.Message, "uniqueness mismatch") {
				found = true
			}
		}
		assert.True(t, found, "expected uniqueness mismatch error, got: %v", errs)
	})

	t.Run("extra_live_index", func(t *testing.T) {
		// The DB has idx_items_name but we don't declare any indexes
		noIndexes := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull().Default("'active'"),
			)

		errs := ValidateSchema(ctx, db, d, noIndexes)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if strings.Contains(e.Message, `unexpected index "idx_items_name"`) {
				found = true
			}
		}
		assert.True(t, found, "expected unexpected index error for idx_items_name, got: %v", errs)
	})

	t.Run("index_columns_mismatch", func(t *testing.T) {
		// idx_items_name covers "Name" in DB, declare as covering "Name, Status"
		mismatch := NewTable("Items").
			Columns(
				AutoIncrCol("ID"),
				Col("Name", TypeString(255)).NotNull(),
				Col("Status", TypeVarchar(50)).NotNull(),
			).
			Indexes(
				Index("idx_items_name", "Name, Status"),
			)

		errs := ValidateSchema(ctx, db, d, mismatch)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if strings.Contains(e.Message, "columns mismatch") {
				found = true
			}
		}
		assert.True(t, found, "expected columns mismatch error, got: %v", errs)
	})
}

func TestValidateSchemaPostgresNormalization(t *testing.T) {
	// This test verifies that ValidateSchema normalizes column names
	// for the Postgres dialect, matching CamelCase declarations against
	// the snake_case columns that DDL creates.
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{} // SQLite doesn't normalize, used as baseline

	table := NewTable("Accounts").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
			Col("PasswordHash", TypeText()).NotNull(),
		)

	// Create with SQLite (no normalization)
	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// Validate should pass — SQLite preserves CamelCase
	errs := ValidateSchema(ctx, db, d, table)
	assert.Nil(t, errs)

	// Verify Postgres normalization produces snake_case names in Snapshot
	pg := chuck.PostgresDialect{}
	snap := table.Snapshot(pg)
	assert.Equal(t, "accounts", snap.Name)
	assert.Equal(t, "email", snap.Columns[1].Name)
	assert.Equal(t, "password_hash", snap.Columns[2].Name)

	// TableNameFor should also normalize
	assert.Equal(t, "accounts", table.TableNameFor(pg))
	assert.Equal(t, "Accounts", table.TableNameFor(d))
}

func TestValidateSchemaIssue11Repro(t *testing.T) {
	// Issue #11: ValidateSchema should normalize CamelCase column names through
	// the dialect before comparing against the live database.
	// On Postgres, DDL creates snake_case columns, so Snapshot must also
	// produce snake_case names for the comparison to succeed.

	pg := chuck.PostgresDialect{}

	// Define table with PascalCase names (how users define schemas)
	table := NewTable("Accounts").
		Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
			Col("PasswordHash", TypeText()).NotNull(),
		).
		WithTimestamps()

	// Snapshot with Postgres dialect must normalize all names
	snap := table.Snapshot(pg)
	assert.Equal(t, "accounts", snap.Name)
	assert.Equal(t, "id", snap.Columns[0].Name)
	assert.Equal(t, "email", snap.Columns[1].Name)
	assert.Equal(t, "password_hash", snap.Columns[2].Name)
	assert.Equal(t, "created_at", snap.Columns[3].Name)
	assert.Equal(t, "updated_at", snap.Columns[4].Name)

	// SelectColumnsFor must also normalize
	pgCols := table.SelectColumnsFor(pg)
	assert.Equal(t, []string{"id", "email", "password_hash", "created_at", "updated_at"}, pgCols)

	// InsertColumnsFor must normalize (excludes auto-increment)
	pgInsert := table.InsertColumnsFor(pg)
	assert.Equal(t, []string{"email", "password_hash", "created_at", "updated_at"}, pgInsert)

	// UpdateColumnsFor must normalize (only mutable)
	pgUpdate := table.UpdateColumnsFor(pg)
	assert.Contains(t, pgUpdate, "email")
	assert.Contains(t, pgUpdate, "password_hash")
	assert.Contains(t, pgUpdate, "updated_at")
	assert.NotContains(t, pgUpdate, "id")
	assert.NotContains(t, pgUpdate, "created_at")

	// End-to-end: validate with SQLite (which doesn't normalize) still works
	// when table and columns match exactly
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	for _, stmt := range table.CreateIfNotExistsSQL(d) {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	errs := ValidateSchema(ctx, db, d, table)
	assert.Nil(t, errs, "expected no errors, got: %v", errs)
}

func TestValidateAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	d := chuck.SQLiteDialect{}

	users := NewTable("Users").
		Columns(AutoIncrCol("ID"), Col("Name", TypeString(255)).NotNull())
	tasks := NewTable("Tasks").
		Columns(AutoIncrCol("ID"), Col("Title", TypeString(255)).NotNull())

	for _, tbl := range []*TableDef{users, tasks} {
		for _, stmt := range tbl.CreateIfNotExistsSQL(d) {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err)
		}
	}

	t.Run("all_valid", func(t *testing.T) {
		errs := ValidateAll(ctx, db, d, users, tasks)
		assert.Nil(t, errs)
	})

	t.Run("one_invalid", func(t *testing.T) {
		bad := NewTable("Tasks").
			Columns(
				AutoIncrCol("ID"),
				Col("Title", TypeString(255)).NotNull(),
				Col("Missing", TypeText()),
			)

		errs := ValidateAll(ctx, db, d, users, bad)
		require.NotNil(t, errs)

		var found bool
		for _, e := range errs {
			if e.Column == "Missing" {
				found = true
			}
		}
		assert.True(t, found, "expected error for missing column 'Missing'")
	})
}

// TestValidateImplicitUniqueIndexes covers issue #65 item 2: a single-column
// unique index that backs a declared column-level UNIQUE constraint must not
// be reported as an unexpected/extra index, regardless of the engine-specific
// auto-generated index name (Postgres "<table>_<col>_key", MSSQL
// "UQ__<prefix>__<hash>", etc.). The matching is done by column set, not by
// name, with case-insensitive normalization to bridge Postgres lowercasing.
//
// These tests exercise the comparator core directly via
// validateAgainstLiveSnapshot so they do not require a live database (the
// SQLite live snapshot path filters out implicit unique indexes via the
// pragma_index_list origin filter, so we need crafted live snapshots to
// reach the implicit-skip code path).
func TestValidateImplicitUniqueIndexes(t *testing.T) {
	d := chuck.SQLiteDialect{}

	// liveUsersBase returns a live snapshot whose columns match a minimal
	// users table (ID + Email). Tests append index entries on top of this.
	liveUsersBase := func(extraIndexes ...LiveIndexSnapshot) LiveTableSnapshot {
		return LiveTableSnapshot{
			Name: "users",
			Columns: []LiveColumnSnapshot{
				{Name: "ID", Type: "INTEGER", Nullable: false},
				{Name: "Email", Type: "VARCHAR(255)", Nullable: false},
			},
			Indexes: extraIndexes,
		}
	}

	tests := []struct {
		name           string
		declared       *TableDef
		live           LiveTableSnapshot
		wantUnexpected []string // index names expected in "unexpected index" errors
	}{
		{
			name: "ColumnDef.Unique() suppresses Postgres-style implicit index",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_key", Columns: []string{"Email"}, Unique: true,
			}),
			wantUnexpected: nil,
		},
		{
			name: "single-column UniqueColumns suppresses implicit index",
			declared: NewTable("users").
				Columns(
					AutoIncrCol("ID"),
					Col("Email", TypeVarchar(255)).NotNull(),
				).
				UniqueColumns("Email"),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_key", Columns: []string{"Email"}, Unique: true,
			}),
			wantUnexpected: nil,
		},
		{
			name: "multi-column UniqueColumns does NOT suppress single-column live unique",
			declared: NewTable("users").
				Columns(
					AutoIncrCol("ID"),
					Col("Email", TypeVarchar(255)).NotNull(),
				).
				UniqueColumns("Email", "ID"),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_idx", Columns: []string{"Email"}, Unique: true,
			}),
			wantUnexpected: []string{"users_email_idx"},
		},
		{
			name: "no declared unique => live unique index is real drift",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_key", Columns: []string{"Email"}, Unique: true,
			}),
			wantUnexpected: []string{"users_email_key"},
		},
		{
			name: "declared unique but live multi-column index is reported",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_idx", Columns: []string{"Email", "ID"}, Unique: true,
			}),
			wantUnexpected: []string{"users_email_idx"},
		},
		{
			name: "declared unique but live non-unique index is reported",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_idx", Columns: []string{"Email"}, Unique: false,
			}),
			wantUnexpected: []string{"users_email_idx"},
		},
		{
			name: "declared unique but live partial unique index is reported",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name:    "users_email_partial",
				Columns: []string{"Email"},
				Unique:  true,
				Where:   "deleted_at IS NULL",
			}),
			wantUnexpected: []string{"users_email_partial"},
		},
		{
			name: "MSSQL-style hash-suffixed name with case-insensitive match",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: liveUsersBase(LiveIndexSnapshot{
				Name:    "UQ__users___ABC12345",
				Columns: []string{"Email"},
				Unique:  true,
			}),
			wantUnexpected: nil,
		},
		{
			name: "Postgres-style lowercase live column matches PascalCase declaration",
			declared: NewTable("users").Columns(
				AutoIncrCol("ID"),
				Col("Email", TypeVarchar(255)).NotNull().Unique(),
			),
			live: LiveTableSnapshot{
				Name: "users",
				Columns: []LiveColumnSnapshot{
					{Name: "ID", Type: "INTEGER", Nullable: false},
					{Name: "Email", Type: "VARCHAR(255)", Nullable: false},
				},
				Indexes: []LiveIndexSnapshot{
					{Name: "users_email_key", Columns: []string{"email"}, Unique: true},
				},
			},
			wantUnexpected: nil,
		},
		{
			name: "explicit declared index with same name does not regress: matched by name path",
			declared: NewTable("users").
				Columns(
					AutoIncrCol("ID"),
					Col("Email", TypeVarchar(255)).NotNull().Unique(),
				).
				Indexes(UniqueIndex("users_email_key", "Email")),
			live: liveUsersBase(LiveIndexSnapshot{
				Name: "users_email_key", Columns: []string{"Email"}, Unique: true,
			}),
			wantUnexpected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tableName := d.NormalizeIdentifier(tc.declared.Name)
			errs := validateAgainstLiveSnapshot(tc.declared, d, tc.live, tableName)

			var gotUnexpected []string
			for _, e := range errs {
				if strings.HasPrefix(e.Message, "unexpected index ") {
					gotUnexpected = append(gotUnexpected, e.Message)
				}
			}

			if len(tc.wantUnexpected) == 0 {
				assert.Empty(t, gotUnexpected, "expected no unexpected-index errors, got: %v (full: %v)", gotUnexpected, errs)
				return
			}
			require.Len(t, gotUnexpected, len(tc.wantUnexpected),
				"unexpected-index error count mismatch: got %v, full errs: %v", gotUnexpected, errs)
			for _, name := range tc.wantUnexpected {
				found := false
				for _, msg := range gotUnexpected {
					if strings.Contains(msg, `"`+name+`"`) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected unexpected-index error for %q, got: %v", name, gotUnexpected)
			}
		})
	}
}

func TestDeclaredSingleColumnUniques(t *testing.T) {
	t.Run("ColumnDef.Unique includes column", func(t *testing.T) {
		td := NewTable("users").Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull().Unique(),
		)
		got := declaredSingleColumnUniques(td)
		assert.True(t, got["email"])
		assert.False(t, got["id"])
	})

	t.Run("single-column UniqueColumns is included", func(t *testing.T) {
		td := NewTable("users").
			Columns(AutoIncrCol("ID"), Col("Email", TypeVarchar(255)).NotNull()).
			UniqueColumns("Email")
		got := declaredSingleColumnUniques(td)
		assert.True(t, got["email"])
	})

	t.Run("multi-column UniqueColumns is excluded", func(t *testing.T) {
		td := NewTable("users").
			Columns(
				AutoIncrCol("ID"),
				Col("TenantID", TypeInt()).NotNull(),
				Col("Email", TypeVarchar(255)).NotNull(),
			).
			UniqueColumns("TenantID", "Email")
		got := declaredSingleColumnUniques(td)
		assert.False(t, got["tenantid"])
		assert.False(t, got["email"])
	})

	t.Run("no unique declarations returns empty map", func(t *testing.T) {
		td := NewTable("users").Columns(
			AutoIncrCol("ID"),
			Col("Email", TypeVarchar(255)).NotNull(),
		)
		got := declaredSingleColumnUniques(td)
		assert.Empty(t, got)
	})
}

// TestValidateIndexColumnNormalization covers issue #65 item 4: the index
// column comparator must run both declared and live column lists through
// the active dialect's NormalizeIdentifier before comparing. Postgres
// lowercases unquoted PascalCase identifiers to snake_case, so a declared
// index on "Name" matches a live index on "name" without engine-specific
// logic. SQLite and MSSQL NormalizeIdentifier are identity, so
// case-preserving engines still see the raw tokens.
//
// Each case builds a *TableDef with one index and a LiveTableSnapshot with
// the corresponding live index, then calls validateAgainstLiveSnapshot
// directly to avoid requiring a real database.
func TestValidateIndexColumnNormalization(t *testing.T) {
	// liveBase returns a live snapshot whose columns match what the
	// declared TableDef below exposes. Each test sets its own Indexes
	// entry. The column set is intentionally broad (covers a, b, c, Name,
	// UserID, FooBar) so the column-presence check does not produce
	// unrelated drift errors that would pollute the assertions.
	liveBase := func(indexes ...LiveIndexSnapshot) LiveTableSnapshot {
		return LiveTableSnapshot{
			Name: "t",
			Columns: []LiveColumnSnapshot{
				{Name: "a", Type: "INTEGER", Nullable: false},
				{Name: "b", Type: "INTEGER", Nullable: false},
				{Name: "c", Type: "INTEGER", Nullable: false},
				{Name: "name", Type: "VARCHAR(255)", Nullable: false},
				{Name: "user_id", Type: "INTEGER", Nullable: false},
				{Name: "foo_bar", Type: "VARCHAR(255)", Nullable: false},
			},
		}
	}
	// liveBaseCaseSensitive is the SQLite/MSSQL variant where column
	// names are preserved verbatim so the declared PascalCase columns
	// line up with the live snapshot's columns without normalization.
	liveBaseCaseSensitive := func(indexes ...LiveIndexSnapshot) LiveTableSnapshot {
		return LiveTableSnapshot{
			Name: "t",
			Columns: []LiveColumnSnapshot{
				{Name: "a", Type: "INTEGER", Nullable: false},
				{Name: "b", Type: "INTEGER", Nullable: false},
				{Name: "c", Type: "INTEGER", Nullable: false},
				{Name: "Name", Type: "VARCHAR(255)", Nullable: false},
				{Name: "UserID", Type: "INTEGER", Nullable: false},
				{Name: "FooBar", Type: "VARCHAR(255)", Nullable: false},
			},
		}
	}

	// declaredTable returns a *TableDef with a broad column set and a
	// single index whose columns string is determined by the test case.
	declaredTable := func(idxCols string) *TableDef {
		return NewTable("t").
			Columns(
				Col("a", TypeInt()).NotNull(),
				Col("b", TypeInt()).NotNull(),
				Col("c", TypeInt()).NotNull(),
				Col("Name", TypeVarchar(255)).NotNull(),
				Col("UserID", TypeInt()).NotNull(),
				Col("FooBar", TypeVarchar(255)).NotNull(),
			).
			Indexes(Index("idx_t", idxCols))
	}

	tests := []struct {
		name           string
		dialect        chuck.Dialect
		declaredCols   string
		liveCols       []string
		caseSensitive  bool // use liveBaseCaseSensitive instead of liveBase
		wantDrift      bool
		wantMsgHas     []string // substrings the drift error must contain (only consulted when wantDrift)
		wantMsgHasNone []string // substrings the drift error must NOT contain (only consulted when wantDrift)
	}{
		{
			name:         "postgres: PascalCase declared vs snake_case live (single)",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "Name",
			liveCols:     []string{"name"},
			wantDrift:    false,
		},
		{
			name:         "postgres: PascalCase with acronym vs snake_case live",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "UserID",
			liveCols:     []string{"user_id"},
			wantDrift:    false,
		},
		{
			name:         "postgres: multi-column canonical form matches",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a, b",
			liveCols:     []string{"a", "b"},
			wantDrift:    false,
		},
		{
			name:         "postgres: multi-column no space tolerated",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a,b",
			liveCols:     []string{"a", "b"},
			wantDrift:    false,
		},
		{
			name:         "postgres: multi-column extra whitespace tolerated",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a , b",
			liveCols:     []string{"a", "b"},
			wantDrift:    false,
		},
		{
			name:          "sqlite: PascalCase preserved both sides",
			dialect:       chuck.SQLiteDialect{},
			declaredCols:  "Name",
			liveCols:      []string{"Name"},
			caseSensitive: true,
			wantDrift:     false,
		},
		{
			name:          "mssql: PascalCase preserved both sides",
			dialect:       chuck.MSSQLDialect{},
			declaredCols:  "Name",
			liveCols:      []string{"Name"},
			caseSensitive: true,
			wantDrift:     false,
		},
		{
			name:         "postgres: order matters (b, a != a, b)",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a, b",
			liveCols:     []string{"b", "a"},
			wantDrift:    true,
			wantMsgHas:   []string{`"a, b"`, `"b, a"`},
		},
		{
			name:         "postgres: length differs live shorter",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a, b",
			liveCols:     []string{"a"},
			wantDrift:    true,
			wantMsgHas:   []string{`"a, b"`, `"a"`},
		},
		{
			name:         "postgres: length differs declared shorter",
			dialect:      chuck.PostgresDialect{},
			declaredCols: "a, b, c",
			liveCols:     []string{"a", "b"},
			wantDrift:    true,
			wantMsgHas:   []string{`"a, b, c"`, `"a, b"`},
		},
		{
			name:           "postgres: real drift preserves original strings in error",
			dialect:        chuck.PostgresDialect{},
			declaredCols:   "FooBar",
			liveCols:       []string{"baz"},
			wantDrift:      true,
			wantMsgHas:     []string{`"FooBar"`, `"baz"`},
			wantMsgHasNone: []string{"foo_bar"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			td := declaredTable(tc.declaredCols)
			liveIdx := LiveIndexSnapshot{
				Name:    "idx_t",
				Columns: tc.liveCols,
			}
			var live LiveTableSnapshot
			if tc.caseSensitive {
				live = liveBaseCaseSensitive(liveIdx)
			} else {
				live = liveBase(liveIdx)
			}
			live.Indexes = []LiveIndexSnapshot{liveIdx}

			tableName := tc.dialect.NormalizeIdentifier(td.Name)
			errs := validateAgainstLiveSnapshot(td, tc.dialect, live, tableName)

			var colMismatch []SchemaError
			for _, e := range errs {
				if strings.Contains(e.Message, "columns mismatch") {
					colMismatch = append(colMismatch, e)
				}
			}

			if !tc.wantDrift {
				assert.Empty(t, colMismatch,
					"expected no columns-mismatch error, got: %v (full errs: %v)", colMismatch, errs)
				return
			}

			require.Len(t, colMismatch, 1,
				"expected exactly one columns-mismatch error, got: %v (full errs: %v)", colMismatch, errs)
			msg := colMismatch[0].Message
			for _, s := range tc.wantMsgHas {
				assert.Contains(t, msg, s,
					"drift message missing required substring %q: %s", s, msg)
			}
			for _, s := range tc.wantMsgHasNone {
				assert.NotContains(t, msg, s,
					"drift message must not contain normalized form %q: %s", s, msg)
			}
		})
	}
}
