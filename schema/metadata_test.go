package schema

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func TestEnsureMetadataTables_RequiresOwner(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	err = EnsureMetadataTables(ctx, db, d, MetadataConfig{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMetadataOwnerMissing),
		"empty Owner must surface as ErrMetadataOwnerMissing")
}

func TestEnsureMetadataTables_CreatesObjectTable_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}
	cfg := MetadataConfig{Owner: "test"}

	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	var got string
	err = db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, DefaultObjectMetadataTableName).
		Scan(&got)
	require.NoError(t, err, "expected ChuckObjectMetadata to exist")
	assert.Equal(t, DefaultObjectMetadataTableName, got)

	// Slim ledger: no database-level table is created.
	var legacy string
	err = db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, "chuck_database_metadata").
		Scan(&legacy)
	assert.ErrorIs(t, err, sql.ErrNoRows,
		"chuck_database_metadata must not be created — feature shrunk to object-only core")

	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg),
		"second invocation must be a no-op (CREATE TABLE IF NOT EXISTS)")
}

func TestHashCodeObjectDefinition_StableAcrossWhitespaceAndComments(t *testing.T) {
	base := "SELECT id FROM things WHERE done = 0"
	variants := []string{
		base,
		"   " + base + "   ",
		base + ";",
		"\n/* leading comment */\n" + base,
		"/* one */\n\n/* two */\n" + base + ";\n",
	}
	want := hashCodeObjectDefinition(base)
	for _, v := range variants {
		got := hashCodeObjectDefinition(v)
		assert.Equal(t, want, got, "hash must ignore comment/whitespace-only variation for %q", v)
	}

	different := hashCodeObjectDefinition("SELECT id FROM things WHERE done = 1")
	assert.NotEqual(t, want, different, "hash must change when executable text changes")
}

func TestRecordCodeObjectMetadata_SnapshotSemantics_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	clock := newFakeClock(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	cfg := MetadataConfig{
		Owner: "owner-a",
		Now:   clock.Now,
	}
	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	obj := chuck.ObjectName{Name: "v_snapshot_target"}
	const hash1 = "hash-one"
	const hash2 = "hash-two"

	// First apply: row created with first/last_applied/last_changed all equal.
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash1))
	row := readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	t0 := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	assert.True(t, row.firstApplied.Equal(t0), "first apply: FirstAppliedAtUtc = t0, got %v", row.firstApplied)
	assert.True(t, row.lastApplied.Equal(t0))
	assert.True(t, row.lastChanged.Equal(t0))
	assert.Equal(t, hash1, row.definitionHash)

	// Second apply, same hash: LastAppliedAtUtc advances, LastChangedAtUtc unchanged.
	clock.Advance(time.Hour)
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash1))
	row = readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	assert.True(t, row.firstApplied.Equal(t0), "second apply same-hash: FirstAppliedAtUtc frozen")
	assert.True(t, row.lastApplied.Equal(t0.Add(time.Hour)), "LastAppliedAtUtc advances")
	assert.True(t, row.lastChanged.Equal(t0), "LastChangedAtUtc unchanged when hash matches")

	// Third apply, different hash: LastAppliedAtUtc AND LastChangedAtUtc
	// advance, FirstAppliedAtUtc still frozen at t0.
	clock.Advance(time.Hour)
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash2))
	row = readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	assert.True(t, row.firstApplied.Equal(t0), "third apply: FirstAppliedAtUtc still frozen")
	assert.True(t, row.lastApplied.Equal(t0.Add(2*time.Hour)))
	assert.True(t, row.lastChanged.Equal(t0.Add(2*time.Hour)), "LastChangedAtUtc must advance on hash change")
	assert.Equal(t, hash2, row.definitionHash)
}

func TestMetadataNoticePointer_QualifiesAcrossDialects(t *testing.T) {
	cases := []struct {
		name    string
		dialect chuck.Dialect
		schema  string
		want    string
	}{
		{
			name:    "sqlite drops schema even when configured",
			dialect: chuck.SQLiteDialect{},
			schema:  "ops",
			want:    `Provenance recorded in "ChuckObjectMetadata".`,
		},
		{
			name:    "sqlite bare unqualified",
			dialect: chuck.SQLiteDialect{},
			schema:  "",
			want:    `Provenance recorded in "ChuckObjectMetadata".`,
		},
		{
			name:    "postgres unqualified renders bare",
			dialect: chuck.PostgresDialect{},
			schema:  "",
			want:    `Provenance recorded in "ChuckObjectMetadata".`,
		},
		{
			name:    "postgres explicit schema renders qualified",
			dialect: chuck.PostgresDialect{},
			schema:  "ops",
			want:    `Provenance recorded in "ops"."ChuckObjectMetadata".`,
		},
		{
			name:    "mssql unqualified renders bracketed bare",
			dialect: chuck.MSSQLDialect{},
			schema:  "",
			want:    `Provenance recorded in [ChuckObjectMetadata].`,
		},
		{
			name:    "mssql explicit schema renders bracketed qualified",
			dialect: chuck.MSSQLDialect{},
			schema:  "ops",
			want:    `Provenance recorded in [ops].[ChuckObjectMetadata].`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := MetadataConfig{Owner: "t", Schema: tc.schema}
			assert.Equal(t, tc.want, metadataNoticePointer(tc.dialect, cfg))
		})
	}
}

func TestEffectiveOptionsForRender_AugmentsOnlyWhenNoticeAndMetadataBothSet(t *testing.T) {
	d := chuck.SQLiteDialect{}
	cfg := MetadataConfig{Owner: "t"}

	t.Run("both nil-like: opts unchanged", func(t *testing.T) {
		opts := CodeObjectOptions{}
		got := effectiveOptionsForRender(d, opts)
		assert.Equal(t, "", got.OwnershipNotice)
		assert.Nil(t, got.Metadata)
	})

	t.Run("notice set, metadata nil: opts unchanged", func(t *testing.T) {
		opts := CodeObjectOptions{OwnershipNotice: "owned"}
		got := effectiveOptionsForRender(d, opts)
		assert.Equal(t, "owned", got.OwnershipNotice, "notice unchanged when no metadata pointer to add")
	})

	t.Run("notice empty, metadata set: opts unchanged (no fresh ownership block invented)", func(t *testing.T) {
		opts := CodeObjectOptions{Metadata: &cfg}
		got := effectiveOptionsForRender(d, opts)
		assert.Equal(t, "", got.OwnershipNotice, "metadata alone must not invent an OwnershipNotice")
	})

	t.Run("both set: notice augmented with provenance pointer", func(t *testing.T) {
		opts := CodeObjectOptions{OwnershipNotice: "owned", Metadata: &cfg}
		got := effectiveOptionsForRender(d, opts)
		assert.Equal(t,
			"owned\nProvenance recorded in \"ChuckObjectMetadata\".",
			got.OwnershipNotice)
	})

	t.Run("does not mutate caller's CodeObjectOptions value", func(t *testing.T) {
		opts := CodeObjectOptions{OwnershipNotice: "owned", Metadata: &cfg}
		_ = effectiveOptionsForRender(d, opts)
		assert.Equal(t, "owned", opts.OwnershipNotice,
			"caller's opts must remain unchanged — effective opts is a value copy")
	})
}

func TestMetadataObjectColumns_DefaultsMatchLiveInspectionSchema(t *testing.T) {
	cases := []struct {
		name       string
		dialect    chuck.Dialect
		obj        chuck.ObjectName
		wantSchema string
		wantName   string
	}{
		{
			name:       "sqlite unqualified records empty schema",
			dialect:    chuck.SQLiteDialect{},
			obj:        chuck.ObjectName{Name: "v_open_tasks"},
			wantSchema: "",
			wantName:   "v_open_tasks",
		},
		{
			name:       "sqlite drops declared schema (no namespace on engine)",
			dialect:    chuck.SQLiteDialect{},
			obj:        chuck.ObjectName{Schema: "ignored", Name: "v_open_tasks"},
			wantSchema: "",
			wantName:   "v_open_tasks",
		},
		{
			name:       "postgres unqualified resolves to public (live default)",
			dialect:    chuck.PostgresDialect{},
			obj:        chuck.ObjectName{Name: "v_open_tasks"},
			wantSchema: "public",
			wantName:   "v_open_tasks",
		},
		{
			name:       "postgres explicit schema wins unchanged",
			dialect:    chuck.PostgresDialect{},
			obj:        chuck.ObjectName{Schema: "reporting", Name: "v_open_tasks"},
			wantSchema: "reporting",
			wantName:   "v_open_tasks",
		},
		{
			name:       "mssql unqualified resolves to dbo (live default)",
			dialect:    chuck.MSSQLDialect{},
			obj:        chuck.ObjectName{Name: "v_OpenTasks"},
			wantSchema: "dbo",
			wantName:   "v_OpenTasks",
		},
		{
			name:       "mssql explicit schema wins unchanged",
			dialect:    chuck.MSSQLDialect{},
			obj:        chuck.ObjectName{Schema: "sg", Name: "v_OpenTasks"},
			wantSchema: "sg",
			wantName:   "v_OpenTasks",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSchema, gotName := metadataObjectColumns(tc.dialect, tc.obj)
			assert.Equal(t, tc.wantSchema, gotSchema, "ObjectSchema")
			assert.Equal(t, tc.wantName, gotName, "ObjectName")
		})
	}
}

func TestRecordCodeObjectMetadata_UnqualifiedRowKeyedByEmptySchema_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	cfg := MetadataConfig{
		Owner: "schema-keying",
		Now:   newFakeClock(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)).Now,
	}
	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	obj := chuck.ObjectName{Name: "v_keying"}
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, "h"))

	var storedSchema, storedName string
	err = db.QueryRowContext(ctx,
		`SELECT "ObjectSchema", "ObjectName" FROM "`+DefaultObjectMetadataTableName+
			`" WHERE "Owner" = ? AND "ObjectName" = ?`, cfg.Owner, "v_keying").
		Scan(&storedSchema, &storedName)
	require.NoError(t, err)
	assert.Equal(t, "", storedSchema, "SQLite must continue recording empty ObjectSchema")
	assert.Equal(t, "v_keying", storedName)
}

func TestMetadataCreateStatements_AllDialectsRender(t *testing.T) {
	cfg := MetadataConfig{Owner: "test"}
	dialects := []chuck.Dialect{
		chuck.SQLiteDialect{},
		chuck.PostgresDialect{},
		chuck.MSSQLDialect{},
	}
	for _, d := range dialects {
		stmts := metadataCreateStatements(d, cfg)
		require.Lenf(t, stmts, 1, "%s: expected exactly 1 create statement (object-only ledger)", d.Engine())
		s := stmts[0]
		assert.Containsf(t, s, DefaultObjectMetadataTableName, "%s: object table name", d.Engine())
		assert.Containsf(t, s, "FirstAppliedAtUtc", "%s: stmt missing FirstAppliedAtUtc: %s", d.Engine(), s)
		assert.Containsf(t, s, "LastAppliedAtUtc", "%s: stmt missing LastAppliedAtUtc: %s", d.Engine(), s)
		assert.Containsf(t, s, "LastChangedAtUtc", "%s: stmt missing LastChangedAtUtc: %s", d.Engine(), s)
		assert.Containsf(t, s, "DefinitionHash", "%s: stmt missing DefinitionHash: %s", d.Engine(), s)
		assert.NotContainsf(t, s, "source_repo", "%s: source_repo must not be rendered", d.Engine())
		assert.NotContainsf(t, s, "source_rev", "%s: source_rev must not be rendered", d.Engine())
		assert.NotContainsf(t, s, "tool_version", "%s: tool_version must not be rendered", d.Engine())
		assert.NotContainsf(t, s, "chuck_database_metadata", "%s: legacy db-level table must not appear", d.Engine())
		assert.NotContainsf(t, s, "chuck_object_metadata", "%s: legacy lowercase table must not appear", d.Engine())
	}
}

func TestMetadataCreateStatements_SchemaQualifiedOnNonSQLite(t *testing.T) {
	cfg := MetadataConfig{Owner: "test", Schema: "metaschema"}
	stmts := metadataCreateStatements(chuck.PostgresDialect{}, cfg)
	require.Len(t, stmts, 1)
	assert.True(t,
		strings.Contains(stmts[0], `"metaschema"."ChuckObjectMetadata"`),
		"postgres schema-qualified table name expected: %s", stmts[0])
}

// --- helpers ----------------------------------------------------------------

type fakeClock struct {
	cur time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{cur: start.UTC()}
}

func (c *fakeClock) Now() time.Time {
	return c.cur
}

func (c *fakeClock) Advance(by time.Duration) {
	c.cur = c.cur.Add(by)
}

type objectMetadataRow struct {
	firstApplied   time.Time
	lastApplied    time.Time
	lastChanged    time.Time
	definitionHash string
}

func readObjectRow(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType string,
	obj chuck.ObjectName,
) objectMetadataRow {
	t.Helper()
	schemaCol, nameCol := metadataObjectColumns(d, obj)
	q := `SELECT "FirstAppliedAtUtc", "LastAppliedAtUtc", "LastChangedAtUtc", "DefinitionHash"
	      FROM "` + DefaultObjectMetadataTableName + `"
	      WHERE "Owner" = ? AND "ObjectType" = ? AND "ObjectSchema" = ? AND "ObjectName" = ?`
	var row objectMetadataRow
	err := db.QueryRowContext(ctx, q, cfg.Owner, objectType, schemaCol, nameCol).Scan(
		&row.firstApplied, &row.lastApplied, &row.lastChanged, &row.definitionHash,
	)
	require.NoError(t, err)
	return row
}
