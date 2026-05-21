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

func TestEnsureMetadataTables_CreatesBothTables_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}
	cfg := MetadataConfig{Owner: "test"}

	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	for _, name := range []string{
		DefaultDatabaseMetadataTableName,
		DefaultObjectMetadataTableName,
	} {
		var got string
		err := db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).
			Scan(&got)
		require.NoError(t, err, "expected table %q to exist", name)
		assert.Equal(t, name, got)
	}

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
		Owner:       "owner-a",
		SourceRepo:  "https://github.com/catgoose/chuck",
		SourceRev:   "deadbeef",
		ToolVersion: "v0.0.1-test",
		Now:         clock.Now,
	}
	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	obj := chuck.ObjectName{Name: "v_snapshot_target"}
	const hash1 = "hash-one"
	const hash2 = "hash-two"

	// First apply: row created with first/last_applied/last_changed all equal.
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash1))
	row := readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	t0 := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	assert.True(t, row.firstApplied.Equal(t0), "first apply: first_applied = t0, got %v", row.firstApplied)
	assert.True(t, row.lastApplied.Equal(t0))
	assert.True(t, row.lastChanged.Equal(t0))
	assert.Equal(t, hash1, row.definitionHash)
	assert.Equal(t, "https://github.com/catgoose/chuck", row.sourceRepo.String)
	assert.Equal(t, "deadbeef", row.sourceRev.String)
	assert.Equal(t, "v0.0.1-test", row.toolVersion.String)

	// Second apply, same hash: last_applied advances, last_changed unchanged.
	clock.Advance(time.Hour)
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash1))
	row = readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	assert.True(t, row.firstApplied.Equal(t0), "second apply same-hash: first_applied frozen")
	assert.True(t, row.lastApplied.Equal(t0.Add(time.Hour)), "last_applied advances")
	assert.True(t, row.lastChanged.Equal(t0), "last_changed unchanged when hash matches")

	// Third apply, different hash: last_applied AND last_changed advance,
	// first_applied still frozen at t0.
	clock.Advance(time.Hour)
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, hash2))
	row = readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	assert.True(t, row.firstApplied.Equal(t0), "third apply: first_applied still frozen")
	assert.True(t, row.lastApplied.Equal(t0.Add(2*time.Hour)))
	assert.True(t, row.lastChanged.Equal(t0.Add(2*time.Hour)), "last_changed must advance on hash change")
	assert.Equal(t, hash2, row.definitionHash)

	// Database row tracks first/last apply too.
	dbRow := readDatabaseRow(t, ctx, db, d, cfg)
	assert.True(t, dbRow.firstApplied.Equal(t0), "database first_applied frozen at first record")
	assert.True(t, dbRow.lastApplied.Equal(t0.Add(2*time.Hour)))
}

func TestRecordCodeObjectMetadata_NilProvenanceWritesNull_SQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	d := chuck.SQLiteDialect{}

	cfg := MetadataConfig{
		Owner: "owner-b",
		Now:   newFakeClock(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)).Now,
	}
	require.NoError(t, EnsureMetadataTables(ctx, db, d, cfg))

	obj := chuck.ObjectName{Name: "v_null_provenance"}
	require.NoError(t, recordCodeObjectMetadata(ctx, db, d, cfg, MetadataObjectTypeView, obj, "h"))

	row := readObjectRow(t, ctx, db, d, cfg, MetadataObjectTypeView, obj)
	assert.False(t, row.sourceRepo.Valid, "empty SourceRepo must write SQL NULL")
	assert.False(t, row.sourceRev.Valid, "empty SourceRev must write SQL NULL")
	assert.False(t, row.toolVersion.Valid, "empty ToolVersion must write SQL NULL")
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
			want:    `Provenance recorded in "chuck_object_metadata".`,
		},
		{
			name:    "sqlite bare unqualified",
			dialect: chuck.SQLiteDialect{},
			schema:  "",
			want:    `Provenance recorded in "chuck_object_metadata".`,
		},
		{
			name:    "postgres unqualified renders bare",
			dialect: chuck.PostgresDialect{},
			schema:  "",
			want:    `Provenance recorded in "chuck_object_metadata".`,
		},
		{
			name:    "postgres explicit schema renders qualified",
			dialect: chuck.PostgresDialect{},
			schema:  "ops",
			want:    `Provenance recorded in "ops"."chuck_object_metadata".`,
		},
		{
			name:    "mssql unqualified renders bracketed bare",
			dialect: chuck.MSSQLDialect{},
			schema:  "",
			want:    `Provenance recorded in [chuck_object_metadata].`,
		},
		{
			name:    "mssql explicit schema renders bracketed qualified",
			dialect: chuck.MSSQLDialect{},
			schema:  "ops",
			want:    `Provenance recorded in [ops].[chuck_object_metadata].`,
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
			"owned\nProvenance recorded in \"chuck_object_metadata\".",
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
			assert.Equal(t, tc.wantSchema, gotSchema, "object_schema")
			assert.Equal(t, tc.wantName, gotName, "object_name")
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
		`SELECT object_schema, object_name FROM `+DefaultObjectMetadataTableName+
			` WHERE owner = ? AND object_name = ?`, cfg.Owner, "v_keying").
		Scan(&storedSchema, &storedName)
	require.NoError(t, err)
	assert.Equal(t, "", storedSchema, "SQLite must continue recording empty object_schema")
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
		require.Lenf(t, stmts, 2, "%s: expected 2 create statements", d.Engine())
		for _, s := range stmts {
			assert.Containsf(t, s, "first_applied_at_utc", "%s: stmt missing first_applied_at_utc: %s", d.Engine(), s)
			assert.Containsf(t, s, "last_applied_at_utc", "%s: stmt missing last_applied_at_utc: %s", d.Engine(), s)
		}
		assert.Containsf(t, stmts[0], DefaultDatabaseMetadataTableName, "%s: db table name", d.Engine())
		assert.Containsf(t, stmts[1], DefaultObjectMetadataTableName, "%s: object table name", d.Engine())
		assert.Containsf(t, stmts[1], "definition_hash", "%s: definition_hash column", d.Engine())
		assert.Containsf(t, stmts[1], "last_changed_at_utc", "%s: last_changed_at_utc column", d.Engine())
	}
}

func TestMetadataCreateStatements_SchemaQualifiedOnNonSQLite(t *testing.T) {
	cfg := MetadataConfig{Owner: "test", Schema: "metaschema"}
	stmts := metadataCreateStatements(chuck.PostgresDialect{}, cfg)
	require.Len(t, stmts, 2)
	for _, s := range stmts {
		assert.True(t,
			strings.Contains(s, `"metaschema"."chuck_database_metadata"`) ||
				strings.Contains(s, `"metaschema"."chuck_object_metadata"`),
			"postgres schema-qualified table name expected: %s", s)
	}
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
	sourceRepo     sql.NullString
	sourceRev      sql.NullString
	toolVersion    sql.NullString
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
	q := `SELECT first_applied_at_utc, last_applied_at_utc, last_changed_at_utc,
	             definition_hash, source_repo, source_rev, tool_version
	      FROM ` + DefaultObjectMetadataTableName + `
	      WHERE owner = ? AND object_type = ? AND object_schema = ? AND object_name = ?`
	var row objectMetadataRow
	err := db.QueryRowContext(ctx, q, cfg.Owner, objectType, schemaCol, nameCol).Scan(
		&row.firstApplied, &row.lastApplied, &row.lastChanged,
		&row.definitionHash, &row.sourceRepo, &row.sourceRev, &row.toolVersion,
	)
	require.NoError(t, err)
	return row
}

type databaseMetadataRow struct {
	firstApplied time.Time
	lastApplied  time.Time
}

func readDatabaseRow(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
) databaseMetadataRow {
	t.Helper()
	_ = d
	q := `SELECT first_applied_at_utc, last_applied_at_utc
	      FROM ` + DefaultDatabaseMetadataTableName + `
	      WHERE owner = ?`
	var row databaseMetadataRow
	err := db.QueryRowContext(ctx, q, cfg.Owner).Scan(&row.firstApplied, &row.lastApplied)
	require.NoError(t, err)
	return row
}
