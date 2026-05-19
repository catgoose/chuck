package schema

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/catgoose/chuck"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripCreateViewPreamble(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "sqlite bare CREATE VIEW",
			in:   `CREATE VIEW v_active AS SELECT id FROM users WHERE active = 1`,
			want: `SELECT id FROM users WHERE active = 1`,
		},
		{
			name: "mssql bracketed schema-qualified",
			in:   `CREATE VIEW [sg].[v_active] AS SELECT [id] FROM [sg].[Users]`,
			want: `SELECT [id] FROM [sg].[Users]`,
		},
		{
			name: "mssql CREATE OR ALTER",
			in:   `CREATE OR ALTER VIEW [sg].[v_active] AS SELECT 1 AS Probe`,
			want: `SELECT 1 AS Probe`,
		},
		{
			name: "postgres CREATE OR REPLACE",
			in:   `CREATE OR REPLACE VIEW "sg"."v_active" AS SELECT id FROM users`,
			want: `SELECT id FROM users`,
		},
		{
			name: "no preamble returned as-is (postgres pg_get_viewdef style)",
			in:   ` SELECT users.id FROM users WHERE users.active = true;`,
			want: ` SELECT users.id FROM users WHERE users.active = true;`,
		},
		{
			name: "multiline preamble strip",
			in:   "CREATE VIEW v\n  AS\n  SELECT 1",
			want: "SELECT 1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stripCreateViewPreamble(tc.in))
		})
	}
}

func TestCanonicalizeStatement(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"trailing semicolon", "SELECT 1;", "SELECT 1"},
		{"whitespace runs collapse", "SELECT\n\t1   FROM   t", "SELECT 1 FROM t"},
		{"leading and trailing trim", "   SELECT 1   ", "SELECT 1"},
		{"multiple trailing semicolons stripped", "SELECT 1;;;  ", "SELECT 1"},
		{"empty input", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, canonicalizeStatement(tc.in))
		})
	}
}

func TestValidateView_SQLite_Match(t *testing.T) {
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER)`)
	v := NewView("v_active_users", "SELECT id FROM users WHERE active = 1")
	require.NoError(t, ApplyView(ctx, db, d, v))

	require.NoError(t, ValidateView(ctx, db, d, v))
}

func TestValidateView_SQLite_Missing(t *testing.T) {
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	v := NewView("v_missing", "SELECT 1")
	err := ValidateView(ctx, db, d, v)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewMissing),
		"missing-only ViewDriftError must unwrap to ErrViewMissing for errors.Is branching")
	var drift *ViewDriftError
	require.ErrorAs(t, err, &drift)
	require.Len(t, drift.Drifts, 1)
	assert.True(t, drift.Drifts[0].Missing)
	assert.Equal(t, "v_missing", drift.Drifts[0].Object.Name)
}

func TestValidateView_SQLite_BodyDrift(t *testing.T) {
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER)`)
	declared := NewView("v_users", "SELECT id FROM users WHERE active = 1")
	require.NoError(t, ApplyView(ctx, db, d, declared))

	// Stomp the live view with a different body to simulate drift.
	mustExec(t, db, `DROP VIEW v_users`)
	mustExec(t, db, `CREATE VIEW v_users AS SELECT id FROM users WHERE active = 0`)

	err := ValidateView(ctx, db, d, declared)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewBodyDrift))
	var drift *ViewDriftError
	require.ErrorAs(t, err, &drift)
	require.Len(t, drift.Drifts, 1)
	assert.True(t, drift.Drifts[0].BodyMismatch)
	assert.NotEqual(t, drift.Drifts[0].DeclaredBody, drift.Drifts[0].LiveBody)
}

func TestValidateView_SQLite_WhitespaceTolerant(t *testing.T) {
	// canonicalizeStatement collapses whitespace, so a declared body with
	// different indentation than the live body must still validate as match.
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE VIEW v_users AS SELECT    id
	FROM users`)

	declared := NewView("v_users", "SELECT id FROM users")
	require.NoError(t, ValidateView(ctx, db, d, declared))
}

func TestValidateViews_SQLite_AggregatesDrift(t *testing.T) {
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	good := NewView("v_good", "SELECT id FROM t")
	require.NoError(t, ApplyView(ctx, db, d, good))

	missing := NewView("v_missing", "SELECT 1")

	err := ValidateViews(ctx, db, d, good, missing)
	require.Error(t, err)
	var drift *ViewDriftError
	require.ErrorAs(t, err, &drift)
	// Only the missing view should land in Drifts; good shouldn't.
	require.Len(t, drift.Drifts, 1)
	assert.Equal(t, "v_missing", drift.Drifts[0].Object.Name)
	assert.True(t, drift.Drifts[0].Missing)
}

func TestApplyView_SQLite_IdempotentReplaceBody(t *testing.T) {
	// ApplyView on SQLite issues DROP IF EXISTS + CREATE — running it twice
	// against the same declared body must succeed, and running it after the
	// body changes must update the live view to match.
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, active INTEGER)`)
	v1 := NewView("v_t", "SELECT id FROM t WHERE active = 1")
	require.NoError(t, ApplyView(ctx, db, d, v1))
	require.NoError(t, ApplyView(ctx, db, d, v1))

	v2 := NewView("v_t", "SELECT id FROM t WHERE active = 0")
	require.NoError(t, ApplyView(ctx, db, d, v2))
	require.NoError(t, ValidateView(ctx, db, d, v2))

	// Validating against the original declaration now reports drift.
	err := ValidateView(ctx, db, d, v1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewBodyDrift))
}

func TestViewDriftError_Unwrap_AllBodyComparisonSkipped(t *testing.T) {
	// When every drift is a Postgres-style body-comparison-skip, the aggregate
	// error must unwrap to ErrViewBodyComparisonUnsupported so callers that
	// want existence-only semantics on those engines can branch on the
	// sentinel and treat it as success.
	e := &ViewDriftError{Drifts: []ViewDrift{
		{Object: chuck.ObjectName{Name: "v_a"}, BodyComparisonSkipped: true, Reason: "pg_get_viewdef canonicalization"},
		{Object: chuck.ObjectName{Name: "v_b"}, BodyComparisonSkipped: true, Reason: "pg_get_viewdef canonicalization"},
	}}
	assert.True(t, errors.Is(e, ErrViewBodyComparisonUnsupported))
	assert.False(t, errors.Is(e, ErrViewMissing))
	assert.False(t, errors.Is(e, ErrViewBodyDrift))
}

func TestViewDriftError_Unwrap_Mixed_SkippedAndMissing_NoWrap(t *testing.T) {
	// Mixed missing + body-comparison-skipped must not wrap either sentinel:
	// the existence failure is a hard error and must not be hidden behind the
	// existence-only escape hatch.
	e := &ViewDriftError{Drifts: []ViewDrift{
		{Object: chuck.ObjectName{Name: "v_a"}, Missing: true},
		{Object: chuck.ObjectName{Name: "v_b"}, BodyComparisonSkipped: true},
	}}
	assert.False(t, errors.Is(e, ErrViewMissing))
	assert.False(t, errors.Is(e, ErrViewBodyDrift))
	assert.False(t, errors.Is(e, ErrViewBodyComparisonUnsupported))
}

func TestApplyViews_SQLite_OrderRespected(t *testing.T) {
	ctx, db, d := openSQLiteForViewLifecycle(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	// v_outer reads v_inner, so v_inner must be created first; the helper
	// must preserve caller-supplied order rather than re-sorting.
	vInner := NewView("v_inner", "SELECT id FROM t")
	vOuter := NewView("v_outer", "SELECT id FROM v_inner")

	require.NoError(t, ApplyViews(ctx, db, d, vInner, vOuter))
	require.NoError(t, ValidateView(ctx, db, d, vInner))
	require.NoError(t, ValidateView(ctx, db, d, vOuter))
}

func openSQLiteForViewLifecycle(t *testing.T) (context.Context, *sql.DB, chuck.Dialect) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	return context.Background(), db, chuck.SQLiteDialect{}
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), stmt)
	require.NoError(t, err, stmt)
}
