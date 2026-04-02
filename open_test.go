package chuck

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/chuck/driver/sqlite"
)

func TestOpenURLUnsupportedScheme(t *testing.T) {
	ctx := context.Background()
	_, _, err := OpenURL(ctx, "mysql://user:pass@localhost/db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database scheme")
}

func TestOpenURLInvalidURL(t *testing.T) {
	ctx := context.Background()
	_, _, err := OpenURL(ctx, "://bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse database URL")
}

func TestOpenURLSQLiteMemory(t *testing.T) {
	ctx := context.Background()
	db, d, err := OpenURL(ctx, "sqlite://:memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	assert.NoError(t, db.PingContext(ctx))
}

func TestOpenURLSQLiteFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, d, err := OpenURL(ctx, "sqlite://"+dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	assert.NoError(t, db.PingContext(ctx))
}

func TestOpenSQLiteMemory(t *testing.T) {
	ctx := context.Background()
	db, d, err := OpenSQLite(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	assert.NoError(t, db.PingContext(ctx))
}

func TestOpenSQLiteFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, d, err := OpenSQLite(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	assert.NoError(t, db.PingContext(ctx))

	// Verify file was created
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
}

func TestOpenSQLiteCreatesDirectory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "subdir", "nested", "test.db")

	db, d, err := OpenSQLite(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
}

func TestOpenSQLiteWALMode(t *testing.T) {
	ctx := context.Background()
	db, _, err := OpenSQLite(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var journalMode string
	err = db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	// In-memory databases may report "memory" instead of "wal"
	assert.Contains(t, []string{"wal", "memory"}, journalMode)
}

func TestOpenSQLiteConnectionPool(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pool_test.db")

	db, _, err := OpenSQLite(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	assert.Equal(t, 1, stats.MaxOpenConnections)
}

func TestOpenURLSQLite3Prefix(t *testing.T) {
	// Test sqlite3:// prefix is also recognized
	ctx := context.Background()
	db, d, err := OpenURL(ctx, "sqlite3://:memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	assert.Equal(t, SQLite, d.Engine())
	assert.NoError(t, db.PingContext(ctx))
}

func TestOpenURLSQLiteEmptyPath(t *testing.T) {
	ctx := context.Background()
	_, _, err := OpenURL(ctx, "sqlite://")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty sqlite path")
}

func TestOpenURLPostgresSchemeNotPingable(t *testing.T) {
	// Verifies that the postgres scheme is recognized and fails at ping, not at scheme detection.
	ctx := context.Background()
	_, _, err := OpenURL(ctx, "postgres://user:pass@localhost:5432/db?sslmode=disable")
	require.Error(t, err)
	// Should fail at ping, not at scheme detection
	assert.NotContains(t, err.Error(), "unsupported database scheme")
}

func TestOpenURLSQLServerSchemeNotPingable(t *testing.T) {
	// Verifies that the sqlserver scheme is recognized and fails at ping, not at scheme detection.
	ctx := context.Background()
	_, _, err := OpenURL(ctx, "sqlserver://user:pass@localhost:1433?database=db")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "unsupported database scheme")
}

func TestOpenSQLiteInvalidPath(t *testing.T) {
	// Attempt to open a path in a directory we can't create (read-only root path).
	// Instead, verify OpenSQLite fails gracefully when MkdirAll fails.
	// On Linux, writing to /proc/invalid is not possible.
	ctx := context.Background()
	_, _, err := OpenSQLite(ctx, "/proc/chuck_test_invalid_dir/test.db")
	require.Error(t, err)
}

func TestOpenSQLiteReturnsDialect(t *testing.T) {
	ctx := context.Background()
	db, d, err := OpenSQLite(ctx, ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Dialect should be usable for SQL generation
	assert.Equal(t, "INTEGER", d.BoolType())
	assert.Equal(t, "?", d.Placeholder(1))
	assert.Equal(t, "RETURNING id", d.ReturningClause("id"))
}

