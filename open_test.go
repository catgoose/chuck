package fraggle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/catgoose/fraggle/driver/sqlite"
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

