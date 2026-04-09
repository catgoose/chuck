package chuck

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// NOTE: This package does not register any database drivers. Import the
// driver sub-packages for the engines you need:
//
//	import _ "github.com/catgoose/chuck/driver/sqlite"
//	import _ "github.com/catgoose/chuck/driver/postgres"
//	import _ "github.com/catgoose/chuck/driver/mssql"

// OpenURL opens a database connection from a URL string. The scheme determines
// the driver and dialect:
//
//	postgres://user:pass@host:5432/dbname?sslmode=disable
//	sqlite:///path/to/db.sqlite  or  sqlite:///:memory:
//	sqlite://:memory:            or  sqlite3://:memory:
//	sqlserver://user:pass@host:1433?database=dbname
//
// For SQLite, both two-slash (sqlite://:memory:) and three-slash
// (sqlite:///:memory:, sqlite:///path/to/db) forms are accepted. The
// three-slash form follows the standard URI convention where the path
// begins after the authority component.
//
// Returns the raw *sql.DB and the matching Dialect for SQL generation.
func OpenURL(ctx context.Context, dsn string) (*sql.DB, Dialect, error) {
	// SQLite URLs need special handling — paths like ":memory:" aren't valid URL hosts.
	for _, prefix := range []string{"sqlite://", "sqlite3://"} {
		if strings.HasPrefix(dsn, prefix) {
			path := strings.TrimPrefix(dsn, prefix)
			if path == "" {
				return nil, nil, fmt.Errorf("empty sqlite path in URL %q", dsn)
			}
			// Handle three-slash form: sqlite:///:memory:
			// After stripping "sqlite://", "/:memory:" remains. The leading
			// slash is the URI path separator (empty authority), not part of
			// the SQLite identifier, so strip it for special names like :memory:.
			// Absolute file paths (e.g. sqlite:///tmp/db → /tmp/db) keep
			// their leading slash because it is part of the filesystem path.
			if strings.HasPrefix(path, "/:memory:") {
				path = path[1:]
			}
			return openSQLiteFromURL(ctx, path)
		}
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("parse database URL: %w", err)
	}

	var engine Engine
	switch u.Scheme {
	case "postgres", "postgresql":
		engine = Postgres
	case "sqlserver", "mssql":
		engine = MSSQL
	default:
		return nil, nil, fmt.Errorf("unsupported database scheme: %q", u.Scheme)
	}

	d, err := New(engine)
	if err != nil {
		return nil, nil, err
	}

	driverName := string(engine)
	connectStr := dsn

	db, err := sql.Open(driverName, connectStr)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", engine, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping %s: %w", engine, err)
	}
	return db, d, nil
}

// openSQLiteFromURL opens a SQLite database from a URL path (after stripping the scheme).
// It applies the same defaults as OpenSQLite — WAL journal mode, 30s busy timeout,
// the conservative connection pool, and parent directory creation for file-backed paths —
// by delegating to openSQLiteWithDefaults.
func openSQLiteFromURL(ctx context.Context, path string) (*sql.DB, Dialect, error) {
	return openSQLiteWithDefaults(ctx, path)
}

// OpenSQLite opens a SQLite database at the given path with standard settings:
// WAL journal mode, 30s busy timeout, and conservative pool settings (1 conn).
// Returns the raw *sql.DB and the SQLite Dialect.
func OpenSQLite(ctx context.Context, dbPath string) (*sql.DB, Dialect, error) {
	return openSQLiteWithDefaults(ctx, dbPath)
}

// openSQLiteWithDefaults opens a SQLite database at the given path and applies
// chuck's standard SQLite settings: parent directory creation (skipped for
// ":memory:"), WAL journal mode, a 30s busy timeout, and a conservative
// connection pool (one connection). It is shared by OpenSQLite and
// openSQLiteFromURL so URL-based opens get the same defaults as direct ones.
func openSQLiteWithDefaults(ctx context.Context, path string) (*sql.DB, Dialect, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, nil, fmt.Errorf("failed to create database directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=30000"); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to ping SQLite database: %w", err)
	}

	return db, SQLiteDialect{}, nil
}
