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
//	sqlserver://user:pass@host:1433?database=dbname
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
// For :memory: and file paths, it opens a basic connection without the WAL/pool settings
// that OpenSQLite applies — use OpenSQLite directly for production SQLite connections.
func openSQLiteFromURL(ctx context.Context, path string) (*sql.DB, Dialect, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, SQLiteDialect{}, nil
}

// OpenSQLite opens a SQLite database at the given path with standard settings:
// WAL journal mode, 30s busy timeout, and conservative pool settings (1 conn).
// Returns the raw *sql.DB and the SQLite Dialect.
func OpenSQLite(ctx context.Context, dbPath string) (*sql.DB, Dialect, error) {
	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, nil, fmt.Errorf("failed to create database directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite3", dbPath)
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
