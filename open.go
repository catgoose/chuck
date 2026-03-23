package fraggle

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// NOTE: This package does not register any database drivers. Import the
// driver sub-packages for the engines you need:
//
//	import _ "github.com/catgoose/fraggle/driver/sqlite"
//	import _ "github.com/catgoose/fraggle/driver/postgres"
//	import _ "github.com/catgoose/fraggle/driver/mssql"

// OpenURL opens a database connection from a URL string. The scheme determines
// the driver and dialect:
//
//	postgres://user:pass@host:5432/dbname?sslmode=disable
//	sqlite:///path/to/db.sqlite  or  sqlite:///:memory:
//	sqlserver://user:pass@host:1433?database=dbname
//
// Returns the raw *sql.DB and the matching Dialect for SQL generation.
func OpenURL(ctx context.Context, dsn string) (*sql.DB, Dialect, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("parse database URL: %w", err)
	}

	var engine Engine
	switch u.Scheme {
	case "postgres", "postgresql":
		engine = Postgres
	case "sqlite", "sqlite3":
		engine = SQLite
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
	if engine == SQLite {
		connectStr = u.Host + u.Path
		if connectStr == "" {
			connectStr = u.Opaque
		}
		driverName = "sqlite3"
	}

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
