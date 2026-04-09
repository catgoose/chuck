package schema

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/catgoose/chuck"
)

// Mode controls how Ensure handles schema state.
type Mode int

const (
	// ModeStrict validates only. Any drift or missing tables cause an error.
	// Use in production.
	ModeStrict Mode = iota

	// ModeDev creates missing tables and seeds them, but errors on drift
	// in existing tables. Use in development.
	ModeDev

	// ModeDryRun reports all drift without making any changes.
	// Use for pre-deploy checks.
	ModeDryRun
)

// String returns the mode name.
func (m Mode) String() string {
	switch m {
	case ModeStrict:
		return "strict"
	case ModeDev:
		return "dev"
	case ModeDryRun:
		return "dryrun"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// EnsureOption configures Ensure behavior.
type EnsureOption func(*ensureConfig)

type ensureConfig struct {
	mode       Mode
	diffOutput io.Writer
	diffPath   string
}

// WithMode sets the ensure mode. Default is ModeStrict.
func WithMode(m Mode) EnsureOption {
	return func(c *ensureConfig) {
		c.mode = m
	}
}

// WithDiffOutput writes structured JSON diffs to the given writer when drift is found.
func WithDiffOutput(w io.Writer) EnsureOption {
	return func(c *ensureConfig) {
		c.diffOutput = w
	}
}

// WithDiffFile writes structured JSON diffs to the given file path when drift is found.
func WithDiffFile(path string) EnsureOption {
	return func(c *ensureConfig) {
		c.diffPath = path
	}
}

// EnsureResult contains the outcome of an Ensure call.
type EnsureResult struct {
	// TablesCreated lists tables that were auto-created (ModeDev only).
	TablesCreated []string
	// TablesSeeded lists tables that had seed data applied (ModeDev only).
	TablesSeeded []string
	// Diffs contains structured diffs for any tables with drift.
	Diffs []*SchemaDiff
}

// EnsureError is returned when Ensure detects schema drift.
type EnsureError struct {
	Diffs []*SchemaDiff
}

func (e *EnsureError) Error() string {
	var parts []string
	for _, d := range e.Diffs {
		if d.TableMissing {
			parts = append(parts, fmt.Sprintf("table %q: missing", d.Table))
			continue
		}
		var issues []string
		if len(d.AddedColumns) > 0 {
			names := make([]string, len(d.AddedColumns))
			for i, c := range d.AddedColumns {
				names[i] = c.Name
			}
			issues = append(issues, fmt.Sprintf("%d missing column(s): %s", len(d.AddedColumns), strings.Join(names, ", ")))
		}
		if len(d.RemovedColumns) > 0 {
			issues = append(issues, fmt.Sprintf("%d extra column(s): %s", len(d.RemovedColumns), strings.Join(d.RemovedColumns, ", ")))
		}
		if len(d.ChangedColumns) > 0 {
			names := make([]string, len(d.ChangedColumns))
			for i, c := range d.ChangedColumns {
				names[i] = c.Name
			}
			issues = append(issues, fmt.Sprintf("%d changed column(s): %s", len(d.ChangedColumns), strings.Join(names, ", ")))
		}
		if len(d.MissingIndexes) > 0 {
			names := make([]string, len(d.MissingIndexes))
			for i, idx := range d.MissingIndexes {
				names[i] = idx.Name
			}
			issues = append(issues, fmt.Sprintf("%d missing index(es): %s", len(d.MissingIndexes), strings.Join(names, ", ")))
		}
		if len(d.ExtraIndexes) > 0 {
			issues = append(issues, fmt.Sprintf("%d extra index(es): %s", len(d.ExtraIndexes), strings.Join(d.ExtraIndexes, ", ")))
		}
		if len(d.ChangedIndexes) > 0 {
			names := make([]string, len(d.ChangedIndexes))
			for i, idx := range d.ChangedIndexes {
				names[i] = idx.Name
			}
			issues = append(issues, fmt.Sprintf("%d changed index(es): %s", len(d.ChangedIndexes), strings.Join(names, ", ")))
		}
		parts = append(parts, fmt.Sprintf("table %q: %s", d.Table, strings.Join(issues, "; ")))
	}
	return fmt.Sprintf("schema drift detected: %s", strings.Join(parts, ", "))
}

// Ensure validates (and optionally bootstraps) the database schema for all given tables.
func Ensure(ctx context.Context, db *sql.DB, d chuck.Dialect, tables []*TableDef, opts ...EnsureOption) (*EnsureResult, error) {
	cfg := &ensureConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	result := &EnsureResult{}

	switch cfg.mode {
	case ModeStrict:
		return ensureStrict(ctx, db, d, tables, cfg, result)
	case ModeDev:
		return ensureDev(ctx, db, d, tables, cfg, result)
	case ModeDryRun:
		return ensureDryRun(ctx, db, d, tables, cfg, result)
	default:
		return nil, fmt.Errorf("unknown ensure mode: %d", int(cfg.mode))
	}
}

func ensureStrict(ctx context.Context, db *sql.DB, d chuck.Dialect, tables []*TableDef, cfg *ensureConfig, result *EnsureResult) (*EnsureResult, error) {
	var drifted []*SchemaDiff
	for _, td := range tables {
		diff, err := DiffSchema(ctx, db, d, td)
		if err != nil {
			return nil, err
		}
		if diff.HasDrift {
			drifted = append(drifted, diff)
		}
	}

	if len(drifted) > 0 {
		result.Diffs = drifted
		writeDiffs(drifted, cfg)
		return result, &EnsureError{Diffs: drifted}
	}
	return result, nil
}

func ensureDev(ctx context.Context, db *sql.DB, d chuck.Dialect, tables []*TableDef, cfg *ensureConfig, result *EnsureResult) (*EnsureResult, error) {
	ordered, err := CreationOrder(tables...)
	if err != nil {
		return nil, fmt.Errorf("ensure dev: %w", err)
	}

	var drifted []*SchemaDiff
	for _, td := range ordered {
		tableName := d.NormalizeIdentifier(td.Name)

		// Check if table exists by attempting a diff
		diff, err := DiffSchema(ctx, db, d, td)
		if err != nil {
			return nil, err
		}

		if diff.TableMissing {
			// Create the table
			for _, stmt := range td.CreateIfNotExistsSQL(d) {
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					return nil, fmt.Errorf("create table %q: %w", tableName, err)
				}
			}
			result.TablesCreated = append(result.TablesCreated, tableName)

			// Seed the table
			if td.HasSeedData() {
				for _, stmt := range td.SeedSQL(d) {
					if _, err := db.ExecContext(ctx, stmt); err != nil {
						return nil, fmt.Errorf("seed table %q: %w", tableName, err)
					}
				}
				result.TablesSeeded = append(result.TablesSeeded, tableName)
			}
			continue
		}

		if diff.HasDrift {
			drifted = append(drifted, diff)
		}
	}

	if len(drifted) > 0 {
		result.Diffs = drifted
		writeDiffs(drifted, cfg)
		return result, &EnsureError{Diffs: drifted}
	}
	return result, nil
}

func ensureDryRun(ctx context.Context, db *sql.DB, d chuck.Dialect, tables []*TableDef, cfg *ensureConfig, result *EnsureResult) (*EnsureResult, error) {
	for _, td := range tables {
		diff, err := DiffSchema(ctx, db, d, td)
		if err != nil {
			return nil, err
		}
		result.Diffs = append(result.Diffs, diff)
	}

	// Write diffs if any have drift
	var drifted []*SchemaDiff
	for _, diff := range result.Diffs {
		if diff.HasDrift {
			drifted = append(drifted, diff)
		}
	}
	if len(drifted) > 0 {
		writeDiffs(drifted, cfg)
	}

	return result, nil
}

func writeDiffs(diffs []*SchemaDiff, cfg *ensureConfig) {
	if cfg.diffOutput != nil {
		//nolint:errcheck // diff output is best-effort; callers opting into cfg.diffOutput accept silent failure
		WriteDiffsTo(diffs, cfg.diffOutput)
	}
	if cfg.diffPath != "" {
		//nolint:errcheck // diff output is best-effort; callers opting into cfg.diffPath accept silent failure
		WriteDiffsJSON(diffs, cfg.diffPath)
	}
}
