package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/catgoose/chuck"
)

// ErrMetadataOwnerMissing is returned by metadata helpers when MetadataConfig
// is populated but Owner is empty. Owner identifies whose apply path is
// writing the row; with no owner the ledger cannot tell two callers apart, so
// the helper fails loud rather than recording an ambiguous row.
var ErrMetadataOwnerMissing = errors.New("schema: metadata owner is required")

// Object-type constants used in chuck_object_metadata.object_type. Kept as
// stable strings rather than an enum so the ledger stays human-readable when
// inspected directly in the database.
const (
	MetadataObjectTypeView      = "view"
	MetadataObjectTypeProcedure = "procedure"
)

// Default unqualified table names for the snapshot ledger. Callers can set
// MetadataConfig.Schema to namespace these under an owned schema; when Schema
// is empty the tables land in the engine's default namespace (e.g. "main" on
// SQLite, "public" on Postgres, "dbo" on MSSQL).
const (
	DefaultDatabaseMetadataTableName = "chuck_database_metadata"
	DefaultObjectMetadataTableName   = "chuck_object_metadata"
)

// MetadataConfig opts an apply-helper call into recording an entry in chuck's
// snapshot operational metadata ledger. Zero value is not usable: Owner is
// required.
//
// The ledger is **opt-in** and **snapshot only**. ApplyViewsWithOptions and
// ApplyProceduresWithOptions write rows when opts.Metadata is non-nil and a
// successful apply has just happened; otherwise the ledger is not touched.
// Validate* helpers do not read or compare metadata rows in this first pass.
//
// Tables are caller-bootstrapped: call EnsureMetadataTables(ctx, db, d, cfg)
// once during your own bootstrap before the first opt-in apply. The apply
// path assumes the tables exist and surfaces driver errors verbatim if they
// do not.
//
// Provenance fields (SourceRepo, SourceRev, ToolVersion) are optional. When
// empty they are stored as SQL NULL so the ledger can distinguish "not
// recorded" from "recorded as empty string".
type MetadataConfig struct {
	// Owner identifies the caller writing rows into the ledger. Recorded
	// on every row in both ledger tables. Required.
	Owner string

	// Schema namespaces the ledger tables. Empty means the engine's default
	// namespace. SQLite ignores this because it has no schema namespace.
	Schema string

	// SourceRepo, SourceRev, ToolVersion are optional provenance recorded
	// on each row. Empty values are stored as SQL NULL.
	SourceRepo  string
	SourceRev   string
	ToolVersion string

	// Now is the clock used for first_applied_at_utc / last_applied_at_utc /
	// last_changed_at_utc. When nil, time.Now().UTC() is used. Tests use
	// this seam to assert deterministic snapshot rows.
	Now func() time.Time
}

func (c MetadataConfig) clock() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c MetadataConfig) validate() error {
	if strings.TrimSpace(c.Owner) == "" {
		return ErrMetadataOwnerMissing
	}
	return nil
}

// databaseMetadataObject returns the ObjectName the ledger uses for the
// per-owner snapshot row table.
func (c MetadataConfig) databaseMetadataObject() chuck.ObjectName {
	return chuck.ObjectName{Schema: c.Schema, Name: DefaultDatabaseMetadataTableName}
}

// objectMetadataObject returns the ObjectName the ledger uses for the
// per-owned-object snapshot row table.
func (c MetadataConfig) objectMetadataObject() chuck.ObjectName {
	return chuck.ObjectName{Schema: c.Schema, Name: DefaultObjectMetadataTableName}
}

// EnsureMetadataTables creates chuck_database_metadata and
// chuck_object_metadata if absent. Safe to run repeatedly; uses
// CREATE TABLE IF NOT EXISTS semantics on every engine. Returns
// ErrMetadataOwnerMissing if Owner is empty so callers see the same
// loud-failure shape as the apply path.
//
// Tables are created in cfg.Schema when set; on engines that lack schema
// namespacing (SQLite) the schema is dropped silently to match how the rest
// of chuck handles unscoped identifiers. Callers running an opt-in apply
// path against a schema-scoped ledger are responsible for creating the
// schema itself first — chuck does not implicitly CREATE SCHEMA.
func EnsureMetadataTables(ctx context.Context, db *sql.DB, d chuck.Dialect, cfg MetadataConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	for _, stmt := range metadataCreateStatements(d, cfg) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: ensure metadata table: %w", err)
		}
	}
	return nil
}

// metadataCreateStatements returns the dialect-correct CREATE TABLE IF NOT
// EXISTS statements for both ledger tables, in the order they should be
// executed.
func metadataCreateStatements(d chuck.Dialect, cfg MetadataConfig) []string {
	ts := d.TimestampType()
	textPK := metadataTextPKType(d)
	textCol := d.TextType()

	dbBody := strings.Join([]string{
		fmt.Sprintf("\t\t\t%s %s NOT NULL PRIMARY KEY", d.QuoteIdentifier("owner"), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("first_applied_at_utc"), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("last_applied_at_utc"), ts),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("source_repo"), textCol),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("source_rev"), textCol),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("tool_version"), textCol),
	}, ",\n")

	objBody := strings.Join([]string{
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("owner"), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("object_type"), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("object_schema"), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("object_name"), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("first_applied_at_utc"), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("last_applied_at_utc"), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("last_changed_at_utc"), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier("definition_hash"), textPK),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("source_repo"), textCol),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("source_rev"), textCol),
		fmt.Sprintf("\t\t\t%s %s NULL", d.QuoteIdentifier("tool_version"), textCol),
		fmt.Sprintf("\t\t\tPRIMARY KEY (%s, %s, %s, %s)",
			d.QuoteIdentifier("owner"),
			d.QuoteIdentifier("object_type"),
			d.QuoteIdentifier("object_schema"),
			d.QuoteIdentifier("object_name"),
		),
	}, ",\n")

	return []string{
		createTableIfNotExistsSQL(d, cfg.databaseMetadataObject(), dbBody),
		createTableIfNotExistsSQL(d, cfg.objectMetadataObject(), objBody),
	}
}

// metadataTextPKType returns a sized text type suitable for primary-key
// columns. MSSQL forbids NVARCHAR(MAX) in a PRIMARY KEY, so on that engine we
// pick a generous-but-bounded size; SQLite/Postgres accept TEXT directly.
func metadataTextPKType(d chuck.Dialect) string {
	if d.Engine() == chuck.MSSQL {
		return d.StringType(450)
	}
	return d.TextType()
}

// hashCodeObjectDefinition returns the sha256 hex digest of the canonical
// form of a body/definition string. Canonicalization mirrors what
// ValidateView / ValidateProcedure do for drift comparison: leading
// block-comment front matter is stripped, then whitespace is collapsed. This
// keeps the hash stable across comment-only changes — matching the
// validation contract introduced in PR #98 — so writes only update
// last_changed_at_utc when the executable text actually differs.
func hashCodeObjectDefinition(body string) string {
	canon := canonicalizeStatement(stripLeadingBlockComments(body))
	sum := sha256.Sum256([]byte(canon))
	return hex.EncodeToString(sum[:])
}

// recordCodeObjectMetadata writes one row into chuck_object_metadata and
// upserts the per-owner row in chuck_database_metadata. Called by the apply
// path after a successful Exec, never on failure.
//
// Snapshot semantics:
//   - new row: first_applied = last_applied = last_changed = now
//   - existing row, same hash: only last_applied advances
//   - existing row, different hash: last_applied AND last_changed advance,
//     definition_hash is replaced; first_applied is preserved.
//
// The two-step read-then-upsert is intentional: it keeps the per-engine SQL
// readable instead of stuffing CASE/MERGE conditional updates inline. The
// ledger is best-effort accounting, not a coordination primitive — callers
// running concurrent applies against the same owner accept that two writers
// may stomp on each other's last_applied.
func recordCodeObjectMetadata(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType string,
	obj chuck.ObjectName,
	definitionHash string,
) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	now := cfg.clock().UTC()
	schemaCol, nameCol := metadataObjectColumns(d, obj)
	if err := upsertObjectRow(ctx, db, d, cfg, objectType, schemaCol, nameCol, definitionHash, now); err != nil {
		return err
	}
	return upsertDatabaseRow(ctx, db, d, cfg, now)
}

// metadataNoticePointer returns the single-line ledger pointer sentence the
// ownership-notice augmentation appends to the rendered chuck-owned comment
// when snapshot metadata is enabled. The pointer references the dialect-
// qualified, quoted form of the chuck_object_metadata table so DB readers
// reading the live SQL can jump straight to the ledger row that records
// this object's provenance. SQLite drops the schema component per the
// standard QualifyTable contract; Postgres / MSSQL render schema-qualified
// when cfg.Schema is set and bare otherwise.
//
// Lives next to MetadataConfig so the table-name rendering stays close to
// the ledger's own DDL.
func metadataNoticePointer(d chuck.Dialect, cfg MetadataConfig) string {
	return "Provenance recorded in " + qualifyTable(d, cfg.objectMetadataObject()) + "."
}

// effectiveOptionsForRender returns opts with OwnershipNotice augmented by a
// metadata-ledger pointer line when both OwnershipNotice is non-empty and
// opts.Metadata is non-nil. Apply and validate paths both call this so the
// rendered comment text and the strict-strip configured-prefix recognition
// stay symmetric — what apply writes, the validate strict-strip sees and
// removes intact rather than relying on the broader leading-comment strip.
//
// When OwnershipNotice is empty no pointer is added: the contract is that a
// metadata pointer rides along with an existing ownership notice rather than
// inventing a fresh ownership block on metadata's behalf.
func effectiveOptionsForRender(d chuck.Dialect, opts CodeObjectOptions) CodeObjectOptions {
	if opts.OwnershipNotice == "" || opts.Metadata == nil {
		return opts
	}
	opts.OwnershipNotice = opts.OwnershipNotice + "\n" + metadataNoticePointer(d, *opts.Metadata)
	return opts
}

// metadataObjectColumns returns the (object_schema, object_name) pair used to
// key chuck_object_metadata rows for the given owned object. The schema
// component reuses the same default-resolution contract as the live-database
// inspection paths (resolveSchemaForInspection): unqualified declarations
// resolve to "public" on Postgres and "dbo" on MSSQL so the ledger row keys
// match where the live object physically lives, while SQLite continues to
// record an empty schema because SQLite has no schema namespace. The name
// component is dialect-normalized identically to inspection.
//
// Without this resolution an unqualified declaration on Postgres or MSSQL
// would record object_schema="" even though the live object lives under
// "public" / "dbo". That would misstate identity and split one physical
// object across two ledger rows if a caller later switched from a bare name
// to the explicit default schema.
func metadataObjectColumns(d chuck.Dialect, obj chuck.ObjectName) (objectSchema, objectName string) {
	_, objectName = normalizedObject(d, obj)
	objectSchema = resolveSchemaForInspection(d, obj)
	return
}

// upsertObjectRow performs the read-then-write for chuck_object_metadata.
func upsertObjectRow(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType, objectSchema, objectName, definitionHash string,
	now time.Time,
) error {
	qt := qualifyTable(d, cfg.objectMetadataObject())
	selectSQL := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s = %s AND %s = %s AND %s = %s AND %s = %s",
		d.QuoteIdentifier("first_applied_at_utc"),
		d.QuoteIdentifier("definition_hash"),
		qt,
		d.QuoteIdentifier("owner"), d.Placeholder(1),
		d.QuoteIdentifier("object_type"), d.Placeholder(2),
		d.QuoteIdentifier("object_schema"), d.Placeholder(3),
		d.QuoteIdentifier("object_name"), d.Placeholder(4),
	)
	var (
		existingFirst sql.NullTime
		existingHash  sql.NullString
	)
	err := db.QueryRowContext(ctx, selectSQL, cfg.Owner, objectType, objectSchema, objectName).
		Scan(&existingFirst, &existingHash)
	switch err {
	case sql.ErrNoRows:
		return insertObjectRow(ctx, db, d, cfg, objectType, objectSchema, objectName, definitionHash, now)
	case nil:
		hashChanged := !existingHash.Valid || existingHash.String != definitionHash
		return updateObjectRow(ctx, db, d, cfg, objectType, objectSchema, objectName, definitionHash, now, hashChanged)
	default:
		return fmt.Errorf("schema: read object metadata row: %w", err)
	}
}

func insertObjectRow(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType, objectSchema, objectName, definitionHash string,
	now time.Time,
) error {
	qt := qualifyTable(d, cfg.objectMetadataObject())
	cols := []string{
		"owner", "object_type", "object_schema", "object_name",
		"first_applied_at_utc", "last_applied_at_utc", "last_changed_at_utc",
		"definition_hash", "source_repo", "source_rev", "tool_version",
	}
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = d.Placeholder(i + 1)
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qt, strings.Join(quoted, ", "), strings.Join(placeholders, ", "),
	)
	_, err := db.ExecContext(ctx, stmt,
		cfg.Owner, objectType, objectSchema, objectName,
		now, now, now,
		definitionHash,
		nullableString(cfg.SourceRepo), nullableString(cfg.SourceRev), nullableString(cfg.ToolVersion),
	)
	if err != nil {
		return fmt.Errorf("schema: insert object metadata row: %w", err)
	}
	return nil
}

func updateObjectRow(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType, objectSchema, objectName, definitionHash string,
	now time.Time,
	hashChanged bool,
) error {
	qt := qualifyTable(d, cfg.objectMetadataObject())
	sets := []string{
		fmt.Sprintf("%s = %s", d.QuoteIdentifier("last_applied_at_utc"), d.Placeholder(1)),
		fmt.Sprintf("%s = %s", d.QuoteIdentifier("source_repo"), d.Placeholder(2)),
		fmt.Sprintf("%s = %s", d.QuoteIdentifier("source_rev"), d.Placeholder(3)),
		fmt.Sprintf("%s = %s", d.QuoteIdentifier("tool_version"), d.Placeholder(4)),
	}
	args := []any{
		now,
		nullableString(cfg.SourceRepo), nullableString(cfg.SourceRev), nullableString(cfg.ToolVersion),
	}
	next := 5
	if hashChanged {
		sets = append(sets,
			fmt.Sprintf("%s = %s", d.QuoteIdentifier("last_changed_at_utc"), d.Placeholder(next)),
			fmt.Sprintf("%s = %s", d.QuoteIdentifier("definition_hash"), d.Placeholder(next+1)),
		)
		args = append(args, now, definitionHash)
		next += 2
	}
	stmt := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = %s AND %s = %s AND %s = %s AND %s = %s",
		qt, strings.Join(sets, ", "),
		d.QuoteIdentifier("owner"), d.Placeholder(next),
		d.QuoteIdentifier("object_type"), d.Placeholder(next+1),
		d.QuoteIdentifier("object_schema"), d.Placeholder(next+2),
		d.QuoteIdentifier("object_name"), d.Placeholder(next+3),
	)
	args = append(args, cfg.Owner, objectType, objectSchema, objectName)
	if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("schema: update object metadata row: %w", err)
	}
	return nil
}

// upsertDatabaseRow does the same read-then-write pattern for the per-owner
// chuck_database_metadata row.
func upsertDatabaseRow(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	now time.Time,
) error {
	qt := qualifyTable(d, cfg.databaseMetadataObject())
	selectSQL := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s",
		d.QuoteIdentifier("first_applied_at_utc"), qt,
		d.QuoteIdentifier("owner"), d.Placeholder(1),
	)
	var existingFirst sql.NullTime
	err := db.QueryRowContext(ctx, selectSQL, cfg.Owner).Scan(&existingFirst)
	switch err {
	case sql.ErrNoRows:
		stmt := fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s, %s)",
			qt,
			d.QuoteIdentifier("owner"),
			d.QuoteIdentifier("first_applied_at_utc"),
			d.QuoteIdentifier("last_applied_at_utc"),
			d.QuoteIdentifier("source_repo"),
			d.QuoteIdentifier("source_rev"),
			d.QuoteIdentifier("tool_version"),
			d.Placeholder(1), d.Placeholder(2), d.Placeholder(3),
			d.Placeholder(4), d.Placeholder(5), d.Placeholder(6),
		)
		if _, err := db.ExecContext(ctx, stmt,
			cfg.Owner, now, now,
			nullableString(cfg.SourceRepo), nullableString(cfg.SourceRev), nullableString(cfg.ToolVersion),
		); err != nil {
			return fmt.Errorf("schema: insert database metadata row: %w", err)
		}
		return nil
	case nil:
		stmt := fmt.Sprintf(
			"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s",
			qt,
			d.QuoteIdentifier("last_applied_at_utc"), d.Placeholder(1),
			d.QuoteIdentifier("source_repo"), d.Placeholder(2),
			d.QuoteIdentifier("source_rev"), d.Placeholder(3),
			d.QuoteIdentifier("tool_version"), d.Placeholder(4),
			d.QuoteIdentifier("owner"), d.Placeholder(5),
		)
		if _, err := db.ExecContext(ctx, stmt,
			now,
			nullableString(cfg.SourceRepo), nullableString(cfg.SourceRev), nullableString(cfg.ToolVersion),
			cfg.Owner,
		); err != nil {
			return fmt.Errorf("schema: update database metadata row: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("schema: read database metadata row: %w", err)
	}
}

// nullableString returns sql.NullString that is invalid (NULL) when s is empty,
// so empty provenance values write as SQL NULL and round-trip cleanly back to
// "" via sql.NullString.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
