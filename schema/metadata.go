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

// Object-type constants used in ChuckObjectMetadata.ObjectType. Kept as
// stable strings rather than an enum so the ledger stays human-readable when
// inspected directly in the database.
const (
	MetadataObjectTypeView      = "view"
	MetadataObjectTypeProcedure = "procedure"
)

// DefaultObjectMetadataTableName is the fixed PascalCase name of the snapshot
// ledger table. Naming is fixed in chuck core; richer or app-specific
// provenance belongs in caller-owned sibling tables outside chuck.
const DefaultObjectMetadataTableName = "ChuckObjectMetadata"

// Column names of the snapshot ledger. Fixed PascalCase. Apps wanting
// alternate naming/layout should manage their own sibling table rather than
// remap chuck core.
const (
	metadataColOwner             = "Owner"
	metadataColObjectType        = "ObjectType"
	metadataColObjectSchema      = "ObjectSchema"
	metadataColObjectName        = "ObjectName"
	metadataColFirstAppliedAtUtc = "FirstAppliedAtUtc"
	metadataColLastAppliedAtUtc  = "LastAppliedAtUtc"
	metadataColLastChangedAtUtc  = "LastChangedAtUtc"
	metadataColDefinitionHash    = "DefinitionHash"
)

// MetadataConfig opts an apply-helper call into recording an entry in chuck's
// snapshot operational metadata ledger. Zero value is not usable: Owner is
// required.
//
// The ledger is **opt-in** and **snapshot only**. ApplyViewsWithOptions and
// ApplyProceduresWithOptions write rows when opts.Metadata is non-nil and a
// successful apply has just happened; otherwise the ledger is not touched.
// Validate* helpers do not read or compare metadata rows.
//
// Tables are caller-bootstrapped: call EnsureMetadataTables(ctx, db, d, cfg)
// once during your own bootstrap before the first opt-in apply. The apply
// path assumes the table exists and surfaces driver errors verbatim if it
// does not.
//
// Richer provenance (source repo, source rev, tool version, environment, etc.)
// belongs in caller-owned sibling tables outside chuck so the chuck core
// ledger stays a small, stable object-only contract.
type MetadataConfig struct {
	// Owner identifies the caller writing rows into the ledger. Recorded on
	// every row. Required.
	Owner string

	// Schema namespaces the ledger table. Empty means the engine's default
	// namespace; chuck does not impose a default schema. SQLite ignores
	// this because it has no schema namespace.
	Schema string

	// Now is the clock used for FirstAppliedAtUtc / LastAppliedAtUtc /
	// LastChangedAtUtc. When nil, time.Now().UTC() is used. Tests use this
	// seam to assert deterministic snapshot rows.
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

// EnsureMetadataTables creates ChuckObjectMetadata if absent. Safe to run
// repeatedly; uses CREATE TABLE IF NOT EXISTS semantics on every engine.
// Returns ErrMetadataOwnerMissing if Owner is empty so callers see the same
// loud-failure shape as the apply path.
//
// The table is created in cfg.Schema when set; on engines that lack schema
// namespacing (SQLite) the schema is dropped silently to match how the rest
// of chuck handles unscoped identifiers. Callers running an opt-in apply
// path against a schema-scoped ledger are responsible for creating the
// schema itself first — chuck does not implicitly CREATE SCHEMA.
//
// The function name retains its plural form for API compatibility with
// earlier preview releases; it now provisions a single ledger table.
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
// EXISTS statement for the snapshot ledger.
func metadataCreateStatements(d chuck.Dialect, cfg MetadataConfig) []string {
	ts := d.TimestampType()
	textPK := metadataTextPKType(d)

	objBody := strings.Join([]string{
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColOwner), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColObjectType), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColObjectSchema), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColObjectName), textPK),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColFirstAppliedAtUtc), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColLastAppliedAtUtc), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColLastChangedAtUtc), ts),
		fmt.Sprintf("\t\t\t%s %s NOT NULL", d.QuoteIdentifier(metadataColDefinitionHash), textPK),
		fmt.Sprintf("\t\t\tPRIMARY KEY (%s, %s, %s, %s)",
			d.QuoteIdentifier(metadataColOwner),
			d.QuoteIdentifier(metadataColObjectType),
			d.QuoteIdentifier(metadataColObjectSchema),
			d.QuoteIdentifier(metadataColObjectName),
		),
	}, ",\n")

	return []string{
		metadataCreateTableSQL(d, cfg, objBody),
	}
}

// metadataQualifyTable renders the ledger table identifier verbatim,
// schema-qualified when cfg.Schema is set and the engine supports schemas.
// Unlike qualifyTable / chuck.QualifyTable it deliberately does NOT route
// the literal name through dialect NormalizeIdentifier, because Postgres'
// camelToSnake convention would rewrite ChuckObjectMetadata to
// chuck_object_metadata. The plan fixes the ledger name as PascalCase across
// every dialect, so identity renders consistently.
//
// SQLite drops any cfg.Schema component because SQLite has no schema
// namespace.
func metadataQualifyTable(d chuck.Dialect, cfg MetadataConfig) string {
	if cfg.Schema == "" || d.Engine() == chuck.SQLite {
		return d.QuoteIdentifier(DefaultObjectMetadataTableName)
	}
	return d.QuoteIdentifier(cfg.Schema) + "." + d.QuoteIdentifier(DefaultObjectMetadataTableName)
}

// metadataCreateTableSQL renders the CREATE TABLE IF NOT EXISTS statement for
// the ledger using metadataQualifyTable, so the PascalCase identifier stays
// intact on Postgres. MSSQL uses the same IF NOT EXISTS / sys.objects guard
// as createTableIfNotExistsSQL.
func metadataCreateTableSQL(d chuck.Dialect, cfg MetadataConfig, body string) string {
	qt := metadataQualifyTable(d, cfg)
	switch d.Engine() {
	case chuck.MSSQL:
		objArg := qt
		objLit := strings.ReplaceAll(objArg, "'", "''")
		return fmt.Sprintf(
			"IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'%s') AND type in (N'U')) BEGIN CREATE TABLE %s (\n%s\n\t\t) END",
			objLit, qt, body,
		)
	default:
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", qt, body)
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
// LastChangedAtUtc when the executable text actually differs.
func hashCodeObjectDefinition(body string) string {
	canon := canonicalizeStatement(stripLeadingBlockComments(body))
	sum := sha256.Sum256([]byte(canon))
	return hex.EncodeToString(sum[:])
}

// recordCodeObjectMetadata writes one row into ChuckObjectMetadata. Called by
// the apply path after a successful Exec, never on failure.
//
// Snapshot semantics:
//   - new row: FirstAppliedAtUtc = LastAppliedAtUtc = LastChangedAtUtc = now
//   - existing row, same hash: only LastAppliedAtUtc advances
//   - existing row, different hash: LastAppliedAtUtc AND LastChangedAtUtc
//     advance, DefinitionHash is replaced; FirstAppliedAtUtc is preserved.
//
// The two-step read-then-upsert is intentional: it keeps the per-engine SQL
// readable instead of stuffing CASE/MERGE conditional updates inline. The
// ledger is best-effort accounting, not a coordination primitive — callers
// running concurrent applies against the same owner accept that two writers
// may stomp on each other's LastAppliedAtUtc.
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
	return upsertObjectRow(ctx, db, d, cfg, objectType, schemaCol, nameCol, definitionHash, now)
}

// metadataNoticePointer returns the single-line ledger pointer sentence the
// ownership-notice augmentation appends to the rendered chuck-owned comment
// when snapshot metadata is enabled. The pointer references the dialect-
// qualified, quoted form of the ChuckObjectMetadata table so DB readers
// reading the live SQL can jump straight to the ledger row that records
// this object's provenance. SQLite drops the schema component per the
// standard QualifyTable contract; Postgres / MSSQL render schema-qualified
// when cfg.Schema is set and bare otherwise.
//
// Lives next to MetadataConfig so the table-name rendering stays close to
// the ledger's own DDL.
func metadataNoticePointer(d chuck.Dialect, cfg MetadataConfig) string {
	return "Provenance recorded in " + metadataQualifyTable(d, cfg) + "."
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

// metadataObjectColumns returns the (ObjectSchema, ObjectName) pair used to
// key ChuckObjectMetadata rows for the given owned object. The schema
// component reuses the same default-resolution contract as the live-database
// inspection paths (resolveSchemaForInspection): unqualified declarations
// resolve to "public" on Postgres and "dbo" on MSSQL so the ledger row keys
// match where the live object physically lives, while SQLite continues to
// record an empty schema because SQLite has no schema namespace. The name
// component is dialect-normalized identically to inspection.
//
// Without this resolution an unqualified declaration on Postgres or MSSQL
// would record ObjectSchema="" even though the live object lives under
// "public" / "dbo". That would misstate identity and split one physical
// object across two ledger rows if a caller later switched from a bare name
// to the explicit default schema.
func metadataObjectColumns(d chuck.Dialect, obj chuck.ObjectName) (objectSchema, objectName string) {
	_, objectName = normalizedObject(d, obj)
	objectSchema = resolveSchemaForInspection(d, obj)
	return
}

// upsertObjectRow performs the read-then-write for ChuckObjectMetadata.
func upsertObjectRow(
	ctx context.Context,
	db *sql.DB,
	d chuck.Dialect,
	cfg MetadataConfig,
	objectType, objectSchema, objectName, definitionHash string,
	now time.Time,
) error {
	qt := metadataQualifyTable(d, cfg)
	selectSQL := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s = %s AND %s = %s AND %s = %s AND %s = %s",
		d.QuoteIdentifier(metadataColFirstAppliedAtUtc),
		d.QuoteIdentifier(metadataColDefinitionHash),
		qt,
		d.QuoteIdentifier(metadataColOwner), d.Placeholder(1),
		d.QuoteIdentifier(metadataColObjectType), d.Placeholder(2),
		d.QuoteIdentifier(metadataColObjectSchema), d.Placeholder(3),
		d.QuoteIdentifier(metadataColObjectName), d.Placeholder(4),
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
	qt := metadataQualifyTable(d, cfg)
	cols := []string{
		metadataColOwner, metadataColObjectType, metadataColObjectSchema, metadataColObjectName,
		metadataColFirstAppliedAtUtc, metadataColLastAppliedAtUtc, metadataColLastChangedAtUtc,
		metadataColDefinitionHash,
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
	qt := metadataQualifyTable(d, cfg)
	sets := []string{
		fmt.Sprintf("%s = %s", d.QuoteIdentifier(metadataColLastAppliedAtUtc), d.Placeholder(1)),
	}
	args := []any{now}
	next := 2
	if hashChanged {
		sets = append(sets,
			fmt.Sprintf("%s = %s", d.QuoteIdentifier(metadataColLastChangedAtUtc), d.Placeholder(next)),
			fmt.Sprintf("%s = %s", d.QuoteIdentifier(metadataColDefinitionHash), d.Placeholder(next+1)),
		)
		args = append(args, now, definitionHash)
		next += 2
	}
	stmt := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = %s AND %s = %s AND %s = %s AND %s = %s",
		qt, strings.Join(sets, ", "),
		d.QuoteIdentifier(metadataColOwner), d.Placeholder(next),
		d.QuoteIdentifier(metadataColObjectType), d.Placeholder(next+1),
		d.QuoteIdentifier(metadataColObjectSchema), d.Placeholder(next+2),
		d.QuoteIdentifier(metadataColObjectName), d.Placeholder(next+3),
	)
	args = append(args, cfg.Owner, objectType, objectSchema, objectName)
	if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("schema: update object metadata row: %w", err)
	}
	return nil
}
