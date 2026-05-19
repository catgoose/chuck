package schema

// CodeObjectOptions controls option-aware rendering and validation for owned
// code objects (ViewDef, ProcedureDef). The zero value preserves the default
// no-notice behavior so existing callers see no change unless they explicitly
// pass a populated CodeObjectOptions to the *WithOptions helpers.
//
// The intended usage is one central config built once in bootstrap and passed
// to ApplyViewsWithOptions / ValidateViewsWithOptions / ApplyProceduresWithOptions
// / ValidateProceduresWithOptions, rather than repeated per-definition
// boilerplate:
//
//	opts := schema.CodeObjectOptions{
//	    OwnershipNotice: schema.DefaultOwnershipNotice,
//	}
//	if err := schema.ApplyViewsWithOptions(ctx, db, d, opts, views...); err != nil {
//	    return err
//	}
//	if err := schema.ValidateViewsWithOptions(ctx, db, d, opts, views...); err != nil {
//	    return err
//	}
type CodeObjectOptions struct {
	// OwnershipNotice is an opt-in soft notice the option-aware Apply* /
	// Validate* helpers prepend to the rendered view body or procedure
	// definition as a SQL block comment. When empty (the default) nothing is
	// emitted, matching the existing zero-option Apply* / Validate* behavior.
	//
	// Validate-side coherence: the option-aware Validate* helpers prepend the
	// same notice to the declared body / definition before canonicalization,
	// so apply followed by validate-with-the-same-options does not report
	// false drift. Callers must use the same options struct for apply and
	// validate, or invoke the bare Apply* / Validate* helpers (which compare
	// against the raw declared body) together.
	//
	// Engine notes:
	//   - SQLite: comment survives in sqlite_master.sql.
	//   - MSSQL:  comment survives in sys.sql_modules.definition.
	//   - Postgres: pg_get_viewdef discards comments when it reconstructs the
	//     SELECT, so the notice will land in the live CREATE statement but
	//     will not be visible when an operator reads pg_get_viewdef output.
	//     This does not produce false drift because Postgres view validation
	//     is existence-only (see ErrViewBodyComparisonUnsupported).
	OwnershipNotice string
}

// DefaultOwnershipNotice is the canonical chuck-owned ownership notice text
// suggested by issue #84. Deliberately soft: it says out-of-band changes
// "may" fail validation or be overwritten because the explicit Validate* /
// Apply* lanes leave that choice to the caller, and Postgres view validation
// is existence-only today (see ErrViewBodyComparisonUnsupported), so a
// stronger promise would be dishonest. The notice embeds the chuck GitHub
// URL rather than the bare project name so DB readers who do not know what
// "chuck" is can find the project directly.
const DefaultOwnershipNotice = "Owned by https://github.com/catgoose/chuck. Do not edit in database.\n" +
	"Out-of-band changes may fail validation or be overwritten by apply/bootstrap."

// renderOwnershipComment formats a notice as a SQL block comment, or returns
// "" when no notice is set. Engine-agnostic `/* ... */` syntax is accepted by
// SQLite, MSSQL, and Postgres at the body / definition payload positions where
// chuck injects it.
func renderOwnershipComment(notice string) string {
	if notice == "" {
		return ""
	}
	return "/* " + notice + " */"
}

// applyOwnershipNoticePrefix returns the body or definition payload prefixed
// with a rendered ownership comment when one is set, otherwise the payload
// unchanged. A single space separates the comment from the payload to keep
// the resulting statement well-formed regardless of whether the payload
// starts with a SELECT keyword (views), a parameter declaration (procedures),
// or an AS keyword (zero-parameter procedures).
func applyOwnershipNoticePrefix(payload string, opts CodeObjectOptions) string {
	comment := renderOwnershipComment(opts.OwnershipNotice)
	if comment == "" {
		return payload
	}
	return comment + " " + payload
}
