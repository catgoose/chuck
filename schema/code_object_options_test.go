package schema

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderOwnershipComment(t *testing.T) {
	t.Run("empty notice returns empty string", func(t *testing.T) {
		assert.Equal(t, "", renderOwnershipComment(""))
	})

	t.Run("non-empty notice wraps in block comment", func(t *testing.T) {
		got := renderOwnershipComment("hello world")
		assert.Equal(t, "/* hello world */", got)
	})

	t.Run("multi-line notice survives unchanged", func(t *testing.T) {
		got := renderOwnershipComment("line1\nline2")
		assert.Equal(t, "/* line1\nline2 */", got)
	})

	t.Run("DefaultOwnershipNotice renders into well-formed block comment", func(t *testing.T) {
		got := renderOwnershipComment(DefaultOwnershipNotice)
		assert.True(t, strings.HasPrefix(got, "/* "))
		assert.True(t, strings.HasSuffix(got, " */"))
		assert.Contains(t, got, "Owned by https://github.com/catgoose/chuck")
		assert.Contains(t, got, "may fail validation")
	})
}

func TestApplyOwnershipNoticePrefix(t *testing.T) {
	t.Run("zero options leaves payload untouched", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1", CodeObjectOptions{}, "")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("notice-only is prepended on its own line", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"}, "")
		assert.Equal(t, "\n/* owned */\nSELECT 1", got)
	})

	t.Run("doc-preamble-only is prepended on its own line", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{DocPreamble: "doc"}, "")
		assert.Equal(t, "\n/* doc */\nSELECT 1", got)
	})

	t.Run("both fields render in notice-then-preamble order", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "")
		assert.Equal(t, "\n/* owned */\n\n/* doc */\nSELECT 1", got)
	})

	t.Run("proc-style payload (leading param) is preserved verbatim", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("@AgentID INT AS BEGIN SELECT 1 END",
			CodeObjectOptions{OwnershipNotice: "owned"}, "")
		assert.Equal(t, "\n/* owned */\n@AgentID INT AS BEGIN SELECT 1 END", got)
	})

	t.Run("declared annotation alone is prepended", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1", CodeObjectOptions{}, "ann")
		assert.Equal(t, "\n/* ann */\nSELECT 1", got)
	})

	t.Run("annotation renders last, after DocPreamble and OwnershipNotice", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "ann")
		assert.Equal(t, "\n/* owned */\n\n/* doc */\n\n/* ann */\nSELECT 1", got)
	})

	t.Run("annotation + notice (no preamble)", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"}, "ann")
		assert.Equal(t, "\n/* owned */\n\n/* ann */\nSELECT 1", got)
	})
}

func TestStripConfiguredApplyPrefix(t *testing.T) {
	t.Run("zero options leaves live unchanged", func(t *testing.T) {
		assert.Equal(t, "SELECT 1",
			stripConfiguredApplyPrefix("SELECT 1", CodeObjectOptions{}, ""))
	})

	t.Run("exact configured notice is stripped", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* owned */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"}, "")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("exact configured preamble is stripped", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* doc */ SELECT 1",
			CodeObjectOptions{DocPreamble: "doc"}, "")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("both prefixes strip in notice-then-preamble order", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* owned */ /* doc */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("different leading comment is NOT stripped", func(t *testing.T) {
		// Live has an unconfigured leading comment. Strip must leave it
		// in place so canonical compare reports drift.
		got := stripConfiguredApplyPrefix("/* something else */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"}, "")
		assert.Equal(t, "/* something else */ SELECT 1", got)
	})

	t.Run("configured-but-absent notice tolerated", func(t *testing.T) {
		// Live has no comment, options has notice. Strip is a no-op so
		// canonical compare passes against raw declared body.
		got := stripConfiguredApplyPrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"}, "")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("wrong-order prefixes not both stripped", func(t *testing.T) {
		// Live has preamble before notice (wrong order now that notice
		// renders first). Notice strip fails, preamble strip succeeds,
		// leaving notice in place.
		got := stripConfiguredApplyPrefix("/* doc */ /* owned */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "")
		assert.Equal(t, "/* owned */ SELECT 1", got)
	})

	t.Run("declared annotation kept while apply-owned prefixes stripped", func(t *testing.T) {
		got := stripConfiguredApplyPrefix(
			"/* owned */ /* doc */ /* ann */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "ann")
		assert.Equal(t, "/* ann */ SELECT 1", got)
	})

	t.Run("annotation alone is kept", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* ann */ SELECT 1",
			CodeObjectOptions{}, "ann")
		assert.Equal(t, "/* ann */ SELECT 1", got)
	})

	t.Run("declared annotation missing in live leaves payload untouched", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("SELECT 1",
			CodeObjectOptions{}, "ann")
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("declared annotation mismatched in live is left in place", func(t *testing.T) {
		// Live carries a different comment in the annotation slot; strip
		// leaves it in place (not matching the declared annotation) since
		// the apply-owned prefixes preceded it and have already been
		// stripped.
		got := stripConfiguredApplyPrefix(
			"/* owned */ /* doc */ /* not the ann */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}, "ann")
		assert.Equal(t, "/* not the ann */ SELECT 1", got)
	})
}

func TestApplyViewWithOptions_DefaultPathUnchanged(t *testing.T) {
	v := NewView("v_x", "SELECT 1")
	d := chuck.SQLiteDialect{}
	got := v.createOrReplaceWithBody(d, applyOwnershipNoticePrefix(v.Body(), CodeObjectOptions{}, v.DocAnnotation()))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS SELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_NoticeRendersIntoBody(t *testing.T) {
	v := NewView("v_x", "SELECT 1")
	opts := CodeObjectOptions{OwnershipNotice: "owned"}
	got := v.createOrReplaceWithBody(chuck.SQLiteDialect{},
		applyOwnershipNoticePrefix(v.Body(), opts, v.DocAnnotation()))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS\n/* owned */\nSELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_DocPreambleAndNoticeRender(t *testing.T) {
	v := NewView("v_x", "SELECT 1")
	opts := CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}
	got := v.createOrReplaceWithBody(chuck.SQLiteDialect{},
		applyOwnershipNoticePrefix(v.Body(), opts, v.DocAnnotation()))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS\n/* owned */\n\n/* doc */\nSELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_NoticeRendersForMSSQLAndPostgres(t *testing.T) {
	v := NewQualifiedView("sg", "v_x", "SELECT 1")
	opts := CodeObjectOptions{OwnershipNotice: "owned"}
	body := applyOwnershipNoticePrefix(v.Body(), opts, v.DocAnnotation())

	mssql := v.createOrReplaceWithBody(chuck.MSSQLDialect{}, body)
	assert.Equal(t, []string{"CREATE OR ALTER VIEW [sg].[v_x] AS\n/* owned */\nSELECT 1"}, mssql)

	pg := v.createOrReplaceWithBody(chuck.PostgresDialect{}, body)
	assert.Equal(t, []string{"CREATE OR REPLACE VIEW \"sg\".\"v_x\" AS\n/* owned */\nSELECT 1"}, pg)
}

func TestApplyViewWithOptions_DocAnnotationRendersLastAfterNoticeAndPreamble(t *testing.T) {
	v := NewView("v_x", "SELECT 1").WithDocAnnotation("ann")
	opts := CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}
	got := v.createOrReplaceWithBody(chuck.SQLiteDialect{},
		applyOwnershipNoticePrefix(v.Body(), opts, v.DocAnnotation()))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS\n/* owned */\n\n/* doc */\n\n/* ann */\nSELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_DocAnnotationAlone(t *testing.T) {
	v := NewView("v_x", "SELECT 1").WithDocAnnotation("ann")
	got := v.createOrReplaceWithBody(chuck.SQLiteDialect{},
		applyOwnershipNoticePrefix(v.Body(), CodeObjectOptions{}, v.DocAnnotation()))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS\n/* ann */\nSELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyProcedureWithOptions_NoticeRendersIntoDefinition(t *testing.T) {
	p := NewQualifiedProcedure("sg", "usp_X",
		"@AgentID INT AS BEGIN SET NOCOUNT ON; SELECT 1 END")
	opts := CodeObjectOptions{OwnershipNotice: "owned"}
	definition := applyOwnershipNoticePrefix(p.Definition(), opts, p.DocAnnotation())
	stmt, err := p.createOrAlterWithDefinition(chuck.MSSQLDialect{}, definition)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_X]\n/* owned */\n@AgentID INT AS BEGIN SET NOCOUNT ON; SELECT 1 END",
		stmt)
}

func TestApplyProcedureWithOptions_DefaultPathUnchanged(t *testing.T) {
	p := NewProcedure("usp_X", "AS BEGIN SELECT 1 END")
	definition := applyOwnershipNoticePrefix(p.Definition(), CodeObjectOptions{}, p.DocAnnotation())
	stmt, err := p.createOrAlterWithDefinition(chuck.MSSQLDialect{}, definition)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [usp_X] AS BEGIN SELECT 1 END",
		stmt)
}

func TestApplyProcedureWithOptions_DocAnnotationRendersLastAfterNoticeAndPreamble(t *testing.T) {
	p := NewQualifiedProcedure("sg", "usp_X",
		"@AgentID INT AS BEGIN SELECT 1 END").WithDocAnnotation("ann")
	opts := CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}
	definition := applyOwnershipNoticePrefix(p.Definition(), opts, p.DocAnnotation())
	stmt, err := p.createOrAlterWithDefinition(chuck.MSSQLDialect{}, definition)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_X]\n/* owned */\n\n/* doc */\n\n/* ann */\n@AgentID INT AS BEGIN SELECT 1 END",
		stmt)
}

func TestApplyProcedureWithOptions_UnsupportedDialectStillErrs(t *testing.T) {
	p := NewProcedure("usp_X", "AS BEGIN SELECT 1 END")
	err := ApplyProcedureWithOptions(context.Background(), nil, chuck.PostgresDialect{},
		CodeObjectOptions{OwnershipNotice: DefaultOwnershipNotice}, p)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestValidateProceduresWithOptions_UnsupportedDialect(t *testing.T) {
	err := ValidateProceduresWithOptions(context.Background(), nil, chuck.SQLiteDialect{},
		CodeObjectOptions{OwnershipNotice: DefaultOwnershipNotice})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}
