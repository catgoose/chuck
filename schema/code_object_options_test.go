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
		got := applyOwnershipNoticePrefix("SELECT 1", CodeObjectOptions{})
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("notice-only is prepended with single space separator", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"})
		assert.Equal(t, "/* owned */ SELECT 1", got)
	})

	t.Run("doc-preamble-only is prepended with single space separator", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{DocPreamble: "doc"})
		assert.Equal(t, "/* doc */ SELECT 1", got)
	})

	t.Run("both fields render in preamble-then-notice order", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"})
		assert.Equal(t, "/* doc */ /* owned */ SELECT 1", got)
	})

	t.Run("proc-style payload (leading param) is preserved verbatim", func(t *testing.T) {
		got := applyOwnershipNoticePrefix("@AgentID INT AS BEGIN SELECT 1 END",
			CodeObjectOptions{OwnershipNotice: "owned"})
		assert.Equal(t, "/* owned */ @AgentID INT AS BEGIN SELECT 1 END", got)
	})
}

func TestStripConfiguredApplyPrefix(t *testing.T) {
	t.Run("zero options leaves live unchanged", func(t *testing.T) {
		assert.Equal(t, "SELECT 1",
			stripConfiguredApplyPrefix("SELECT 1", CodeObjectOptions{}))
	})

	t.Run("exact configured notice is stripped", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* owned */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"})
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("exact configured preamble is stripped", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* doc */ SELECT 1",
			CodeObjectOptions{DocPreamble: "doc"})
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("both prefixes strip in preamble-then-notice order", func(t *testing.T) {
		got := stripConfiguredApplyPrefix("/* doc */ /* owned */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"})
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("different leading comment is NOT stripped", func(t *testing.T) {
		// Live has an unconfigured leading comment. Strip must leave it
		// in place so canonical compare reports drift.
		got := stripConfiguredApplyPrefix("/* something else */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"})
		assert.Equal(t, "/* something else */ SELECT 1", got)
	})

	t.Run("configured-but-absent notice tolerated", func(t *testing.T) {
		// Live has no comment, options has notice. Strip is a no-op so
		// canonical compare passes against raw declared body.
		got := stripConfiguredApplyPrefix("SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned"})
		assert.Equal(t, "SELECT 1", got)
	})

	t.Run("wrong-order prefixes not both stripped", func(t *testing.T) {
		// Live has notice before preamble (wrong order). Preamble strip
		// fails, notice strip succeeds, leaving preamble in place.
		got := stripConfiguredApplyPrefix("/* owned */ /* doc */ SELECT 1",
			CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"})
		assert.Equal(t, "/* doc */ SELECT 1", got)
	})
}

func TestApplyViewWithOptions_DefaultPathUnchanged(t *testing.T) {
	v := NewView("v_x", "SELECT 1")
	d := chuck.SQLiteDialect{}
	got := v.createOrReplaceWithBody(d, applyOwnershipNoticePrefix(v.Body(), CodeObjectOptions{}))
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
		applyOwnershipNoticePrefix(v.Body(), opts))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS /* owned */ SELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_DocPreambleAndNoticeRender(t *testing.T) {
	v := NewView("v_x", "SELECT 1")
	opts := CodeObjectOptions{OwnershipNotice: "owned", DocPreamble: "doc"}
	got := v.createOrReplaceWithBody(chuck.SQLiteDialect{},
		applyOwnershipNoticePrefix(v.Body(), opts))
	want := []string{
		"DROP VIEW IF EXISTS \"v_x\"",
		"CREATE VIEW \"v_x\" AS /* doc */ /* owned */ SELECT 1",
	}
	assert.Equal(t, want, got)
}

func TestApplyViewWithOptions_NoticeRendersForMSSQLAndPostgres(t *testing.T) {
	v := NewQualifiedView("sg", "v_x", "SELECT 1")
	opts := CodeObjectOptions{OwnershipNotice: "owned"}
	body := applyOwnershipNoticePrefix(v.Body(), opts)

	mssql := v.createOrReplaceWithBody(chuck.MSSQLDialect{}, body)
	assert.Equal(t, []string{"CREATE OR ALTER VIEW [sg].[v_x] AS /* owned */ SELECT 1"}, mssql)

	pg := v.createOrReplaceWithBody(chuck.PostgresDialect{}, body)
	assert.Equal(t, []string{`CREATE OR REPLACE VIEW "sg"."v_x" AS /* owned */ SELECT 1`}, pg)
}

func TestApplyProcedureWithOptions_NoticeRendersIntoDefinition(t *testing.T) {
	p := NewQualifiedProcedure("sg", "usp_X",
		"@AgentID INT AS BEGIN SET NOCOUNT ON; SELECT 1 END")
	opts := CodeObjectOptions{OwnershipNotice: "owned"}
	definition := applyOwnershipNoticePrefix(p.Definition(), opts)
	stmt, err := p.createOrAlterWithDefinition(chuck.MSSQLDialect{}, definition)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_X] /* owned */ @AgentID INT AS BEGIN SET NOCOUNT ON; SELECT 1 END",
		stmt)
}

func TestApplyProcedureWithOptions_DefaultPathUnchanged(t *testing.T) {
	p := NewProcedure("usp_X", "AS BEGIN SELECT 1 END")
	definition := applyOwnershipNoticePrefix(p.Definition(), CodeObjectOptions{})
	stmt, err := p.createOrAlterWithDefinition(chuck.MSSQLDialect{}, definition)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [usp_X] AS BEGIN SELECT 1 END",
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
