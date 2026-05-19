package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripCreateProcedurePreamble(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "mssql CREATE OR ALTER with bracketed schema",
			in:   `CREATE OR ALTER PROCEDURE [sg].[usp_Refresh] @AgentID INT AS BEGIN SELECT 1 END`,
			want: `@AgentID INT AS BEGIN SELECT 1 END`,
		},
		{
			name: "mssql CREATE bare-name no schema",
			in:   `CREATE PROCEDURE usp_Refresh AS BEGIN SET NOCOUNT ON; SELECT 1 END`,
			want: `AS BEGIN SET NOCOUNT ON; SELECT 1 END`,
		},
		{
			name: "mssql leading whitespace and newlines",
			in:   "\n   CREATE OR ALTER PROCEDURE [dbo].[usp_X]\n@Probe INT = 1\nAS\nBEGIN SELECT @Probe END",
			want: "@Probe INT = 1\nAS\nBEGIN SELECT @Probe END",
		},
		{
			name: "no preamble returned unchanged",
			in:   `AS BEGIN SELECT 1 END`,
			want: `AS BEGIN SELECT 1 END`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stripCreateProcedurePreamble(tc.in))
		})
	}
}

func TestValidateProcedure_UnsupportedDialect_Postgres(t *testing.T) {
	// Procedure validation is MSSQL-only; non-MSSQL calls must fail loud
	// rather than silently no-op, so bootstrap callers running on the wrong
	// engine cannot accidentally drop ownership coverage from the apply set.
	err := ValidateProcedure(context.Background(), nil, chuck.PostgresDialect{},
		NewQualifiedProcedure("sg", "usp_X", "AS BEGIN SELECT 1 END"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestValidateProcedure_UnsupportedDialect_SQLite(t *testing.T) {
	err := ValidateProcedure(context.Background(), nil, chuck.SQLiteDialect{},
		NewProcedure("usp_X", "AS BEGIN SELECT 1 END"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestValidateProcedures_UnsupportedDialect_FailsBeforeQuery(t *testing.T) {
	// Even when the caller passes zero procedures, the dialect guard must
	// fire so misconfigured deployments fail loud rather than silently
	// reporting success.
	err := ValidateProcedures(context.Background(), nil, chuck.PostgresDialect{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestApplyProcedure_UnsupportedDialect_Postgres(t *testing.T) {
	err := ApplyProcedure(context.Background(), nil, chuck.PostgresDialect{},
		NewQualifiedProcedure("sg", "usp_X", "AS BEGIN SELECT 1 END"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestApplyProcedures_UnsupportedDialect_FailsBeforeExec(t *testing.T) {
	err := ApplyProcedures(context.Background(), nil, chuck.SQLiteDialect{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestProcedureDriftError_Unwrap_AllMissing(t *testing.T) {
	e := &ProcedureDriftError{Drifts: []ProcedureDrift{
		{Object: chuck.ObjectName{Schema: "sg", Name: "usp_A"}, Missing: true},
		{Object: chuck.ObjectName{Schema: "sg", Name: "usp_B"}, Missing: true},
	}}
	assert.True(t, errors.Is(e, ErrProcedureMissing))
	assert.False(t, errors.Is(e, ErrProcedureDefinitionDrift))
}

func TestProcedureDriftError_Unwrap_AllDefinition(t *testing.T) {
	e := &ProcedureDriftError{Drifts: []ProcedureDrift{
		{Object: chuck.ObjectName{Name: "usp_A"}, DefinitionMismatch: true},
	}}
	assert.True(t, errors.Is(e, ErrProcedureDefinitionDrift))
	assert.False(t, errors.Is(e, ErrProcedureMissing))
}

func TestProcedureDriftError_Unwrap_MixedCauses_NoWrap(t *testing.T) {
	// Mixed missing + definition-drift cases intentionally do not wrap
	// either sentinel so callers can't branch on a single cause when the
	// real failure mode is heterogeneous.
	e := &ProcedureDriftError{Drifts: []ProcedureDrift{
		{Object: chuck.ObjectName{Name: "usp_A"}, Missing: true},
		{Object: chuck.ObjectName{Name: "usp_B"}, DefinitionMismatch: true},
	}}
	assert.False(t, errors.Is(e, ErrProcedureMissing))
	assert.False(t, errors.Is(e, ErrProcedureDefinitionDrift))
}

func TestViewDriftError_Unwrap_AllMissing(t *testing.T) {
	e := &ViewDriftError{Drifts: []ViewDrift{
		{Object: chuck.ObjectName{Name: "v_a"}, Missing: true},
		{Object: chuck.ObjectName{Name: "v_b"}, Missing: true},
	}}
	assert.True(t, errors.Is(e, ErrViewMissing))
	assert.False(t, errors.Is(e, ErrViewBodyDrift))
}

func TestViewDriftError_Unwrap_AllBody(t *testing.T) {
	e := &ViewDriftError{Drifts: []ViewDrift{
		{Object: chuck.ObjectName{Name: "v_a"}, BodyMismatch: true},
	}}
	assert.True(t, errors.Is(e, ErrViewBodyDrift))
	assert.False(t, errors.Is(e, ErrViewMissing))
}
