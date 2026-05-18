package schema

import (
	"errors"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// procedureBodyNoPrefix is the raw body callers pass to NewProcedure — the
// text that follows `AS`. The lifecycle helpers prepend `CREATE OR ALTER
// PROCEDURE <name> AS` themselves, so the body must not duplicate `AS`.
const procedureBodyNoPrefix = "BEGIN SET NOCOUNT ON; DELETE FROM [sg].[StaleRefresh]; END"

func TestNewProcedure_UnqualifiedIdentity(t *testing.T) {
	p := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix)
	assert.Equal(t, "usp_RefreshDashboard", p.Name)
	assert.Equal(t, "", p.Schema())
	assert.Equal(t, procedureBodyNoPrefix, p.Body())
	assert.Equal(t, chuck.ObjectName{Name: "usp_RefreshDashboard"}, p.Object())
}

func TestNewQualifiedProcedure_PreservesSchema(t *testing.T) {
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	assert.Equal(t, "sg", p.Schema())
	assert.Equal(t,
		chuck.ObjectName{Schema: "sg", Name: "usp_RefreshDashboard"},
		p.Object(),
	)
}

func TestProcedureDef_WithSchema_Equivalence(t *testing.T) {
	// WithSchema must produce the same identity as NewQualifiedProcedure so
	// callers can mix declaration styles in the same owned-schema bundle.
	a := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	b := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix).WithSchema("sg")
	assert.Equal(t, a.Object(), b.Object())
	assert.Equal(t, a.Body(), b.Body())
}

func TestProcedureDef_QualifiedNameFor_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	assert.Equal(t, "[sg].[usp_RefreshDashboard]", p.QualifiedNameFor(d))
}

func TestProcedureDef_CreateOrAlterSQL_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.CreateOrAlterSQL(d)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_RefreshDashboard] AS "+procedureBodyNoPrefix,
		got,
		"MSSQL 2016+ supports CREATE OR ALTER PROCEDURE as a single batch",
	)
}

func TestProcedureDef_CreateOrAlterSQL_UnqualifiedMSSQL(t *testing.T) {
	// Unqualified declarations must still emit valid CREATE OR ALTER batches;
	// MSSQL will resolve them under the caller's default schema (typically dbo).
	d := chuck.MSSQLDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.CreateOrAlterSQL(d)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [usp_RefreshDashboard] AS "+procedureBodyNoPrefix,
		got,
	)
}

func TestProcedureDef_CreateOrAlterSQL_Postgres_ReturnsExplicitError(t *testing.T) {
	// Procedure ownership is MSSQL-only in this release. Returning an error
	// (instead of a silent no-op) ensures bootstrap callers running on the
	// wrong engine fail loud instead of dropping owned procedures from the
	// apply set.
	d := chuck.PostgresDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.CreateOrAlterSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported),
		"non-MSSQL CreateOrAlterSQL must wrap ErrProcedureDialectUnsupported so callers can errors.Is-detect it")
}

func TestProcedureDef_CreateOrAlterSQL_SQLite_ReturnsExplicitError(t *testing.T) {
	d := chuck.SQLiteDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.CreateOrAlterSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestProcedureDef_DropSQL_MSSQL_QualifiedExistenceProbe(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.DropSQL(d)
	require.NoError(t, err)
	assert.Contains(t, got, "sys.procedures",
		"MSSQL drop must probe sys.procedures, not sys.objects, so it matches the procedure-only object class")
	assert.Contains(t, got, "OBJECT_ID(N'[sg].[usp_RefreshDashboard]')",
		"MSSQL existence probe should use the schema-qualified object literal")
	assert.Contains(t, got, "DROP PROCEDURE [sg].[usp_RefreshDashboard]")
}

func TestProcedureDef_DropSQL_MSSQL_UnqualifiedExistenceProbe(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.DropSQL(d)
	require.NoError(t, err)
	// Without a schema namespace, the existence probe must still produce a
	// valid OBJECT_ID literal — bare name in brackets.
	assert.Contains(t, got, "OBJECT_ID(N'[usp_RefreshDashboard]')")
	assert.Contains(t, got, "DROP PROCEDURE [usp_RefreshDashboard]")
}

func TestProcedureDef_DropSQL_Postgres_ReturnsExplicitError(t *testing.T) {
	d := chuck.PostgresDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.DropSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestProcedureDef_DropSQL_SQLite_ReturnsExplicitError(t *testing.T) {
	d := chuck.SQLiteDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureBodyNoPrefix)
	got, err := p.DropSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}
