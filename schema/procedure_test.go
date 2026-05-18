package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/catgoose/chuck"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// procedureDefinition is the canonical full-shape T-SQL definition callers
// pass to NewProcedure: parameter declarations, then AS, then body. The
// lifecycle helpers prepend `CREATE OR ALTER PROCEDURE <qualified-name>`
// themselves; everything after the qualified name is caller-owned text.
const procedureDefinition = "@AgentID INT, @AsOf DATETIME2 = NULL\n" +
	"AS\n" +
	"BEGIN\n" +
	"    SET NOCOUNT ON;\n" +
	"    DELETE FROM [sg].[StaleRefresh] WHERE [AgentID] = @AgentID;\n" +
	"END"

// procedureDefinitionNoParams is a zero-parameter shape — definition still
// must include the AS keyword caller-side, since T-SQL grammar places
// parameters between the name and AS and the package cannot inject AS without
// closing off the parameter slot.
const procedureDefinitionNoParams = "AS BEGIN SET NOCOUNT ON; SELECT 1 AS Probe; END"

func TestNewProcedure_UnqualifiedIdentity(t *testing.T) {
	p := NewProcedure("usp_RefreshDashboard", procedureDefinition)
	assert.Equal(t, "usp_RefreshDashboard", p.Name)
	assert.Equal(t, "", p.Schema())
	assert.Equal(t, procedureDefinition, p.Definition())
	assert.Equal(t, chuck.ObjectName{Name: "usp_RefreshDashboard"}, p.Object())
}

func TestNewQualifiedProcedure_PreservesSchema(t *testing.T) {
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	assert.Equal(t, "sg", p.Schema())
	assert.Equal(t,
		chuck.ObjectName{Schema: "sg", Name: "usp_RefreshDashboard"},
		p.Object(),
	)
}

func TestProcedureDef_WithSchema_Equivalence(t *testing.T) {
	// WithSchema must produce the same identity as NewQualifiedProcedure so
	// callers can mix declaration styles in the same owned-schema bundle.
	a := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	b := NewProcedure("usp_RefreshDashboard", procedureDefinition).WithSchema("sg")
	assert.Equal(t, a.Object(), b.Object())
	assert.Equal(t, a.Definition(), b.Definition())
}

func TestProcedureDef_QualifiedNameFor_MSSQL(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	assert.Equal(t, "[sg].[usp_RefreshDashboard]", p.QualifiedNameFor(d))
}

func TestProcedureDef_CreateOrAlterSQL_MSSQL_Parameterized(t *testing.T) {
	// Parameters in T-SQL belong between the procedure name and AS, not
	// after AS. The render path must place the caller's definition
	// immediately after the qualified name so
	// `@AgentID INT, @AsOf DATETIME2 = NULL\nAS\n...` lands in the right slot.
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	got, err := p.CreateOrAlterSQL(d)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_RefreshDashboard] "+procedureDefinition,
		got,
		"MSSQL CREATE OR ALTER PROCEDURE must let parameter declarations sit between the qualified name and AS",
	)
	// Order check independent of the equality assertion: the parameter list
	// must precede AS, which must precede BEGIN. A render that drops in AS
	// itself would invert this order.
	asIdx := strings.Index(got, "\nAS\n")
	paramIdx := strings.Index(got, "@AgentID INT")
	beginIdx := strings.Index(got, "BEGIN")
	require.True(t, paramIdx > 0 && asIdx > paramIdx && beginIdx > asIdx,
		"params must precede AS which must precede BEGIN; got=%q", got)
}

func TestProcedureDef_CreateOrAlterSQL_MSSQL_NoParams(t *testing.T) {
	// A zero-parameter procedure still works — the caller's definition just
	// starts with AS.
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinitionNoParams)
	got, err := p.CreateOrAlterSQL(d)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [sg].[usp_RefreshDashboard] "+procedureDefinitionNoParams,
		got,
	)
}

func TestProcedureDef_CreateOrAlterSQL_UnqualifiedMSSQL(t *testing.T) {
	// Unqualified declarations must still emit valid CREATE OR ALTER batches;
	// MSSQL will resolve them under the caller's default schema (typically dbo).
	d := chuck.MSSQLDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureDefinitionNoParams)
	got, err := p.CreateOrAlterSQL(d)
	require.NoError(t, err)
	assert.Equal(t,
		"CREATE OR ALTER PROCEDURE [usp_RefreshDashboard] "+procedureDefinitionNoParams,
		got,
	)
}

func TestProcedureDef_CreateOrAlterSQL_Postgres_ReturnsExplicitError(t *testing.T) {
	// Procedure ownership is MSSQL-only in this release. Returning an error
	// (instead of a silent no-op) ensures bootstrap callers running on the
	// wrong engine fail loud instead of dropping owned procedures from the
	// apply set.
	d := chuck.PostgresDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	got, err := p.CreateOrAlterSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported),
		"non-MSSQL CreateOrAlterSQL must wrap ErrProcedureDialectUnsupported so callers can errors.Is-detect it")
}

func TestProcedureDef_CreateOrAlterSQL_SQLite_ReturnsExplicitError(t *testing.T) {
	d := chuck.SQLiteDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureDefinitionNoParams)
	got, err := p.CreateOrAlterSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestProcedureDef_DropSQL_MSSQL_QualifiedExistenceProbe(t *testing.T) {
	d := chuck.MSSQLDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
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
	p := NewProcedure("usp_RefreshDashboard", procedureDefinitionNoParams)
	got, err := p.DropSQL(d)
	require.NoError(t, err)
	// Without a schema namespace, the existence probe must still produce a
	// valid OBJECT_ID literal — bare name in brackets.
	assert.Contains(t, got, "OBJECT_ID(N'[usp_RefreshDashboard]')")
	assert.Contains(t, got, "DROP PROCEDURE [usp_RefreshDashboard]")
}

func TestProcedureDef_DropSQL_Postgres_ReturnsExplicitError(t *testing.T) {
	d := chuck.PostgresDialect{}
	p := NewQualifiedProcedure("sg", "usp_RefreshDashboard", procedureDefinition)
	got, err := p.DropSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}

func TestProcedureDef_DropSQL_SQLite_ReturnsExplicitError(t *testing.T) {
	d := chuck.SQLiteDialect{}
	p := NewProcedure("usp_RefreshDashboard", procedureDefinitionNoParams)
	got, err := p.DropSQL(d)
	assert.Empty(t, got)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcedureDialectUnsupported))
}
