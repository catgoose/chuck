package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreationOrder_LinearChain(t *testing.T) {
	// Users -> Posts -> Comments (each references the previous)
	users := NewTable("Users").Columns(AutoIncrCol("ID"))
	posts := NewTable("Posts").Columns(
		AutoIncrCol("ID"),
		Col("UserID", TypeInt()).NotNull().References("Users", "ID"),
	)
	comments := NewTable("Comments").Columns(
		AutoIncrCol("ID"),
		Col("PostID", TypeInt()).NotNull().References("Posts", "ID"),
	)

	// Pass in scrambled order.
	sorted, err := CreationOrder(comments, users, posts)
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	idx := nameIndex(sorted)
	assert.Less(t, idx["Users"], idx["Posts"], "Users before Posts")
	assert.Less(t, idx["Posts"], idx["Comments"], "Posts before Comments")
}

func TestDropOrder_LinearChain(t *testing.T) {
	users := NewTable("Users").Columns(AutoIncrCol("ID"))
	posts := NewTable("Posts").Columns(
		AutoIncrCol("ID"),
		Col("UserID", TypeInt()).NotNull().References("Users", "ID"),
	)
	comments := NewTable("Comments").Columns(
		AutoIncrCol("ID"),
		Col("PostID", TypeInt()).NotNull().References("Posts", "ID"),
	)

	sorted, err := DropOrder(comments, users, posts)
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	idx := nameIndex(sorted)
	assert.Less(t, idx["Comments"], idx["Posts"], "Comments before Posts")
	assert.Less(t, idx["Posts"], idx["Users"], "Posts before Users")
}

func TestCreationOrder_Diamond(t *testing.T) {
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	a := NewTable("A").Columns(AutoIncrCol("ID"))
	b := NewTable("B").Columns(
		AutoIncrCol("ID"),
		Col("AID", TypeInt()).References("A", "ID"),
	)
	c := NewTable("C").Columns(
		AutoIncrCol("ID"),
		Col("AID", TypeInt()).References("A", "ID"),
	)
	d := NewTable("D").Columns(
		AutoIncrCol("ID"),
		Col("BID", TypeInt()).References("B", "ID"),
		Col("CID", TypeInt()).References("C", "ID"),
	)

	sorted, err := CreationOrder(d, c, b, a)
	require.NoError(t, err)
	require.Len(t, sorted, 4)

	idx := nameIndex(sorted)
	assert.Less(t, idx["A"], idx["B"])
	assert.Less(t, idx["A"], idx["C"])
	assert.Less(t, idx["B"], idx["D"])
	assert.Less(t, idx["C"], idx["D"])
}

func TestCreationOrder_SelfReference(t *testing.T) {
	// A table with WithParent-style self-reference should not error.
	categories := NewTable("Categories").Columns(
		AutoIncrCol("ID"),
		Col("Name", TypeVarchar(255)).NotNull(),
		Col("ParentID", TypeInt()).References("Categories", "ID"),
	)

	sorted, err := CreationOrder(categories)
	require.NoError(t, err)
	require.Len(t, sorted, 1)
	assert.Equal(t, "Categories", sorted[0].Name)
}

func TestCreationOrder_SelfReferenceWithOtherTables(t *testing.T) {
	// Self-referencing table that also depends on another table.
	orgs := NewTable("Orgs").Columns(AutoIncrCol("ID"))
	departments := NewTable("Departments").Columns(
		AutoIncrCol("ID"),
		Col("OrgID", TypeInt()).NotNull().References("Orgs", "ID"),
		Col("ParentID", TypeInt()).References("Departments", "ID"),
	)

	sorted, err := CreationOrder(departments, orgs)
	require.NoError(t, err)
	require.Len(t, sorted, 2)

	idx := nameIndex(sorted)
	assert.Less(t, idx["Orgs"], idx["Departments"])
}

func TestCreationOrder_CycleError(t *testing.T) {
	// A -> B -> A
	a := NewTable("A").Columns(
		AutoIncrCol("ID"),
		Col("BID", TypeInt()).References("B", "ID"),
	)
	b := NewTable("B").Columns(
		AutoIncrCol("ID"),
		Col("AID", TypeInt()).References("A", "ID"),
	)

	_, err := CreationOrder(a, b)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCyclicDependency))
	assert.Contains(t, err.Error(), "A")
	assert.Contains(t, err.Error(), "B")
}

func TestCreationOrder_IndependentTables(t *testing.T) {
	a := NewTable("Alpha").Columns(AutoIncrCol("ID"))
	b := NewTable("Beta").Columns(AutoIncrCol("ID"))
	c := NewTable("Gamma").Columns(AutoIncrCol("ID"))

	sorted, err := CreationOrder(a, b, c)
	require.NoError(t, err)
	require.Len(t, sorted, 3)

	// All three should be present; order is stable but unspecified.
	names := make(map[string]bool)
	for _, tbl := range sorted {
		names[tbl.Name] = true
	}
	assert.True(t, names["Alpha"])
	assert.True(t, names["Beta"])
	assert.True(t, names["Gamma"])
}

func TestCreationOrder_Empty(t *testing.T) {
	sorted, err := CreationOrder()
	require.NoError(t, err)
	assert.Empty(t, sorted)
}

func TestCreationOrder_ExternalReference(t *testing.T) {
	// A table referencing a table NOT in the input set should not block ordering.
	posts := NewTable("Posts").Columns(
		AutoIncrCol("ID"),
		Col("UserID", TypeInt()).NotNull().References("Users", "ID"),
	)

	sorted, err := CreationOrder(posts)
	require.NoError(t, err)
	require.Len(t, sorted, 1)
	assert.Equal(t, "Posts", sorted[0].Name)
}

func TestDropOrder_Diamond(t *testing.T) {
	a := NewTable("A").Columns(AutoIncrCol("ID"))
	b := NewTable("B").Columns(
		AutoIncrCol("ID"),
		Col("AID", TypeInt()).References("A", "ID"),
	)
	c := NewTable("C").Columns(
		AutoIncrCol("ID"),
		Col("AID", TypeInt()).References("A", "ID"),
	)
	d := NewTable("D").Columns(
		AutoIncrCol("ID"),
		Col("BID", TypeInt()).References("B", "ID"),
		Col("CID", TypeInt()).References("C", "ID"),
	)

	sorted, err := DropOrder(d, c, b, a)
	require.NoError(t, err)
	require.Len(t, sorted, 4)

	idx := nameIndex(sorted)
	assert.Less(t, idx["D"], idx["B"])
	assert.Less(t, idx["D"], idx["C"])
	assert.Less(t, idx["B"], idx["A"])
	assert.Less(t, idx["C"], idx["A"])
}

// nameIndex builds a map from table name to its position in the slice.
func nameIndex(tables []*TableDef) map[string]int {
	m := make(map[string]int, len(tables))
	for i, t := range tables {
		m[t.Name] = i
	}
	return m
}
