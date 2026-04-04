package dbrepo_test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/catgoose/chuck"
	"github.com/catgoose/chuck/dbrepo"
)

func ExampleColumns() {
	fmt.Println(dbrepo.Columns("ID", "Name", "Email"))
	// Output:
	// ID, Name, Email
}

func ExamplePlaceholders() {
	fmt.Println(dbrepo.Placeholders("ID", "Name", "Email"))
	// Output:
	// @ID, @Name, @Email
}

func ExampleSetClause() {
	fmt.Println(dbrepo.SetClause("Name", "Email"))
	// Output:
	// Name = @Name, Email = @Email
}

func ExampleInsertInto() {
	fmt.Println(dbrepo.InsertInto("Users", "Name", "Email"))
	// Output:
	// INSERT INTO Users (Name, Email) VALUES (@Name, @Email)
}

func ExampleInsertIntoQ() {
	d, _ := chuck.New(chuck.Postgres)
	fmt.Println(dbrepo.InsertIntoQ(d, "Users", "Name", "Email"))
	// Output:
	// INSERT INTO "Users" ("Name", "Email") VALUES (@Name, @Email)
}

func ExampleColumnsQ() {
	d, _ := chuck.New(chuck.Postgres)
	fmt.Println(dbrepo.ColumnsQ(d, "ID", "Name", "Email"))
	// Output:
	// "ID", "Name", "Email"
}

func ExampleSetClauseQ() {
	d, _ := chuck.New(chuck.Postgres)
	fmt.Println(dbrepo.SetClauseQ(d, "Name", "Email"))
	// Output:
	// "Name" = @Name, "Email" = @Email
}

func ExampleNewWhere() {
	w := dbrepo.NewWhere().
		And("DepartmentID = @DeptID", sql.Named("DeptID", 5)).
		And("Active = @Active", sql.Named("Active", true))

	fmt.Println(w.String())
	// Output:
	// WHERE DepartmentID = @DeptID AND Active = @Active
}

func ExampleWhereBuilder_AndIf() {
	// AndIf only adds the condition when the first argument is true.
	// This is useful for optional filters from user input.
	searchTerm := "alice"
	minAge := 0 // zero means no filter

	w := dbrepo.NewWhere().
		And("Active = 1").
		AndIf(searchTerm != "", "Name LIKE @Pattern", sql.Named("Pattern", "%"+searchTerm+"%")).
		AndIf(minAge > 0, "Age >= @MinAge", sql.Named("MinAge", minAge))

	fmt.Println(w.String())
	// Output:
	// WHERE Active = 1 AND Name LIKE @Pattern
}

func ExampleWhereBuilder_NotDeleted() {
	w := dbrepo.NewWhere().NotDeleted()
	fmt.Println(w.String())

	// With a custom column name for snake_case schemas
	w2 := dbrepo.NewWhere().NotDeleted("deleted_at")
	fmt.Println(w2.String())
	// Output:
	// WHERE DeletedAt IS NULL
	// WHERE deleted_at IS NULL
}

func ExampleWhereBuilder_domainFilters() {
	w := dbrepo.NewWhere().
		NotDeleted().
		NotExpired().
		HasStatus("active").
		NotReplaced()

	fmt.Println(w.String())
	// Output:
	// WHERE DeletedAt IS NULL AND (ExpiresAt IS NULL OR ExpiresAt > CURRENT_TIMESTAMP) AND Status = @Status AND ReplacedByID IS NULL
}

func ExampleWhereBuilder_treeQueries() {
	// Find root nodes
	roots := dbrepo.NewWhere().IsRoot()
	fmt.Println("Roots:", roots.String())

	// Find children of a specific node
	children := dbrepo.NewWhere().HasParent(42)
	fmt.Println("Children:", children.String())
	// Output:
	// Roots: WHERE ParentID IS NULL
	// Children: WHERE ParentID = @ParentID
}

func ExampleWhereBuilder_Search() {
	d, _ := chuck.New(chuck.Postgres)

	w := dbrepo.NewWhere().WithDialect(d).
		NotDeleted("deleted_at").
		Search("chuck", "name", "bio")

	fmt.Println(w.String())
	// Output:
	// WHERE deleted_at IS NULL AND (name ILIKE @SearchPattern OR bio ILIKE @SearchPattern)
}

func ExampleNewSelect() {
	query, _ := dbrepo.NewSelect("Tasks", "ID", "Title", "Status").
		Where(
			dbrepo.NewWhere().
				NotDeleted().
				HasStatus("active"),
		).
		OrderBy("Title ASC").
		Paginate(20, 0).
		Build()

	fmt.Println(query)
	// Output:
	// SELECT ID, Title, Status FROM Tasks WHERE DeletedAt IS NULL AND Status = @Status ORDER BY Title ASC LIMIT @Limit OFFSET @Offset
}

func ExampleSelectBuilder_WithDialect() {
	d, _ := chuck.New(chuck.Postgres)

	query, _ := dbrepo.NewSelect("tasks", "id", "title").
		Where(dbrepo.NewWhere().And("id = @ID", sql.Named("ID", 1))).
		WithDialect(d).
		Build()

	fmt.Println(query)
	// Output:
	// SELECT id, title FROM "tasks" WHERE id = @ID
}

func ExampleSelectBuilder_OrderByMap() {
	columnMap := map[string]string{
		"title":      "Title",
		"created_at": "CreatedAt",
		"status":     "Status",
	}

	query, _ := dbrepo.NewSelect("Tasks", "ID", "Title", "Status").
		OrderByMap("title:asc,created_at:desc", columnMap, "ID ASC").
		Build()

	fmt.Println(query)
	// Output:
	// SELECT ID, Title, Status FROM Tasks ORDER BY Title ASC, CreatedAt DESC
}

func ExampleSelectBuilder_CountQuery() {
	w := dbrepo.NewWhere().HasStatus("active")

	sb := dbrepo.NewSelect("Tasks", "ID", "Title").Where(w)
	countQuery, _ := sb.CountQuery()

	fmt.Println(countQuery)
	// Output:
	// SELECT COUNT(*) FROM Tasks WHERE Status = @Status
}

func ExampleSetCreateTimestamps() {
	// Freeze time for deterministic output
	fixed := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	dbrepo.NowFunc = func() time.Time { return fixed }
	defer func() { dbrepo.NowFunc = time.Now }()

	type Task struct {
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	var t Task
	dbrepo.SetCreateTimestamps(&t.CreatedAt, &t.UpdatedAt)

	fmt.Println("CreatedAt:", t.CreatedAt.Format(time.RFC3339))
	fmt.Println("UpdatedAt:", t.UpdatedAt.Format(time.RFC3339))
	// Output:
	// CreatedAt: 2025-06-15T12:00:00Z
	// UpdatedAt: 2025-06-15T12:00:00Z
}

func ExampleInitVersion() {
	var version int
	dbrepo.InitVersion(&version)
	fmt.Println("Version:", version)

	dbrepo.IncrementVersion(&version)
	fmt.Println("After increment:", version)
	// Output:
	// Version: 1
	// After increment: 2
}

func ExampleSetStatus() {
	var status string
	dbrepo.SetStatus(&status, "published")
	fmt.Println(status)
	// Output:
	// published
}

func ExampleNamedArgs() {
	args := dbrepo.NamedArgs(map[string]any{
		"Name":  "Alice",
		"Email": "alice@test.com",
	})
	// Keys are sorted for deterministic output
	for _, arg := range args {
		na := arg.(sql.NamedArg)
		fmt.Printf("%s=%v\n", na.Name, na.Value)
	}
	// Output:
	// Email=alice@test.com
	// Name=Alice
}

func ExampleUpsertIntoQ() {
	d, _ := chuck.New(chuck.Postgres)
	fmt.Println(dbrepo.UpsertIntoQ(d, "Users", []string{"Email"}, "Email", "Name", "Age"))
	// Output:
	// INSERT INTO "Users" ("Email", "Name", "Age") VALUES (@Email, @Name, @Age) ON CONFLICT ("Email") DO UPDATE SET "Name" = EXCLUDED."Name", "Age" = EXCLUDED."Age"
}

func ExampleBuildOrderByClause() {
	columnMap := map[string]string{
		"name":       "Name",
		"created_at": "CreatedAt",
	}

	fmt.Println(dbrepo.BuildOrderByClause("name:asc", columnMap, "ID ASC"))
	fmt.Println(dbrepo.BuildOrderByClause("", columnMap, "ID ASC"))
	fmt.Println(dbrepo.BuildOrderByClause("unknown:desc", columnMap, "ID ASC"))
	// Output:
	// ORDER BY Name ASC
	// ORDER BY ID ASC
	// ORDER BY ID ASC
}
