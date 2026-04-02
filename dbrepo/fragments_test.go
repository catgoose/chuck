package dbrepo

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	assert.Equal(t, "ID, Name, Email", Columns("ID", "Name", "Email"))
	assert.Equal(t, "ID", Columns("ID"))
}

func TestPlaceholders(t *testing.T) {
	assert.Equal(t, "@ID, @Name, @Email", Placeholders("ID", "Name", "Email"))
	assert.Equal(t, "@ID", Placeholders("ID"))
}

func TestSetClause(t *testing.T) {
	assert.Equal(t, "Name = @Name, Email = @Email", SetClause("Name", "Email"))
}

func TestInsertInto(t *testing.T) {
	result := InsertInto("Users", "Name", "Email")
	assert.Equal(t, "INSERT INTO Users (Name, Email) VALUES (@Name, @Email)", result)
}

func TestNamedArgs(t *testing.T) {
	args := NamedArgs(map[string]any{
		"Name":  "Gobo",
		"Email": "gobo@chuck.rock",
	})
	assert.Len(t, args, 2)
	// Keys sorted: Email, Name
	assert.Equal(t, sql.Named("Email", "gobo@chuck.rock"), args[0])
	assert.Equal(t, sql.Named("Name", "Gobo"), args[1])
}
