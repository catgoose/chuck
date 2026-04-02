package chuck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCamelToSnake(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"ID", "id"},
		{"id", "id"},
		{"UserID", "user_id"},
		{"CreatedAt", "created_at"},
		{"HTMLParser", "html_parser"},
		{"already_snake", "already_snake"},
		{"UUID", "uuid"},
		{"DeletedAt", "deleted_at"},
		{"ReplacedByID", "replaced_by_id"},
		{"SortOrder", "sort_order"},
		{"ExpiresAt", "expires_at"},
		{"ParentID", "parent_id"},
		{"a", "a"},
		{"A", "a"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, camelToSnake(tt.in))
		})
	}
}
