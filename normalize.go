package chuck

import (
	"strings"
	"unicode"
)

// camelToSnake converts a CamelCase or PascalCase identifier to snake_case.
// Consecutive uppercase letters are treated as an acronym — e.g. "UserID" → "user_id".
func camelToSnake(s string) string {
	if s == "" {
		return s
	}

	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(s) + 4)

	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				// Insert underscore before an uppercase letter when:
				// 1. Previous char is lowercase (e.g. "userN" → "user_n")
				// 2. Previous char is uppercase but next is lowercase (e.g. "userID" at 'D' when next doesn't exist, or "HTMLParser" at 'P')
				if unicode.IsLower(prev) {
					b.WriteRune('_')
				} else if unicode.IsUpper(prev) && i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
					b.WriteRune('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}

	return b.String()
}
