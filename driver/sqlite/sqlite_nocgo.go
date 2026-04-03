//go:build !cgo

// Package sqlite registers the SQLite database driver.
// Without CGO, the pure-Go ncruces/go-sqlite3 driver is used.
// Import this package for side effects to use chuck with SQLite:
//
//	import _ "github.com/catgoose/chuck/driver/sqlite"
package sqlite

import _ "github.com/ncruces/go-sqlite3/driver"
