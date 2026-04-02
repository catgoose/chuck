package chuck_test

import (
	"fmt"

	"github.com/catgoose/chuck"
)

func ExampleNew() {
	d, err := chuck.New(chuck.Postgres)
	if err != nil {
		panic(err)
	}

	fmt.Println(d.AutoIncrement())
	fmt.Println(d.TimestampType())
	fmt.Println(d.Now())
	fmt.Println(d.Placeholder(1))
	fmt.Println(d.Pagination())
	// Output:
	// SERIAL PRIMARY KEY
	// TIMESTAMPTZ
	// NOW()
	// $1
	// LIMIT @Limit OFFSET @Offset
}

func ExampleNew_sqlite() {
	d, err := chuck.New(chuck.SQLite)
	if err != nil {
		panic(err)
	}

	fmt.Println(d.AutoIncrement())
	fmt.Println(d.TimestampType())
	fmt.Println(d.Now())
	fmt.Println(d.BoolType())
	// Output:
	// INTEGER PRIMARY KEY AUTOINCREMENT
	// TIMESTAMP
	// CURRENT_TIMESTAMP
	// INTEGER
}

func ExampleNew_mssql() {
	d, err := chuck.New(chuck.MSSQL)
	if err != nil {
		panic(err)
	}

	fmt.Println(d.AutoIncrement())
	fmt.Println(d.Pagination())
	fmt.Println(d.QuoteIdentifier("users"))
	// Output:
	// INT PRIMARY KEY IDENTITY(1,1)
	// OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY
	// [users]
}

func ExampleNew_columnTypes() {
	d, _ := chuck.New(chuck.Postgres)

	fmt.Println(d.StringType(255))
	fmt.Println(d.VarcharType(100))
	fmt.Println(d.IntType())
	fmt.Println(d.BigIntType())
	fmt.Println(d.FloatType())
	fmt.Println(d.DecimalType(10, 2))
	fmt.Println(d.TextType())
	fmt.Println(d.UUIDType())
	fmt.Println(d.JSONType())
	// Output:
	// TEXT
	// VARCHAR(100)
	// INTEGER
	// BIGINT
	// DOUBLE PRECISION
	// NUMERIC(10,2)
	// TEXT
	// UUID
	// JSONB
}

func ExampleNew_normalizeIdentifier() {
	pg, _ := chuck.New(chuck.Postgres)
	sq, _ := chuck.New(chuck.SQLite)

	fmt.Println(pg.NormalizeIdentifier("CreatedAt"))
	fmt.Println(pg.NormalizeIdentifier("UserID"))
	fmt.Println(sq.NormalizeIdentifier("CreatedAt"))
	// Output:
	// created_at
	// user_id
	// CreatedAt
}

func ExampleParseEngine() {
	e, err := chuck.ParseEngine("postgres")
	if err != nil {
		panic(err)
	}
	fmt.Println(e)

	_, err = chuck.ParseEngine("mysql")
	fmt.Println(err)
	// Output:
	// postgres
	// unknown database engine: "mysql" (expected sqlserver, mssql, sqlite3, sqlite, postgres, or postgresql)
}

func ExampleQuoteColumns() {
	d, _ := chuck.New(chuck.Postgres)

	fmt.Println(chuck.QuoteColumns(d, "CreatedAt, Title DESC"))
	// Output:
	// "created_at", "title" DESC
}
