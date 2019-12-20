package mssql

import (
	"context"
	"fmt"

	// "encoding/json"
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
)

// CreateSchema create a schema on DB
func CreateSchema(db *sql.DB, name string) (int64, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%[1]v')
	BEGIN
		EXEC('CREATE SCHEMA %[1]v')
	END`, name)

	// Execute non-query with named parameters
	result, err := db.ExecContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}
