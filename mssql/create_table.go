package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// CreateAPISrcTable create table
func CreateAPISrcTable(db *sql.DB) (int64, error) {
	var err error

	ctx := context.Background()

	// Check if database is alive.
	err = db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	var tsql string
	tsql, err = ReadSQLFile("./mssql/src_api_table.sql", "src")
	if err != nil {
		log.Fatal("Error on read sql file")
	}

	// Execute non-query with named parameters
	result, err := db.ExecContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}

// CreateIdentitySrcTable create table
func CreateIdentitySrcTable(db *sql.DB) (int64, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	var tsql string
	tsql, err = ReadSQLFile("./mssql/src_identity_table.sql", "src")
	if err != nil {
		log.Fatal("Error on read sql file")
	}

	// Execute non-query with named parameters
	result, err := db.ExecContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}

// CreateTables create tables
func CreateTables(db *sql.DB) (int64, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	var tsql string
	tsql, err = ReadSQLFile("./target_table.sql", "target")
	if err != nil {
		log.Fatal("Error on read sql file")
	}

	// Execute non-query with named parameters
	result, err := db.ExecContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}

// TruncateAPISrcTable truncate API source table
func TruncateAPISrcTable(db *sql.DB) error {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return err
	}

	tsql, err := ReadSQLFile("./mssql/src_table_truncate.sql", "src")
	if err != nil {
		log.Fatal("Error on read sql file")
	}

	// Execute non-query with named parameters
	result, err := db.ExecContext(ctx, tsql)
	if err != nil {
		return err
	}

	fmt.Printf("%v", result)

	return nil
}
