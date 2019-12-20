package mssql_test

import (
	"fmt"
	"testing"

	"DBConversionCliTool/mssql"
	"DBConversionCliTool/postgres"
)

func TestCreateSchemas(t *testing.T) {
	var rowsAffected int64
	var err error

	db, err := mssql.ConnectMSDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowsAffected, err = mssql.CreateSchema(db, "aws")
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("aws schema created: %d", rowsAffected)
	}

	rowsAffected, err = mssql.CreateSchema(db, "archiving")
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("archiving schema created: %d", rowsAffected)
	}
}

func TestReadSQLFileAPISrcTable(t *testing.T) {
	tsql, err := mssql.ReadSQLFile("./src_api_table.sql", "src")
	if err != nil {
		t.Fatal("Error on read sql file")
	}

	fmt.Println(tsql)
}

func TestReadSQLFileIdentitySrcTable(t *testing.T) {
	tsql, err := mssql.ReadSQLFile("./src_identity_table.sql", "src")
	if err != nil {
		t.Fatal("Error on read sql file")
	}

	fmt.Println(tsql)
}

func TestReadSQLFileTargetTable(t *testing.T) {
	tsql, err := mssql.ReadSQLFile("./target_table.sql", "target")
	if err != nil {
		t.Fatal("Error on read sql file")
	}

	fmt.Println(tsql)
}

func TestCreateAPISrcTable(t *testing.T) {
	db, err := mssql.ConnectMSDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowsAffected, err := mssql.CreateAPISrcTable(db)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(rowsAffected)
}

func TestCreateIdentitySrcTable(t *testing.T) {
	db, err := mssql.ConnectMSDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowsAffected, err := mssql.CreateIdentitySrcTable(db)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(rowsAffected)
}

func TestTruncateAPISrcTable(t *testing.T) {
	db, err := mssql.ConnectMSDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = mssql.TruncateAPISrcTable(db)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopyIntoUsersFromPostgres(t *testing.T) {
	db, err := mssql.ConnectMSDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pgdb, err := postgres.ConnectPGSQL()
	if err != nil {
		panic("Cannot connect Postgres server")
	}
	user := new(postgres.User)
	rows, err := user.GetAll(pgdb)

	mssql.CopyIntoUsersFromPostgres(db, rows)
}
