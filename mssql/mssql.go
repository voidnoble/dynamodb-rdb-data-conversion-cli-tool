package mssql

import (
	"DBConversionCliTool/postgres"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/joho/godotenv"

	"context"
	"errors"
	"fmt"
	"log"
	"os"

	// "encoding/json"
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	mssql "github.com/denisenkom/go-mssqldb"
)

// 변수 선언
var msdb *sql.DB

// ConnectMSDB is Connect to MS SQL Database
func ConnectMSDB() (*sql.DB, error) {
	var err error

	godotenv.Load()
	env := os.Getenv("MSSQL_SERVER")
	if env == "" {
		err = godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	var (
		server = os.Getenv("MSSQL_SERVER")
		port   = os.Getenv("MSSQL_PORT")
		user   = os.Getenv("MSSQL_USER")
		passwd = os.Getenv("MSSQL_PASSWD")
		dbname = os.Getenv("MSSQL_DBNAME")
	)

	portNumber, err := strconv.Atoi(port)

	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, passwd, portNumber, dbname)

	// Create connection pool
	msdb, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}

	ctx := context.Background()
	// Check if database is alive.
	err = msdb.PingContext(ctx)
	if err != nil {
		log.Fatal("Error connection dead: ", err.Error())
	}

	fmt.Printf("Connected!\n")

	return msdb, nil
}

// CreateSchemas create DB schema namespaces
func CreateSchemas(db *sql.DB) {
	var rowsAffected int64
	var err error

	schemaNames := [2]string{"aws", "archiving"}

	for _, schemaName := range schemaNames {
		rowsAffected, err = CreateSchema(db, schemaName)
		if err != nil {
			panic(err)
		} else {
			fmt.Printf("Rows affected: %d", rowsAffected)
		}
	}
}

// ReadSQLFile read sql query file
func ReadSQLFile(filePath string, schemaType string) (string, error) {
	var (
		err        error
		schemaName string
		tsql       string
	)

	godotenv.Load()
	env := os.Getenv("MSSQL_SERVER")
	if env == "" {
		err = godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	if schemaType == "src" {
		schemaName = os.Getenv("MSSQL_SRC_SCHEMA")
	} else if schemaType == "target" {
		schemaName = os.Getenv("MSSQL_TARGET_SCHEMA")
	} else {
		panic("schemaType parameter must src or target")
	}

	sqlData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	tsql = string(sqlData)

	// 원본 스키마명을 기본값인 aws 와 다르게 지정한 경우 반영
	if schemaType == "src" && schemaName != "aws" {
		tsql = strings.Replace(tsql, "[aws]", fmt.Sprintf("[%s]", schemaName), -1)
	}

	// 대상 스키마명을 기본값인 dbo 와 다르게 지정한 경우 반영
	if schemaType == "target" && schemaName != "dbo" {
		tsql = strings.Replace(tsql, "[dbo]", fmt.Sprintf("[%s]", schemaName), -1)
	}

	return tsql, nil
}

// CountTableRows is counting rows in table
func CountTableRows(db *sql.DB, tableName string) int64 {
	var cnt int64
	err := db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&cnt)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("No data in users table.")
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Printf("count is %d\n", cnt)
	}

	return cnt
}

// CopyIntoUsersFromPostgres copy in to users table from Postgres
// https://godoc.org/github.com/denisenkom/go-mssqldb#CopyIn
func CopyIntoUsersFromPostgres(db *sql.DB, users []postgres.User) {
	tableName := "aws.users"
	columns := getUsersTableColumns()

	fmt.Println("<!-- Start bulk insert -->")
	// Begin transaction
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// mssqldb.CopyIn creates string to be consumed by Prepare
	stmt, err := txn.Prepare(mssql.CopyIn(
		tableName,           // table
		mssql.BulkOptions{}, // options
		columns...,          // columns
	))
	if err != nil {
		log.Fatal(err.Error())
	}

	for userIndex, user := range users {
		fmt.Printf("\nCopyIn row %d. %s\n", userIndex, *user.OldUserId)

		_, err = stmt.Exec(
			user.OldUserId,
			user.OpenId,
			user.Photo,
			user.Email,
			user.FirstName,
			user.LastName,
			user.NormalizedName,
			user.Gender,
			user.AccountId,
			user.DateOfBirth,
			user.Url,
			user.LoginId,
			user.IsDeleted,
			user.OnCreated,
			user.OnUpdated,
			user.OnDeleted,
		)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	rowCount, _ := result.RowsAffected()
	log.Printf("%d row copied\n", rowCount)
	log.Printf("bye\n")

	fmt.Println("Bulk insert Done -->")
}

// PrintMSDBGroups is print mssql db group
func PrintMSDBGroups() {
	count, err := BrowseGroups()
	if err != nil {
		log.Fatal("Error browsing Groups: ", err.Error())
	}
	fmt.Printf("Browse %d row(s) successfully.\n", count)

	/* // Create employee
	createID, err := CreateEmployee("Jake", "United States")
	if err != nil {
		log.Fatal("Error creating Employee: ", err.Error())
	}
	fmt.Printf("Inserted ID: %d successfully.\n", createID)

	// Read employees
	count, err := ReadEmployees()
	if err != nil {
		log.Fatal("Error reading Employees: ", err.Error())
	}
	fmt.Printf("Read %d row(s) successfully.\n", count)

	// Update from database
	updatedRows, err := UpdateEmployee("Jake", "Poland")
	if err != nil {
		log.Fatal("Error updating Employee: ", err.Error())
	}
	fmt.Printf("Updated %d row(s) successfully.\n", updatedRows)

	// Delete from database
	deletedRows, err := DeleteEmployee("Jake")
	if err != nil {
		log.Fatal("Error deleting Employee: ", err.Error())
	}
	fmt.Printf("Deleted %d row(s) successfully.\n", deletedRows) */
}

// CreateEmployee inserts an employee record
func CreateEmployee(name string, location string) (int64, error) {
	ctx := context.Background()
	var err error

	if msdb == nil {
		err = errors.New("CreateEmployee: db is null")
		return -1, err
	}

	// Check if database is alive.
	err = msdb.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := "INSERT INTO TestSchema.Employees (Name, Location) VALUES (@Name, @Location); select convert(bigint, SCOPE_IDENTITY());"

	stmt, err := msdb.Prepare(tsql)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(
		ctx,
		sql.Named("Name", name),
		sql.Named("Location", location))
	var newID int64
	err = row.Scan(&newID)
	if err != nil {
		return -1, err
	}

	return newID, nil
}

// BrowseGroups is browse groups
func BrowseGroups() (int, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := msdb.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("SELECT Id, Name FROM dbo.Groups;")

	// Execute query
	rows, err := msdb.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	var count int
	var group Group
	// var groups []Group

	// Iterate through the result set.
	for rows.Next() {
		// Get values from row.
		err := rows.Scan(&group.Id, &group.Name)
		if err != nil {
			return -1, err
		}

		fmt.Printf("ID: %s, Name: %s\n", group.Id, group.Name)
		count++
	}

	return count, nil
}

// ReadEmployees reads all employee records
func ReadEmployees() (int, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := msdb.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("SELECT Id, Name, Location FROM TestSchema.Employees;")

	// Execute query
	rows, err := msdb.QueryContext(ctx, tsql)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	var count int

	// Iterate through the result set.
	for rows.Next() {
		var name, location string
		var id int

		// Get values from row.
		err := rows.Scan(&id, &name, &location)
		if err != nil {
			return -1, err
		}

		fmt.Printf("ID: %d, Name: %s, Location: %s\n", id, name, location)
		count++
	}

	return count, nil
}

// UpdateEmployee updates an employee's information
func UpdateEmployee(name string, location string) (int64, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := msdb.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("UPDATE TestSchema.Employees SET Location = @Location WHERE Name = @Name")

	// Execute non-query with named parameters
	result, err := msdb.ExecContext(
		ctx,
		tsql,
		sql.Named("Location", location),
		sql.Named("Name", name))
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}

// DeleteEmployee deletes an employee from the database
func DeleteEmployee(name string) (int64, error) {
	ctx := context.Background()

	// Check if database is alive.
	err := msdb.PingContext(ctx)
	if err != nil {
		return -1, err
	}

	tsql := fmt.Sprintf("DELETE FROM TestSchema.Employees WHERE Name = @Name;")

	// Execute non-query with named parameters
	result, err := msdb.ExecContext(ctx, tsql, sql.Named("Name", name))
	if err != nil {
		return -1, err
	}

	return result.RowsAffected()
}
