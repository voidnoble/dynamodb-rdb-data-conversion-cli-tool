package postgres_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/joho/godotenv"

	ddb "DBConversionCliTool/dynamodb"
	"DBConversionCliTool/postgres"
)

func TestConnect(t *testing.T) {
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestCreateSchema(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	schemaName := os.Getenv("PGSQL_SCHEMA")

	// Connect Postgres
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if postgres.IsSchemaExist(db, schemaName) < 1 {
		postgres.CreateSchema(db, schemaName)
	}

	if postgres.IsSchemaExist(db, schemaName) > 0 {
		t.Log("Create schema test done.")
	} else {
		t.Fatal("Create schema test fail.")
	}
}

func TestDropSchema(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	schemaName := os.Getenv("PGSQL_SCHEMA")

	// Connect Postgres
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if postgres.IsSchemaExist(db, schemaName) > 0 {
		postgres.DropSchema(db, schemaName)
	}

	if postgres.IsSchemaExist(db, schemaName) < 1 {
		t.Log("Drop schema test done.")
	} else {
		t.Fatal("Drop schema test fail.")
	}
}

func TestCreateTables(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	stage := os.Getenv("STAGE")

	// Connect Postgres
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	postgres.CreateTables(db)

	// Make sure we can query after an error
	var i int

	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."TEAMS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check TEAMS-%s table exist expected 1, got %d", stage, i)
	}

	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."USERS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check USERS-%s table exist expected 1, got %d", stage, i)
	}

	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."GAMES-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check GAMES-%s table exist expected 1, got %d", stage, i)
	}

	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."PAYMENTS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check PAYMENTS-%s table exist expected 1, got %d", stage, i)
	}

	for _, tableName := range postgres.GetTableNames() {
		if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."%s"'));`, tableName)).Scan(&i); err != nil {
			postgres.DropTables(db)
			t.Fatal(err)
		} else if i != 1 {
			postgres.DropTables(db)
			t.Fatalf("Check %s table exist expected 1, got %d", tableName, i)
		}
	}

	postgres.DropTables(db)
}

func TestDropTables(t *testing.T) {
	// Connect Postgres
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	postgres.DropTables(db)
}

// TestCopyIntoTeamsFromDDB is data conversion testing from DDB teams to Postgres teams
func TestCopyIntoTeamsFromDDB(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	stage := os.Getenv("STAGE")

	// Postgres 접속
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var i int
	// Postgres 에 Table 생성
	postgres.CreateTables(db)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."TEAMS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check table count TEAMS-%s expected 1, got %d", stage, i)
	}

	// DynamoDB 에서 data scan
	team := new(ddb.Team)
	teams, err := team.GetAll()
	if err != nil {
		t.Fatalf("failed to get teams, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoTeamsFromDDB(db, teams)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "public"."TEAMS-%s";`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i < 1 {
		postgres.DropTables(db)
		t.Fatalf("SELECT count(*) FROM TEAMS-%s expected more than 0, got %d", stage, i)
	}

	// DropTables(db)
}

// TestCopyIntoUsersFromDDB is data conversion testing from DDB teams to Postgres teams
// Record count: 74503
// 소요시간:
// 	KST AM 09:24 ~ AM 10:20 == 60 min
// 	KST PM 04:19 ~ PM  == 04:22 == 3 min
// 	KST PM 05:25 ~ PM  == 05:28 == 3 min
func TestCopyIntoUsersFromDDB(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	stage := os.Getenv("STAGE")

	// Postgres 접속
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var i int
	// Postgres 에 Table 생성
	postgres.CreateTables(db)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."USERS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check table count USERS-%s expected 1, got %d", stage, i)
	}

	// DynamoDB 에서 data scan
	user := new(ddb.User)
	users, err := user.GetAll()
	if err != nil {
		t.Fatalf("failed to get groups, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoUsersFromDDB(db, users)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "public"."USERS-%s";`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i < 1 {
		postgres.DropTables(db)
		t.Fatalf("SELECT count(*) FROM USERS-%s expected more than 0, got %d", stage, i)
	}

	// DropTables(db)
}

// TestCopyIntoGamesFromDDB is data conversion testing from DDB games to Postgres games
// Record count / 소요시간: 11057 / 8 min Or 4min
func TestCopyIntoGamesFromDDB(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	stage := os.Getenv("STAGE")

	// Postgres 접속
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var i int
	// Postgres 에 Table 생성
	postgres.CreateTables(db)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."GAMES-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check table count GAMES-DEV expected 1, got %d", i)
	}

	// DynamoDB 에서 data scan
	game := new(ddb.Game)
	games, err := game.GetAll()
	if err != nil {
		t.Fatalf("failed to get games, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoGamesFromDDB(db, games)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "public"."GAMES-%s";`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i < 1 {
		postgres.DropTables(db)
		t.Fatalf("SELECT count(*) FROM GAMES-%s expected more than 0, got %d", stage, i)
	}

	// DropTables(db)
}

// TestCopyIntoPaymentFromDDB is data conversion testing from DDB payment to Postgres payment
func TestCopyIntoPaymentFromDDB(t *testing.T) {
	// Load Env
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}
	// Load Stage Env
	stage := os.Getenv("STAGE")

	// Postgres 접속
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var i int
	// Postgres 에 Table 생성
	postgres.CreateTables(db)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(to_regclass('"public"."PAYMENTS-%s"'));`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i != 1 {
		postgres.DropTables(db)
		t.Fatalf("Check table count PAYMENTS-%s expected 1, got %d", stage, i)
	}

	// DynamoDB 에서 data scan
	pay := new(ddb.Payment)
	pays, err := pay.GetAll()
	if err != nil {
		t.Fatalf("failed to get payments, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoPaymentFromDDB(db, pays)

	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "public"."PAYMENTS-%s";`, stage)).Scan(&i); err != nil {
		postgres.DropTables(db)
		t.Fatal(err)
	} else if i < 1 {
		postgres.DropTables(db)
		t.Fatalf("SELECT count(*) FROM PAYMENTS-%s expected more than 0, got %d", stage, i)
	}

	// DropTables(db)
}

func TestCountUsersTable(t *testing.T) {
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		panic("Cannot connect Postgres server")
	}

	user := new(postgres.User)
	cnt := user.Count(db)

	fmt.Printf("users table record count = %d\n", cnt)

	if cnt < 0 {
		t.Fatal("Can't count users table")
	}
}

func TestGetAllUsersTable(t *testing.T) {
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		panic("Cannot connect Postgres server")
	}

	user := new(postgres.User)
	rows, err := user.GetAll(db)

	for _, row := range rows {
		fmt.Println("id | url | created ")
		fmt.Printf("%3v | %6v | %6v\n", *row.OldUserId, *row.Url, *row.OnCreated)
	}
}

// TestConversion is data conversion testing in Postgres tables
func TestConversion(t *testing.T) {
	// Postgres 접속
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 주의! 순서대로 실행되어야 함
	postgres.ConvUsersTable(db)
	postgres.ConvGroupsTable(db)
	postgres.ConvEventsTable(db)
	postgres.ConvPhysicalInformationsTable(db)
	postgres.ConvSportsInterestTable(db)
	postgres.ConvUserLocationsTable(db)
	postgres.ConvMembersTable(db)
	postgres.ConvMemberRolesTable(db)
	postgres.ConvGroupLocationsTable(db)
	postgres.ConvRsvpsTable(db)
	postgres.ConvEventLocationsTable(db)
	postgres.ConvEventAmenitiesTable(db)
	postgres.ConvPaymentTable(db)
	postgres.ConvCustomerTable(db)
	postgres.ConvAspNetUsersTable(db)
	postgres.ConvAspNetUserLoginsTable(db)
}
