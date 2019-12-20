package postgres

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"database/sql"

	"github.com/joho/godotenv"
	"github.com/lib/pq"

	ddb "DBConversionCliTool/dynamodb"
)

// 변수 선언
// var pgsql *sql.DB

// Hello is just hello
func Hello() string {
	return "Hello Postgres"
}

// ConnectPGSQL is Connect PostgreSQL
// 이 함수 호출 후 반드시 defer db.Close() 호출할것
func ConnectPGSQL() (db *sql.DB, err error) {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err = godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	var (
		host   = os.Getenv("PGSQL_SERVER")
		port   = os.Getenv("PGSQL_PORT")
		user   = os.Getenv("PGSQL_USER")
		passwd = os.Getenv("PGSQL_PASSWD")
		dbname = os.Getenv("PGSQL_DBNAME")
	)

	// type convert
	portNumber, err := strconv.Atoi(port)

	if host == "" || port == "0" || user == "" || passwd == "" || dbname == "" {
		panic("Can't load env variables.")
	}

	// make DB connection string
	pgsqlConnString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, portNumber, user, passwd, dbname)
	// Connect to PostgreSQL server
	db, err = sql.Open("postgres", pgsqlConnString)
	if err != nil {
		panic(err)
	}

	// Ping to checking online
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully connected to PostgreSQL!")

	return db, err
}

func getTableNameFromDDB(tableNameShortHand string) string {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	stage := os.Getenv("STAGE")

	tableName := fmt.Sprintf("%sS-%s", strings.ToUpper(tableNameShortHand), stage)

	return tableName
}

func getTableNamesShortHand() []string {
	return []string{"team", "user", "game", "payment"}
}

// CreateSchema ceate schema
func CreateSchema(db *sql.DB, name string) {
	query := fmt.Sprintf(`CREATE SCHEMA %s;`, name)
	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}

// DropSchema ceate schema
func DropSchema(db *sql.DB, name string) {
	query := fmt.Sprintf(`DROP SCHEMA IF EXISTS %s;`, name)
	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}

// IsSchemaExist is schema exist
func IsSchemaExist(db *sql.DB, name string) int {
	var i int = 0
	// Make sure we can query after an error
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM information_schema.schemata WHERE schema_name = '%s';`, name)).Scan(&i); err != nil {
		log.Fatal(err)
	}

	return i
}

// GetTableNames get table names for conversion
func GetTableNames() []string {
	// 테이블명 slice 선언
	return []string{
		"asp_net_users",
		"asp_net_user_logins",
		"users",
		"groups",
		"events",
		"physical_informations",
		"user_locations",
		"sports_interest",
		"members",
		"member_roles",
		"group_locations",
		"rsvps",
		"event_locations",
		"event_amenities",
		"payment",
		"customer",
	}
}

// CreateTables is create tables in PostgreSQL
func CreateTables(db *sql.DB) {
	// 고루틴 처리 기다리기 변수 선언
	var wg sync.WaitGroup
	wg.Add(5)

	go CreateTeamsTableFromDDB(db, &wg)
	go CreateUsersTableFromDDB(db, &wg)
	go CreateGamesTableFromDDB(db, &wg)
	go CreatePaymentTableFromDDB(db, &wg)
	go CreateConversionTables(db, &wg)

	// wg count = 0 때까지 기다리기
	wg.Wait()

	fmt.Println("CreateTables Done")
}

// DropTables is drop tables in PostgreSQL
func DropTables(db *sql.DB) {
	var tableName string
	var query string

	tableNameShortHands := getTableNamesShortHand()

	// Drop table from DDB
	for _, tableNameShortHand := range tableNameShortHands {
		tableName = getTableNameFromDDB(tableNameShortHand)
		query = fmt.Sprintf(`DROP TABLE IF EXISTS "public"."%s";`, tableName)
		if _, err := db.Exec(query); err != nil {
			log.Fatal(err)
		}
	}

	// Drop table
	for _, tableName := range GetTableNames() {
		query = fmt.Sprintf(`DROP TABLE IF EXISTS "public"."%s";`, tableName)
		if _, err := db.Exec(query); err != nil {
			log.Fatal(err)
		}
	}
}

// TruncateTables is truncate tables in PostgreSQL
func TruncateTables(db *sql.DB) {
	var tableName string
	var query string

	tableNameShortHands := getTableNamesShortHand()

	// Truncate table from DDB
	for _, tableNameShortHand := range tableNameShortHands {
		tableName = getTableNameFromDDB(tableNameShortHand)
		query = fmt.Sprintf(`TRUNCATE TABLE "public"."%s";`, tableName)
		if _, err := db.Exec(query); err != nil {
			log.Fatal(err)
		}
	}

	// Truncate table
	for _, tableName := range GetTableNames() {
		query = fmt.Sprintf(`DROP TABLE IF EXISTS "public"."%s";`, tableName)
		if _, err := db.Exec(query); err != nil {
			log.Fatal(err)
		}
	}
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

// UpdateSpecificCrawledGroupToNotCrawled update specific crawled group to not crawled
func UpdateSpecificCrawledGroupToNotCrawled(db *sql.DB, whereInCrawledGroupIds string) (int64, error) {
	// Check param db
	if db == nil {
		panic("Need db connection!")
	}

	var (
		query string
		res   sql.Result
		err   error
		count int64
	)

	// 특정 그룹들을 not crawled 로
	query = fmt.Sprintf(`Update "public"."TEAMS-PROD"
	Set
		crawled = NULL
	Where
		id IN (%s)
	;`, whereInCrawledGroupIds)
	res, err = db.Exec(query)
	if err != nil {
		panic(err)
	}
	count, err = res.RowsAffected()
	if err != nil {
		panic(err)
	}

	// 특정 그룹들의 이벤트들을 not crawled 로
	// 조건 startat >= 2019-12-01 이상 (1575158400000 - 28800000) PST -8 시간
	query = fmt.Sprintf(`Update "public"."GAMES-PROD"
	Set
		crawled = NULL
	Where
		ownerid IN (%s)
		And startat::int8 >= 1575129600000
	;`, whereInCrawledGroupIds)
	res, err = db.Exec(query)
	if err != nil {
		panic(err)
	}
	count, err = res.RowsAffected()
	if err != nil {
		panic(err)
	}

	return count, nil
}

// CopyIntoTeamsFromDDB is bulk copy into teams Table
func CopyIntoTeamsFromDDB(db *sql.DB, teams []ddb.Team) error {
	// Check param db
	if db == nil {
		panic("Need db connection!")
	}
	// Check param teams
	if len(teams) < 1 {
		panic("teams array size must more than zero!")
	}

	tableName := getTableNameFromDDB("team")
	columns := getTeamsTableColumns()

	fmt.Println("<!-- Start bulk insert -->")
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback() // 실행중 Panic() 하면 롤백하도록 defer

	stmt, err := tx.Prepare(pq.CopyIn(
		tableName, // table
		columns...,
	))
	if err != nil {
		log.Panic(err)
	}

	for teamIndex, team := range teams {
		fmt.Printf("\nFormat row %d.\n", teamIndex)

		item, err := team.ConvForPostgresTeamsTable()

		// fmt.Printf("lastLocation = %v %v\n", reflect.TypeOf(lastLocation), lastLocation)
		fmt.Printf("CopyIn row %d. %s | %s\n", teamIndex, *team.Id, *team.Name)
		// spew.Dump(user)

		_, err = stmt.Exec(
			team.Timezone,
			team.Type,
			team.CreatorsRole,
			team.CreatedAt,
			team.DisplayStat,
			team.IsPublic,
			team.Id,
			team.UpdatedAt,
			item["seasons"],
			item["follower"],
			team.Sports,
			item["lastlocation"],
			team.CanWrite,
			team.AgeGroup,
			item["families"],
			team.Url,
			team.Picture,
			item["coaches"],
			team.Deleted,
			item["rosters"],
			team.CreatedBy,
			team.IsPrivateAccount,
			team.Name,
			team.DefaultPicture,
			item["location"],
			item["admins"],
			team.Desc,
			item["scorekeeper"],
			team.FoundedIn,
			item["leagues"],
			team.IsServiceVersion1,
			team.Abbreviation,
			team.ServiceVersion1TeamSeq,
			item["pendingleaguesfromrequest"],
			item["pendingrostersfromteamrequest"],
			item["pendingrostersfromrequest"],
			item["coach"],
			item["homefield"],
			team.UpdatedBy,
			item["pendingfamilies"],
			item["pendingfollow"],
			team.Crawled,
			item["notjoinedfamilyemails"],
		)
		if err != nil {
			log.Panic(err)
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Bulk insert Done -->")
	// Final return
	return nil
}

// CopyIntoUsersFromDDB is bulk copy into users Table
func CopyIntoUsersFromDDB(db *sql.DB, users []ddb.User) error {
	// Check param db
	if db == nil {
		panic("Need db connection!")
	}
	// Check param users
	if len(users) < 1 {
		panic("users array size must more than zero!")
	}

	tableName := getTableNameFromDDB("user")
	columns := getUsersTableColumns()

	fmt.Println("<!-- Start bulk insert -->")
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback() // 실행중 Panic() 하면 롤백하도록 defer

	stmt, err := tx.Prepare(pq.CopyIn(
		tableName, // table
		columns...,
	))
	if err != nil {
		log.Panic(err)
	}

	for userIndex, user := range users {
		fmt.Printf("\nFormat row %d.\n", userIndex)

		item, err := user.ConvForPostgresUsersTable()

		// fmt.Printf("lastLocation = %v %v\n", reflect.TypeOf(lastLocation), lastLocation)
		fmt.Printf("\nCopyIn row %d. %s\n", userIndex, *user.UserId)
		// spew.Dump(user)

		_, err = stmt.Exec(
			user.UserId,
			user.Password,
			user.Salt,
			user.Email,
			user.EmailVerified,
			user.FirstName,
			user.LastName,
			user.Url,
			user.Birthday,
			user.Gender,
			user.FirstContactAt,
			user.ServiceVersion1UserSeq,
			user.Ispublic,
			item["followingleagues"],
			item["physical"],
			user.UpdatedAt,
			user.NamePattern,
			item["follower"],
			item["teams"],
			item["leagues"],
			item["devices"],
			item["lastlocation"],
			user.IsServiceVersion1,
			item["followingpendingteams"],
			item["following"],
			user.IsPrivateAccount,
			user.Name,
			item["favorsports"],
			item["followingteams"],
			user.ServiceVersion1Password,
			user.FacebookId,
			user.Picture,
			user.LoginId,
			user.LastTimeToViewFeed,
			item["facebook"],
			user.SignupSourceIp,
			user.SignupUserAgent,
			user.ProviderType,
			user.Googleid,
			item["google"],
			user.UserTimezone,
			item["livesin"],
			item["teamsasfamily"],
			item["pendingteamstojoinbyteamrequest"],
			item["pendingteamstojoinbyemail"],
			item["pendingteamstojoinbymyrequest"],
			item["followpendingusers"],
			user.PhoneNumber,
			user.CountryCode,
			item["birthplace"],
			item["pending"],
			user.CustomerId,
			item["disconnectedaccounts"],
			item["followpendingteams"],
			item["followpendingleagues"],
			user.AccountId,
			user.Locale,
			item["hiddenparties"],
			item["usedparties"],
			item["pendingteamsasfamily"],
			user.Country,
			user.Deleted,
			item["pages"],
			user.Createdat,
		)

		if err != nil {
			log.Panic(err)
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Bulk insert Done -->")
	// Final return
	return nil
}

// CopyIntoGamesFromDDB is bulk copy into games Table
func CopyIntoGamesFromDDB(db *sql.DB, games []ddb.Game) error {
	// Check param db
	if db == nil {
		panic("Need db connection!")
	}
	// Check param users
	if len(games) < 1 {
		panic("users array size must more than zero!")
	}

	tableName := getTableNameFromDDB("game")
	columns := getGamesTableColumns()

	fmt.Println("<!-- Start bulk insert -->")
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback() // 실행중 Panic() 하면 롤백하도록 defer

	stmt, err := tx.Prepare(pq.CopyIn(
		tableName, // table
		columns...,
	))
	if err != nil {
		log.Panic(err)
	}

	for gameIndex, game := range games {
		fmt.Printf("\nFormat row %d.\n", gameIndex)

		item, err := game.ConvForPostgresGamesTable()

		// fmt.Printf("lastLocation = %v %v\n", reflect.TypeOf(lastLocation), lastLocation)
		// fmt.Printf("\nCopyIn row %d. %s %v\n", gameIndex, game.Id, item)
		fmt.Printf("\nCopyIn row %d. %v\n", gameIndex, *game.Id)
		// spew.Dump(user)

		_, err = stmt.Exec(
			game.OwnerType,
			game.GameType,
			game.TimeZone,
			game.OwnerId,
			game.Title,
			item["photos"],
			game.CreatedAt,
			game.ServiceVersion1GameSeq,
			game.IsLocationTbd,
			game.Ispublic,
			game.Id,
			game.StartAt,
			game.Postponed,
			game.PushNotification,
			item["comments"],
			game.Sports,
			game.HasEnd,
			item["going"],
			item["lastlocation"],
			game.IsServiceVersion1,
			item["coachcomments"],
			game.Deleted,
			game.CreatedBy,
			item["locationdetails"],
			game.Cancelled,
			item["location"],
			item["notgoing"],
			item["maybe"],
			game.Description,
			game.AllowRsvpDeadline,
			game.ParticipantLimit,
			game.UpdatedAt,
			game.UpdatedBy,
			game.Crawled,
			game.AllowGuestLimit,
			game.Photo,
			game.EndAt,
			game.AllowParticipantLimit,
			game.RsvpDeadline,
			game.LimitGuestPerParticipant,
			item["guests"],
			game.AllowGuest,
			item["hometeamgoing"],
			item["awayteam"],
			game.MatchType,
			game.OwnerTeamType,
			game.Division,
			game.Season,
			item["hometeam"],
			item["awayteamgoing"],
			game.Fee,
			item["detailfee"],
			game.Gender,
			game.SkillLevel,
			game.FieldType,
			item["result"],
			game.ResultType,
			game.ArriveEarly,
			item["invited"],
			item["awayteamnotgoing"],
			item["hometeamnotgoing"],
			item["hometeammaybe"],
			item["awayteammaybe"],
			game.Uniform,
			game.EsultType,
		)

		if err != nil {
			log.Panic(err)
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Bulk insert Done -->")
	// Final return
	return nil
}

// CopyIntoPaymentFromDDB is bulk copy into payment Table
func CopyIntoPaymentFromDDB(db *sql.DB, pays []ddb.Payment) error {
	// Check param db
	if db == nil {
		panic("Need db connection!")
	}
	// Check param users
	if len(pays) < 1 {
		panic("users array size must more than zero!")
	}

	tableName := getTableNameFromDDB("payment")
	columns := getPaymentTableColumns()

	fmt.Println("<!-- Start bulk insert -->")
	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback() // 실행중 Panic() 하면 롤백하도록 defer

	stmt, err := tx.Prepare(pq.CopyIn(
		tableName, // table
		columns...,
	))
	if err != nil {
		log.Panic(err)
	}

	for payIndex, pay := range pays {
		fmt.Printf("\nFormat row %d.\n", payIndex)

		item, err := pay.ConvForPostgresPaymentTable()

		// fmt.Printf("lastLocation = %v %v\n", reflect.TypeOf(lastLocation), lastLocation)
		fmt.Printf("\nCopyIn row %d. %s\n", payIndex, *pay.Id)
		// spew.Dump(user)

		_, err = stmt.Exec(
			pay.RecipientUserId,
			pay.AccountId,
			pay.CreatedAt,
			item["charge"],
			item["meta"],
			pay.Id,
			pay.Title,
			pay.UserId,
			pay.Status,
			item["refund"],
		)

		if err != nil {
			log.Panic(err)
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Bulk insert Done -->")
	// Final return
	return nil
}
