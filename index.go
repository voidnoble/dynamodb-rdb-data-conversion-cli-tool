package main

import (
	"fmt"
	"log"
	"time"

	ddb "DBConversionCliTool/dynamodb"
	"DBConversionCliTool/mssql"
	"DBConversionCliTool/postgres"
)

func main() {
	//
	// 작업 시작 시간 기록
	//
	startTime := time.Now()

	// TODO:

	// Connect Postgres
	db, err := postgres.ConnectPGSQL()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create tables in Postgres
	postgres.CreateTables(db)

	//
	// users-prod --> users
	//
	// DynamoDB 에서 data scan
	user := new(ddb.User)
	users, err := user.GetAll()
	if err != nil {
		log.Fatalf("failed to get groups, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoUsersFromDDB(db, users)

	//
	// teams-prod --> groups
	//
	// DynamoDB 에서 data scan
	team := new(ddb.Team)
	teams, err := team.GetAll()
	if err != nil {
		log.Fatalf("Failed to get teams, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoTeamsFromDDB(db, teams)

	//
	// games-prod --> events
	//
	// DynamoDB 에서 data scan
	game := new(ddb.Game)
	games, err := game.GetAll()
	if err != nil {
		log.Fatalf("failed to get games, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoGamesFromDDB(db, games)

	// DynamoDB 에서 data scan
	pay := new(ddb.Payment)
	pays, err := pay.GetAll()
	if err != nil {
		log.Fatalf("failed to get payments, %v", err)
	}

	// 위에서 scan 한 data 들을 Postgres 에 bulk copy
	postgres.CopyIntoPaymentFromDDB(db, pays)

	//
	// 특정 crawled 그룹들과 이벤트들을 not crawled 로 업데이트
	//
	var whereInCrawledGroupIds string = "'meetup_group_21043741', 'meetup_group_2120701', 'meetup_group_6058882', 'meetup_group_19677123', 'meetup_group_19649516', 'meetup_group_22082862', 'meetup_group_20748188'"
	rowsAffected, err := postgres.UpdateSpecificCrawledGroupToNotCrawled(db, whereInCrawledGroupIds)
	if err != nil {
		log.Fatalf("Failed to update specific crawled group to not crawled, %v", err)
	}
	log.Printf("Update specific crawled group to not crawled count = %d\n", rowsAffected)

	//
	// Conversion in Postgres
	//
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

	//
	// Connect mssql
	//
	msdb, err := mssql.ConnectMSDB()
	if err != nil {
		panic(err)
	}
	defer msdb.Close()

	//
	// Create schema name on mssql
	//
	mssql.CreateSchema(msdb, "aws")
	mssql.CreateSchema(msdb, "archiving")

	//
	// Create src table from postgres on mssql API server
	//
	mssql.CreateAPISrcTable(msdb)
	mssql.CreateIdentitySrcTable(msdb)

	//
	// 작업 종료 후 시간 계산
	//
	elapsed := time.Since(startTime)
	fmt.Println("작업 소요시간: ", elapsed)
}
