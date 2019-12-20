package dynamodb_test

import (
	"fmt"
	"testing"

	ddb "DBConversionCliTool/dynamodb"
)

func TestCountUsers(t *testing.T) {
	user := new(ddb.User)
	cnt := user.Count()
	if cnt == -1 {
		t.Errorf("Cannot counting DynamoDB users-prod table\n")
	}

	fmt.Printf("DynamoDB users-prod records count = %d\n", cnt)
}

func TestCountTeams(t *testing.T) {
	team := new(ddb.Team)
	cnt := team.Count()
	if cnt == -1 {
		t.Errorf("Cannot counting DynamoDB teams-prod table\n")
	}

	fmt.Printf("DynamoDB teams-prod records count = %d\n", cnt)
}

func TestCountGames(t *testing.T) {
	game := new(ddb.Game)
	cnt := game.Count()
	if cnt == -1 {
		t.Errorf("Cannot counting DynamoDB games-prod table\n")
	}

	fmt.Printf("DynamoDB games-prod records count = %d\n", cnt)
}

func TestCountPayments(t *testing.T) {
	payment := new(ddb.Payment)
	cnt := payment.Count()
	if cnt == -1 {
		t.Errorf("Cannot counting DynamoDB payments-prod table\n")
	}

	fmt.Printf("DynamoDB payments-prod records count = %d\n", cnt)
}

func TestGetUsersAll(t *testing.T) {
	user := new(ddb.User)
	users, err := user.GetAll()
	if err != nil {
		t.Fatal(err)
	}

	usersCount := len(users)
	wantCountLess := 1

	if usersCount < wantCountLess {
		t.Errorf("Dynamodb Users records count got %d want less than %d", usersCount, wantCountLess)
	}

	t.Logf("Dynamodb Users records count = %d", usersCount+1)
}

func TestGetTeamsAll(t *testing.T) {
	team := new(ddb.Team)
	teams, err := team.GetAll()
	if err != nil {
		t.Fatal(err)
	}

	teamsCount := len(teams)
	wantCountLess := 1

	if teamsCount < wantCountLess {
		t.Errorf("Dynamodb Teams records count got %d want less than %d", teamsCount, wantCountLess)
	}

	t.Logf("Dynamodb Teams records count = %d", teamsCount+1)
}

func TestGetGamesAll(t *testing.T) {
	game := new(ddb.Game)
	games, err := game.GetAll()
	if err != nil {
		t.Fatal(err)
	}

	gamesCount := len(games)
	wantCountLess := 1

	if gamesCount < wantCountLess {
		t.Errorf("Dynamodb games records count got %d want less than %d", gamesCount, wantCountLess)
	}

	t.Logf("Dynamodb games records count = %d", gamesCount+1)
}

func TestGetPaymentAll(t *testing.T) {
	pay := new(ddb.Payment)
	pays, err := pay.GetAll()
	if err != nil {
		t.Fatal(err)
	}

	paysCount := len(pays)
	wantCountLess := 1

	if paysCount < wantCountLess {
		t.Errorf("Dynamodb pays records count got %d want less than %d", paysCount, wantCountLess)
	}

	t.Logf("Dynamodb pays records count = %d", paysCount+1)
}
