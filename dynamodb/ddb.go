package dynamodb

import (
	"fmt"
	"os"
	"strings"

	// "os"
	// "encoding/json"

	"github.com/joho/godotenv"
	// "github.com/citysir/aws-sdk-go/aws/awsutil"
)

func getTableName(id string) string {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	stage := os.Getenv("STAGE")
	stage = strings.ToLower(stage)

	var tableName string
	if id == "payment" {
		tableName = fmt.Sprintf("example-%s-%s", id, stage)
	} else {
		tableName = fmt.Sprintf("example-%ss-%s", id, stage)
	}

	return tableName
}
