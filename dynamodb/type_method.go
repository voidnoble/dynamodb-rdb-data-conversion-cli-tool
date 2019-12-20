package dynamodb

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/joho/godotenv"
)

// Count is counting users table
func (user *User) Count() int64 {
	tableName := getTableName("user")

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := svc.DescribeTable(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return -1
	}

	return *result.Table.ItemCount
}

// Count is counting teams table
func (team *Team) Count() int64 {
	tableName := getTableName("team")

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := svc.DescribeTable(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return -1
	}

	return *result.Table.ItemCount
}

// Count is counting games table
func (game *Game) Count() int64 {
	tableName := getTableName("game")

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := svc.DescribeTable(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return -1
	}

	return *result.Table.ItemCount
}

// Count is counting payment table
func (payment *Payment) Count() int64 {
	tableName := getTableName("payment")

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := svc.DescribeTable(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return -1
	}

	return *result.Table.ItemCount
}

// GetAll get all data from DynamoDB users table
func (user *User) GetAll() ([]User, error) {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	tableName := getTableName("user")
	var Limit int64 = 0
	items := []User{}

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	if Limit > 0 {
		params.Limit = aws.Int64(Limit)
	}

	// Make the DynamoDB Query API call
	err := svc.ScanPages(params, func(page *dynamodb.ScanOutput, isLastPage bool) bool {
		pageItems := []User{}

		// Unmarshal the Items field in the result value to the Item Go type.
		err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			// print the error and continue receiving pages
			fmt.Printf("\nCould not unmarshal AWS Dynamodb Scan pageItems: err = %v\n", err)

			// print the response data
			for _, item := range pageItems {
				fmt.Printf("User item: %s\n", *item.UserId)
			}

			return true
		}

		items = append(items, pageItems...)

		// if not done receiving all of the pages
		if isLastPage == false {
			fmt.Printf("\n*** NOT DONE RECEIVING PAGES ***\n\n")
		} else {
			fmt.Printf("\n*** RECEIVED LAST PAGE ***\n\n")
		}

		// continue receiving pages (can be used to limit the number of pages)
		return true
	})
	if err != nil {
		log.Fatalf("failed to Scan, %v", err)
		return items, err
	}

	fmt.Println("User.GetAll Done")

	return items, nil
}

// GetAll get all data from DynamoDB teams table
func (team *Team) GetAll() ([]Team, error) {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	tableName := getTableName("team")
	var Limit int64 = 0
	items := []Team{}

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	if Limit > 0 {
		params.Limit = aws.Int64(Limit)
	}

	// Make the DynamoDB Query API call
	err := svc.ScanPages(params, func(page *dynamodb.ScanOutput, isLastPage bool) bool {
		pageItems := []Team{}

		// Unmarshal the Items field in the result value to the Item Go type.
		err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			// print the error and continue receiving pages
			fmt.Printf("\nCould not unmarshal AWS Dynamodb Scan pageItems: err = %v\n", err)

			// print the response data
			for _, item := range pageItems {
				fmt.Printf("Team item: %s | %s | %s\n", *item.Id, *item.Name, *item.Url)
			}

			return true
		}

		items = append(items, pageItems...)

		// if not done receiving all of the pages
		if isLastPage == false {
			fmt.Printf("\n*** NOT DONE RECEIVING PAGES ***\n\n")
		} else {
			fmt.Printf("\n*** RECEIVED LAST PAGE ***\n\n")
		}

		// continue receiving pages (can be used to limit the number of pages)
		return true
	})
	if err != nil {
		log.Fatalf("failed to Scan, %v", err)
		return items, err
	}

	fmt.Println("Team.GetAll Done")

	return items, nil
}

// GetAll get all data from DynamoDB games table
func (game *Game) GetAll() ([]Game, error) {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	tableName := getTableName("game")
	var Limit int64 = 0
	items := []Game{}

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	if Limit > 0 {
		params.Limit = aws.Int64(Limit)
	}

	// Make the DynamoDB Query API call
	err := svc.ScanPages(params, func(page *dynamodb.ScanOutput, isLastPage bool) bool {
		pageItems := []Game{}

		// Unmarshal the Items field in the result value to the Item Go type.
		err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			// print the error and continue receiving pages
			fmt.Printf("\nCould not unmarshal AWS Dynamodb Scan pageItems: err = %v\n", err)

			// print the response data
			for _, item := range pageItems {
				fmt.Printf("Game item: %v | %v\n", *item.Id, *item.Title)
			}

			return true
		}

		items = append(items, pageItems...)

		// if not done receiving all of the pages
		if isLastPage == false {
			fmt.Printf("\n*** NOT DONE RECEIVING PAGES ***\n\n")
		} else {
			fmt.Printf("\n*** RECEIVED LAST PAGE ***\n\n")
		}

		// continue receiving pages (can be used to limit the number of pages)
		return true
	})
	if err != nil {
		log.Fatalf("failed to Scan, %v", err)
		return items, err
	}

	fmt.Println("Game.GetAll Done")

	return items, nil
}

// GetAll get all data from DynamoDB payment table
func (pay *Payment) GetAll() ([]Payment, error) {
	godotenv.Load()
	env := os.Getenv("PGSQL_SERVER")
	if env == "" {
		err := godotenv.Load("../.env")
		if err != nil {
			panic("Can't load env file.")
		}
	}

	tableName := getTableName("payment")
	var Limit int64 = 0
	items := []Payment{}

	// Create the DynamoDB service client to make the query request with.
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	if Limit > 0 {
		params.Limit = aws.Int64(Limit)
	}

	// Make the DynamoDB Query API call
	err := svc.ScanPages(params, func(page *dynamodb.ScanOutput, isLastPage bool) bool {
		pageItems := []Payment{}

		// Unmarshal the Items field in the result value to the Item Go type.
		err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			// print the error and continue receiving pages
			fmt.Printf("\nCould not unmarshal AWS Dynamodb Scan pageItems: err = %v\n", err)

			// print the response data
			for _, item := range pageItems {
				fmt.Printf("Payment item: %s | %s\n", *item.Id, *item.Title)
			}

			return true
		}

		items = append(items, pageItems...)

		// if not done receiving all of the pages
		if isLastPage == false {
			fmt.Printf("\n*** NOT DONE RECEIVING PAGES ***\n\n")
		} else {
			fmt.Printf("\n*** RECEIVED LAST PAGE ***\n\n")
		}

		// continue receiving pages (can be used to limit the number of pages)
		return true
	})
	if err != nil {
		log.Fatalf("failed to Scan, %v", err)
		return items, err
	}

	fmt.Println("Payment.GetAll Done")

	return items, nil
}

// ConvForPostgresUsersTable convert for copy-in to Postgres users table
func (user *User) ConvForPostgresUsersTable() (map[string]interface{}, error) {
	var bytes []byte
	var err error

	item := make(map[string]interface{})

	// interface{} to json string
	bytes, err = json.Marshal(user.Teams)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["teams"] = string(bytes)

	bytes, err = json.Marshal(user.Devices)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["devices"] = string(bytes)

	bytes, err = json.Marshal(user.Facebook)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["facebook"] = string(bytes)

	bytes, err = json.Marshal(user.Google)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["google"] = string(bytes)

	bytes, err = json.Marshal(user.Pages)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["pages"] = string(bytes)

	// item 항목 값이 "null"일 경우 nil 할당
	for key, val := range item {
		if val == "null" {
			item[key] = nil

			// fmt.Printf("\nitem[%s] == null, val = %v\n", key, item[key])
		}
	}

	return item, nil
}

// ConvForPostgresTeamsTable convert for copy-in to Postgres teams table
func (team *Team) ConvForPostgresTeamsTable() (map[string]interface{}, error) {
	var bytes []byte
	var err error

	item := make(map[string]interface{})

	// interface{} to json string
	bytes, err = json.Marshal(team.Admins)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["admins"] = string(bytes)

	// item 항목 값이 "null"일 경우 nil 할당
	for key, val := range item {
		if val == "null" {
			item[key] = nil

			// fmt.Printf("\nitem[%s] == null, val = %v\n", key, item[key])
		}
	}

	return item, nil
}

// ConvForPostgresGamesTable convert for copy-in to Postgres games table
func (game *Game) ConvForPostgresGamesTable() (map[string]interface{}, error) {
	var bytes []byte
	var err error

	item := make(map[string]interface{})

	// interface{} to json string
	bytes, err = json.Marshal(game.Photos)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["photos"] = string(bytes)

	bytes, err = json.Marshal(game.Comments)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["comments"] = string(bytes)

	bytes, err = json.Marshal(game.Going)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["going"] = string(bytes)

	bytes, err = json.Marshal(game.LastLocation)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["lastlocation"] = string(bytes)

	bytes, err = json.Marshal(game.LocationDetails)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	locationdetails := string(bytes)
	locationdetails = strings.ReplaceAll(locationdetails, `"`, "")
	item["locationdetails"] = locationdetails

	bytes, err = json.Marshal(game.Location)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["location"] = string(bytes)

	bytes, err = json.Marshal(game.NotGoing)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["notgoing"] = string(bytes)

	bytes, err = json.Marshal(game.Guests)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["guests"] = string(bytes)

	bytes, err = json.Marshal(game.Result)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	item["result"] = string(bytes)

	// item 항목 값이 "null"일 경우 nil 할당
	for key, val := range item {
		if val == "null" {
			item[key] = nil

			// fmt.Printf("\nitem[%s] == null, val = %v\n", key, item[key])
		}
	}

	return item, nil
}

// ConvForPostgresPaymentTable convert for copy-in to Postgres payment table
func (pay *Payment) ConvForPostgresPaymentTable() (map[string]interface{}, error) {
	var bytes []byte
	var err error

	item := make(map[string]interface{})

	// interface{} to json string
	bytes, err = json.Marshal(pay.Charge)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	charge := string(bytes)
	charge = strings.Replace(charge, `\"`, `"`, -1)
	charge = strings.Replace(charge, `"{`, "{", -1)
	charge = strings.Replace(charge, `\}"`, "}", -1)
	charge = strings.Replace(charge, `}"`, "}", -1)
	item["charge"] = charge

	bytes, err = json.Marshal(pay.Meta)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	meta := string(bytes)
	meta = strings.Replace(meta, `\"`, `"`, -1)
	meta = strings.Replace(meta, `"{`, "{", -1)
	meta = strings.Replace(meta, `\}"`, "}", -1)
	meta = strings.Replace(meta, `}"`, "}", -1)
	item["meta"] = meta

	bytes, err = json.Marshal(pay.Refund)
	if err != nil {
		log.Panic(err)
		return item, err
	}
	refund := string(bytes)
	refund = strings.Replace(refund, `\"`, `"`, -1)
	refund = strings.Replace(refund, `"{`, "{", -1)
	refund = strings.Replace(refund, `\}"`, "}", -1)
	refund = strings.Replace(refund, `}"`, "}", -1)
	item["refund"] = refund

	// item 항목 값이 "null"일 경우 nil 할당
	for key, val := range item {
		if val == "null" {
			item[key] = nil

			// fmt.Printf("\nitem[%s] == null, val = %v\n", key, item[key])
		}
	}

	return item, nil
}
