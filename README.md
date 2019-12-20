# Notice

This program will not working.
이 프로그램은 동작하지 않을것입니다.

Because I was change codes for public that made private purpose before.
단지 go 언어 코딩 기록을 남기기 위해 private 코드를 개방 위해 수정을 가했기 때문입니다.

# Purpose

1. Data migrate from AWS dyanmoDB to PostgreSQL in the office server
2. MS-SQL 까지 이전 중 필요한 SQL 처리들 대응

# Initial setting

## Setting AWS Cli

1. https://docs.aws.amazon.com/ko_kr/cli/latest/userguide/cli-chap-install.html
2. https://docs.aws.amazon.com/ko_kr/cli/latest/userguide/cli-chap-configure.html#cli-quick-configuration
>aws configure 설정시 DynamoDB 권한 가진 IAM Access Key 로 셋팅 할것!

## Set env

Environment configuration

```bash
$ cp .env-example .env
$ vi .env
```

# Execute

기본 실행
```bash
$ go run DBConversionCliTool
```

로그 남기기
```bash
$ go run DBConversionCliTool 2>> exec.log
```

# Test

## Terminal

>주의! 전체 테스트에는 Drop table 이 섞여 있음

```bash
go test
```

## VSCode
..._test.go 파일에서
각 func Test... 위에 뜨는 메뉴 중
debug test 클릭

# Reference

- https://wiki.example.net/display/~620896009/Research+conversion+DynamoDB+to+MSSQL
- AWS SDK for Go API Reference
  - https://godoc.org/github.com/citysir/aws-sdk-go/service/dynamodb
  - https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/
  - https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/expression/
  - https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/dynamodbattribute/
- DynamoDB 연동
  - https://qiita.com/sakayuka/items/4af7fead94d589716f4d
  - https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/go/example_code/dynamodb
- Azure SQL 연동
  - https://docs.microsoft.com/ko-kr/azure/sql-database/sql-database-connect-query-go
- GoLang struct 모델
  - https://github.com/markuscraig/dynamodb-examples/blob/master/go/types/movie.go
