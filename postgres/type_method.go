package postgres

import (
	"database/sql"
	"fmt"
	"log"
)

// Count is counting users table
func (user *User) Count(db *sql.DB) int64 {
	return CountTableRows(db, "users")
}

// Count is counting groups table
func (group *Group) Count(db *sql.DB) int64 {
	return CountTableRows(db, "groups")
}

// Count is counting events table
func (event *Event) Count(db *sql.DB) int64 {
	return CountTableRows(db, "events")
}

// Count is counting payment table
func (payment *Payment) Count(db *sql.DB) int64 {
	return CountTableRows(db, "payment")
}

// GetAll get all data from Postgres users table
func (user *User) GetAll(db *sql.DB) ([]User, error) {
	tableName := "public.users"
	items := []User{}

	rows, err := db.Query("SELECT * FROM " + tableName + " LIMIT 100")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		if err := rows.Scan(
			&user.Photo,
			&user.Email,
			&user.Name,
			&user.Gender,
			&user.Url,
			&user.IsDeleted,
			&user.OnCreated,
			&user.OnUpdated,
			&user.OnDeleted,
		); err != nil {
			log.Fatal(err)
		}

		// fmt.Println("id | url | created ")
		// fmt.Printf("%3v | %6v | %6v\n", *user.OldUserId, *user.Url, *user.OnCreated)

		items = append(items, *user)
	}

	fmt.Println("User.GetAll Done")

	return items, nil
}
