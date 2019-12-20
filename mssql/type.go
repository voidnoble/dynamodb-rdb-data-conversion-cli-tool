package mssql

// Group is a representation of a group
type Group struct {
	Id         string `json:"id"`        // Hash key
	CreatedAt  int64  `json:"createdAt"` // Range key, GSI Sort key
	OnCreated  string `json:"-"`
	Photo      string `json:"picture"`
	Name       string `json:"name"`
	Desc       string `json:"desc"`
	Sports     string `json:"sports"`
	SportsType int    `json:"-"`
	Agegroup   string `json:"ageGroup"`
	Age        int    `json:"-"`
	Url        string `json:"url"`
	Deleted    bool   `json:"deleted"`
	IsDeleted  int    `json:"-"`
	IsPublic   bool   `json:"isPublic"`
	IsPrivate  int    `json:"-"`
}

// User is a representation of a user
type User struct {
	OpenId    string // UUID v4
	Email     string `json:"email"`
	Name      string `json:"Name"`
	Url       string `json:"url"`
	IsDeleted string `json:"-"`
	OnCreated string `json:"createdAt"`
	OnUpdated string `json:"-"`
	OnDeleted string `json:"-"`
}
