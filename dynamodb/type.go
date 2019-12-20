package dynamodb

// Team is a representation of a group
type Team struct {
	Timezone       *string     `json:"timezone" dynamodbav:"timezone"`
	Type           *string     `json:"type" dynamodbav:"type"`
	CreatedAt      *int64      `json:"createdAt" dynamodbav:"createdAt"`
	DisplayStat    *bool       `json:"displayStat" dynamodbav:"displayStat"`
	IsPublic       *bool       `json:"isPublic" dynamodbav:"isPublic"`
	Id             *string     `json:"id" dynamodbav:"id"`
	UpdatedAt      *int64      `json:"updatedAt" dynamodbav:"updatedAt"`
	Seasons        interface{} `json:"seasons" dynamodbav:"seasons"`
	Follower       interface{} `json:"follower" dynamodbav:"follower"`
	LastLocation   interface{} `json:"lastLocation" dynamodbav:"lastLocation"`
	Url            *string     `json:"url" dynamodbav:"url"`
	Deleted        *bool       `json:"deleted" dynamodbav:"deleted"`
	CreatedBy      *string     `json:"createdBy" dynamodbav:"createdBy"`
	Name           *string     `json:"name" dynamodbav:"name"`
	DefaultPicture *string     `json:"defaultPicture" dynamodbav:"defaultPicture"`
	Location       interface{} `json:"location" dynamodbav:"location"`
	Admins         interface{} `json:"admins" dynamodbav:"admins"`
	Desc           *string     `json:"desc" dynamodbav:"desc"`
	UpdatedBy      *string     `json:"updatedBy" dynamodbav:"updatedBy"`
	Crawled        *bool       `json:"crawled" dynamodbav:"crawled"`
}

// User is a representation of a user
type User struct {
	UserId        *string     `json:"userId" dynamodbav:"userId"`
	Password      *string     `json:"password" dynamodbav:"password"`
	Salt          *string     `json:"salt" dynamodbav:"salt"`
	Email         *string     `json:"email" dynamodbav:"email"`
	EmailVerified *bool       `json:"emailVerified" dynamodbav:"emailVerified"`
	FirstName     *string     `json:"firstName" dynamodbav:"firstName"`
	LastName      *string     `json:"lastName" dynamodbav:"lastName"`
	Url           *string     `json:"url" dynamodbav:"url"`
	Birthday      *string     `json:"birthday" dynamodbav:"birthday"`
	Gender        *string     `json:"gender" dynamodbav:"gender"`
	Ispublic      *bool       `json:"isPublic" dynamodbav:"isPublic"`
	UpdatedAt     *int64      `json:"updatedAt" dynamodbav:"updatedAt"`
	Teams         []string    `json:"teams" dynamodbav:"teams"`
	Devices       interface{} `json:"devices" dynamodbav:"devices"`
	LastLocation  interface{} `json:"lastLocation" dynamodbav:"lastLocation"`
	Name          *string     `json:"name" dynamodbav:"name"`
	FacebookId    *string     `json:"facebookId" dynamodbav:"facebookId"`
	Picture       *string     `json:"picture" dynamodbav:"picture"`
	LoginId       *string     `json:"loginId" dynamodbav:"loginId"`
	Facebook      interface{} `json:"facebook" dynamodbav:"facebook"`
	Googleid      *string     `json:"googleId" dynamodbav:"googleId"`
	Google        interface{} `json:"google" dynamodbav:"google"`
	UserTimezone  *string     `json:"userTimeZone" dynamodbav:"userTimeZone"`
	PhoneNumber   *string     `json:"phoneNumber" dynamodbav:"phoneNumber"`
	Locale        *string     `json:"locale" dynamodbav:"locale"`
	Deleted       *bool       `json:"deleted" dynamodbav:"deleted"`
	Createdat     *int64      `json:"createdAt" dynamodbav:"createdAt"`
}

// Game is a representation of a game
type Game struct {
	OwnerType   *string     `json:"ownerType" dynamodbav:"ownerType"`
	GameType    *string     `json:"gameType" dynamodbav:"gameType"`
	TimeZone    *string     `json:"timezone" dynamodbav:"timezone"`
	Title       *string     `json:"title" dynamodbav:"title"`
	Photos      interface{} `json:"photos" dynamodbav:"photos"`
	CreatedAt   *int64      `json:"createdAt" dynamodbav:"createdAt"`
	Ispublic    *bool       `json:"isPublic" dynamodbav:"isPublic"`
	Id          *string     `json:"id" dynamodbav:"id"`
	StartAt     *int64      `json:"startAt" dynamodbav:"startAt"`
	Comments    interface{} `json:"comments" dynamodbav:"comments"`
	Deleted     *bool       `json:"deleted" dynamodbav:"deleted"`
	CreatedBy   *string     `json:"createdBy" dynamodbav:"createdBy"`
	Cancelled   *bool       `json:"cancelled" dynamodbav:"cancelled"`
	Location    interface{} `json:"location" dynamodbav:"location"`
	NotGoing    interface{} `json:"notGoing" dynamodbav:"notGoing"`
	Description *string     `json:"description" dynamodbav:"description"`
	UpdatedAt   *int64      `json:"updatedAt" dynamodbav:"updatedAt"`
	UpdatedBy   *string     `json:"updatedBy" dynamodbav:"updatedBy"`
	Crawled     *bool       `json:"crawled" dynamodbav:"crawled"`
	Photo       *string     `json:"photo" dynamodbav:"photo"`
	EndAt       *int64      `json:"endAt" dynamodbav:"endAt"`
	Guests      interface{} `json:"guests" dynamodbav:"guests"`
	Gender      *string     `json:"gender" dynamodbav:"gender"`
	FieldType   *string     `json:"fieldType" dynamodbav:"fieldType"`
}

// Payment is a representation of a payment
type Payment struct {
	RecipientUserId *string     `json:"recipientUserId" dynamodbav:"recipientUserId"`
	AccountId       *string     `json:"accountId" dynamodbav:"accountId"`
	CreatedAt       *int64      `json:"createdAt" dynamodbav:"createdAt"`
	Charge          interface{} `json:"charge" dynamodbav:"charge"`
	Meta            interface{} `json:"meta" dynamodbav:"meta"`
	Id              *string     `json:"id" dynamodbav:"id"`
	Title           *string     `json:"title" dynamodbav:"title"`
	UserId          *string     `json:"userId" dynamodbav:"userId"`
	Status          *string     `json:"status" dynamodbav:"status"`
	Refund          interface{} `json:"refund" dynamodbav:"refund"`
}
