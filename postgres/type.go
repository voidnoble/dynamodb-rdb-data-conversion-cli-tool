package postgres

// Group is a representation of a group
type Group struct {
	Id        string `json:"id"`        // Hash key
	CreatedAt int64  `json:"createdAt"` // Range key, GSI Sort key
	OnCreated string `json:"-"`
	Photo     string `json:"picture"`
	Name      string `json:"name"`
	Desc      string `json:"desc"`
	Url       string `json:"url"`
	Deleted   bool   `json:"deleted"`
	IsDeleted int    `json:"-"`
	IsPublic  bool   `json:"isPublic"`
	IsPrivate int    `json:"-"`
}

// User is a representation of a user
type User struct {
	Email     *string
	Name      *string
	Gender    *int
	Url       *string
	IsDeleted *int
	OnCreated *string
	OnUpdated *string
	OnDeleted *string
}

// Event is a representation of a event
type Event struct {
	Photo       string `json:"-"`
	Title       string `json:"-"`
	Start       string `json:"-"`
	End         string `json:"-"`
	TimeZone    string `json:"-"`
	Description string `json:"-"`
	Gender      string `json:"-"`
	FieldType   string `json:"-"`
	IsDeleted   string `json:"-"`
	OnCreated   string `json:"-"`
	OnUpdated   string `json:"-"`
	OnDeleted   string `json:"-"`
}

// AddressComponent is Location struct in User, Group, Event
type AddressComponent struct {
	ID      int
	Type    string
	Lon     int
	Lat     int
	Country string
	State   string
	County  string
	City    string
	ZipCode string
	Address string
}

type Payment struct {
	Id              *string
	Amount          *float64
	FeeAmount       *float64
	CustomerId      *string
	IsRefunded      *int
	RefundId        *string
	CardBrand       *string
	CardLast4Digits *string
	Currency        *string
	OldEventId      *string
	OldUserId       *string
	OnCreated       *string
}
