package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)

// CreateTeamsTableFromDDB is creating teams table in Postgres
func CreateTeamsTableFromDDB(db *sql.DB, wg *sync.WaitGroup) {
	tableName := getTableNameFromDDB("team")

	// 고루틴
	go func() {
		defer wg.Done()

		var query string

		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "public"."%s" (
			timezone                      varchar(1048576),
			type                          varchar(1048576),
			creatorsrole                  varchar(1048576),
			createdat                     double precision,
			displaystat                   varchar(5),
			ispublic                      varchar(5),
			id                            varchar(1048576),
			updatedat                     double precision,
			seasons                       varchar(1048576),
			follower                      varchar(1048576),
			sports                        varchar(1048576),
			lastlocation                  varchar(1048576),
			canwrite                      varchar(5),
			agegroup                      varchar(1048576),
			families                      varchar(1048576),
			url                           varchar(1048576),
			picture                       varchar(1048576),
			coaches                       varchar(1048576),
			deleted                       varchar(5),
			rosters                       varchar(1048576),
			createdby                     varchar(1048576),
			isprivateaccount              varchar(5),
			name                          varchar(1048576),
			defaultpicture                varchar(1048576),
			location                      varchar(1048576),
			admins                        varchar(1048576),
			"desc"                        varchar(1048576),
			scorekeeper                   varchar(1048576),
			foundedin                     double precision,
			leagues                       varchar(1048576),
			isserviceversion1             varchar(5),
			abbreviation                  varchar(1048576),
			serviceversion1teamseq        double precision,
			pendingleaguesfromrequest     varchar(1048576),
			pendingrostersfromteamrequest varchar(1048576),
			pendingrostersfromrequest     varchar(1048576),
			coach                         varchar(1048576),
			homefield                     varchar(1048576),
			updatedby                     varchar(1048576),
			pendingfamilies               varchar(1048576),
			pendingfollow                 varchar(1048576),
			crawled                       varchar(5),
			notjoinedfamilyemails         varchar(1048576)
		);
		ALTER TABLE IF EXISTS "public"."%s" OWNER TO example;`, tableName, tableName)

		// DB 에 쿼리 실행
		if _, err := db.Exec(query); err != nil {
			log.Fatal(err)
		}
	}()
}

// CreateUsersTableFromDDB is creating users table in Postgres
func CreateUsersTableFromDDB(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	tableName := getTableNameFromDDB("user")

	var query string

	query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		birthday                        varchar,
		lastname                        varchar(1048576),
		gender                          varchar(1048576),
		firstcontactat                  double precision,
		serviceversion1userseq          double precision,
		createdat                       double precision,
		password                        varchar(1048576),
		ispublic                        varchar(5),
		followingleagues                varchar(1048576),
		physical                        varchar(1048576),
		email                           varchar(1048576),
		updatedat                       double precision,
		namepattern                     varchar(1048576),
		salt                            varchar(1048576),
		follower                        varchar(1048576),
		teams                           varchar(1048576),
		leagues                         varchar(1048576),
		devices                         varchar(1048576),
		lastlocation                    varchar(1048576),
		isserviceversion1               varchar(5),
		userid                          varchar(1048576),
		url                             varchar(1048576),
		firstname                       varchar(1048576),
		emailverified                   varchar(5),
		followingpendingteams           varchar(1048576),
		following                       varchar(1048576),
		isprivateaccount                varchar(5),
		name                            varchar(1048576),
		favorsports                     varchar(1048576),
		followingteams                  varchar(1048576),
		serviceversion1password         varchar(1048576),
		facebookid                      varchar(1048576),
		picture                         varchar(1048576),
		loginid                         varchar(1048576),
		lasttimetoviewfeed              double precision,
		facebook                        varchar(1048576),
		signupsourceip                  varchar(1048576),
		signupuseragent                 varchar(1048576),
		provider_type                   varchar(1048576),
		googleid                        varchar(1048576),
		google                          varchar(1048576),
		usertimezone                    varchar(1048576),
		livesin                         varchar(1048576),
		teamsasfamily                   varchar(1048576),
		pendingteamstojoinbyteamrequest varchar(1048576),
		pendingteamstojoinbyemail       varchar(1048576),
		pendingteamstojoinbymyrequest   varchar(1048576),
		followpendingusers              varchar(1048576),
		phonenumber                     varchar(1048576),
		countrycode                     double precision,
		birthplace                      varchar(1048576),
		pending                         varchar(1048576),
		customerid                      varchar(1048576),
		disconnectedaccounts            varchar(1048576),
		followpendingteams              varchar(1048576),
		followpendingleagues            varchar(1048576),
		accountid                       varchar(1048576),
		locale                          varchar(1048576),
		hiddenparties                   varchar(1048576),
		usedparties                     varchar(1048576),
		pendingteamsasfamily            varchar(1048576),
		country                         varchar(1048576),
		deleted                         varchar(5),
		pages                           varchar(1048576)
	);
	ALTER TABLE IF EXISTS "%s" OWNER TO example;`, tableName, tableName)

	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}

// CreateGamesTableFromDDB is creating games table in Postgres
func CreateGamesTableFromDDB(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	tableName := getTableNameFromDDB("game")

	var query string

	query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		ownertype                varchar(1048576),
		gametype                 varchar(1048576),
		timezone                 varchar(1048576),
		ownerid                  varchar(1048576),
		title                    varchar(1048576),
		photos                   varchar(1048576),
		createdat                double precision,
		serviceversion1gameseq   double precision,
		islocationtbd            varchar(5),
		ispublic                 varchar(5),
		id                       varchar(1048576),
		startat                  double precision,
		postponed                varchar(5),
		pushnotification         varchar(5),
		comments                 varchar(1048576),
		sports                   varchar(1048576),
		hasend                   varchar(5),
		going                    varchar(1048576),
		lastlocation             varchar(1048576),
		isserviceversion1        varchar(5),
		coachcomments            varchar(1048576),
		deleted                  varchar(5),
		createdby                varchar(1048576),
		locationdetails          varchar(1048576),
		cancelled                varchar(5),
		location                 varchar(1048576),
		notgoing                 varchar(1048576),
		maybe                    varchar(1048576),
		description              varchar(1048576),
		allowrsvpdeadline        varchar(5),
		participantlimit         double precision,
		updatedat                double precision,
		updatedby                varchar(1048576),
		crawled                  varchar(5),
		allowguestlimit          varchar(5),
		photo                    varchar(1048576),
		endat                    double precision,
		allowparticipantlimit    varchar(5),
		rsvpdeadline             double precision,
		limitguestperparticipant double precision,
		guests                   varchar(1048576),
		allowguest               varchar(5),
		hometeamgoing            varchar(1048576),
		awayteam                 varchar(1048576),
		matchtype                varchar(1048576),
		ownerteamtype            varchar(1048576),
		division                 varchar(1048576),
		season                   varchar(1048576),
		hometeam                 varchar(1048576),
		awayteamgoing            varchar(1048576),
		fee                      varchar(1048576),
		detailfee                varchar(1048576),
		gender                   varchar(1048576),
		skilllevel               varchar(1048576),
		fieldtype                varchar(1048576),
		result                   varchar(1048576),
		resulttype               varchar(1048576),
		arriveearly              double precision,
		invited                  varchar(1048576),
		awayteamnotgoing         varchar(1048576),
		hometeamnotgoing         varchar(1048576),
		hometeammaybe            varchar(1048576),
		awayteammaybe            varchar(1048576),
		uniform                  varchar(1048576),
		esulttype                varchar(1048576)
	);
	ALTER TABLE IF EXISTS "%s" OWNER TO example;`, tableName, tableName)

	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}

// CreatePaymentTableFromDDB is creating payment table in Postgres
func CreatePaymentTableFromDDB(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	tableName := getTableNameFromDDB("payment")

	var query string

	query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		recipientuserid varchar(1048576),
		accountid       varchar(1048576),
		createdat       double precision,
		charge          varchar(1048576),
		meta            varchar(1048576),
		id              varchar(1048576),
		title           varchar(1048576),
		userid          varchar(1048576),
		status          varchar(1048576),
		refund          varchar(1048576)
	);
	ALTER TABLE IF EXISTS "%s" OWNER TO example;`, tableName, tableName)

	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}

// CreateConversionTables is table creation for conversion
func CreateConversionTables(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	var query string

	query = `CREATE TABLE "public"."asp_net_users" (
		id varchar(450) NOT NULL PRIMARY KEY,
		old_user_id varchar(450) NOT NULL,
		user_name varchar(256),
		normalized_user_name varchar(256),
		email varchar(256),
		normalized_email varchar(256),
		email_confirmed smallint,
		password_hash varchar(10485760),
		security_stamp varchar(10485760),
		concurrency_stamp varchar(10485760),
		phone_number varchar(10485760),
		phone_number_confirmed smallint,
		two_factor_enabled smallint,
		lockout_end varchar(20),
		lockout_enabled smallint,
		access_failed_count smallint
	);

	CREATE TABLE "public"."asp_net_user_logins" (
		login_provider varchar(450),
		provider_key varchar(450),
		provider_display_name varchar(10485760),
		user_id varchar(450),
		old_user_id varchar(450),
		PRIMARY KEY (login_provider, provider_key)
	);

	CREATE TABLE "public"."users" (
		old_user_id varchar(150),
		open_id varchar(36),
		photo varchar(256),
		email varchar(128),
		first_name varchar(50),
		last_name varchar(50),
		normalized_name varchar(100),
		gender smallint,
		account_id varchar(250),
		date_of_birth varchar(10),
		url varchar(100),
		login_id varchar(150),
		is_deleted smallint NOT NULL default (0),
		on_created varchar(20) NOT NULL default (to_timestamp(0) at time zone 'utc'),
		on_updated varchar(20),
		on_deleted varchar(20)
	);

	CREATE TABLE "public"."groups" (
		old_group_id varchar(150) NOT NULL,
		photo varchar(256),
		name varchar(100) NOT NULL,
		normalized_name varchar(100) NOT NULL,
		description TEXT NULL,
		sports_type smallint NOT NULL,
		age smallint,
		gender smallint,
		skill_level smallint,
		url varchar(100),
		created_by varchar(150),
		on_created varchar(20) NOT NULL default (SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20)),
		on_updated varchar(20) NULL,
		on_deleted varchar(20) NULL,
		is_deleted smallint NOT NULL DEFAULT 0,
		is_private smallint NOT NULL DEFAULT 0
	);

	CREATE TABLE "public"."events" (
		photo varchar(256),
		sports_type smallint,
		title varchar(150),
		normalized_title varchar(150),
		"start" varchar(20),
		"end" varchar(20),
		time_zone varchar(64),
		place_id varchar(250),
		place_name varchar(1100),
		location_details varchar(250),
		description varchar(10000),
		fee real,
		age smallint,
		gender smallint,
		skill_level smallint,
		field_type smallint,
		has_rsvp_deadline smallint DEFAULT (0),
		rsvp_deadline varchar(20),
		has_participant_limit smallint DEFAULT (0),
		participant_limit int,
		allow_guests smallint DEFAULT (0),
		guests int,
		is_recurring smallint DEFAULT (0),
		old_event_id varchar(250),
		old_group_id varchar(250),
		old_host_id varchar(250),
		is_deleted smallint DEFAULT (0),
		on_created varchar(20),
		on_updated varchar(20),
		on_deleted varchar(20)
	);

	CREATE TABLE "public"."physical_informations" (
		old_user_id varchar(150) NOT NULL,
		handed smallint NOT NULL,
		footed smallint NOT NULL,
		bats smallint NOT NULL,
		throws smallint NOT NULL,
		height varchar(20) NOT NULL,
		height_unit smallint NOT NULL default -1,
		weight varchar(20) NOT NULL,
		weight_unit smallint NOT NULL default -1
	);

	CREATE TABLE "public"."user_locations" (
		location_type smallint NOT NULL,
		old_user_id varchar(150) NOT NULL,
		lon float8 NOT NULL,
		lat float8 NOT NULL,
		country varchar(16) NULL,
		state varchar(100) NULL,
		county varchar(100) NULL,
		city varchar(100) NULL,
		zip_code varchar(20) NULL,
		address varchar(128) NULL
	);

	CREATE TABLE "public"."sports_interest" (
		"value" int NOT NULL DEFAULT (0),
		old_user_id varchar(150) NOT NULL,
		old_value varchar(100)
	);

	CREATE TABLE "public"."members" (
		old_group_id varchar(150) NOT NULL,
		old_user_id varchar(150) NOT NULL,
		on_created varchar(20) NOT NULL default (SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20)),
		on_updated varchar(20) NULL,
		on_deleted varchar(20) NULL,
		is_deleted smallint NOT NULL DEFAULT 0,
		order_seq int NOT NULL DEFAULT 0,
		is_hidden smallint NOT NULL DEFAULT 0
	);

	CREATE TABLE "public"."member_roles" (
		old_group_id varchar(150) NOT NULL,
		old_user_id varchar(150) NOT NULL,
		role smallint NOT NULL DEFAULT 100,
		is_deleted smallint NOT NULL DEFAULT 0,
		on_created varchar(20) NULL,
		on_deleted varchar(20) NULL
	);

	CREATE TABLE "public"."group_locations" (
		old_group_id varchar(150) NOT NULL,
		lon float8 NOT NULL,
		lat float8 NOT NULL,
		country varchar(50) NULL,
		state varchar(100) NULL,
		county varchar(100) NULL,
		city varchar(100) NULL,
		zip_code varchar(20) NULL,
		address varchar(200) NULL
	);

	CREATE TABLE "public"."rsvps" (
		old_event_id varchar(150),  -- FK: Events.Id
		old_group_id varchar(150),  -- FK: Groups.Id
		old_user_id varchar(150) NOT NULL,
		guests int,
		rsvp_state smallint DEFAULT 2,
		on_created varchar(20) NOT NULL default (SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20)),
		on_updated varchar(20) NULL,
		on_deleted varchar(20) NULL,
		is_deleted smallint DEFAULT 0
	);

	CREATE TABLE "public"."event_locations" (
		old_event_id varchar(250) NOT NULL,   -- FK: Events.Id
		lon float8 NOT NULL,
		lat float8 NOT NULL,
		country varchar(50),
		state varchar(100),
		county varchar(100),
		city varchar(100),
		zip_code varchar(20),
		address varchar(200)
	);

	CREATE TABLE "public"."event_amenities" (
		"value" smallint DEFAULT 0,
		old_event_id varchar(250)   -- FK: Events.Id
	);

	CREATE TABLE "public"."payment" (
		charge_id varchar(128) NOT NULL,   -- stripe charge id
		amount real,
		fee_amount real,
		quantity int not null default 0,
		customer_id varchar(36),   -- FK : Customer.Id
		refund_id varchar(128),
		charge_status smallint,
		refund_status smallint,
		payment_status smallint,
		card_brand varchar(16),
		card_last4_digits char(4),
		currency varchar(8) NOT NULL default 'usd',
		old_event_id varchar(250),
		old_user_id varchar(150),
		on_created varchar(20) NOT NULL default (SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20))
	);

	CREATE TABLE "public"."customer" (
		id varchar(36) PRIMARY KEY NOT NULL,   -- stripe customer id
		vendor_type int DEFAULT 0,   -- 0:Stripe
		old_user_id varchar(150),
		on_created varchar(20) NOT NULL default (SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20))
	);`

	if _, err := db.Exec(query); err != nil {
		log.Fatal(err)
	}
}
