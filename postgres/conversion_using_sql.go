package postgres

import (
	"database/sql"
)

// ConvUsersTable is conversion from ddb users-prod to users table
func ConvUsersTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."users";
	INSERT INTO "public"."users" (
		old_user_id,
		open_id,
		photo,
		email,
		first_name,
		last_name,
		normalized_name,
		gender,
		account_id,
		date_of_birth,
		url,
		login_id,
		is_deleted,
		on_created,
		on_updated,
		on_deleted
	)
	SELECT
		u.userid as old_user_id,
		uuid_generate_v4() as open_id,
		NULL as photo,   
		u.email,
		CASE
			WHEN LENGTH(u.firstname) > 50 THEN SUBSTRING(u.firstname FOR 50)
			ELSE u.firstname
		END as first_name,
		CASE
			WHEN LENGTH(u.lastname) > 50 THEN SUBSTRING(u.lastname FOR 50)
			ELSE u.lastname
		END as last_name,
		CASE
			WHEN LENGTH(u."name") > 100 THEN LOWER(SUBSTRING(u."name" FOR 100))
			ELSE LOWER(u."name")
		END as normalized_name,
		CASE Lower(u.gender)
			WHEN 'male' THEN 0
			WHEN 'female' THEN 1
			ELSE NULL
		END as gender,
		u.accountid as account_id,
		u.birthday as date_of_birth,
		u.url,
		u.loginid,
		0 as is_deleted,
		CASE
			WHEN u.createdat IS NULL THEN SUBSTRING(to_timestamp(0/1000)::VARCHAR, 0, 20)
			ELSE SUBSTRING((to_timestamp(u.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
		END as on_created,
		NULL as on_updated,
		NULL as on_deleted
	FROM
		"USERS-PROD" as u
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvGroupsTable is conversion from ddb teams-prod to groups table
func ConvGroupsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."groups";
	INSERT INTO "public"."groups" (
		old_group_id,
		photo,
		"name",
		normalized_name,
		description,
		sports_type,
		"age",
		"gender",
		skill_level,
		"url",
		is_deleted,
		is_private,
		on_created,
		on_updated,
		on_deleted
	)
	SELECT
		team."id" as old_group_id,
		case
			when team.picture is null then null
			when
				team.picture ~ '\yhttp://file.example.com'
				then replace(team.picture, 'http://file.example.com', 'https://images.example.co')
			when    -- 아래 조건에 부합하면
				team.picture !~ '\yhttp'   -- http 로 시작하지 않고
				And team.picture ~ 'family/|user/|group/|team/|cloudfront|google' -- 문자열을 포함하는 경우
				then concat('https://images.example.co/', team.picture)    -- 값 앞에 이미지 url prefix 붙여 출력
			else                        -- 위의 경우 외
				team.picture            -- 그대로 출력
		end as photo,
		CASE
			WHEN LENGTH(team."name") > 100 THEN SUBSTRING(team."name" FOR 100)
			ELSE team."name"
		END as "name",
		CASE
			WHEN LENGTH(team."name") > 100 THEN LOWER(SUBSTRING(team."name" FOR 100))
			ELSE LOWER(team."name")
		END as normalized_name,
		team.desc as description,
		CASE
			WHEN LOWER(team.sports) = 'soccer' Or team.sports = '축구' THEN 0
			WHEN LOWER(team.sports) = 'basketball' Or team.sports = '농구' THEN 1
			WHEN LOWER(team.sports) = 'football' THEN 2
			WHEN LOWER(team.sports) = 'baseball' Or team.sports = '야구' THEN 3
			WHEN LOWER(team.sports) = 'tennis' Or team.sports = '테니스' THEN 4
			WHEN LOWER(team.sports) = 'softball' THEN 5
			ELSE 0  -- 0:soccer
		END as sports_type,
		CASE
			WHEN team.agegroup = 'adult' THEN 0
			WHEN team.agegroup = 'college' THEN 1
			ELSE NULL
		END as age,
		NULL as gender,
		NULL as skill_level,
		team.url,
		CASE Lower(team.deleted)
			WHEN 'true' THEN 1
			ELSE 0
		END as is_deleted,
		CASE Lower(team.isprivateaccount)
			WHEN 'true' THEN 1
			ELSE 0
		END as is_private,
		CASE
			WHEN team.createdat is NULL THEN SUBSTRING(to_timestamp(0/1000)::VARCHAR, 0, 20)
			ELSE SUBSTRING((to_timestamp(team.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
		END as on_created,
		NULL as on_updated,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team
	WHERE
		team."type" = 'group'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
		And team.sports = 'soccer'
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvEventsTable is conversion from ddb games-prod to events table
func ConvEventsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."events";
	INSERT INTO "public"."events" (
		photo,
		sports_type,
		title,
		normalized_title,
		start,
		"end",
		time_zone,
		place_id,
		place_name,
		location_details,
		description,
		fee,
		age,
		gender,
		skill_level,
		field_type,
		has_rsvp_deadline,
		rsvp_deadline,
		has_participant_limit,
		participant_limit,
		allow_guests,
		guests,
		is_recurring,
		old_event_id,
		old_group_id,
		old_host_id,  -- 최종적으로 mssql.dbo.HostId 와 mssql.dbo.OwnerId 에 사용
		is_deleted,
		on_created,
		on_updated,
		on_deleted
	)
	SELECT
		NULL as photo,
		0 as sports_type,
		e.title,
		LOWER(e.title) as normalized_title,
		SUBSTRING((to_timestamp(e.startat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as "start",
		SUBSTRING((to_timestamp(e.endat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as "end",
		e.timezone as time_zone,
		loc.place_id as place_id,
		CASE
			WHEN e."location" IS NULL THEN NULL
			WHEN loc."title" IS NOT NULL THEN loc."title"
			WHEN loc."name" IS NOT NULL THEN loc."name"
			ELSE e.locationdetails
		END as place_name,
		e.locationdetails as location_details,
		e.description,
		CASE
			WHEN e.fee~E'^\\d+$' THEN e.fee::REAL
			WHEN e.fee~E'^\\d+\.?\\d+$' THEN e.fee::REAL
			ELSE 0
		END as fee,
		NULL as age,
		CASE Lower(e.gender)
			WHEN 'male' THEN 0
			WHEN 'female' THEN 1
			WHEN 'co' THEN 2
			WHEN 'boysngirls' THEN 3
			WHEN 'boys' THEN 4
			WHEN 'girls' THEN 5
			ELSE NULL
		END as gender,
		0 as skill_level,
		0 as field_type,
		CASE e.allowrsvpdeadline
			WHEN 'true' THEN 1
			ELSE 0
		END as has_rsvp_deadline,
		CASE
			WHEN e.rsvpdeadline = '0' THEN SUBSTRING((to_timestamp(e.startat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
			WHEN e.rsvpdeadline is NULL THEN SUBSTRING((to_timestamp(e.startat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
			ELSE SUBSTRING((to_timestamp((e.startat - e.rsvpdeadline)/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
		END as rsvp_deadline,
		CASE e.allowparticipantlimit
			WHEN 'true' THEN 1
			ELSE 0
		END as has_participant_limit,
		CASE
			WHEN e.allowparticipantlimit = 'true' And e.participantlimit Is NULL THEN 50
			WHEN e.allowparticipantlimit != 'true' And e.participantlimit Is NULL THEN 0
			ELSE e.participantlimit
		END as participant_limit,
		CASE e.allowguest
			WHEN 'true' THEN 1
			ELSE 0
		END as allow_guests,
		CASE
			WHEN json_array_length(guests::json) IS NULL THEN 0
			ELSE json_array_length(guests::json)
		END as "guests",
		0 as is_recurring,
		e."id" as old_event_id,
		e.ownerid as old_group_id,
		CASE  -- meetup-organizer-* not in users-prod Table. So, RDB 이관시 외부키 제약 이슈 발생 대비 그룹 생성자로 대처
			WHEN e.createdby ~* '^meetup-organizer-*' THEN g.createdby
			ELSE e.createdby
		END as old_host_id,
		CASE e.deleted
			WHEN 'true' THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING((to_timestamp(e.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		CASE
			WHEN e.updatedat IS NULL THEN NULL
			ELSE SUBSTRING((to_timestamp(e.updatedat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
		END as on_updated,
		NULL as on_deleted
	FROM
		"GAMES-PROD" as e
		JOIN "TEAMS-PROD" as g ON e.ownerid = g."id" And g."type" = 'group' And g.sports = 'soccer',
		json_to_record(e."location"::json) as loc("place_id" varchar, "title" varchar, "name" varchar)
	WHERE
		(e.crawled != 'true' Or e.crawled IS NULL)  -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvPhysicalInformationsTable is conversion from ddb users-prod to physical_informations table
func ConvPhysicalInformationsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."physical_informations";
	INSERT INTO "public"."physical_informations" (
		old_user_id,
		handed,
		footed,
		bats,
		throws,
		height,
		height_unit,
		weight,
		weight_unit
	)
	SELECT
		u.userid as old_user_id,
		CASE Lower(physical.handed)
			WHEN 'both' THEN 2
			WHEN 'right' THEN 1
			ELSE 0
		END as handed,
		CASE Lower(physical.footed)
			WHEN 'both' THEN 2
			WHEN 'right' THEN 1
			ELSE 0
		END as footed,
		CASE Lower(physical.batting)
			WHEN 'both' THEN 2
			WHEN 'right' THEN 1
			ELSE 0
		END as bats,
		CASE Lower(physical.throwing)
			WHEN 'both' THEN 2
			WHEN 'right' THEN 1
			ELSE 0
		END as throws,
		CASE
			WHEN physical.height IS NULL THEN 0
			ELSE 0
		END as height,
		CASE
			WHEN physical.heightUnit IS NULL THEN 0
			ELSE 0  -- 0:Inch, 1:Centimeter
		END as height_unit,
		CASE
			WHEN physical.weight IS NULL THEN 0
			ELSE 0
		END as weight,
		CASE
			WHEN physical.weightUnit IS NULL THEN 0
			ELSE 0  -- 0:Pound, 1:Kilogram
		END as weight_unit
	FROM
		"USERS-PROD" as u,
		json_to_record(u.physical::json) as physical(handed VARCHAR, footed VARCHAR, throwing VARCHAR, height VARCHAR, weight VARCHAR, heightUnit VARCHAR, weightUnit VARCHAR, batting VARCHAR)
	WHERE
		physical::json->'handed' is not null
		Or physical::json->'footed' is not null
		Or physical::json->'throwing' is not null
		Or physical::json->'height' is not null
		Or physical::json->'weight' is not null
		Or physical::json->'heightUnit' is not null
		Or physical::json->'weightUnit' is not null
		Or physical::json->'batting' is not null
	ORDER BY createdat
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvSportsInterestTable is conversion from ddb users-prod to sports_interest table
func ConvSportsInterestTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."sports_interest";
	INSERT INTO "public"."sports_interest" (
		value,
		old_user_id,
		old_value
	)
	SELECT
		CASE Lower(favor_sports)
			WHEN 'soccer' THEN 0
			WHEN 'basketball' THEN 1
			WHEN 'football' THEN 2
			WHEN 'baseball' THEN 3
			WHEN 'tennis' THEN 4
			WHEN 'ultimatefrisbee' THEN 5
			WHEN 'rugby' THEN 6
			WHEN 'lacrosse' THEN 7
			WHEN 'running' THEN 8
			WHEN 'golf' THEN 9
			WHEN 'yoga' THEN 10
			WHEN 'pilates' THEN 11
			WHEN 'icehockey' THEN 12
			WHEN 'fieldhockey' THEN 13
			WHEN 'skateboarding' THEN 14
			WHEN 'volleyball' THEN 15
			WHEN 'cricket' THEN 16
			WHEN 'pingpong' THEN 17
			WHEN 'skiing' THEN 18
			WHEN 'squash' THEN 19
			WHEN 'bowling' THEN 20
			WHEN 'cycling' THEN 21
			WHEN 'crossfit' THEN 22
			WHEN 'hiking' THEN 23
			WHEN 'badminton' THEN 24
			WHEN 'boxing' THEN 25
			WHEN 'ultimatesports' THEN 26
			WHEN 'foosball' THEN 27
			WHEN 'dodgeball' THEN 28
			WHEN 'paintball' THEN 29
			ELSE 0    -- 0:Soccer
		END as "value",
		u.userid as old_user_id,
		favor_sports as old_value
	FROM
		"USERS-PROD" as u,
		json_array_elements_text(u.favorsports::json) as favor_sports
	WHERE
		u.favorsports is not null
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvUserLocationsTable is conversion from ddb users-prod to user_locations table
func ConvUserLocationsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."user_locations";
	INSERT INTO "public"."user_locations" (
		location_type,
		old_user_id,
		lon,
		lat,
		country,
		state,
		city,
		county,
		zip_code,
		address
	)
	-- LivesIn
	SELECT
		0 as location_type, -- 0:LivesIn
		u.userId as old_user_id,
		CASE
			WHEN livesin_geometry.lng is null THEN livesin_geometry.lon
			ELSE livesin_geometry.lng
		END as lng,  -- pg_typeof() = float8
		livesin_geometry.lat,   -- pg_typeof() = float8
		(SELECT regexp_replace(livesin_addrs.value->>'short_name', '(^"|"$)', '') as country FROM json_array_elements(livesin.address_components) as livesin_addrs WHERE livesin_addrs->'types'->>0 = 'country'), -- country
		(
			SELECT regexp_replace(livesin_addrs.value->>'short_name', '(^"|"$)', '') as state
			FROM json_array_elements(livesin.address_components) as livesin_addrs
			WHERE
				livesin_addrs->'types'->>0 = 'administrative_area_level_1'
				And livesin_addrs->'types'->>1 = 'political'
			LIMIT 1
		), -- State
		(SELECT regexp_replace(livesin_addrs.value->>'short_name', '(^"|"$)', '') as city FROM json_array_elements(livesin.address_components) as livesin_addrs WHERE livesin_addrs->'types'->>0 = 'locality'), -- city
		(
			SELECT regexp_replace(livesin_addrs.value->>'short_name', '(^"|"$)', '') as county
			FROM json_array_elements(livesin.address_components) as livesin_addrs
			WHERE livesin_addrs->'types'->>0 = 'administrative_area_level_2'
			LIMIT 1
		), -- county
		(SELECT regexp_replace(livesin_addrs.value->>'short_name', '(^"|"$)', '') as zipcode FROM json_array_elements(livesin.address_components) as livesin_addrs WHERE livesin_addrs->'types'->>0 = 'postal_code'), -- ZipCode
		livesin.formatted_address as address
	FROM
		"USERS-PROD" as u,
		json_to_record(u.livesin::json) as livesin(address_components json, formatted_address varchar),
		json_to_record(u.livesin::json->'geometry'->'location') as livesin_geometry(lat FLOAT, lng FLOAT, lon FLOAT)
	WHERE
		livesin_geometry.lat is not null
		And (livesin_geometry.lng is not null Or livesin_geometry.lon is not null)

	UNION ALL

	-- BirthPlace
	SELECT
		1 as location_type, -- 1:BirthPlace
		u.userId as old_user_id,
		CASE
			WHEN birthplace_geometry.lng is null THEN birthplace_geometry.lon
			ELSE birthplace_geometry.lng
		END as lng,  -- pg_typeof() = float8
		birthplace_geometry.lat,   -- pg_typeof() = float8
		(SELECT regexp_replace(birthplace_addrs.value->>'short_name', '(^"|"$)', '') as country FROM json_array_elements(birthplace.address_components) as birthplace_addrs WHERE birthplace_addrs->'types'->>0 = 'country'), -- country
		(
			SELECT regexp_replace(birthplace_addrs.value->>'short_name', '(^"|"$)', '') as state
			FROM json_array_elements(birthplace.address_components) as birthplace_addrs
			WHERE
				birthplace_addrs->'types'->>0 = 'administrative_area_level_1'
				And birthplace_addrs->'types'->>1 = 'political'
			LIMIT 1
		), -- State
		(SELECT regexp_replace(birthplace_addrs.value->>'short_name', '(^"|"$)', '') as city FROM json_array_elements(birthplace.address_components) as birthplace_addrs WHERE birthplace_addrs->'types'->>0 = 'locality'), -- city
		(
			SELECT regexp_replace(birthplace_addrs.value->>'short_name', '(^"|"$)', '') as county
			FROM json_array_elements(birthplace.address_components) as birthplace_addrs
			WHERE birthplace_addrs->'types'->>0 = 'administrative_area_level_2'
			LIMIT 1
		), -- county
		(SELECT regexp_replace(birthplace_addrs.value->>'short_name', '(^"|"$)', '') as zipcode FROM json_array_elements(birthplace.address_components) as birthplace_addrs WHERE birthplace_addrs->'types'->>0 = 'postal_code'), -- ZipCode
		birthplace.formatted_address as address
	From
		"USERS-PROD" as u,
		json_to_record(u.birthplace::json) as birthplace(address_components json, formatted_address varchar),
		json_to_record(u.birthplace::json->'geometry'->'location') as birthplace_geometry(lat FLOAT, lng FLOAT, lon FLOAT)
	WHERE
		u.birthplace is not null
		And u.birthplace != '{"utc_offset":0}'
		And (birthplace_geometry.lng is not null Or birthplace_geometry.lon is not null)
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvMembersTable is conversion from ddb teams-prod to members table
func ConvMembersTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."members";
	INSERT INTO "public"."members" (
		old_group_id,
		old_user_id,
		on_created,
		on_updated,
		on_deleted,
		is_deleted,
		order_seq,
		is_hidden
	)
	SELECT
		team."id" as old_group_id,
		roster."userId" as old_user_id,
		CASE
			WHEN roster."createdAt" IS NULL THEN SUBSTRING(to_timestamp(0/1000)::VARCHAR, 0, 20)
			ELSE SUBSTRING((to_timestamp(roster."createdAt"/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20)
		END as on_created,
		NULL as on_updated,
		NULL as on_deleted,
		CASE
			WHEN roster."deleted" = true THEN 1
			ELSE 0
		END as is_deleted,
		0 as order_seq,
		0 as is_hidden
	FROM
		"TEAMS-PROD" as team,
		json_to_recordset(team.rosters::json) as roster(
			"userId" varchar
			,"isAdmin" boolean
			,"deleted" boolean
			,"isVirtual" BOOLEAN
			,"createdAt" BIGINT
		)
	WHERE
		roster."userId" IS NOT NULL
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
	;

	--
	-- pendings...
	--
	INSERT INTO "public"."members" (
		old_group_id,
		old_user_id,
		on_created,
		on_updated,
		on_deleted,
		is_deleted,
		order_seq,
		is_hidden
	)
	SELECT
		team."id" as old_group_id,
		pending_user_id as old_user_id,
		SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20) as on_created,
		NULL as on_updated,
		NULL as on_deleted,
		0 as is_deleted,
		0 as order_seq,
		0 as is_hidden
	FROM
		"TEAMS-PROD" as team
		,json_array_elements_text(team.pendingRostersFromRequest::json) as pending_user_id
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
	
	UNION ALL
	
	SELECT
		team."id" as old_group_id,
		pending_user_id as old_user_id,
		SUBSTRING(to_timestamp(0)::VARCHAR, 0, 20) as on_created,
		NULL as on_updated,
		NULL as on_deleted,
		0 as is_deleted,
		0 as order_seq,
		0 as is_hidden
	FROM
		"TEAMS-PROD" as team
		,json_array_elements_text(team.pendingRostersFromTeamRequest::json) as pending_user_id
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled is NULL)  -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvMemberRolesTable is conversion from ddb teams-prod to member_roles table
func ConvMemberRolesTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."member_roles";
	INSERT INTO "public"."member_roles" (
		old_group_id,
		old_user_id,
		role,
		is_deleted,
		on_created,
		on_deleted
	)
	-- Owner
	SELECT
		team."id" as old_group_id,
		team.createdby as old_user_id,
		0 as role, -- Owner
		CASE
			WHEN "user".userid IS NULL THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING((to_timestamp(team."createdat"/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team
		LEFT JOIN "USERS-PROD" as "user" on team.createdby = "user".userid
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외

	UNION ALL
	-- Admin
	SELECT
		team."id" as old_group_id,
		admin as old_user_id,
		1 as role, -- Admin
		CASE
			WHEN "user".userid IS NULL THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING((to_timestamp(team."createdat"/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team,
		json_array_elements_text(team.admins::json) as admin
		LEFT JOIN "USERS-PROD" as "user" on "user".userid = admin
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외

	UNION ALL
	-- Member
	SELECT
		team."id" as old_group_id,
		roster."userId" as old_user_id,
		100 as role,    -- Member
		CASE
			WHEN "user".userid IS NULL THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING((to_timestamp(roster."createdAt"/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team,
		json_to_recordset(team.rosters::json) as roster(
			"userId" varchar
			,"isAdmin" boolean
			,"deleted" boolean
			,"isVirtual" BOOLEAN
			,"createdAt" BIGINT
		)
		LEFT JOIN "USERS-PROD" as "user" on "user".userid = roster."userId"
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
		AND roster."userId" is not null

	UNION ALL
	-- Candidate in pendingRostersFromRequest
	SELECT
		team."id" as old_group_id,
		pending_user_id as old_user_id,
		200 as role,    -- Candidate
		CASE
			WHEN "user".userid IS NULL THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING(to_timestamp(0/1000)::VARCHAR, 0, 20) as on_created,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team
		,json_array_elements_text(team.pendingRostersFromRequest::json) as pending_user_id
		LEFT JOIN "USERS-PROD" as "user" on "user".userid = pending_user_id
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외

	UNION ALL
	-- Candidate in pendingRostersFromTeamRequest
	SELECT
		team."id" as old_group_id,
		pending_user_id as old_user_id,
		200 as role,    -- Candidate
		CASE
			WHEN "user".userid IS NULL THEN 1
			ELSE 0
		END as is_deleted,
		SUBSTRING(to_timestamp(0/1000)::VARCHAR, 0, 20) as on_created,
		NULL as on_deleted
	FROM
		"TEAMS-PROD" as team
		,json_array_elements_text(team.pendingRostersFromTeamRequest::json) as pending_user_id
		LEFT JOIN "USERS-PROD" as "user" on "user".userid = pending_user_id
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvGroupLocationsTable is conversion from ddb teams-prod to group_locations table
func ConvGroupLocationsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."group_locations";
	INSERT INTO "public"."group_locations" (
		old_group_id,
		lon,
		lat,
		country,
		state,
		county,
		city,
		zip_code,
		address
	)
	SELECT
		team."id" as old_group_id,
		CASE
			WHEN loc_geometry.lng is null THEN loc_geometry.lon
			ELSE loc_geometry.lng
		END as lng,  -- pg_typeof() = float8
		loc_geometry.lat,   -- pg_typeof() = float8
		(SELECT regexp_replace(loc_addrs.value->>'short_name', '(^"|"$)', '') as country FROM json_array_elements(loc.address_components) as loc_addrs WHERE loc_addrs->'types'->>0 = 'country'), -- country
		(SELECT regexp_replace(loc_addrs.value->>'short_name', '(^"|"$)', '') as state FROM json_array_elements(loc.address_components) as loc_addrs WHERE loc_addrs->'types'->>0 = 'administrative_area_level_1'), -- State
		(SELECT regexp_replace(loc_addrs.value->>'short_name', '(^"|"$)', '') as county FROM json_array_elements(loc.address_components) as loc_addrs WHERE loc_addrs->'types'->>0 = 'administrative_area_level_2'), -- county
		(SELECT regexp_replace(loc_addrs.value->>'short_name', '(^"|"$)', '') as city FROM json_array_elements(loc.address_components) as loc_addrs WHERE loc_addrs->'types'->>0 = 'locality'), -- city
		(SELECT regexp_replace(loc_addrs.value->>'short_name', '(^"|"$)', '') as zipcode FROM json_array_elements(loc.address_components) as loc_addrs WHERE loc_addrs->'types'->>0 = 'postal_code'), -- ZipCode
		loc.formatted_address as address
	FROM
		"TEAMS-PROD" as team,
		json_to_record(team."location"::json) as loc("geometry" json, address_components json, formatted_address varchar),
		json_to_record(team."location"::json->'geometry'->'location') as loc_geometry(lat FLOAT, lng FLOAT, lon FLOAT)
	WHERE
		team."id" is not null
		And team."type" = 'group'
		And team.sports = 'soccer'
		And (team.crawled != 'true' Or team.crawled IS NULL)  -- 크롤링 제외
		And team."location" IS NOT NULL
		And loc_geometry.lat is not null
		And (loc_geometry.lng is not null Or loc_geometry.lon  is not null)
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvRsvpsTable is conversion from ddb games-prod to rsvps table
func ConvRsvpsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."rsvps";
	INSERT INTO rsvps (
		old_event_id,
		old_group_id,
		old_user_id,
		guests,
		rsvp_state,
		on_created,
		on_updated,
		on_deleted,
		is_deleted
	)
	-- Going
	SELECT
		game."id" as old_event_id,
		game.ownerid as old_group_id,
		going_user_id as old_user_id,
		guest."guestCount" as guests,
		0 as rsvp_state,    -- 0:Going
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_updated,
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_deleted,
		0 as is_deleted
	FROM
		"GAMES-PROD" as game
		JOIN "TEAMS-PROD" as team ON game.ownerid = team."id" And team."type" = 'group' And team.sports = 'soccer'
		LEFT JOIN json_array_elements_text(game.going::json) as going_user_id ON TRUE
		LEFT JOIN json_to_recordset(game.guests::json) as guest(
			"userId" varchar
			,"guestCount" INTEGER
		) ON guest."userId" = going_user_id
	WHERE
		going_user_id is NOT NULL
		And (game.crawled != 'true' Or game.crawled is NULL)    -- 크롤링 제외

	UNION

	-- NotGoing
	SELECT
		game."id" as old_event_id,
		game.ownerid as old_group_id,
		notgoing_user_id as old_user_id,
		0 as guests,
		1 as rsvp_state,    -- 1:NotGoing
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created,
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_updated,
		SUBSTRING((to_timestamp(0) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_deleted,
		0 as is_deleted
	FROM
		"GAMES-PROD" as game
		JOIN "TEAMS-PROD" as team ON game.ownerid = team."id" And team."type" = 'group' And team.sports = 'soccer'
		LEFT JOIN json_array_elements_text(game.notgoing::json) as notgoing_user_id ON TRUE
	WHERE
		notgoing_user_id is NOT NULL
		And (game.crawled != 'true' Or game.crawled is NULL)    -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvEventLocationsTable is conversion from ddb games-prod to event_locations table
func ConvEventLocationsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."event_locations";
	INSERT INTO "public"."event_locations" (
		old_event_id,   -- FK: Events.Id
		lon,
		lat,
		country,
		state,
		county,
		city,
		zip_code,
		address
	)
	SELECT
		game."id" as old_event_id,
		CASE
			WHEN location_geometry.lng is null THEN location_geometry.lon
			ELSE location_geometry.lng
		END as lng,  -- pg_typeof() = float8
		location_geometry.lat,  -- pg_typeof() = float8
		(SELECT regexp_replace(location_addrs.value->>'short_name', '(^"|"$)', '') as country FROM json_array_elements(loc.address_components) as location_addrs WHERE location_addrs->'types'->>0 = 'country'), -- country
		(SELECT regexp_replace(location_addrs.value->>'short_name', '(^"|"$)', '') as state FROM json_array_elements(loc.address_components) as location_addrs WHERE location_addrs->'types'->>0 = 'administrative_area_level_1'), -- State
		(SELECT regexp_replace(location_addrs.value->>'short_name', '(^"|"$)', '') as county FROM json_array_elements(loc.address_components) as location_addrs WHERE location_addrs->'types'->>0 = 'administrative_area_level_2'), -- county
		(SELECT regexp_replace(location_addrs.value->>'short_name', '(^"|"$)', '') as city FROM json_array_elements(loc.address_components) as location_addrs WHERE location_addrs->'types'->>0 = 'locality'), -- city
		(SELECT regexp_replace(location_addrs.value->>'short_name', '(^"|"$)', '') as zipcode FROM json_array_elements(loc.address_components) as location_addrs WHERE location_addrs->'types'->>0 = 'postal_code'), -- ZipCode
		loc.formatted_address as address
	FROM
		"GAMES-PROD" as game
		JOIN "TEAMS-PROD" as team ON game.ownerid = team."id" And team."type" = 'group' And team.sports = 'soccer',
		json_to_record(game."location"::json) as loc("address_components" json, "formatted_address" varchar),
		json_to_record(game."location"::json->'geometry'->'location') as location_geometry(lat FLOAT, lng FLOAT, lon FLOAT)
	WHERE
		location_geometry.lat is not null
		And (location_geometry.lng is not null Or location_geometry.lon  is not null)
		And (game.crawled != 'true' Or game.crawled is NULL)    -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvEventAmenitiesTable is conversion from ddb games-prod to event_amenities table
func ConvEventAmenitiesTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."event_amenities";
	INSERT INTO event_amenities (
		value,
		old_event_id
	)
	SELECT
		CASE Lower(game.fieldtype)
			WHEN 'grass' THEN 0
			WHEN 'turf' THEN 1
			WHEN 'synthetic' THEN 2
			WHEN 'sand' THEN 3
			WHEN 'wood' THEN 4
			WHEN 'stree' THEN 5
			ELSE 0
		END As "value",
		game."id" as old_event_id
	FROM
		"GAMES-PROD" as game
		JOIN "TEAMS-PROD" as team ON game.ownerid = team."id" And team."type" = 'group' And team.sports = 'soccer'
	WHERE
		(game.crawled != 'true' Or game.crawled is NULL)    -- 크롤링 제외
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvPaymentTable is conversion from ddb payment-prod to payment table
func ConvPaymentTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."payment";
	INSERT INTO "public"."payment" (
		charge_id,
		amount,
		fee_amount,
		quantity,
		customer_id,
		refund_id,
		charge_status,
		refund_status,
		payment_status,
		card_brand,
		card_last4_digits,
		currency,
		old_event_id,
		old_user_id,
		on_created
	)
	-- charge
	SELECT
		pay."id" as "charge_id",
		meta.amount as amount,
		meta.fee as fee_amount,
		meta.seat as quantity,
		charge.customer as customer_id,
		NULL as refund_id,
		CASE charge.paid
			WHEN false THEN 2
			ELSE 1
		END as charge_status,
		NULL as refund_status,
		CASE
			WHEN charge.paid = false THEN 0
			ELSE 1
		END as payment_status,
		card.brand,
		card.last4,
		charge.currency,
		meta."gameId" as old_event_id,
		pay.userid as old_user_id,
		SUBSTRING((to_timestamp(pay.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created
	FROM
		"PAYMENTS-PROD" as pay,
		json_to_record(pay.charge::json) as charge(
			id varchar,
			object varchar,
			amount int,
			amount_refunded int,
			application_fee VARCHAR,
			application_fee_amount int,
			created BIGINT,
			currency VARCHAR,
			customer VARCHAR,
			description VARCHAR,
			destination VARCHAR,
			source json,
			paid bool,
			refunded bool
		),
		json_to_record(pay.meta::json) as meta("gameId" varchar, amount int, fee int, seat int, card json),
		json_to_record(meta.card::json) as card(brand varchar, last4 varchar)
	WHERE
		pay."id" IS NOT NULL
		And pay.status = 'charge'

	UNION

	-- request_refund
	SELECT
		pay."id" as "charge_id",
		meta.amount as amount,
		meta.fee as fee_amount,
		meta.seat as quantity,
		charge.customer as customer_id,
		NULL as refund_id,
		CASE charge.paid
			WHEN false THEN 2
			ELSE 1
		END as charge_status,
		0 as refund_status,
		2 as payment_status,
		card.brand,
		card.last4,
		charge.currency,
		meta."gameId" as old_event_id,
		pay.userid as old_user_id,
		SUBSTRING((to_timestamp(pay.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created
	FROM
		"PAYMENTS-PROD" as pay,
		json_to_record(pay.charge::json) as charge(
			id varchar,
			object varchar,
			amount int,
			amount_refunded int,
			application_fee VARCHAR,
			application_fee_amount int,
			created BIGINT,
			currency VARCHAR,
			customer VARCHAR,
			description VARCHAR,
			destination VARCHAR,
			source json,
			paid bool,
			refunded bool
		),
		json_to_record(pay.meta::json) as meta("gameId" varchar, amount int, fee int, seat int, card json),
		json_to_record(meta.card::json) as card(brand varchar, last4 varchar),
		json_to_record(pay.refund::json) as refund("id" VARCHAR, amount int, charge varchar, created bigint)
	WHERE
		pay."id" IS NOT NULL
		And pay.status = 'request_refund'

	UNION

	-- refund
	SELECT
		pay."id" as "charge_id",
		meta.amount as amount,
		meta.fee as fee_amount,
		meta.seat as quantity,
		charge.customer as customer_id,
		refund.id as refund_id,
		CASE charge.paid
			WHEN false THEN 2
			ELSE 1
		END as charge_status,
		1 as refund_status,
		3 as payment_status,
		card.brand,
		card.last4,
		charge.currency,
		meta."gameId" as old_event_id,
		pay.userid as old_user_id,
		SUBSTRING((to_timestamp(pay.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created
	FROM
		"PAYMENTS-PROD" as pay,
		json_to_record(pay.charge::json) as charge(
			id varchar,
			object varchar,
			amount int,
			amount_refunded int,
			application_fee VARCHAR,
			application_fee_amount int,
			created BIGINT,
			currency VARCHAR,
			customer VARCHAR,
			description VARCHAR,
			destination VARCHAR,
			source json,
			paid bool,
			refunded bool
		),
		json_to_record(pay.meta::json) as meta("gameId" varchar, amount int, fee int, seat int, card json),
		json_to_record(meta.card::json) as card(brand varchar, last4 varchar),
		json_to_record(pay.refund::json) as refund("id" VARCHAR, amount int, charge varchar, created bigint)
	WHERE
		pay."id" IS NOT NULL
		And pay.status = 'refund'
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvCustomerTable is conversion from ddb payment-prod to customer table
func ConvCustomerTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."customer";
	INSERT INTO "public"."customer" (
		id,
		vendor_type,
		old_user_id,
		on_created
	)
	SELECT
		charge.customer as "id",
		0 as vendor_type,
		pay.userid as old_user_id,
		SUBSTRING((to_timestamp(pay.createdat/1000) AT TIME ZONE 'UTC')::VARCHAR, 0, 20) as on_created
	FROM
		(-- charge.customer 중복 제거 후 날짜 순 첫 1개 마다의 레코드들만 조회
			SELECT DISTINCT ON (charge.customer) *
			FROM
				"PAYMENTS-PROD" as pay,
				json_to_record(pay.charge::json) as charge(
					id varchar,
					object varchar,
					amount int,
					amount_refunded int,
					application_fee VARCHAR,
					application_fee_amount int,
					created BIGINT,
					currency VARCHAR,
					customer VARCHAR,
					description VARCHAR,
					destination VARCHAR,
					source json
				)
			WHERE
				charge.customer In (
					SELECT
						charge.customer
					FROM
						"PAYMENTS-PROD" as pay,
						json_to_record(pay.charge::json) as charge(
							id varchar,
							object varchar,
							amount int,
							amount_refunded int,
							application_fee VARCHAR,
							application_fee_amount int,
							created BIGINT,
							currency VARCHAR,
							customer VARCHAR,
							description VARCHAR,
							destination VARCHAR,
							source json
						)
					GROUP BY charge.customer
					HAVING COUNT(*) > 1

					UNION

					SELECT
						charge.customer
					FROM
						"PAYMENTS-PROD" as pay,
						json_to_record(pay.charge::json) as charge(
							id varchar,
							object varchar,
							amount int,
							amount_refunded int,
							application_fee VARCHAR,
							application_fee_amount int,
							created BIGINT,
							currency VARCHAR,
							customer VARCHAR,
							description VARCHAR,
							destination VARCHAR,
							source json
						)
					GROUP BY charge.customer
					HAVING COUNT(*) = 1
				)
			ORDER BY charge.customer, charge.created
		) as pay,
		json_to_record(pay.charge::json) as charge(
			id varchar,
			object varchar,
			amount int,
			amount_refunded int,
			application_fee VARCHAR,
			application_fee_amount int,
			created BIGINT,
			currency VARCHAR,
			customer VARCHAR,
			description VARCHAR,
			destination VARCHAR,
			source json
		)
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvAspNetUsersTable is conversion from ddb users-prod to asp_net_users table
func ConvAspNetUsersTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."asp_net_users";
	INSERT INTO "public"."asp_net_users" (
		id,
		old_user_id,
		user_name,
		normalized_user_name,
		email,
		normalized_email,
		email_confirmed,
		password_hash,
		security_stamp,
		concurrency_stamp,
		phone_number,
		phone_number_confirmed,
		two_factor_enabled,
		lockout_end,
		lockout_enabled,
		access_failed_count
	)
	SELECT
		u_to.open_id as id,
		u_to.old_user_id,
		u_from.email as user_name,
		Upper(u_from.email) as normalized_user_name,
		u_from.email as email,
		UPPER(u_from.email) as normalized_email,
		CASE u_from.emailVerified
			WHEN 'true' THEN 1
			ELSE 0
		END as email_confirmed,
		CASE
			WHEN u_from."password" is NULL THEN NULL
			ELSE concat(u_from."password", '$', coalesce(u_from.salt, ''))
		END as password_hash,
		UPPER(MD5(u_from.userid)) as security_stamp,
		uuid_generate_v4()::text as concurrency_stamp,
		NULL as phone_number,
		0 as phone_number_confirmed,
		0 as two_factor_enabled,
		NULL as lockout_end,
		1 as lockout_enabled,
		0 as access_failed_count
	FROM
		users as u_to
		Left Join "USERS-PROD" as u_from ON u_from.userid = u_to.old_user_id
	WHERE
		u_from.userid is not null
	ORDER BY createdat
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

// ConvAspNetUserLoginsTable is conversion from ddb users-prod to asp_net_user_logins table
func ConvAspNetUserLoginsTable(db *sql.DB) {
	var query string

	query = `TRUNCATE TABLE "public"."asp_net_user_logins";
	-- google
	INSERT INTO asp_net_user_logins (
		login_provider,
		provider_key,
		provider_display_name,
		user_id,
		old_user_id
	)
	SELECT
		'Google' as login_provider,
		u.googleid as provider_key,
		'Google' as provider_display_name,
		u_to.open_id,
		u.userid as old_user_id
	FROM
		"USERS-PROD" as u
		Left Join users as u_to On u.userid = u_to.old_user_id
	WHERE
		u.googleid is not null
		And u.googleid  != 'NONE'
	ORDER BY createdat;
	
	-- facebook
	INSERT INTO asp_net_user_logins (
		login_provider,
		provider_key,
		provider_display_name,
		user_id,
		old_user_id
	)
	SELECT
		'Facebook' as login_provider,
		u.facebookid as provider_key,
		'Facebook' as provider_display_name,
		u_to.open_id,
		u_to.old_user_id
	FROM
		(-- 중복 facebookId row들 중에서 createdAt 이 최근인 row들만 조회
		SELECT DISTINCT ON (facebookid) *
			FROM public."USERS-PROD" As u
			WHERE facebookid In (
				SELECT facebookid
				FROM public."USERS-PROD" group by public."USERS-PROD".facebookid having count(facebookid) > 0
			)
			And u.facebookid Is Not Null
				And u.facebookid != 'NONE'
			ORDER BY facebookid, createdat DESC
		) as u
		Left Join users as u_to On u.userid = u_to.old_user_id
	WHERE
		u.facebookid is not null
		And u.facebookid != 'NONE'
	ORDER BY u.createdat
	;`

	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}
