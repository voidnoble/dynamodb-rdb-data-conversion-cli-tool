package mssql

// Query is sql query
type Query string

// ApiQuery for insert api table data
// https://wiki.example.net/pages/viewpage.action?pageId=52494392#ResearchconversionDynamoDBtoMSSQL-MSSQLaws.xxx테이블들→dbo.xxx테이블들
const ApiQuery = Query(`
--
-- Users
--
INSERT INTO [dbo].[Users] (
    OpenId,
    Photo,
    Email,
    FirstName,
    LastName,
    NormalizedName,
    Gender,
    AccountId,
    DateOfBirth,
    IsDeleted,
    OnCreated,
    OnUpdated,
    _Id,
    _LoginId,
    _Url
)
SELECT
    open_id,
    photo,
    email,
    first_name,
    last_name,
    normalized_name,
    gender,
    account_id,
    IIF(date_of_birth IS NULL, '0001-01-01T00:00:00.0000000', CAST(date_of_birth AS datetime2)) as date_of_birth,
    is_deleted,
    CAST( LEFT( TRIM(on_created), 19) as datetime2(7) ) as on_created,
    NULL as on_updated, --CAST( LEFT( TRIM(on_updated), 19) as datetime2 ),
    old_user_id,
    login_id,
    [url]
FROM [aws].[users];

--
-- Groups
--
INSERT INTO [dbo].[Groups] (
    [Photo],
    [Name], -- [nvarchar](60) NOT NULL,
    [NormalizedName], -- [nvarchar](60) NOT NULL,
    [Description], --[nvarchar](400) NULL,
    [SportsType], --[tinyint] NOT NULL,
    [Age], --[tinyint] NOT NULL,
    [Gender], --[tinyint] NOT NULL,
    [SkillLevel], --[tinyint] NOT NULL,
    [OwnerId],  --[int] NOT NULL,
    [OnCreated], -- [datetime2](7) NOT NULL,
    [OnUpdated], -- [datetime2](7) NULL,
    [OnDeleted], -- [datetime2](7) NULL,
    [IsDeleted], -- [bit] NOT NULL,
    [IsPrivate], -- [bit] NOT NULL,
    [_id], --nvarchar(128)
    [_Url] --nvarchar(128)
)
SELECT
    g.photo,
    g.name, -- varchar(100) NOT NULL,
    g.normalized_name, -- varchar(100) NOT NULL,
    g.description, -- TEXT NULL,
    g.sports_type, -- smallint NOT NULL,
    g.age, -- smallint NOT NULL,
    g.gender, -- smallint NOT NULL,
    g.skill_level, -- smallint NOT NULL,
    u.Id,
    CAST( LEFT( TRIM(g.on_created), 19) as datetime2(7) ) as OnCreated, -- timestamptz NOT NULL default (now() at time zone 'utc'),
    CAST( LEFT( TRIM(g.on_updated), 19) as datetime2(7) ) as OnUpdated, -- timestamptz NULL,
    CAST( LEFT( TRIM(g.on_deleted), 19) as datetime2(7) ) as OnDeleted, -- timestamptz NULL,
    g.is_deleted, -- smallint NOT NULL DEFAULT 0,
    g.is_private, -- smallint NOT NULL DEFAULT 0
    g.old_group_id, --varchar(150) NOT NULL,
    g.url
FROM
    [aws].[groups] as g
    LEFT JOIN [dbo].[Users] as u ON g.created_by = u._Id
;

--
-- Members
--
INSERT INTO [dbo].[Members] (
    [GroupId], -- [int] NOT NULL,
    [UserId], -- [int] NOT NULL,
    [OnCreated], -- [datetime2](7) NOT NULL,
    [OnUpdated], -- [datetime2](7),
    [OnDeleted], -- [datetime2](7),
    [IsDeleted], -- [bit] NOT NULL,
    [OrderSeq], -- [int] NOT NULL,
    [IsHidden], -- [bit] NOT NULL,
    [_GroupId], -- nvarchar(128),
    [_UserId] -- nvarchar(128)
)
SELECT
    g.Id as GroupID,
    u.Id as UserId,
    CAST( LEFT( TRIM(m.on_created), 19) as datetime2(7) ) as OnCreated,
    CAST( LEFT( TRIM(m.on_updated), 19) as datetime2(7) ) as OnUpdated,
    CAST( LEFT( TRIM(m.on_deleted), 19) as datetime2(7) ) as OnDeleted,
    m.is_deleted,
    m.order_seq,
    m.is_hidden,
    m.old_group_id as _GroupId,
    m.old_user_id as _UserId
FROM
    [aws].[members] as m
    LEFT JOIN [dbo].[groups] as g ON m.old_group_id = g._id
    LEFT JOIN [dbo].[users] as u ON m.old_user_id = u._Id
WHERE
    m.old_user_id NOT LIKE 'V-%'
    And u.Id is NOT NULL
;

--
-- MemberRoles
--
INSERT INTO [dbo].[MemberRoles] (
    [GroupId], -- [int] NOT NULL,  -- FK: dbo.Groups.Id
    [UserId], -- [int] NOT NULL,  -- FK: dbo.Users.Id
    [Role], -- [tinyint] NOT NULL,
    [IsDeleted], -- [bit] NOT NULL,
    [OnCreated], -- [datetime2](7) NOT NULL,
    [OnDeleted], -- [datetime2](7) NULL,
    [_GroupId], -- nvarchar(150),
    [_UserId] -- nvarchar(450)
)
SELECT
    m.GroupId,
    m.UserId,
    mr.role,
    mr.is_deleted,
    CAST( LEFT( ISNULL(TRIM(mr.on_created), '1970-01-01 09:00:00'), 19) as datetime2(7) ) as OnCreated,
    CAST( LEFT( TRIM(mr.on_deleted), 19) as datetime2(7) ) as OnDeleted,
    mr.old_group_id as _GroupId,
    mr.old_user_id as _UserId
FROM
    [aws].[member_roles] as mr
    LEFT JOIN [dbo].[Members] as m ON mr.old_group_id = m._GroupId And mr.old_user_id = m._UserId
WHERE
    mr.old_user_id NOT LIKE 'V-%'
    And m.GroupId IS NOT NULL
    And m.UserId IS NOT NULL
;

--
-- Events
--
INSERT INTO [dbo].[Events] (
    [Photo],
    [SportsType], --tinyint not null,
    [Title], --nvarchar(150) not null,
    [NormalizedTitle], --nvarchar(150) not null,
    [Start], --datetime2(7) not null,
    [End], --datetime2(7),
    [TimeZone], --varchar(64),
    [PlaceId], --varchar(250),
    [PlaceName], --nvarchar(32),
    [LocationDetails], --nvarchar(250),
    [Description], --nvarchar(max),
    [Fee], --real not null,
    [ApplicationFee],
    [Age], --tinyint not null,
    [Gender], --tinyint not null,
    [SkillLevel], --tinyint not null,
    [FieldType], --tinyint not null,
    [HasRSVPDeadLine], --bit not null DEFAULT (0),
    [RSVPDeadLine], --datetime2(7),
    [HasParticipantLimit], --bit not null DEFAULT (0),
    [ParticipantLimit], --int not null,
    [AllowGuests], --bit not null DEFAULT (0),
    [Guests], --int not null,
    [IsRecurring], --bit not null DEFAULT (0),
    [IsCanceled],  --bit not null,
    [IsDeleted], --bit not null DEFAULT (0),
    [ParentId], --int,     -- FK: Events.Id
    [GroupId], --int NOT NULL,      -- FK: Groups.Id
    [HostId], --int NOT NULL,       -- FK: Members.Id
    [OwnerId],  -- FK: Members.Id
    [OnCreated], --datetime2(7) not null,
    [OnUpdated], --datetime2(7),
    [OnDeleted], --datetime2(7),
    [_Id], --nvarchar(250),
    [_GroupId], --nvarchar(250),
    [_HostId] --nvarchar(250)
)
SELECT
    e.photo,
    e.sports_type,
    e.title,
    e.normalized_title,
    CAST( LEFT( TRIM(e.[start]), 19) as datetime2(7) ) as [Start],
    CAST( LEFT( TRIM(e.[end]), 19) as datetime2(7) ) as [End],
    e.time_zone,
    e.place_id,
    LEFT(place_name,32),
    e.location_details,
    e.description,
    e.fee,
    0.0,
    e.age,
    e.gender,
    e.skill_level,
    e.field_type,
    e.has_rsvp_deadline,
    CAST( LEFT( TRIM(e.rsvp_deadline), 19) as datetime2(7) ) as [RSVPDeadLine],
    e.has_participant_limit,
    CASE
        WHEN e.participant_limit IS NULL THEN 50
        WHEN e.participant_limit > 50 THEN 50
        ELSE e.participant_limit
    END as [ParticipantLimit],
    e.allow_guests,
    ISNULL(e.guests, 0) as [Guests],
    e.is_recurring,
    0 as IsCanceled,
    e.is_deleted,
    NULL as ParentId,
    g.Id as GroupId,
    m.UserId as HostId,
    m.UserId as OwnerId,
    CAST( LEFT( TRIM(e.on_created), 19) as datetime2(7) ) as OnCreated,
    CAST( LEFT( TRIM(e.on_updated), 19) as datetime2(7) ) as OnUpdated,
    CAST( LEFT( TRIM(e.on_deleted), 19) as datetime2(7) ) as OnDeleted,
    e.old_event_id,
    e.old_group_id,
    e.old_host_id
FROM
    [aws].[events] as e
    LEFT JOIN [dbo].[Members] as m ON e.old_host_id = m._UserId AND e.old_group_id = m._GroupId
    LEFT JOIN [dbo].[Groups] as g ON e.old_group_id = g._Id
WHERE
    (e.old_event_id NOT LIKE 'league_%' AND e.old_event_id NOT LIKE 'user_%')
    And g.Id IS NOT NULL
    And m.UserId IS NOT NULL
;

--
-- UserLocations
--
INSERT INTO [dbo].[UserLocations] (
    [LocationType], -- [tinyint] NOT NULL,
    [UserId], -- [int] NOT NULL,
    [Point], -- [geography] NOT NULL,
    [Country], -- [nvarchar](50) NULL,
    [State], -- [nvarchar](100) NULL,
    [County], -- [nvarchar](100) NULL,
    [City], -- [nvarchar](100) NULL,
    [ZipCode], -- [varchar](20) NULL,
    [Address], -- [nvarchar](128) NULL,
    [_UserId] -- nvarchar(128)
)
SELECT
    ul.location_type,
    u.Id as UserId,
    geography::STPointFromText('POINT('+ Cast(ul.lon as varchar(100)) +' '+ Cast(ul.lat as varchar(100)) +')', 4326),
    ul.country,
    ul.state,
    ul.county,
    ul.city,
    ul.zip_code,
    ul.address,
    ul.old_user_id as _UserId
FROM
    [aws].[user_locations] as ul
    LEFT JOIN [dbo].[Users] as u ON ul.old_user_id = u._Id
WHERE
    u.Id IS NOT NULL
;

--
-- EventLocations
--
INSERT INTO [dbo].[EventLocations] (
    [EventId], -- int NOT NULL PRIMARY KEY,   -- FK: Events.Id
    [Point], -- geography NOT NULL,
    [Country], -- nvarchar(50),
    [State], -- nvarchar(100),
    [County], -- nvarchar(100),
    [City], -- nvarchar(100),
    [ZipCode], -- varchar(20),
    [Address], -- nvarchar(128),
    [_EventId] -- nvarchar(128)
)
SELECT
    e.Id as EventId,
    geography::STPointFromText('POINT('+ Cast(el.lon as varchar(100)) +' '+ Cast(el.lat as varchar(100)) +')', 4326),
    el.country,
    el.state,
    el.county,
    el.city,
    el.zip_code,
    el.address,
    el.old_event_id as _EventId
FROM
    [aws].[event_locations] as el
    LEFT JOIN [dbo].[Events] as e ON el.old_event_id = e._Id
WHERE
    (e._Id NOT LIKE 'league_%' AND e._Id NOT LIKE 'user_%')
    And e.Id IS NOT NULL
;

--
-- GroupLocations
--
INSERT INTO [dbo].[GroupLocations] (
    [GroupId], -- [int] NOT NULL,
    [Point], -- [geography] NOT NULL,
    [Country], -- [nvarchar](50) NULL,
    [State], -- [nvarchar](100) NULL,
    [County], -- [nvarchar](100) NULL,
    [City], -- [nvarchar](100) NULL,
    [ZipCode], -- [varchar](20) NULL,
    [Address], -- [nvarchar](128) NULL,
    [_GroupId] -- nvarchar(128)
)
SELECT
    g.Id as GroupID,
    geography::STPointFromText('POINT('+ Cast(gl.lon as varchar(100)) +' '+ Cast(gl.lat as varchar(100)) +')', 4326),
    gl.country,
    gl.state,
    gl.county,
    gl.city,
    gl.zip_code,
    gl.address,
    gl.old_group_id as _GroupId
FROM
    [aws].[group_locations] as gl
    LEFT JOIN [dbo].[Groups] as g ON gl.old_group_id = g._id
WHERE
    g.Id IS NOT NULL
;

--
-- PhysicalInformations
--
INSERT INTO [dbo].[PhysicalInformations] (
    [UserId], -- [int] NOT NULL,
    [Handed], -- [tinyint] NOT NULL,
    [Footed], -- [tinyint] NOT NULL,
    [Bats], -- [tinyint] NOT NULL,
    [Throws], -- [tinyint] NOT NULL,
    [Height], -- [real] NOT NULL,
    [HeightUnit], -- [tinyint] NOT NULL,
    [Weight], -- [real] NOT NULL,
    [WeightUnit], -- [tinyint] NOT NULL,
    [_UserId]
)
SELECT
    u.Id as UserId,
    bf.handed,
    bf.footed,
    bf.bats,
    bf.throws,
    bf.height,
    CASE
        WHEN bf.height_unit = -1 THEN 1
        ELSE bf.height_unit
    END as height_unit,
    bf.weight,
    CASE
        WHEN bf.weight_unit = -1 THEN 1
        ELSE bf.weight_unit
    END as weight_unit,
    old_user_id as _UserId
FROM
    [aws].[physical_informations] as bf
    LEFT JOIN [dbo].[users] as u ON u._Id = bf.old_user_id
;

--
-- SportsInterest
--
INSERT INTO [dbo].[SportsInterest] (
    [Value],
    [UserId]
)
SELECT
    si.value,
    u.Id as UserId
FROM
    [aws].[sports_interest] as si
    LEFT JOIN [dbo].[Users] as u ON si.old_user_id = u._Id
WHERE
    u.Id IS NOT NULL
;

--
-- EventAmenities
--
INSERT INTO [dbo].[EventAmenities] (
    [Value], -- tinyint,
    [EventId], -- int,   -- FK: Events.Id
    [_EventId] -- nvarchar(128)
)
SELECT
    ea.value,
    e.Id as EventId,
    ea.old_event_id as _EventId
FROM
    [aws].[event_amenities] as ea
    LEFT JOIN [dbo].[Events] as e ON ea.old_event_id = e._Id
WHERE
    (e._Id NOT LIKE 'league_%' AND e._Id NOT LIKE 'user_%')
    And e.Id IS NOT NULL
;

--
-- RSVPs
--
INSERT INTO [dbo].[RSVPs] (
    [EventId], -- int NOT NULL,  -- FK: Events.Id
    [GroupId], -- int NOT NULL, -- FK : Members.GroupId
    [UserId], -- int NOT NULL, -- FK : Members.UserId
    [Guests], -- int NOT NULL,
    [RSVPStatus], -- tinyint NOT NULL DEFAULT (0),
    [OnCreated], -- datetime2(7) NOT NULL DEFAULT (getutcdate()),
    [OnUpdated], -- datetime2(7),
    [_EventId], -- varchar(150),
    [_GroupId], -- varchar(150),
    [_UserId] -- varchar(150)
)
SELECT
    e.Id as EventId,
    m.GroupId as GroupId,
    m.UserId as UserId,
    ISNULL(r.guests, 0) as Guests,
    r.rsvp_state,
    CAST( LEFT( TRIM(r.on_created), 19) as datetime2(7) ) as OnCreated,
    CAST( LEFT( TRIM(r.on_updated), 19) as datetime2(7) ) as OnUpdated,
    r.old_event_id as _EventId,
    r.old_group_id as _GroupId,
    r.old_user_id as _UserId
FROM
    [aws].[rsvps] as r
    LEFT JOIN [dbo].[Events] as e ON r.old_event_id = e._Id
    LEFT JOIN [dbo].[Members] as m ON r.old_group_id = m._GroupId And r.old_user_id = m._UserId
WHERE
    e.Id IS NOT NULL
    AND m.GroupId IS NOT NULL
    AND m.UserId IS NOT NULL
;

--
-- Customer
--
INSERT INTO [dbo].[Customers] (
    [Id], -- varchar(36) PK NOT NULL,   -- stripe customer id
    [Vendor], -- tinyint NOT NULL,   -- 0:Stripe
    [UserId], -- int NOT NULL,   -- FK : Users.Id
    [IsDeleted],
    [OnCreated], -- datetime2(7) NOT NULL DEFAULT (getutcdate()),
    [_UserId] -- varchar(128)
)
SELECT
    c.id,
    c.vendor_type,
    u.Id,
    0 as IsDeleted,
    CAST( LEFT( TRIM(c.on_created), 19) as datetime2(7) ) as OnCreated,
    c.old_user_id as _UserId
FROM
    [aws].[customer] as c
    LEFT JOIN [dbo].[Users] as u ON u._Id = c.old_user_id
WHERE
    u.Id IS NOT NULL
;

--
-- Payments
--
INSERT INTO [dbo].[Payments] (
    [ChargeId], -- varchar(128) NOT NULL,   -- stripe charge id
    [EventId], -- int,  -- FK: RSVPs.EventId
    [UserId], -- int, -- FK : RSVPs.UserId
    [Amount], -- real,
    [FeeAmount], -- real,
    [Quantity], -- int not null default (0),
    [OnCreated], -- datetime2(7) DEFAULT (getutcdate()),
    [CustomerId], -- varchar(36),   -- FK : Customer.Id
    [RefundId], -- varchar(128)
    [ChargeStatus],  -- tinyint
    [RefundStatus],  -- tinyint
    [PaymentStatus], -- tinyint
    [OnUpdated],    -- datetime2(7)
    [CardBrand],    -- varchar(16)
    [CardLast4Digits],  -- char(4)
    [Currency], -- varchar(8)
    [_EventId], -- nvarchar(128),
    [_UserId] -- nvarchar(128)
)
SELECT
    pay.charge_id,
    r.EventId,
    r.UserId,
    pay.amount,
    pay.fee_amount,
    pay.quantity,
    CAST( LEFT( TRIM(pay.on_created), 19) as datetime2(7) ) as OnCreated,
    pay.customer_id,
    pay.refund_id,
    pay.charge_status,
    pay.refund_status,
    pay.payment_status,
    NULL as OnUpdated,
    pay.card_brand,
    pay.card_last4_digits,
    pay.currency,
    pay.old_event_id as _EventId,
    pay.old_user_id as _UserId
FROM
    [aws].[payment] as pay
    LEFT JOIN [dbo].[RSVPs] as r ON pay.old_event_id = r._EventId And pay.old_user_id = r._UserId
WHERE
    r.Id IS NOT NULL
;

--
-- Account
--
INSERT INTO [dbo].[Accounts] (
    [Id],
    [Vendor],
    [OnCreated],
    [UserId]
)
SELECT
    u.AccountId as Id,
    0 as Vendor,
    u.OnCreated as OnCreated,
    u.Id as UserId
FROM
    [dbo].[Users] as u
WHERE
    u.AccountId IS NOT NULL
    And u.AccountId NOT IN (
        SELECT
            u.AccountId
        FROM
            [dbo].[Users] as u
        WHERE
            u.AccountId IS NOT NULL
        group by AccountId
        having count(u.AccountId) > 1
    )
;
`)

// IdentityQuery for insert identity table data
// https://wiki.example.net/pages/viewpage.action?pageId=52494392#ResearchconversionDynamoDBtoMSSQL-MSSQLaws.xxx테이블들→dbo.xxx테이블들
const IdentityQuery = Query(`
--
-- AspNetUsers
--
INSERT INTO [aws].[AspNetUsers] (
    [Id],   -- nvarchar(450) NOT NULL PRIMARY KEY DEFAULT LOWER(NEWID()),
    [_UserId], -- varchar(150),
    [UserName], -- nvarchar(256),
    [NormalizedUserName], -- nvarchar(256),
    [Email], -- nvarchar(256),
    [NormalizedEmail], -- nvarchar(256),
    [EmailConfirmed], -- bit,
    [PasswordHash], -- nvarchar(MAX),
    [SecurityStamp], -- nvarchar(MAX),
    [ConcurrencyStamp], -- nvarchar(MAX),
    [PhoneNumber], -- nvarchar(MAX),
    [PhoneNumberConfirmed], -- bit,
    [TwoFactorEnabled], -- bit,
    [LockoutEnd], -- datetimeoffset,
    [LockoutEnabled], -- bit,
    [AccessFailedCount] -- int
)
SELECT
    u.OpenId,
    iu.old_user_id as _UserId,
    iu.user_name,
    LOWER(iu.normalized_user_name),
    iu.email,
    LOWER(iu.normalized_email),
    iu.email_confirmed,
    iu.password_hash,
    iu.security_stamp,
    iu.concurrency_stamp,
    iu.phone_number,
    iu.phone_number_confirmed,
    iu.two_factor_enabled,
    iu.lockout_end,
    iu.lockout_enabled,
    iu.access_failed_count
FROM
    (-- email 중복 제거를 위해, email 중복 1건 이상 레코드 중 id 높은것들 배제
        SELECT
            *
        FROM [aws].[asp_net_users] as iu1
        WHERE
            iu1.id NOT IN (
                SELECT
                    MAX(iu2.id)
                FROM
                    [aws].[asp_net_users] as iu2
                    LEFT JOIN [dbo].[Users] as u2 ON iu2.old_user_id = u2._Id
                WHERE
                    iu2.email is not null
                GROUP BY iu2.email
                HAVING COUNT(*) > 1
            )
    ) as iu
    LEFT JOIN [dbo].[Users] as u ON iu.old_user_id = u._Id
WHERE
    u.OpenId IS NOT NULL
;

--
-- AspNetUserLogins
--
INSERT INTO [aws].[AspNetUserLogins] (
    [LoginProvider], -- nvarchar(450),
    [ProviderKey], -- nvarchar(450),
    [ProviderDisplayName], -- nvarchar(MAX),
    [UserId], -- nvarchar(150), FK : AspNetUsers.Id
    [_UserId] -- varchar(150)
)
SELECT
    iul.login_provider,
    iul.provider_key,
    iul.provider_display_name,
    iu.Id as UserId,
    iul.old_user_id as _UserId
FROM
    [aws].[asp_net_user_logins] as iul
    LEFT JOIN [aws].[AspNetUsers] as iu ON iul.old_user_id = iu._UserId
WHERE
    iu.Id IS NOT NULL
;
`)
