CREATE TABLE [dbo].[AspNetUsers]
(
    [Id] nvarchar(450) NOT NULL PRIMARY KEY DEFAULT LOWER(NEWID()),
    [UserName] nvarchar(256),
    [NormalizedUserName] nvarchar(256),
    [Email] nvarchar(256),
    [NormalizedEmail] nvarchar(256),
    [EmailConfirmed] bit NOT NULL,
    [PasswordHash] nvarchar(MAX),
    [SecurityStamp] nvarchar(MAX),
    [ConcurrencyStamp] nvarchar(MAX),
    [PhoneNumber] nvarchar(MAX),
    [PhoneNumberConfirmed] bit NOT NULL,
    [TwoFactorEnabled] bit NOT NULL,
    [LockoutEnd] datetimeoffset,
    [LockoutEnabled] bit NOT NULL,
    [AccessFailedCount] int NOT NULL
);

CREATE TABLE [dbo].[AspNetUserLogins]
(
    [LoginProvider] nvarchar(450) NOT NULL,
    [ProviderKey] nvarchar(450) NOT NULL,
    [ProviderDisplayName] nvarchar(MAX),
    [UserId] nvarchar(450) NOT NULL,
    CONSTRAINT [FK_AspNetUserLogins_AspNetUsers_UserId] FOREIGN KEY ([UserId]) REFERENCES [dbo].[AspNetUsers]([Id]) ON DELETE CASCADE,
    PRIMARY KEY ([LoginProvider],[ProviderKey])
);

CREATE TABLE [dbo].[Users]
(
    [Id] int IDENTITY,
    [OpenId] char(36),
    [Email] varchar(128),
    [Name] nvarchar(100),
    [Gender] tinyint,
    [IsDeleted] [bit] NOT NULL,
    [OnCreated] datetime2(7) DEFAULT (getutcdate()),
    [OnUpdated] datetime2(7),
    [OnDeleted] datetime2(7),
    PRIMARY KEY ([Id])
);

CREATE TABLE [dbo].[Groups]
(
    [Id] int IDENTITY,
    [Name] nvarchar(100),
    [Description] nvarchar(MAX),
    [OnCreated] datetime2(7),
    [OnUpdated] datetime2(7),
    [OnDeleted] datetime2(7),
    [IsDeleted] bit DEFAULT ((0)),
    [IsPrivate] bit DEFAULT ((0)),
    PRIMARY KEY ([Id])
);
