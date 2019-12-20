-- DROP TABLE [aws].[AspNetUsers];
CREATE TABLE [aws].[AspNetUsers]
(
    [Id] nvarchar(450),
    [UserName] nvarchar(256),
    [NormalizedUserName] nvarchar(256),
    [Email] nvarchar(256),
    [NormalizedEmail] nvarchar(256),
    [EmailConfirmed] bit,
    [PasswordHash] nvarchar(MAX),
    [SecurityStamp] nvarchar(MAX),
    [ConcurrencyStamp] nvarchar(MAX),
    [PhoneNumber] nvarchar(MAX),
    [PhoneNumberConfirmed] bit,
    [TwoFactorEnabled] bit,
    [LockoutEnd] datetimeoffset,
    [LockoutEnabled] bit,
    [AccessFailedCount] int,
    PRIMARY KEY ([Id])
);

CREATE NONCLUSTERED INDEX [EmailIndex] ON [aws].[AspNetUsers]
(
    [NormalizedEmail] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY];

CREATE UNIQUE NONCLUSTERED INDEX [UserNameIndex] ON [aws].[AspNetUsers]
(
    [NormalizedUserName] ASC
)
WHERE ([NormalizedUserName] IS NOT NULL)
WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY];

-- DROP TABLE [aws].[AspNetUserLogins];
CREATE TABLE [aws].[AspNetUserLogins]
(
    [LoginProvider] nvarchar(450),
    [ProviderKey] nvarchar(450),
    [ProviderDisplayName] nvarchar(MAX),
    [UserId] nvarchar(450),
    PRIMARY KEY ([LoginProvider],[ProviderKey])
);

CREATE NONCLUSTERED INDEX [IX_AspNetUserLogins_UserId] ON [aws].[AspNetUserLogins] (
    [UserId] ASC
) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY];

ALTER TABLE [aws].[AspNetUserLogins]  WITH CHECK ADD  CONSTRAINT [FK_AspNetUserLogins_AspNetUsers_UserId] FOREIGN KEY([UserId])
REFERENCES [aws].[AspNetUsers] ([Id])
ON DELETE CASCADE;

ALTER TABLE [aws].[AspNetUserLogins] CHECK CONSTRAINT [FK_AspNetUserLogins_AspNetUsers_UserId];
