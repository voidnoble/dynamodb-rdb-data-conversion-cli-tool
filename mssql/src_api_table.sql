CREATE TABLE [aws].[users]
(
    email varchar(128),
    name nvarchar(100),
    account_id nvarchar(250),
    urlname nvarchar(100),
    login_id varchar(150),
    is_deleted smallint NOT NULL,
    on_created varchar(20) NOT NULL,
    on_updated varchar(20),
    on_deleted varchar(20)
);

CREATE TABLE [aws].[groups]
(
    name nvarchar(100) NOT NULL,
    description nvarchar(max) NULL,
    urlname nvarchar(100),
    on_created varchar(20) NOT NULL,
    on_updated varchar(20) NULL,
    on_deleted varchar(20) NULL,
    is_deleted smallint NOT NULL DEFAULT (0)
);