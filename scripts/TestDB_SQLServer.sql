
-- Create objects for a test database

USE TestDB
GO

IF SCHEMA_ID('alt') IS NULL
	EXEC ('CREATE SCHEMA [alt] AUTHORIZATION [dbo];');
GO

-- Tables

DROP TABLE IF EXISTS alt.TestTable;

CREATE TABLE alt.TestTable(
	ID INT NOT NULL IDENTITY(1,1) CONSTRAINT pk_TestTable PRIMARY KEY,
	DisplayName VARCHAR(50) NOT NULL,
	IsActive BIT NOT NULL
);

DROP TABLE IF EXISTS dbo.TestTable;

CREATE TABLE dbo.TestTable(
	ID INT NOT NULL IDENTITY(1,1) CONSTRAINT pk_TestTable PRIMARY KEY,
	DisplayName VARCHAR(50) NOT NULL,
	DisplayOrder INT NOT NULL CHECK(DisplayOrder > 0),
	IsActive BIT NOT NULL
);

CREATE NONCLUSTERED INDEX ix_dbo_TestTable_DisplayNameDisplayOrder ON dbo.TestTable(DisplayName, DisplayOrder);

INSERT INTO dbo.TestTable
VALUES 
	('Record1', 1, 1),
	('Record2', 2, 1);

GO

DROP TABLE IF EXISTS dbo.TestTable2;

CREATE TABLE dbo.TestTable2(
	ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
	Amount DECIMAL(10,4) NULL,
	Amount2 NUMERIC(10,4) NULL
);

INSERT INTO dbo.TestTable2(Amount, Amount2)
VALUES
	(12.345, 67.89),
	(2.00, 7.134);

GO

DROP TABLE IF EXISTS dbo.CompoundKey;

DROP TABLE IF EXISTS dbo.GroupTable;

CREATE TABLE dbo.GroupTable (
	GroupId INT NOT NULL PRIMARY KEY,
	GroupName NVARCHAR(100) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dbo.CompoundKey;

CREATE TABLE dbo.CompoundKey (
	GroupId INT NOT NULL REFERENCES dbo.GroupTable (GroupId),
	ItemId INT NOT NULL,
	RecordName VARCHAR(MAX) NOT NULL,
	CreateDate DATETIME NOT NULL DEFAULT GETDATE(),
	PRIMARY KEY (GroupId, ItemId)
);

GO

DROP TABLE IF EXISTS dbo.[State];

CREATE TABLE dbo.[State](
	StateId INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
	StateCode CHAR(2) NOT NULL UNIQUE,
	StateName VARCHAR(100) NOT NULL
);

CREATE UNIQUE NONCLUSTERED INDEX ix_dbo_state_StateName ON dbo.[State] (StateName);

GO

-- Table Type

DROP PROCEDURE IF EXISTS dbo.GetStateTypeCode;

DROP TYPE IF EXISTS dbo.StateType;

CREATE TYPE dbo.StateType AS TABLE
(
	StateId INT,
	StateCode CHAR(2),
	StateName VARCHAR(100)
);

GO

-- Stored Procedures
GO
CREATE OR ALTER PROCEDURE dbo.GetStateTypeCode
	@StateType dbo.StateType READONLY
AS

SELECT StateCode
FROM @StateType;

GO
CREATE OR ALTER PROCEDURE dbo.GetTestTable 
	@ID INT
AS

SELECT ID, DisplayName, DisplayOrder, IsActive
FROM dbo.TestTable
WHERE ID = @ID;

GO

CREATE OR ALTER VIEW dbo.vwTestTable
AS

SELECT ID, DisplayName, DisplayOrder, IsActive
FROM dbo.TestTable

GO

-- FUNCTION dbo.FormatDisplayName

GO
CREATE OR ALTER FUNCTION dbo.FormatDisplayName
	(@DisplayName VARCHAR(50))
RETURNS VARCHAR(60)
AS
BEGIN
	RETURN 'Name = ' + @DisplayName;
END
GO

-- Synonym

DROP SYNONYM IF EXISTS dbo.MasterObjects;
GO
CREATE SYNONYM dbo.MasterObjects
	FOR [master].sys.objects;

GO

-- Test objects ------------------------------------------------------------------------------------------

-- Test the Table Type

DECLARE @StateType AS dbo.StateType;

INSERT INTO @StateType(StateId, StateCode, StateName)
VALUES (1, 'GA', 'Georgia');

EXEC dbo.GetStateTypeCode @StateType;

GO

-- Test the function

SELECT *, dbo.FormatDisplayName(DisplayName) AS FormattedName
FROM dbo.vwTestTable;

GO

-- Test the synonym

SELECT * FROM dbo.MasterObjects;

GO
