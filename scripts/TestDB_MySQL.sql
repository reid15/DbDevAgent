-- Create objects for a MySQL test database
-- Converted from SQL Server

-- Create and select the database (equivalent to USE TestDB)
CREATE DATABASE IF NOT EXISTS TestDB;
USE TestDB;

-- ============================================================
-- Tables
-- ============================================================

-- NOTE: MySQL does not support schemas the way SQL Server does.
-- "dbo" and "alt" schemas are not applicable in MySQL.
-- All tables are placed in the TestDB database.
-- If schema separation is required, create separate databases
-- (e.g., TestDB_alt, TestDB_dbo) and prefix table references accordingly.

DROP TABLE IF EXISTS alt_TestTable;

CREATE TABLE alt_TestTable (
    ID          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,  -- IDENTITY -> AUTO_INCREMENT
    DisplayName VARCHAR(50)  NOT NULL,
    IsActive    TINYINT(1)   NOT NULL   -- BIT -> TINYINT(1), MySQL's boolean convention
);


DROP TABLE IF EXISTS TestTable;

CREATE TABLE TestTable (
    ID           INT         NOT NULL AUTO_INCREMENT,
    DisplayName  VARCHAR(50) NOT NULL,
    DisplayOrder INT         NOT NULL CHECK (DisplayOrder > 0),  -- CHECK supported in MySQL 8.0.16+
    IsActive     TINYINT(1)  NOT NULL,
    CONSTRAINT pk_TestTable PRIMARY KEY (ID)
);

CREATE INDEX ix_dbo_TestTable_DisplayNameDisplayOrder
    ON TestTable (DisplayName, DisplayOrder);

INSERT INTO TestTable (DisplayName, DisplayOrder, IsActive)
VALUES
    ('Record1', 1, 1),
    ('Record2', 2, 1);


DROP TABLE IF EXISTS TestTable2;

CREATE TABLE TestTable2 (
    ID      INT             NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Amount  DECIMAL(10, 4)  NULL,
    Amount2 DECIMAL(10, 4)  NULL,      -- NUMERIC -> DECIMAL (equivalent in MySQL)
    Blob1   BINARY(10)      NULL,      -- BINARY(n) supported natively in MySQL
    Blob2   VARBINARY(10)   NULL,
    Blob3   LONGBLOB        NULL       -- VARBINARY(MAX) -> LONGBLOB
);

INSERT INTO TestTable2 (Amount, Amount2, Blob1)
VALUES
    (12.345, 67.89, CAST('ABC' AS BINARY(10))),
    (2.00,   7.134, CAST('DEF' AS BINARY(10)));


DROP TABLE IF EXISTS CompoundKey;
DROP TABLE IF EXISTS GroupTable;

CREATE TABLE GroupTable (
    GroupId   INT          NOT NULL PRIMARY KEY,
    GroupName VARCHAR(100) NOT NULL UNIQUE  -- NVARCHAR -> VARCHAR (MySQL uses utf8mb4 charset for Unicode)
);

CREATE TABLE CompoundKey (
    GroupId    INT          NOT NULL,
    ItemId     INT          NOT NULL,
    RecordName LONGTEXT     NOT NULL,        -- VARCHAR(MAX) -> LONGTEXT
    CreateDate DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- GETDATE() -> CURRENT_TIMESTAMP
    PRIMARY KEY (GroupId, ItemId),
    CONSTRAINT fk_CompoundKey_GroupTable FOREIGN KEY (GroupId) REFERENCES GroupTable (GroupId)
);


DROP TABLE IF EXISTS State;

CREATE TABLE State (
    StateId   INT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    StateCode CHAR(2)     NOT NULL UNIQUE,
    StateName VARCHAR(100) NOT NULL,
    UpdatedAt DATETIME    NULL
);

CREATE UNIQUE INDEX ix_dbo_state_StateName ON State (StateName);


-- ============================================================
-- Trigger for State
-- MySQL triggers cannot update the same table they fire on in
-- an AFTER trigger (causes error). Use BEFORE trigger instead
-- to modify NEW directly, same net effect.
-- ============================================================

DROP TRIGGER IF EXISTS tr_State_Insert;
DROP TRIGGER IF EXISTS tr_State_Update;

-- MySQL requires separate triggers for INSERT and UPDATE
-- (no combined "AFTER INSERT, UPDATE" syntax)

DELIMITER $$

CREATE TRIGGER tr_State_Insert
BEFORE INSERT ON State
FOR EACH ROW
BEGIN
    SET NEW.UpdatedAt = NOW();
END$$

CREATE TRIGGER tr_State_Update
BEFORE UPDATE ON State
FOR EACH ROW
BEGIN
    SET NEW.UpdatedAt = NOW();
END$$

DELIMITER ;


-- ============================================================
-- State data
-- ============================================================

INSERT INTO State (StateCode, StateName)
VALUES ('GA', 'GA');

UPDATE State
SET StateName = 'Georgia'
WHERE StateCode = 'GA';

SELECT * FROM State;


-- ============================================================
-- Table Types -> MySQL does not support user-defined table types.
-- The GetStateTypeCode procedure is rewritten to accept a
-- StateCode input parameter, or callers can use a temporary table.
-- See the test section at the bottom for the temp table pattern.
-- ============================================================


-- ============================================================
-- Stored Procedures
-- ============================================================

DROP PROCEDURE IF EXISTS GetStateTypeCode;
DROP PROCEDURE IF EXISTS GetTestTable;

DELIMITER $$

-- Rewritten: accepts a comma-separated list or a temp table pattern.
-- Here we use a temp table (statetype) populated by the caller.
CREATE PROCEDURE GetStateTypeCode()
BEGIN
    SELECT StateCode
    FROM statetype;  -- caller must CREATE TEMPORARY TABLE statetype(...) and populate it first
END$$


CREATE PROCEDURE GetTestTable(IN p_ID INT)
BEGIN
    SELECT ID, DisplayName, DisplayOrder, IsActive
    FROM TestTable
    WHERE ID = p_ID;
END$$

DELIMITER ;


-- ============================================================
-- View
-- ============================================================

CREATE OR REPLACE VIEW vwTestTable AS
    SELECT ID, DisplayName, DisplayOrder, IsActive
    FROM TestTable;


-- ============================================================
-- Scalar Function
-- NOTE: MySQL requires DETERMINISTIC or NO SQL / READS SQL DATA
-- declaration for functions when binary logging is enabled.
-- ============================================================

DROP FUNCTION IF EXISTS FormatDisplayName;

DELIMITER $$

CREATE FUNCTION FormatDisplayName(p_DisplayName VARCHAR(50))
RETURNS VARCHAR(60)
DETERMINISTIC
BEGIN
    RETURN CONCAT('Name = ', p_DisplayName);  -- + -> CONCAT() for string concatenation
END$$

DELIMITER ;


-- ============================================================
-- Synonym -> MySQL does not support synonyms.
-- A VIEW over the target object is the closest equivalent.
-- MySQL also has no sys.objects equivalent; information_schema.tables
-- provides similar metadata about database objects.
-- ============================================================

CREATE OR REPLACE VIEW MasterObjects AS
    SELECT TABLE_SCHEMA AS SchemaName,
           TABLE_NAME   AS ObjectName,
           TABLE_TYPE   AS ObjectType
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys');


-- ============================================================
-- Test objects
-- ============================================================

-- Test the table-type equivalent (using a temporary table)
DROP TEMPORARY TABLE IF EXISTS statetype;

CREATE TEMPORARY TABLE statetype (
    StateId   INT,
    StateCode CHAR(2),
    StateName VARCHAR(100)
);

INSERT INTO statetype (StateId, StateCode, StateName)
VALUES (1, 'GA', 'Georgia');

CALL GetStateTypeCode();


-- Test the scalar function via the view
SELECT *, FormatDisplayName(DisplayName) AS FormattedName
FROM vwTestTable;


-- Test the synonym equivalent
SELECT * FROM MasterObjects;
