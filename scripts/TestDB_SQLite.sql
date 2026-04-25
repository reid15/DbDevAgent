
-- Create objects for a SQLite test database

-- Tables

DROP TABLE IF EXISTS TestTable;

CREATE TABLE TestTable(
	ID INTEGER PRIMARY KEY AUTOINCREMENT,
	DisplayName TEXT NOT NULL,
	IsActive INTEGER NOT NULL CHECK(IsActive IN (0,1))
);

DROP TABLE IF EXISTS TestTableAlt;

CREATE TABLE TestTableAlt(
	ID INTEGER CONSTRAINT pk_TestTable PRIMARY KEY AUTOINCREMENT,
	DisplayName TEXT NOT NULL,
	DisplayOrder INTEGER NOT NULL CHECK(DisplayOrder > 0),
	IsActive INTEGER NOT NULL CHECK(IsActive IN (0,1))
) STRICT;

CREATE INDEX ix_TestTableAlt_DisplayNameDisplayOrder ON TestTableAlt(DisplayName, DisplayOrder);

INSERT INTO TestTableAlt (DisplayName, DisplayOrder, IsActive)
VALUES 
	('Record1', 1, 1),
	('Record2', 2, 1);

DROP TABLE IF EXISTS TestTable2;

CREATE TABLE TestTable2(
	ID INTEGER PRIMARY KEY AUTOINCREMENT,
	Amount REAL NULL,
	Amount2 REAL NULL
);

INSERT INTO TestTable2(Amount, Amount2)
VALUES
	(12.345, 67.89),
	(2.00, 7.134);

DROP TABLE IF EXISTS CompoundKey;

DROP TABLE IF EXISTS GroupTable;

CREATE TABLE GroupTable (
	GroupId INTEGER NOT NULL PRIMARY KEY,
	GroupName TEXT NOT NULL UNIQUE
);

DROP TABLE IF EXISTS CompoundKey;

CREATE TABLE CompoundKey (
	GroupId INT NOT NULL REFERENCES GroupTable (GroupId),
	ItemId INT NOT NULL,
	RecordName BLOB NOT NULL,
	CreateDate TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (GroupId, ItemId)
);

DROP TABLE IF EXISTS State;

CREATE TABLE State(
	StateId INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	StateCode TEXT NOT NULL UNIQUE,
	StateName TEXT NOT NULL CHECK(length(StateName) <= 50),
	UpdatedAt TEXT NULL
);

CREATE UNIQUE INDEX ix_state_StateName ON State (StateName);

-- Trigger

CREATE TRIGGER trg_State_Update
AFTER UPDATE ON State
BEGIN
	UPDATE State
	SET UpdatedAt = CURRENT_TIMESTAMP
	WHERE StateId = NEW.StateId;
END;

-- State data

INSERT INTO State (StateCode, StateName)
VALUES ('GA', 'GA');

UPDATE State SET
	StateName = 'Georgia'
WHERE StateCode = 'GA';

SELECT * FROM State;

-- View

DROP VIEW IF EXISTS vw_TestTableAlt;

CREATE VIEW vw_TestTableAlt
AS

SELECT DisplayName, DisplayOrder, IsActive
FROM TestTableAlt;

-- Test objects ------------------------------------------------------------------------------------------

SELECT * FROM vw_TestTableAlt;

-- Return objects in DB

SELECT type, name, tbl_name, sql
FROM sqlite_master
-- Exclude the sequence object - The Table SQL will designate autoincrement
WHERE name <> 'sqlite_sequence'
-- Don't include indexes not explicitly created
	AND name NOT LIKE 'sqlite_autoindex%'
ORDER BY tbl_name;
