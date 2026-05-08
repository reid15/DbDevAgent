-- Create objects for a PostgreSQL test database
-- Converted from SQL Server

-- Create schema
CREATE SCHEMA IF NOT EXISTS alt;

-- ============================================================
-- Tables
-- ============================================================

DROP TABLE IF EXISTS alt.testtable;

CREATE TABLE alt.testtable (
    id          SERIAL PRIMARY KEY,
    displayname VARCHAR(50)  NOT NULL,
    isactive    BOOLEAN      NOT NULL
);

DROP TABLE IF EXISTS dbo.testtable;

-- NOTE: In PostgreSQL, "dbo" is not a built-in schema.
-- Create it if it doesn't already exist.
CREATE SCHEMA IF NOT EXISTS dbo;

CREATE TABLE dbo.testtable (
    id           SERIAL PRIMARY KEY,
    displayname  VARCHAR(50) NOT NULL,
    displayorder INT         NOT NULL CHECK (displayorder > 0),
    isactive     BOOLEAN     NOT NULL
);

CREATE INDEX ix_dbo_testtable_displaynamedisplayorder
    ON dbo.testtable (displayname, displayorder);

INSERT INTO dbo.testtable (displayname, displayorder, isactive)
VALUES
    ('Record1', 1, TRUE),
    ('Record2', 2, TRUE);


DROP TABLE IF EXISTS dbo.testtable2;

CREATE TABLE dbo.testtable2 (
    id      SERIAL PRIMARY KEY,
    amount  NUMERIC(10, 4)  NULL,
    amount2 NUMERIC(10, 4)  NULL,
    -- BINARY(n) -> BYTEA in PostgreSQL (fixed-length binary not natively supported)
    blob1   BYTEA           NULL,
    blob2   BYTEA           NULL,
    blob3   BYTEA           NULL
);

INSERT INTO dbo.testtable2 (amount, amount2, blob1)
VALUES
    (12.345, 67.89, RPAD('ABC', 10, '\x00')::BYTEA),
    (2.00,   7.134, RPAD('DEF', 10, '\x00')::BYTEA);


DROP TABLE IF EXISTS dbo.compoundkey;
DROP TABLE IF EXISTS dbo.grouptable;

CREATE TABLE dbo.grouptable (
    groupid   INT           NOT NULL PRIMARY KEY,
    groupname VARCHAR(100)  NOT NULL UNIQUE  -- NVARCHAR -> VARCHAR (PostgreSQL is Unicode by default)
);

CREATE TABLE dbo.compoundkey (
    groupid    INT          NOT NULL REFERENCES dbo.grouptable (groupid),
    itemid     INT          NOT NULL,
    recordname TEXT         NOT NULL,           -- VARCHAR(MAX) -> TEXT
    createdate TIMESTAMP    NOT NULL DEFAULT NOW(),  -- DATETIME/GETDATE() -> TIMESTAMP/NOW()
    PRIMARY KEY (groupid, itemid)
);


DROP TABLE IF EXISTS dbo.state;

CREATE TABLE dbo.state (
    stateid   SERIAL       PRIMARY KEY,
    statecode CHAR(2)      NOT NULL UNIQUE,
    statename VARCHAR(100) NOT NULL,
    updatedat TIMESTAMP    NULL
);

CREATE UNIQUE INDEX ix_dbo_state_statename ON dbo.state (statename);


-- ============================================================
-- Trigger for State (replaces SQL Server AFTER INSERT, UPDATE trigger)
-- PostgreSQL requires a trigger function + trigger definition
-- ============================================================

CREATE OR REPLACE FUNCTION trg_state_insertorupdate()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updatedat := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_state_insertupdate ON dbo.state;

CREATE TRIGGER tr_state_insertupdate
BEFORE INSERT OR UPDATE ON dbo.state   -- BEFORE so we can modify NEW directly
FOR EACH ROW
EXECUTE FUNCTION trg_state_insertorupdate();


-- ============================================================
-- State data
-- ============================================================

INSERT INTO dbo.state (statecode, statename)
VALUES ('GA', 'GA');

UPDATE dbo.state
SET statename = 'Georgia'
WHERE statecode = 'GA';

SELECT * FROM dbo.state;


-- ============================================================
-- Table Type -> PostgreSQL does not support table-valued types.
-- The closest equivalent is a regular (temporary) table or
-- passing data via a REFCURSOR / set-returning function.
-- Here we use a regular table used as a parameter stand-in,
-- and rewrite the stored procedure as a function.
-- ============================================================

-- Reusable "type" table (session-scoped temp table pattern)
-- Callers should CREATE TEMP TABLE statetype (...) before calling the function,
-- or pass data via a CTE / VALUES clause.

-- NOTE: SQL Server Table Types have no direct equivalent.
-- The procedure below is rewritten to accept a temp table named "statetype"
-- that the caller must populate before invoking.

CREATE OR REPLACE FUNCTION dbo.getstatematch_from_type()
RETURNS TABLE (statecode CHAR(2)) AS $$
BEGIN
    RETURN QUERY
        SELECT s.statecode
        FROM statetype s;   -- caller must CREATE TEMP TABLE statetype(...) first
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- Stored Procedures -> PostgreSQL uses functions (or procedures for DML)
-- ============================================================

CREATE OR REPLACE FUNCTION dbo.gettesttable(p_id INT)
RETURNS TABLE (
    id           INT,
    displayname  VARCHAR(50),
    displayorder INT,
    isactive     BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
        SELECT t.id, t.displayname, t.displayorder, t.isactive
        FROM dbo.testtable t
        WHERE t.id = p_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- View
-- ============================================================

CREATE OR REPLACE VIEW dbo.vwtesttable AS
    SELECT id, displayname, displayorder, isactive
    FROM dbo.testtable;


-- ============================================================
-- Scalar Function
-- ============================================================

CREATE OR REPLACE FUNCTION dbo.formatdisplayname(p_displayname VARCHAR(50))
RETURNS VARCHAR(60) AS $$
BEGIN
    RETURN 'Name = ' || p_displayname;  -- + -> || for string concatenation
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- Synonym -> PostgreSQL does not support synonyms.
-- The closest equivalent is a VIEW over the target object.
-- sys.objects is SQL Server-specific; pg_catalog.pg_class is the
-- rough PostgreSQL equivalent.
-- ============================================================

CREATE OR REPLACE VIEW dbo.masterobjects AS
    SELECT oid, relname AS name, relkind AS type, relnamespace AS schema_id
    FROM pg_catalog.pg_class;


-- ============================================================
-- Test objects
-- ============================================================

-- Test the table-type equivalent (using a temp table)
DROP TABLE IF EXISTS statetype;

CREATE TEMP TABLE statetype (
    stateid   INT,
    statecode CHAR(2),
    statename VARCHAR(100)
);

INSERT INTO statetype (stateid, statecode, statename)
VALUES (1, 'GA', 'Georgia');

-- Call the function that reads from the temp table
SELECT * FROM dbo.getstatematch_from_type();


-- Test the scalar function via the view
SELECT *, dbo.formatdisplayname(displayname) AS formattedname
FROM dbo.vwtesttable;


-- Test the synonym equivalent
SELECT * FROM dbo.masterobjects;
