-- =============================================================================
-- EV CHARGING NETWORK ANALYTICS - DATA WAREHOUSE
-- =============================================================================
-- Course       : IST 722 - Data Warehouse (Syracuse University, Spring 2026)
-- Instructor   : Joseph Kinn
-- Team         : Ameya Bhalerao, Gautam Balgi, Siddhant Kasture
--
-- Platform     : Snowflake (cloud data warehouse)
-- Methodology  : Kimball dimensional modeling with enterprise bus architecture
-- Scope        : New York State EV charging infrastructure & registrations
--
-- -----------------------------------------------------------------------------
-- PURPOSE
-- -----------------------------------------------------------------------------
-- This is the single "golden path" script that rebuilds the entire data
-- warehouse from scratch. Running this file end-to-end on an empty Snowflake
-- account (with the three source CSVs uploaded to the stage) produces the
-- final, validated star schema used by the Power BI dashboards.
--
-- -----------------------------------------------------------------------------
-- ARCHITECTURE OVERVIEW
-- -----------------------------------------------------------------------------
-- Three-layer ELT design:
--
--   RAW      -> Unmodified source data loaded via COPY INTO.
--   STAGING  -> Cleaned and cast intermediate table (raw_ev_registrations_clean).
--   MARTS    -> Final dimensional model (4 dimensions + 1 fact).
--
-- -----------------------------------------------------------------------------
-- STAR SCHEMA SUMMARY
-- -----------------------------------------------------------------------------
-- Fact table       : fact_charging_session (one row per charging session)
-- Dimensions       : dim_date, dim_location, dim_station, dim_ev_market
-- Conformed dims   : dim_date, dim_location (shared across processes)
--
-- -----------------------------------------------------------------------------
-- DATA SOURCES
-- -----------------------------------------------------------------------------
-- 1. NY EV Charging Stations      (data.ny.gov)        5,192 rows
-- 2. NY EV Registrations          (data.ny.gov)        212,063 rows (204,856 NY)
-- 3. Synthetic Charging Sessions  (Python-generated)    50,000 rows
--
-- -----------------------------------------------------------------------------
-- EXECUTION ORDER
-- -----------------------------------------------------------------------------
--   Section 1 : Database, schemas, stage, file format
--   Section 2 : RAW layer - load source CSVs
--   Section 3 : STAGING layer - clean registrations (fix column alignment)
--   Section 4 : MARTS layer - build dimensions
--   Section 5 : MARTS layer - build fact table
--   Section 6 : Data quality checks
--   Section 7 : Analytics queries for BI dashboards
-- =============================================================================


-- =============================================================================
-- SECTION 1 : ENVIRONMENT SETUP
-- =============================================================================
-- Creates the database, three-layer schema structure, and an internal stage
-- where source CSV files will be uploaded via the Snowflake UI or SnowSQL PUT.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS EV_CHARGING_DW;
USE DATABASE EV_CHARGING_DW;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE SCHEMA RAW;

-- Internal stage to hold the three source CSV files.
-- Upload files to @ev_stage via Snowsight UI or:  PUT file://<path> @ev_stage
CREATE OR REPLACE STAGE ev_stage
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );


-- =============================================================================
-- SECTION 2 : RAW LAYER - LOAD SOURCE DATA
-- =============================================================================
-- Raw layer stores source data with minimal transformation. All columns are
-- loaded as STRING to avoid COPY failures on malformed values; type casting
-- happens in the STAGING layer.

USE DATABASE EV_CHARGING_DW;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- 2.1 raw_ev_stations - NY EV charging station infrastructure
-- -----------------------------------------------------------------------------
-- Source: https://data.ny.gov  (Alternative Fuel Stations, filtered to NY)
-- Grain : One row per charging station
-- Schema is auto-inferred from the CSV header to match column names exactly.

CREATE OR REPLACE FILE FORMAT csv_format_infer
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    PARSE_HEADER = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE = TRUE
    NULL_IF = ('', 'NULL', 'null');

CREATE OR REPLACE TABLE raw_ev_stations
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION    => '@ev_stage/Electric_Vehicle_Charging_Stations_in_New_York.csv',
            FILE_FORMAT => 'csv_format_infer',
            IGNORE_CASE => TRUE
        )
    )
);

COPY INTO raw_ev_stations
FROM @ev_stage/Electric_Vehicle_Charging_Stations_in_New_York.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format_infer')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 2.2 raw_ev_registrations - NY State EV vehicle registrations
-- -----------------------------------------------------------------------------
-- Source: https://data.ny.gov  (DMV Vehicle Registrations, EVs only)
-- Grain : One row per registered EV
-- Loaded with generic col1..col20 names; real column mapping is applied in
-- Section 3 where we build raw_ev_registrations_clean.

CREATE OR REPLACE TABLE raw_ev_registrations (
    col1 STRING,  col2 STRING,  col3 STRING,  col4 STRING,  col5 STRING,
    col6 STRING,  col7 STRING,  col8 STRING,  col9 STRING,  col10 STRING,
    col11 STRING, col12 STRING, col13 STRING, col14 STRING, col15 STRING,
    col16 STRING, col17 STRING, col18 STRING, col19 STRING, col20 STRING
);

COPY INTO raw_ev_registrations
FROM @ev_stage/Electric_Vehicle_Registrations.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
)
ON_ERROR = 'CONTINUE';

-- -----------------------------------------------------------------------------
-- 2.3 raw_sessions - synthetic EV charging sessions (Python-generated)
-- -----------------------------------------------------------------------------
-- Source: Python script (numpy, pandas) seeded with real station_id values
-- Grain : One row per charging session
-- Used because real session-level transaction data is not publicly available.

CREATE OR REPLACE TABLE raw_sessions (
    session_id        INT,
    station_id        INT,
    session_date      DATE,
    kwh_delivered     DECIMAL(10,2),
    duration_minutes  INT,
    revenue_usd       DECIMAL(10,2)
);

COPY INTO raw_sessions
FROM @ev_stage/synthetic_sessions.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
)
ON_ERROR = 'CONTINUE';

-- Verify all three raw tables loaded successfully
SELECT 'raw_ev_stations'      AS table_name, COUNT(*) AS row_count FROM raw_ev_stations
UNION ALL
SELECT 'raw_ev_registrations',                COUNT(*)              FROM raw_ev_registrations
UNION ALL
SELECT 'raw_sessions',                        COUNT(*)              FROM raw_sessions;


-- =============================================================================
-- SECTION 3 : STAGING LAYER - CLEAN REGISTRATIONS
-- =============================================================================
-- The NY DMV registrations CSV has 20 generic columns that need to be mapped
-- to their real field names. The column layout (verified by inspection) is:
--
--   col1 = Record Type         col2 = VIN Suffix        col3 = Registration Class
--   col4 = City                col5 = State             col6 = Zip
--   col7 = County              col8 = Model Year        col9 = Make
--   col10 = Body Type          col11 = Fuel Type        ... and so on
--
-- We filter to NY-only registrations (col5 = 'NY'), normalize casing, and
-- standardize the ZIP column to a 5-digit format for reliable joining.

USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_ev_registrations_clean AS
SELECT
    col1                                                            AS record_type,
    col2                                                            AS vin_suffix,
    col3                                                            AS registration_class,
    UPPER(TRIM(col4))                                               AS city,
    UPPER(TRIM(col5))                                               AS state,
    LPAD(TRIM(REGEXP_REPLACE(col6, '[^0-9]', '')), 5, '0')          AS zip,
    UPPER(TRIM(col7))                                               AS county,
    TRY_TO_NUMBER(col8)                                             AS model_year,
    UPPER(TRIM(col9))                                               AS make,
    col10                                                           AS body_type,
    col11                                                           AS fuel_type,
    col12                                                           AS unladen_weight,
    col13                                                           AS max_gross_weight,
    col14                                                           AS passengers,
    col15                                                           AS reg_valid_date,
    col16                                                           AS reg_expiry_date,
    col17                                                           AS color
FROM raw_ev_registrations
WHERE col5 = 'NY';

-- Verification
SELECT COUNT(*) AS ny_registration_count FROM raw_ev_registrations_clean;


-- =============================================================================
-- SECTION 4 : MARTS LAYER - DIMENSION TABLES
-- =============================================================================
-- Four dimensions support the single fact table. All dimensions use a surrogate
-- key named <entity>_key and follow lowercase snake_case naming.
-- Uniqueness of the business key on each dimension is enforced by the GROUP BY.

USE SCHEMA MARTS;

-- -----------------------------------------------------------------------------
-- 4.1 dim_date - calendar dimension for 2025 (conformed dimension)
-- -----------------------------------------------------------------------------
-- Grain : One row per calendar day
-- Key   : date_key (YYYYMMDD integer)

CREATE OR REPLACE TABLE dim_date AS
WITH date_range AS (
    SELECT DATEADD(DAY, SEQ4(), TO_DATE('2025-01-01')) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
)
SELECT
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS NUMBER) AS date_key,
    full_date,
    YEAR(full_date)                                AS year,
    QUARTER(full_date)                             AS quarter,
    MONTH(full_date)                               AS month,
    MONTHNAME(full_date)                           AS month_name,
    DAY(full_date)                                 AS day,
    DAYOFWEEK(full_date)                           AS day_of_week,
    DAYNAME(full_date)                             AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
FROM date_range;

-- -----------------------------------------------------------------------------
-- 4.2 dim_station - NY EV charging station attributes
-- -----------------------------------------------------------------------------
-- Grain : One row per charging station
-- Key   : station_key (surrogate); station_id (natural business key)

CREATE OR REPLACE TABLE dim_station AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ID)       AS station_key,
    TRY_TO_NUMBER(ID)                     AS station_id,
    STATIONNAME                           AS station_name,
    EVNETWORK                             AS network,
    TRIM(ZIP)                             AS zip,
    UPPER(TRIM(CITY))                     AS city,
    UPPER(TRIM(STATE))                    AS state,
    TRY_TO_NUMBER(EVLEVEL1EVSENUM)        AS level1_ports,
    TRY_TO_NUMBER(EVLEVEL2EVSENUM)        AS level2_ports,
    TRY_TO_NUMBER(EVDCFASTCOUNT)          AS dc_fast_ports,
    TRY_TO_DECIMAL(LATITUDE, 10, 6)       AS latitude,
    TRY_TO_DECIMAL(LONGITUDE, 10, 6)      AS longitude,
    STATUSCODE                            AS status_code,
    TRY_TO_DATE(OPENDATE)                 AS open_date
FROM EV_CHARGING_DW.RAW.raw_ev_stations
WHERE ID IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 4.3 dim_location - conformed geographic dimension (unique zip + city)
-- -----------------------------------------------------------------------------
-- Grain : One row per unique (zip, city) combination
-- Key   : location_key (surrogate)
-- Built by UNION-ing ZIPs from stations AND registrations, then deduplicated
-- to guarantee uniqueness of the natural key (prevents fact-table fan-out).

CREATE OR REPLACE TABLE dim_location AS
WITH all_locations AS (
    -- Locations present in the station infrastructure data
    SELECT DISTINCT
        TRIM(zip)           AS zip,
        UPPER(TRIM(city))   AS city,
        UPPER(TRIM(state))  AS state,
        NULL                AS county
    FROM dim_station
    WHERE zip IS NOT NULL
      AND LENGTH(TRIM(zip)) = 5
      AND city IS NOT NULL

    UNION

    -- Locations present in the EV registrations data (adds county info)
    SELECT DISTINCT
        zip,
        city,
        state,
        county
    FROM EV_CHARGING_DW.RAW.raw_ev_registrations_clean
    WHERE zip IS NOT NULL
      AND LENGTH(zip) = 5
      AND city IS NOT NULL
),
dedup AS (
    -- Collapse to one row per (zip, city); keep non-null state/county when present
    SELECT
        zip,
        city,
        MAX(state)  AS state,
        MAX(county) AS county
    FROM all_locations
    GROUP BY zip, city
)
SELECT
    ROW_NUMBER() OVER (ORDER BY zip, city) AS location_key,
    zip,
    city,
    state,
    county
FROM dedup;

-- -----------------------------------------------------------------------------
-- 4.4 dim_ev_market - EV demand aggregated at ZIP-code level
-- -----------------------------------------------------------------------------
-- Grain : One row per ZIP code (enforces uniqueness of join key)
-- Key   : market_key (surrogate); zip (natural business key)
-- When multiple city spellings exist for a ZIP, MODE() picks the most common.

CREATE OR REPLACE TABLE dim_ev_market AS
WITH market_agg AS (
    SELECT
        zip,
        COUNT(*)                       AS total_ev_registrations,
        COUNT(DISTINCT make)           AS unique_makes,
        MODE(make)                     AS most_common_make,
        MODE(city)                     AS primary_city,
        MODE(county)                   AS primary_county
    FROM EV_CHARGING_DW.RAW.raw_ev_registrations_clean
    WHERE zip IS NOT NULL AND LENGTH(zip) = 5
    GROUP BY zip
)
SELECT
    ROW_NUMBER() OVER (ORDER BY zip) AS market_key,
    zip,
    primary_city                     AS city,
    primary_county                   AS county,
    total_ev_registrations,
    unique_makes,
    most_common_make
FROM market_agg;


-- =============================================================================
-- SECTION 5 : MARTS LAYER - FACT TABLE
-- =============================================================================
-- fact_charging_session
-- Grain : One row per charging session (exactly 50,000 rows)
--
-- IMPORTANT ON THE STATION JOIN
-- -----------------------------
-- Synthetic sessions were seeded with station_id values 1..5000. Real
-- station_key values in dim_station are arbitrary ROW_NUMBER() outputs, so a
-- direct equi-join would miss most sessions. We bridge the two with a
-- deterministic modulo mapping:
--
--     synthetic_id = ((s.station_id - 1) MOD count(dim_station)) + 1
--
-- This guarantees every session resolves to exactly one real station_key,
-- preserving the fact grain at 50,000 rows.

USE SCHEMA MARTS;

CREATE OR REPLACE TABLE fact_charging_session AS
WITH station_lookup AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY station_key) AS synthetic_id,
        station_key,
        zip,
        city
    FROM dim_station
),
sessions_mapped AS (
    SELECT
        s.session_id,
        s.session_date,
        s.kwh_delivered,
        s.duration_minutes,
        s.revenue_usd,
        sl.station_key,
        sl.zip,
        sl.city
    FROM EV_CHARGING_DW.RAW.raw_sessions s
    JOIN station_lookup sl
        ON ((s.station_id - 1) % (SELECT COUNT(*) FROM dim_station)) + 1 = sl.synthetic_id
)
SELECT
    sm.session_id,
    sm.station_key,
    dd.date_key,
    dl.location_key,
    dm.market_key,
    sm.kwh_delivered,
    sm.duration_minutes,
    sm.revenue_usd,
    ROUND(sm.revenue_usd / NULLIF(sm.kwh_delivered, 0), 2)           AS revenue_per_kwh,
    ROUND(sm.kwh_delivered / NULLIF(sm.duration_minutes, 0) * 60, 2) AS avg_kw_rate
FROM sessions_mapped sm
LEFT JOIN dim_date     dd ON sm.session_date = dd.full_date
LEFT JOIN dim_location dl ON sm.zip = dl.zip AND sm.city = dl.city
LEFT JOIN dim_ev_market dm ON sm.zip = dm.zip;


-- =============================================================================
-- SECTION 6 : DATA QUALITY CHECKS
-- =============================================================================
-- Four validations confirm the warehouse is correct:
--   (a) Row counts match expected grain on every table.
--   (b) No duplicate natural keys on dimensions used as join keys.
--   (c) Fact table row count = 50,000 (matches declared grain).
--   (d) Foreign keys populated on the fact table.

-- (a) Row counts
SELECT 'dim_date'              AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_location',               COUNT(*)             FROM dim_location
UNION ALL SELECT 'dim_station',                COUNT(*)             FROM dim_station
UNION ALL SELECT 'dim_ev_market',              COUNT(*)             FROM dim_ev_market
UNION ALL SELECT 'fact_charging_session',      COUNT(*)             FROM fact_charging_session;
-- Expected: 365 / 3,905 / 5,192 / 1,782 / 50,000

-- (b) No duplicate ZIPs in dim_ev_market  (expected: 0 rows returned)
SELECT zip, COUNT(*) AS dup_count
FROM dim_ev_market
GROUP BY zip
HAVING COUNT(*) > 1;

-- (b) No duplicate (zip, city) in dim_location  (expected: 0 rows returned)
SELECT zip, city, COUNT(*) AS dup_count
FROM dim_location
GROUP BY zip, city
HAVING COUNT(*) > 1;

-- (c) + (d) Fact table quality: total and FK fill rates
SELECT
    COUNT(*)             AS total_sessions,
    COUNT(station_key)   AS has_station,
    COUNT(date_key)      AS has_date,
    COUNT(location_key)  AS has_location,
    COUNT(market_key)    AS has_market
FROM fact_charging_session;
-- Expected: 50000 / 50000 / 50000 / 50000 / ~49864
-- (~136 sessions have no market because some station ZIPs have no EV registrations)


-- =============================================================================
-- SECTION 7 : ANALYTICS QUERIES (POWER BI FEEDS)
-- =============================================================================
-- Three business-question queries that drive the three BI dashboards:
--   Q1 : Station performance -> "which stations earn the most?"
--   Q2 : Time-series trend   -> "how does revenue flow over the year?"
--   Q3 : Market opportunity  -> "where is EV demand outpacing supply?"

-- -----------------------------------------------------------------------------
-- Q1 - Top 10 stations by total revenue
-- -----------------------------------------------------------------------------
-- Dashboard: Station Performance
-- KPIs     : Total revenue, total sessions, total kWh delivered

SELECT
    ds.station_name,
    ds.network,
    ds.city,
    COUNT(*)                          AS total_sessions,
    ROUND(SUM(f.revenue_usd), 2)      AS total_revenue,
    ROUND(SUM(f.kwh_delivered), 2)    AS total_kwh
FROM fact_charging_session f
JOIN dim_station ds ON f.station_key = ds.station_key
GROUP BY ds.station_name, ds.network, ds.city
ORDER BY total_revenue DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Q2 - Monthly revenue trend for 2025
-- -----------------------------------------------------------------------------
-- Dashboard: Station Performance (time-series view)
-- KPIs     : Monthly revenue, session count, average session revenue

SELECT
    dd.month,
    dd.month_name,
    COUNT(*)                          AS session_count,
    ROUND(SUM(f.revenue_usd), 2)      AS monthly_revenue,
    ROUND(AVG(f.revenue_usd), 2)      AS avg_session_revenue
FROM fact_charging_session f
JOIN dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.month, dd.month_name
ORDER BY dd.month;

-- -----------------------------------------------------------------------------
-- Q3 - EV demand vs charging supply by city (expansion recommendation)
-- -----------------------------------------------------------------------------
-- Dashboard: Expansion Recommendation
-- KPI      : EVs per station = total_ev_registrations / station_count
--
-- Interpretation:
--   * High ratio (>500)  -> underserved market, strong expansion candidate
--   * Low ratio (<50)    -> saturated or oversupplied market
-- Real output highlights Brooklyn (983 EVs/station) as the top expansion target.

SELECT
    dm.city,
    dm.county,
    dm.total_ev_registrations,
    COUNT(DISTINCT ds.station_key)    AS station_count,
    ROUND(
        dm.total_ev_registrations::FLOAT
        / NULLIF(COUNT(DISTINCT ds.station_key), 0),
        2
    ) AS evs_per_station
FROM dim_ev_market dm
LEFT JOIN dim_station ds ON dm.zip = ds.zip
GROUP BY dm.city, dm.county, dm.total_ev_registrations
HAVING station_count > 0
ORDER BY dm.total_ev_registrations DESC
LIMIT 10;

-- =============================================================================
-- END OF SCRIPT
-- =============================================================================
