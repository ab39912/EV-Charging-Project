-- ============================================================
-- BP1 + BP2 Full Build
-- BP1 (Ameya's): fact_charging_session — transaction grain
-- BP2 (Gautam's): fact_station_daily_snapshot — periodic snapshot
-- Prereq: run 03_extend_dim_date_daily.sql and 04_build_dim_station.sql first
-- ============================================================

USE DATABASE EV_CHARGING_DW;
USE WAREHOUSE COMPUTE_WH;


-- ---------- 1. Create raw_sessions landing table ----------
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_sessions (
    session_id          VARCHAR,
    station_id          VARCHAR,
    session_date        VARCHAR,
    kwh_delivered       VARCHAR,
    duration_minutes    VARCHAR,
    revenue_usd         VARCHAR
);

-- After creating: load the CSV via Snowflake UI
--   Data > Databases > EV_CHARGING_DW > RAW > raw_sessions > Load Data
--   Upload your sessions CSV from GitHub
--   Skip header: 1
--
-- Verify after load:
SELECT COUNT(*) AS session_count FROM raw_sessions;
SELECT * FROM raw_sessions LIMIT 5;


-- ---------- 2. BP1: fact_charging_session ----------
USE SCHEMA MARTS;

CREATE OR REPLACE TABLE fact_charging_session (
    session_key             INT AUTOINCREMENT PRIMARY KEY,
    session_id              STRING,             -- business key
    station_key             INT,                -- FK to dim_station
    date_key                INT,                -- FK to dim_date (daily)
    location_key            INT,                -- FK to dim_location (conformed)
    kwh_delivered           NUMBER(10,3),
    duration_minutes        INT,
    revenue_usd             NUMBER(10,2),
    revenue_per_kwh         NUMBER(6,3),
    session_date            DATE
);

INSERT INTO fact_charging_session (
    session_id, station_key, date_key, location_key,
    kwh_delivered, duration_minutes, revenue_usd, revenue_per_kwh, session_date
)
SELECT
    s.session_id,
    ds.station_key,
    TO_NUMBER(TO_CHAR(TRY_TO_DATE(s.session_date), 'YYYYMMDD'))  AS date_key,
    ds.location_key,                                              -- inherited from dim_station
    TRY_TO_NUMBER(s.kwh_delivered)                                AS kwh_delivered,
    TRY_TO_NUMBER(s.duration_minutes)                             AS duration_minutes,
    TRY_TO_NUMBER(s.revenue_usd)                                  AS revenue_usd,
    CASE
        WHEN TRY_TO_NUMBER(s.kwh_delivered) > 0
        THEN TRY_TO_NUMBER(s.revenue_usd) / TRY_TO_NUMBER(s.kwh_delivered)
        ELSE NULL
    END                                                           AS revenue_per_kwh,
    TRY_TO_DATE(s.session_date)                                   AS session_date
FROM RAW.raw_sessions s
LEFT JOIN dim_station ds ON s.station_id = ds.station_id
WHERE ds.station_key IS NOT NULL          -- drop orphan sessions (no matching station)
  AND TRY_TO_DATE(s.session_date) IS NOT NULL;


-- Verify BP1
SELECT COUNT(*) AS session_count FROM fact_charging_session;

-- Orphan check — should be small or zero
SELECT
    (SELECT COUNT(*) FROM RAW.raw_sessions)      AS raw_rows,
    (SELECT COUNT(*) FROM fact_charging_session) AS loaded_rows,
    (SELECT COUNT(*) FROM RAW.raw_sessions) -
    (SELECT COUNT(*) FROM fact_charging_session) AS dropped_rows;

-- Sample demo query: revenue by network
SELECT
    ds.ev_network,
    COUNT(*)                        AS session_count,
    SUM(f.kwh_delivered)            AS total_kwh,
    SUM(f.revenue_usd)              AS total_revenue,
    AVG(f.revenue_per_kwh)          AS avg_price_per_kwh
FROM fact_charging_session f
JOIN dim_station ds ON f.station_key = ds.station_key
GROUP BY ds.ev_network
ORDER BY total_revenue DESC
LIMIT 10;


-- ---------- 3. BP2: fact_station_daily_snapshot ----------
-- Grain: one row per station per day (aggregated from BP1)
-- Only includes days where at least one session occurred (sparse snapshot)
-- A dense snapshot (every station × every day) would be much larger

CREATE OR REPLACE TABLE fact_station_daily_snapshot (
    snapshot_key            INT AUTOINCREMENT PRIMARY KEY,
    station_key             INT,                -- FK to dim_station
    date_key                INT,                -- FK to dim_date
    location_key            INT,                -- FK to dim_location
    total_sessions          INT,
    total_kwh               NUMBER(12,3),
    total_revenue           NUMBER(12,2),
    avg_session_duration    NUMBER(8,2),
    avg_revenue_per_session NUMBER(8,2),
    -- Utilization approximation: assume 24hr availability × port count = max minutes/day
    -- Then utilization = actual session minutes / max minutes
    utilization_rate_pct    NUMBER(5,2)
);

INSERT INTO fact_station_daily_snapshot (
    station_key, date_key, location_key,
    total_sessions, total_kwh, total_revenue,
    avg_session_duration, avg_revenue_per_session, utilization_rate_pct
)
SELECT
    f.station_key,
    f.date_key,
    f.location_key,
    COUNT(*)                                      AS total_sessions,
    SUM(f.kwh_delivered)                          AS total_kwh,
    SUM(f.revenue_usd)                            AS total_revenue,
    AVG(f.duration_minutes)                       AS avg_session_duration,
    AVG(f.revenue_usd)                            AS avg_revenue_per_session,
    -- Utilization: total session minutes / (ports × 1440 min/day) × 100
    CASE
        WHEN MAX(ds.total_port_count) > 0 THEN
            ROUND(
                (SUM(f.duration_minutes)::NUMBER / (MAX(ds.total_port_count) * 1440.0)) * 100,
                2
            )
        ELSE NULL
    END                                           AS utilization_rate_pct
FROM fact_charging_session f
JOIN dim_station ds ON f.station_key = ds.station_key
GROUP BY f.station_key, f.date_key, f.location_key;


-- Verify BP2
SELECT COUNT(*) AS snapshot_count FROM fact_station_daily_snapshot;

-- Utilization distribution
SELECT
    CASE
        WHEN utilization_rate_pct < 20 THEN '0-20% (low)'
        WHEN utilization_rate_pct < 40 THEN '20-40%'
        WHEN utilization_rate_pct < 60 THEN '40-60% (healthy)'
        WHEN utilization_rate_pct < 80 THEN '60-80% (high)'
        ELSE '80%+ (saturated)'
    END AS utilization_tier,
    COUNT(*) AS snapshot_count
FROM fact_station_daily_snapshot
GROUP BY utilization_tier
ORDER BY MIN(utilization_rate_pct);

-- Top 10 stations by total revenue
SELECT
    ds.station_name,
    ds.city,
    COUNT(*)                  AS active_days,
    SUM(fs.total_sessions)    AS lifetime_sessions,
    SUM(fs.total_revenue)     AS lifetime_revenue,
    AVG(fs.utilization_rate_pct) AS avg_utilization
FROM fact_station_daily_snapshot fs
JOIN dim_station ds ON fs.station_key = ds.station_key
GROUP BY ds.station_name, ds.city
ORDER BY lifetime_revenue DESC
LIMIT 10;


-- ---------- 4. Final summary for team ----------
SELECT 'raw_sessions'              AS tbl, COUNT(*) AS row_count FROM RAW.raw_sessions
UNION ALL SELECT 'dim_date',              COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_station',           COUNT(*) FROM dim_station
UNION ALL SELECT 'dim_location',          COUNT(*) FROM dim_location
UNION ALL SELECT 'fact_market_demand',    COUNT(*) FROM fact_market_demand
UNION ALL SELECT 'fact_charging_session', COUNT(*) FROM fact_charging_session
UNION ALL SELECT 'fact_station_daily_snapshot', COUNT(*) FROM fact_station_daily_snapshot;
