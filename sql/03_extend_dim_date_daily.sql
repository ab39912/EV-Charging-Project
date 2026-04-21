-- ============================================================
-- Extend MARTS.dim_date to daily grain
-- Replaces the yearly version with a daily version spanning 2014-2026
-- Keeps all existing columns compatible; adds day/month/quarter/weekday
-- ============================================================

USE DATABASE EV_CHARGING_DW;
USE SCHEMA MARTS;
USE WAREHOUSE COMPUTE_WH;

-- Drop the yearly dim_date and rebuild at daily grain
-- Safe because fact_market_demand only stores date_key as INT
-- so existing joins still work as long as Jan 1 of each year is in the new dim
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,    -- YYYYMMDD
    full_date       DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      STRING,
    day             INT,
    day_of_week     INT,                -- 0 = Sunday, 6 = Saturday
    day_name        STRING,
    is_weekend      BOOLEAN
);

-- Generate every day from 2014-01-01 to 2026-12-31 (~4,748 rows)
INSERT INTO dim_date
WITH date_series AS (
    SELECT DATEADD(DAY, SEQ4(), '2014-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 4749))
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))                    AS date_key,
    d                                                     AS full_date,
    YEAR(d)                                               AS year,
    QUARTER(d)                                            AS quarter,
    MONTH(d)                                              AS month,
    MONTHNAME(d)                                          AS month_name,
    DAY(d)                                                AS day,
    DAYOFWEEK(d)                                          AS day_of_week,
    DAYNAME(d)                                            AS day_name,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series
WHERE d <= '2026-12-31';


-- Verify
SELECT COUNT(*) AS total_days FROM dim_date;                -- ~4,748
SELECT MIN(full_date), MAX(full_date) FROM dim_date;        -- 2014-01-01 to 2026-12-31

-- Spot check: sample a week
SELECT date_key, full_date, day_name, is_weekend
FROM dim_date
WHERE full_date BETWEEN '2025-06-01' AND '2025-06-07'
ORDER BY full_date;
