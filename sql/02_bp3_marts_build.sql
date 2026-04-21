-- ============================================================
-- IST 722 — BP3: NY Market Demand (MARTS build)
-- Owner: Siddhant Kasture
-- Prereq: raw_stations, raw_ev_registrations, raw_census_pop,
--         raw_census_income all loaded (VARCHAR) in RAW schema
-- Grain: one row per NY county per year
-- ============================================================

USE DATABASE EV_CHARGING_DW;
USE WAREHOUSE COMPUTE_WH;


-- ---------- 1. Dim: Date ----------
USE SCHEMA MARTS;

CREATE OR REPLACE TABLE dim_date (
    date_key        INT,
    year            INT,
    full_date       DATE
);

INSERT INTO dim_date (date_key, year, full_date)
SELECT
    TO_NUMBER(TO_CHAR(DATEADD(YEAR, SEQ4(), '2014-01-01'::DATE), 'YYYYMMDD')),
    YEAR(DATEADD(YEAR, SEQ4(), '2014-01-01'::DATE)),
    DATEADD(YEAR, SEQ4(), '2014-01-01'::DATE)
FROM TABLE(GENERATOR(ROWCOUNT => 13));

SELECT * FROM dim_date ORDER BY year;


-- ---------- 2. Helper: ZIP -> County lookup from registrations ----------
-- Most-common county per ZIP (some ZIPs span counties)
CREATE OR REPLACE TABLE zip_to_county AS
WITH ranked AS (
    SELECT
        zip,
        UPPER(TRIM(county)) AS county,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (PARTITION BY zip ORDER BY COUNT(*) DESC) AS rn
    FROM RAW.raw_ev_registrations
    WHERE zip IS NOT NULL AND county IS NOT NULL AND TRIM(county) != ''
    GROUP BY zip, UPPER(TRIM(county))
)
SELECT zip, county FROM ranked WHERE rn = 1;


-- ---------- 3. Dim: Location (NY county grain) ----------
CREATE OR REPLACE TABLE dim_location (
    location_key        INT AUTOINCREMENT PRIMARY KEY,
    county_name         STRING,
    state_code          STRING,
    region              STRING,
    population          INT,
    median_income       NUMBER(12,2)
);

INSERT INTO dim_location (county_name, state_code, region, population, median_income)
WITH census_zip AS (
    SELECT
        REGEXP_SUBSTR(p.name, '[0-9]{5}') AS zip,
        TRY_TO_NUMBER(p.population_estimate) AS population,
        TRY_TO_NUMBER(i.median_income)       AS median_income
    FROM RAW.raw_census_pop p
    LEFT JOIN RAW.raw_census_income i ON p.geo_id = i.geo_id
    WHERE TRY_TO_NUMBER(p.population_estimate) IS NOT NULL
),
county_census AS (
    SELECT
        z.county,
        SUM(c.population)                              AS population,
        SUM(c.population * c.median_income)
          / NULLIF(SUM(CASE WHEN c.median_income IS NOT NULL
                           THEN c.population ELSE 0 END), 0) AS median_income
    FROM census_zip c
    JOIN zip_to_county z ON c.zip = z.zip
    GROUP BY z.county
)
SELECT
    county,
    'NY' AS state_code,
    CASE
        WHEN county IN ('NEW YORK','KINGS','QUEENS','BRONX','RICHMOND') THEN 'NYC'
        WHEN county IN ('NASSAU','SUFFOLK') THEN 'Long Island'
        WHEN county IN ('WESTCHESTER','ROCKLAND','PUTNAM','ORANGE','DUTCHESS')
            THEN 'Hudson Valley'
        WHEN county IN ('ERIE','NIAGARA','CHAUTAUQUA','CATTARAUGUS','ALLEGANY')
            THEN 'Western NY'
        WHEN county IN ('MONROE','ONTARIO','WAYNE','LIVINGSTON','YATES','SENECA')
            THEN 'Finger Lakes'
        WHEN county IN ('ONONDAGA','OSWEGO','CAYUGA','MADISON','CORTLAND')
            THEN 'Central NY'
        WHEN county IN ('ALBANY','RENSSELAER','SCHENECTADY','SARATOGA','WASHINGTON','WARREN')
            THEN 'Capital District'
        ELSE 'Other NY'
    END AS region,
    population,
    median_income
FROM county_census
WHERE county IS NOT NULL;

SELECT COUNT(*) AS county_count FROM dim_location;
SELECT * FROM dim_location ORDER BY population DESC LIMIT 10;


-- ---------- 4. Fact: Market Demand ----------
CREATE OR REPLACE TABLE fact_market_demand (
    market_key              INT AUTOINCREMENT PRIMARY KEY,
    date_key                INT,
    location_key            INT,
    ev_registrations_cum    INT,
    new_ev_registrations    INT,
    station_count_cum       INT,
    new_stations            INT,
    ev_per_station          NUMBER(10,2),
    population              INT,
    median_income           NUMBER(12,2)
);

INSERT INTO fact_market_demand
    (date_key, location_key, ev_registrations_cum, new_ev_registrations,
     station_count_cum, new_stations, ev_per_station, population, median_income)
WITH years AS (
    SELECT year FROM dim_date WHERE year BETWEEN 2015 AND 2026
),
counties AS (
    SELECT location_key, county_name, population, median_income FROM dim_location
),
-- New EVs per county per year (by Model Year, filtered to currently registered)
new_evs AS (
    SELECT
        UPPER(TRIM(county))              AS county,
        TRY_TO_NUMBER(model_year)        AS year,
        COUNT(*)                         AS new_evs
    FROM RAW.raw_ev_registrations
    WHERE TRY_TO_NUMBER(model_year) BETWEEN 2015 AND 2026
      AND fuel_type = 'ELECTRIC'
      AND TRY_TO_DATE(reg_valid_date, 'MM/DD/YYYY') >= '2024-01-01'
      AND county IS NOT NULL
    GROUP BY UPPER(TRIM(county)), TRY_TO_NUMBER(model_year)
),
ev_matrix AS (
    SELECT
        c.location_key,
        c.county_name,
        y.year,
        COALESCE(n.new_evs, 0) AS new_evs
    FROM counties c
    CROSS JOIN years y
    LEFT JOIN new_evs n
        ON UPPER(c.county_name) = n.county AND y.year = n.year
),
ev_cum AS (
    SELECT
        location_key,
        county_name,
        year,
        new_evs,
        SUM(new_evs) OVER (
            PARTITION BY county_name ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ev_cumulative
    FROM ev_matrix
),
-- Stations per county per year, from ZIP lookup
station_county AS (
    SELECT
        z.county,
        YEAR(TRY_TO_DATE(s.open_date)) AS year,
        COUNT(*) AS new_stations
    FROM RAW.raw_stations s
    LEFT JOIN zip_to_county z ON s.zip = z.zip
    WHERE s.fuel_type_code = 'ELEC'
      AND TRY_TO_DATE(s.open_date) IS NOT NULL
      AND z.county IS NOT NULL
    GROUP BY z.county, YEAR(TRY_TO_DATE(s.open_date))
),
station_matrix AS (
    SELECT
        c.location_key,
        c.county_name,
        y.year,
        COALESCE(sc.new_stations, 0) AS new_stations
    FROM counties c
    CROSS JOIN years y
    LEFT JOIN station_county sc
        ON UPPER(c.county_name) = sc.county AND y.year = sc.year
),
station_cum AS (
    SELECT
        location_key,
        county_name,
        year,
        new_stations,
        SUM(new_stations) OVER (
            PARTITION BY county_name ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS station_cumulative
    FROM station_matrix
)
SELECT
    TO_NUMBER(e.year || '0101')     AS date_key,
    e.location_key,
    e.ev_cumulative                 AS ev_registrations_cum,
    e.new_evs                       AS new_ev_registrations,
    s.station_cumulative            AS station_count_cum,
    s.new_stations,
    CASE
        WHEN s.station_cumulative = 0 THEN NULL
        ELSE e.ev_cumulative::NUMBER / s.station_cumulative
    END                             AS ev_per_station,
    c.population,
    c.median_income
FROM ev_cum e
JOIN station_cum s
    ON e.location_key = s.location_key AND e.year = s.year
JOIN counties c
    ON e.location_key = c.location_key;


-- ---------- 5. Verification ----------
SELECT COUNT(*) AS fact_row_count FROM fact_market_demand;
-- Expected: ~62 counties × 12 years = ~744 rows

-- Top underserved NY markets (latest year)
SELECT
    dl.county_name,
    dl.region,
    f.ev_registrations_cum,
    f.station_count_cum,
    ROUND(f.ev_per_station, 1)                  AS ev_per_station,
    TO_CHAR(dl.median_income, '$999,999')       AS median_income
FROM fact_market_demand f
JOIN dim_location dl ON f.location_key = dl.location_key
JOIN dim_date d      ON f.date_key = d.date_key
WHERE d.year = 2025
  AND f.station_count_cum > 5
  AND f.ev_registrations_cum > 100
ORDER BY f.ev_per_station DESC
LIMIT 10;

-- Regional growth story
SELECT
    dl.region,
    d.year,
    SUM(f.new_ev_registrations) AS new_evs_added,
    SUM(f.new_stations)         AS new_stations_added
FROM fact_market_demand f
JOIN dim_location dl ON f.location_key = dl.location_key
JOIN dim_date d      ON f.date_key = d.date_key
WHERE d.year BETWEEN 2020 AND 2025
GROUP BY dl.region, d.year
ORDER BY dl.region, d.year;
