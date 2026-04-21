-- ============================================================
-- Build dim_station from RAW.raw_stations
-- Conformed dimension used by BP1 (charging sessions) and BP2 (utilization)
-- ============================================================

USE DATABASE EV_CHARGING_DW;
USE SCHEMA MARTS;

CREATE OR REPLACE TABLE dim_station (
    station_key             INT AUTOINCREMENT PRIMARY KEY,
    station_id              STRING,             -- DOE ID (business key)
    station_name            STRING,
    ev_network              STRING,
    city                    STRING,
    zip                     STRING,
    county                  STRING,
    location_key            INT,                -- FK to dim_location
    level1_count            INT,
    level2_count            INT,
    dc_fast_count           INT,
    total_port_count        INT,
    connector_types         STRING,
    access_code             STRING,
    facility_type           STRING,
    open_date               DATE,
    latitude                FLOAT,
    longitude               FLOAT,
    is_active               BOOLEAN
);

INSERT INTO dim_station (
    station_id, station_name, ev_network, city, zip, county, location_key,
    level1_count, level2_count, dc_fast_count, total_port_count,
    connector_types, access_code, facility_type,
    open_date, latitude, longitude, is_active
)
SELECT
    s.id                                         AS station_id,
    s.station_name,
    COALESCE(s.ev_network, 'Non-Networked')       AS ev_network,
    s.city,
    s.zip,
    z.county                                       AS county,
    dl.location_key,
    TRY_TO_NUMBER(s.ev_level1_evse_num)            AS level1_count,
    TRY_TO_NUMBER(s.ev_level2_evse_num)            AS level2_count,
    TRY_TO_NUMBER(s.ev_dc_fast_count)              AS dc_fast_count,
    COALESCE(TRY_TO_NUMBER(s.ev_level1_evse_num), 0)
      + COALESCE(TRY_TO_NUMBER(s.ev_level2_evse_num), 0)
      + COALESCE(TRY_TO_NUMBER(s.ev_dc_fast_count), 0) AS total_port_count,
    s.ev_connector_types                           AS connector_types,
    s.access_code,
    s.facility_type,
    TRY_TO_DATE(s.open_date)                       AS open_date,
    TRY_TO_NUMBER(s.latitude)                      AS latitude,
    TRY_TO_NUMBER(s.longitude)                     AS longitude,
    CASE WHEN s.status_code = 'E' THEN TRUE ELSE FALSE END AS is_active
FROM RAW.raw_stations s
LEFT JOIN zip_to_county z  ON s.zip = z.zip
LEFT JOIN dim_location dl  ON z.county = dl.county_name
WHERE s.fuel_type_code = 'ELEC'
  AND s.id IS NOT NULL;


-- Verify
SELECT COUNT(*) AS station_count FROM dim_station;
-- Expected: close to 5,460 (some may drop if ZIP has no county mapping)

-- Top networks
SELECT ev_network, COUNT(*) AS station_count
FROM dim_station
GROUP BY ev_network
ORDER BY station_count DESC
LIMIT 10;

-- Port distribution
SELECT
    SUM(level1_count)     AS total_level1,
    SUM(level2_count)     AS total_level2,
    SUM(dc_fast_count)    AS total_dc_fast,
    SUM(total_port_count) AS total_ports
FROM dim_station;

-- Sanity check: stations per county
SELECT
    dl.county_name,
    COUNT(*) AS station_count,
    SUM(ds.total_port_count) AS total_ports
FROM dim_station ds
JOIN dim_location dl ON ds.location_key = dl.location_key
GROUP BY dl.county_name
ORDER BY station_count DESC
LIMIT 10;
