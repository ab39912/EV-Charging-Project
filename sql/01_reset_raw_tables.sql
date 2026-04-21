-- ============================================================
-- IST 722 — Reset raw layer so UI loader works cleanly
-- Strategy: create raw tables that exactly match source CSV shape
-- All columns VARCHAR — we cast types later in MARTS
-- ============================================================

USE DATABASE EV_CHARGING_DW;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- ---------- Clean slate ----------
DROP TABLE IF EXISTS raw_stations;
DROP TABLE IF EXISTS raw_ev_registrations;
DROP TABLE IF EXISTS raw_census_pop;
DROP TABLE IF EXISTS raw_census_income;


-- ---------- Stations: 75 columns, all VARCHAR ----------
-- Matches the DOE Alt Fuel Stations export exactly
CREATE OR REPLACE TABLE raw_stations (
    fuel_type_code                          VARCHAR,
    station_name                            VARCHAR,
    street_address                          VARCHAR,
    intersection_directions                 VARCHAR,
    city                                    VARCHAR,
    state                                   VARCHAR,
    zip                                     VARCHAR,
    plus4                                   VARCHAR,
    station_phone                           VARCHAR,
    status_code                             VARCHAR,
    expected_date                           VARCHAR,
    groups_with_access_code                 VARCHAR,
    access_days_time                        VARCHAR,
    cards_accepted                          VARCHAR,
    bd_blends                               VARCHAR,
    ng_fill_type_code                       VARCHAR,
    ng_psi                                  VARCHAR,
    ev_level1_evse_num                      VARCHAR,
    ev_level2_evse_num                      VARCHAR,
    ev_dc_fast_count                        VARCHAR,
    ev_other_info                           VARCHAR,
    ev_network                              VARCHAR,
    ev_network_web                          VARCHAR,
    geocode_status                          VARCHAR,
    latitude                                VARCHAR,
    longitude                               VARCHAR,
    date_last_confirmed                     VARCHAR,
    id                                      VARCHAR,
    updated_at                              VARCHAR,
    owner_type_code                         VARCHAR,
    federal_agency_id                       VARCHAR,
    federal_agency_name                     VARCHAR,
    open_date                               VARCHAR,
    hydrogen_status_link                    VARCHAR,
    ng_vehicle_class                        VARCHAR,
    lpg_primary                             VARCHAR,
    e85_blender_pump                        VARCHAR,
    ev_connector_types                      VARCHAR,
    country                                 VARCHAR,
    intersection_directions_french          VARCHAR,
    access_days_time_french                 VARCHAR,
    bd_blends_french                        VARCHAR,
    groups_with_access_code_french          VARCHAR,
    hydrogen_is_retail                      VARCHAR,
    access_code                             VARCHAR,
    access_detail_code                      VARCHAR,
    federal_agency_code                     VARCHAR,
    facility_type                           VARCHAR,
    cng_dispenser_num                       VARCHAR,
    cng_on_site_renewable_source            VARCHAR,
    cng_total_compression_capacity          VARCHAR,
    cng_storage_capacity                    VARCHAR,
    lng_on_site_renewable_source            VARCHAR,
    e85_other_ethanol_blends                VARCHAR,
    ev_pricing                              VARCHAR,
    ev_pricing_french                       VARCHAR,
    lpg_nozzle_types                        VARCHAR,
    hydrogen_pressures                      VARCHAR,
    hydrogen_standards                      VARCHAR,
    cng_fill_type_code                      VARCHAR,
    cng_psi                                 VARCHAR,
    cng_vehicle_class                       VARCHAR,
    lng_vehicle_class                       VARCHAR,
    ev_on_site_renewable_source             VARCHAR,
    restricted_access                       VARCHAR,
    rd_blends                               VARCHAR,
    rd_blends_french                        VARCHAR,
    rd_blended_with_biodiesel               VARCHAR,
    rd_maximum_biodiesel_level              VARCHAR,
    nps_unit_name                           VARCHAR,
    cng_station_sells_renewable_natural_gas VARCHAR,
    lng_station_sells_renewable_natural_gas VARCHAR,
    maximum_vehicle_class                   VARCHAR,
    ev_workplace_charging                   VARCHAR,
    funding_sources                         VARCHAR
);


-- ---------- Registrations: 20 columns, all VARCHAR ----------
CREATE OR REPLACE TABLE raw_ev_registrations (
    record_type             VARCHAR,
    vin                     VARCHAR,
    registration_class      VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    zip                     VARCHAR,
    county                  VARCHAR,
    model_year              VARCHAR,
    make                    VARCHAR,
    body_type               VARCHAR,
    fuel_type               VARCHAR,
    unladen_weight          VARCHAR,
    maximum_gross_weight    VARCHAR,
    passengers              VARCHAR,
    reg_valid_date          VARCHAR,
    reg_expiration_date     VARCHAR,
    color                   VARCHAR,
    scofflaw_indicator      VARCHAR,
    suspension_indicator    VARCHAR,
    revocation_indicator    VARCHAR
);


-- ---------- Census tables: 5 columns each (including empty trailing col) ----------
CREATE OR REPLACE TABLE raw_census_pop (
    geo_id                  VARCHAR,
    name                    VARCHAR,
    population_estimate     VARCHAR,
    population_moe          VARCHAR,
    unnamed_extra           VARCHAR   -- the trailing empty column
);

CREATE OR REPLACE TABLE raw_census_income (
    geo_id                  VARCHAR,
    name                    VARCHAR,
    median_income           VARCHAR,
    income_moe              VARCHAR,
    unnamed_extra           VARCHAR
);


-- ---------- Verify tables are empty and ready ----------
SELECT 'raw_stations' AS tbl, COUNT(*) FROM raw_stations
UNION ALL SELECT 'raw_ev_registrations', COUNT(*) FROM raw_ev_registrations
UNION ALL SELECT 'raw_census_pop', COUNT(*) FROM raw_census_pop
UNION ALL SELECT 'raw_census_income', COUNT(*) FROM raw_census_income;
