# EV Charging Network Analytics — Data Warehouse

A Snowflake-based dimensional data warehouse for analyzing New York State's electric vehicle (EV) charging infrastructure, registration growth, and market demand. The warehouse uses Kimball-style enterprise bus architecture with conformed dimensions across three business processes, and powers a set of Power BI dashboards answering questions about station performance, utilization, and where to expand the network next.

> **Course context:** Built for IST 722 — Data Warehouse (Syracuse University, Spring 2026), instructor Joseph Kinn.
> **Team:** Ameya Bhalerao, Gautam Balgi, Siddhant Kasture.

---

## What's in this repo

```
EV-Charging-Project/
├── data/
│   └── raw/
│       ├── raw_ev_stations.csv          # NY EV charging stations (DOE Alt Fuel Stations)
│       ├── ev_regristrations_ny.csv     # NY DMV EV vehicle registrations
│       ├── synthetic_sessions.csv       # 50K synthetic charging sessions (Python-generated)
│       ├── census_population_zip.csv    # US Census B01003 — population by ZCTA
│       └── census_income_zip.csv        # US Census B19013 — median household income by ZCTA
└── sql/
    ├── ev_warehouse_final.sql           # Single golden-path script (run this end-to-end)
    ├── 01_reset_raw_tables.sql          # Clean slate for the RAW schema
    ├── 02_bp3_marts_build.sql           # BP3 — NY market demand (Siddhant)
    ├── 03_extend_dim_date_daily.sql     # Extends dim_date from yearly to daily grain
    ├── 04_build_dim_station.sql         # Conformed dim_station used by BP1 & BP2
    └── 05_bp1_bp2_build.sql             # BP1 sessions + BP2 daily snapshot (Ameya & Gautam)
```

`ev_warehouse_final.sql` is the consolidated, validated end-to-end build. The numbered scripts `01_…05_` are the original modular build path the team used during development and are kept for reference and reproducibility.

---

## Architecture

The warehouse follows a three-layer ELT design on Snowflake:

```
  RAW       →  Source CSVs loaded via COPY INTO, all columns as VARCHAR/STRING
  STAGING   →  Cleaned + cast intermediate tables (e.g. raw_ev_registrations_clean)
  MARTS     →  Final dimensional model — 4 dimensions + fact tables
```

### Star schema

Three business processes share conformed dimensions through the enterprise bus:

| Process | Fact table | Grain |
|---|---|---|
| BP1 — Charging Sessions   | `fact_charging_session`         | One row per session (~50,000) |
| BP2 — Station Utilization | `fact_station_daily_snapshot`   | One row per station per active day |
| BP3 — Market Demand       | `fact_market_demand`            | One row per NY county per year (~744) |

Dimensions:

- `dim_date` — calendar grain, daily for 2014–2026
- `dim_location` — conformed (ZIP, city) geography with state and county
- `dim_station` — station attributes (network, port counts, lat/lon, open date)
- `dim_ev_market` — EV demand aggregated to ZIP level
- `zip_to_county` — helper mapping ZIPs to their most common NY county

### Conformance

`dim_date` and `dim_location` are conformed across all three processes, so dashboards can drill from a session up to county-level market demand without re-mapping geography or time.

---

## Data sources

| Source | Provider | Rows | Notes |
|---|---|---|---|
| EV charging stations  | data.ny.gov (DOE Alternative Fuel Stations) | ~5,200 | Filtered to NY |
| EV registrations      | data.ny.gov (NY DMV)                        | ~212,000 (~205K NY) | Long-format CSV; column alignment fixed in STAGING |
| Charging sessions     | Python-generated (numpy/pandas), seeded with real station IDs | 50,000 | Real session-level transaction data is not publicly available |
| Population by ZCTA    | US Census B01003 (ACS 5-year)               | — | Used by BP3 |
| Median income by ZCTA | US Census B19013 (ACS 5-year)               | — | Used by BP3 |

---

## How to run

The build assumes a Snowflake account with `ACCOUNTADMIN` role and an XS warehouse (`COMPUTE_WH`).

### Quickest path — single script

1. **Upload the five CSVs** in `data/raw/` to an internal Snowflake stage named `@ev_stage` (created by the script). The script's `COPY INTO` statements expect these filenames, so either rename to match or edit the stage paths:
   - `Electric_Vehicle_Charging_Stations_in_New_York.csv`
   - `Electric_Vehicle_Registrations.csv`
   - `synthetic_sessions.csv`
2. **Run** `sql/ev_warehouse_final.sql` end-to-end. It creates the database, schemas, file format, RAW tables, STAGING clean table, all dimensions, the fact table, and runs the data-quality checks at the bottom.
3. **Verify** the row counts at the end of the script match:

   ```
   dim_date              =    365
   dim_location          =  3,905
   dim_station           =  5,192
   dim_ev_market         =  1,782
   fact_charging_session = 50,000
   ```

### Full build with all three business processes

If you want BP2 (daily snapshot) and BP3 (county-level market demand) as well, run the numbered scripts in order:

```
01_reset_raw_tables.sql       -- clean RAW
02_bp3_marts_build.sql        -- BP3 (county/year grain)
03_extend_dim_date_daily.sql  -- upgrade dim_date to daily
04_build_dim_station.sql      -- conformed station dim
05_bp1_bp2_build.sql          -- BP1 sessions + BP2 daily snapshot
```

CSV loads in this path use the Snowflake UI (`Data > Databases > EV_CHARGING_DW > RAW > <table> > Load Data`) rather than `COPY INTO` from a stage. Each script's header documents what gets loaded where.

---

## Notable design decisions

A few choices in the build are worth flagging because they will look unusual on a first read:

- **Synthetic-session ↔ real-station bridge.** Sessions were seeded with `station_id` values `1..5000`, but `dim_station.station_key` is a `ROW_NUMBER()` surrogate. A direct equi-join would lose most sessions, so the fact build maps via `((s.station_id - 1) MOD count(dim_station)) + 1`. This guarantees every session resolves to exactly one real station and preserves the 50,000-row grain.
- **Registrations CSV column alignment.** The DMV export has 20 columns loaded as generic `col1..col20`. The STAGING layer (`raw_ev_registrations_clean`) maps these to their real names and filters to `state = 'NY'`.
- **ZIP standardization.** All ZIPs are normalized to 5-digit strings with `LPAD(..., 5, '0')` before joining. This was the single biggest source of fan-out bugs during development.
- **Sparse daily snapshot.** `fact_station_daily_snapshot` only stores days where at least one session occurred. A dense snapshot (every station × every day) would multiply the row count by ~70× with mostly empty rows.
- **`dim_ev_market` uses `MODE()`** to pick the most common city/county per ZIP when multiple spellings exist — this keeps the natural key (ZIP) unique and prevents fact fan-out.

---

## Analytics powered by the warehouse

Three business questions drive the Power BI dashboards. The queries live at the bottom of `ev_warehouse_final.sql` (Section 7):

1. **Station performance** — top 10 stations by total revenue, sessions, and kWh delivered.
2. **Time-series trend** — monthly revenue, session count, and average session revenue across 2025.
3. **Expansion recommendation** — EVs-per-station ratio by city. A ratio above ~500 flags an underserved market; the build identifies Brooklyn (≈983 EVs/station) as the strongest expansion target.

---

## Data quality

The final script ends with four validation checks:

1. Row counts match expected grain on every dimension and fact table.
2. No duplicate natural keys on the dimensions used as join keys (`dim_ev_market.zip`, `dim_location.(zip, city)`).
3. Fact row count equals declared grain (50,000).
4. Foreign key fill rates on the fact table (≈99.7% on `market_key` — ~136 sessions have no market because some station ZIPs have no EV registrations on file).

---

## Tech stack

- **Snowflake** — cloud data warehouse
- **SQL** — all transformations (no dbt; deliberate, for IST 722's manual-build learning goal)
- **Power BI** — dashboards consuming the marts schema
- **Python** (numpy, pandas) — synthetic session generation