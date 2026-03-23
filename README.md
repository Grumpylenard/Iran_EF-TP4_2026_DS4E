# Iran War Visualisation

This repo builds a provider-backed infrastructure, incident-verification, and capacity-at-risk workflow for the Gulf / Iran study area.

## Reproducibility

This repo now has two clearly separated paths:

1. **Reproducible local workflow**
   - Uses only local files already on disk plus the deterministic scripts listed as active below.
   - Does **not** require internet access if you use existing local raw data and keep refresh/download flags off.
   - This is the preferred path for maps, verification summaries, and capacity-at-risk analysis.

2. **Optional refresh / scraping workflow**
   - Uses external APIs or websites and can change over time.
   - These scripts are useful for updating the dataset, but they are **not** the guaranteed reproducible baseline.

If exact reproduction matters, use the local workflow only and do not enable:
- `DOWNLOAD_GDELT=TRUE`
- `REFRESH_FIRMS=TRUE`
- any web-scraping script

For incident scraping specifically:
- a **fresh scrape** is reproducible in method and script configuration, but not guaranteed to be bit-for-bit stable because source websites can change, disappear, or block requests
- a **downstream rerun from saved scrape outputs** is reproducible, because `02_build_incidents.R` consumes the saved local files rather than scraping live pages itself

## Provider stack

The infrastructure base is now:

1. `OGIM`
2. `GEM` core trackers:
   - `GOIT`
   - `GGIT`
   - `GOGET`
   - `GChI`
3. `KAPSARC` GCC oil fields, when present under `data_raw/`
4. Optional `OSM` gap-fill only when explicitly enabled

`NETL GOGI` is scaffolded as an optional later provider, but it is not required for the main path.

## Data Input Structure

### Raw Data Sources (`data_raw/`)

The intake pipeline auto-detects provider files from these directories:

**Infrastructure Providers (`data_raw/Geodata/`):**
- `OGIM_v2.5.1.gpkg` - Open Gas Infrastructure Mapping database
- `GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx` - Global Energy Monitor Oil & Gas Infrastructure Tracker pipelines
- `GEM-GGIT-Gas-Pipelines-2025-11.xlsx` - Global Energy Monitor Gas Infrastructure Tracker pipelines  
- `GEM-GGIT-LNG-Teminals-2025-09.xlsx` - Global Energy Monitor LNG terminals
- `Global-Oil-and-Gas-Plant-Tracker-GOGPT-January-2026.xlsx` - Global Oil & Gas Plant Tracker

**Incident Data Sources:**
- `data_raw/osint_tp4/` - Contains OSINT data including:
  - `waves.csv/geojson/json/kml` - Incident wave classifications
  - `iranian_media_reports.json` - Iranian media coverage
  - `international_reactions.json` - International response data
  - `x_post_snippets.json` - Social media references

**Economic Data:**
- `data_raw/UN_comtrade_oil_gas/` - UN Comtrade oil and gas trade flows:
  - `UNdata_Export_*_lightoils_refined.csv` - Refined petroleum exports
  - Multiple trade flow datasets by commodity and country

**Additional Sources:**
- `data_raw/Middle-East_aggregated_data_up_to-2026-03-07.csv` - Regional aggregated data
- `data_raw/web_scraped/` - Web-scraped incident validation data

### Data Processing Pipeline

**Supported formats:** `gpkg`, `geojson`, `json`, `shp`, `gdb`, `csv`, `parquet`, `xlsx`, `xls`

**Core reproducible flow:**
1. **`01_ingest_sources.R`** → Normalizes local provider files into standardized site and line layers
2. **`02_build_incidents.R`** → Builds merged `energy_sites`, links incidents to sites, and writes review/link tables
3. **`03_make_maps.R`** → Compiles `output/map_inputs.gpkg` and renders map products from processed local inputs
4. **`analysis/01_build_site_capacity_base_FINAL.R`** → Builds canonical site-capacity base for capacity-at-risk work
5. **`analysis/02_build_incident_site_capacity_FINAL.R`** → Builds incident-site-capacity fact table, site-day risk table, and country/time summaries
6. **`analysis/03_plot_oil_country_bars_FINAL.R`** → Renders combined country capacity-risk figure
7. **`analysis/05_plot_capacity_risk_timeseries_FINAL.R`** → Renders capacity-at-risk time series

**Optional reproducible local add-ons:**
- **`07_build_trade_lookup.R`** → Combines local UN Comtrade country-level commodity files into processed trade summary tables

**Optional network refresh path:**
- **`05_firms_anomaly.R`** → Refreshes FIRMS data and rebuilds anomaly layers when `REFRESH_FIRMS=TRUE`
- **`01_ingest_sources.R` with `DOWNLOAD_GDELT=TRUE`** → Refreshes GDELT article inputs

**Key Integration Points:**
- Infrastructure providers are merged by fuel type and facility class
- Incidents are spatially linked to nearby infrastructure using the configured site-match radius
- FIRMS anomalies are calculated against pre-conflict and historical baselines
- GDELT articles provide validation evidence for incident reports  
- UN Comtrade data contextualizes economic impacts at the country/commodity level

## Scripts

### Active reproducible scripts

- `01_ingest_sources.R`
  Reads local TP4 / ACLED / provider files and normalizes provider-native site and line layers with typed output-capacity fields.
- `02_build_incidents.R`
  Builds merged `energy_sites` / `energy_lines`, links incidents to sites, and writes review/link tables.
- `03_make_maps.R`
  Compiles `output/map_inputs.gpkg` and renders the main map products from processed local inputs.
- `07_build_trade_lookup.R`
  Combines local UN Comtrade CSVs into processed country-level commodity trade tables.
- `analysis/00_shared_functions_FINAL.R`
  Shared helpers for the stand-alone capacity-at-risk analysis flow.
- `analysis/01_build_site_capacity_base_FINAL.R`
  Builds `analysis_output/site_capacity_base.csv` and site-capacity diagnostics.
- `analysis/02_build_incident_site_capacity_FINAL.R`
  Builds the incident-site-capacity fact table, deduplicated site-day risk table, and country/time summaries.
- `analysis/03_plot_oil_country_bars_FINAL.R`
  Canonical renderer for the combined country capacity-risk figure.
- `analysis/04_plot_gas_country_bars_FINAL.R`
  Compatibility wrapper that regenerates the same combined country figure.
- `analysis/05_plot_capacity_risk_timeseries_FINAL.R`
  Renders the stand-alone capacity-at-risk time-series figure.

### Active but optional refresh scripts

- `05_firms_anomaly.R`
  Refreshes FIRMS conflict, pre-conflict, and historical anomaly inputs when explicitly enabled. Uses network access when refreshing.
- `01_ingest_sources.R` with `DOWNLOAD_GDELT=TRUE`
  Refreshes GDELT inputs. Uses network access and is not part of the strict local-only reproducible path.

### Optional / experimental scripts, not required for the main workflow

- `03a_map_infrastructure_FINAL.R`
  Stand-alone infrastructure map extracted from the main map pipeline.
- `03b_map_validation_FINAL.R`
  Stand-alone validation map extracted from the main map pipeline.
- `03c_map_capacity_at_risk_FINAL.R`
  Capacity-at-risk map prototype / skeleton; not the canonical capacity-at-risk analysis flow.
- `map_utils.R`
  Shared helper file for the extracted stand-alone map scripts.

### Network-dependent research / scraping scripts

- `04_web_scraping_confirmation.R`
- `05_web_scrape_incidents.R`
- `06_scrape_incidents.R`

These are useful research utilities, but they are **not** required for reproducible local reruns and should be treated as optional enrichment tools.

### Incident scraping path

There are two separate scrape-related paths in the repo:

- `05_web_scrape_incidents.R`
  - Scrapes CENTCOM, CTP-ISW, and Defense.gov incident mentions
  - Writes `data_raw/web_scraped/incidents.csv`
  - Supports reproducible reruns of the **same workflow configuration** via explicit flags such as:
    - `SCRAPE_CENTCOM`
    - `SCRAPE_ISW`
    - `SCRAPE_DEFENSE`
    - `GEOCODE_LOCATIONS`
    - `WRITE_OUTPUT`
  - This is a raw acquisition/enrichment step, not the canonical processed incident dataset used by the main pipeline

- `06_scrape_incidents.R`
  - Scrapes and normalizes structured incidents from Wikipedia, iranstrikemap.com, and CENTCOM fact sheets
  - Writes:
    - `data_processed/scraped_incidents_raw.csv`
    - `data_processed/centcom_factsheets_parsed.csv`
  - This is the main scrape-to-processed-data path relevant to the incident pipeline
  - It is controlled by:
    - `SCRAPE_WIKIPEDIA`
    - `SCRAPE_IRANSTRIKEMAP`
    - `SCRAPE_CENTCOM`
    - `WRITE_OUTPUT`

`04_web_scraping_confirmation.R` is an older standalone scraper/prototype and should not be treated as part of the canonical workflow.

### Scraping reproducibility rules

To keep scraping explainable and reproducible:

1. **Preferred reproducible downstream use**
   - Treat `data_processed/scraped_incidents_raw.csv` as the frozen local input once it has been written
   - From that point onward, reruns of `02_build_incidents.R`, `03_make_maps.R`, and the capacity-at-risk analysis are reproducible without re-scraping

2. **Fresh scrape runs**
   - Use explicit flags and keep the same script/version/configuration
   - Record the scrape date and the script used
   - Expect source websites to change over time, which means a fresh scrape is not guaranteed to reproduce the exact same rows later

3. **Canonical recommendation**
   - For a reproducible project run, scrape once, persist the outputs locally, and treat those saved outputs as inputs for all later reruns

### Not part of the workflow

- `testing.R`
  Scratch file only.

## Workflow

### Main reproducible local workflow

1. Put provider files under `data_raw/Geodata/` and incident files under `data_raw/`
2. Run `WRITE_OUTPUT=TRUE Rscript --vanilla 01_ingest_sources.R`
3. Run `WRITE_OUTPUT=TRUE Rscript --vanilla 02_build_incidents.R`
4. Run `WRITE_OUTPUT=TRUE Rscript --vanilla 03_make_maps.R`

### Capacity-at-risk workflow

1. Run the main reproducible local workflow first
2. Run `Rscript --vanilla analysis/01_build_site_capacity_base_FINAL.R`
3. Run `Rscript --vanilla analysis/02_build_incident_site_capacity_FINAL.R`
4. Run `Rscript --vanilla analysis/03_plot_oil_country_bars_FINAL.R`
5. Run `Rscript --vanilla analysis/05_plot_capacity_risk_timeseries_FINAL.R`

### Optional refresh steps

- FIRMS refresh:
  - `REFRESH_FIRMS=TRUE WRITE_OUTPUT=TRUE Rscript --vanilla 05_firms_anomaly.R`
- GDELT refresh:
  - `DOWNLOAD_GDELT=TRUE WRITE_OUTPUT=TRUE Rscript --vanilla 01_ingest_sources.R`
- Structured incident scrape refresh:
  - `SCRAPE_WIKIPEDIA=TRUE SCRAPE_IRANSTRIKEMAP=TRUE SCRAPE_CENTCOM=TRUE WRITE_OUTPUT=TRUE Rscript --vanilla 06_scrape_incidents.R`
- Raw incident web scrape refresh:
  - `SCRAPE_CENTCOM=TRUE SCRAPE_ISW=TRUE SCRAPE_DEFENSE=TRUE GEOCODE_LOCATIONS=TRUE WRITE_OUTPUT=TRUE Rscript --vanilla 05_web_scrape_incidents.R`
- UN Comtrade processing:
  - `WRITE_OUTPUT=TRUE Rscript --vanilla 07_build_trade_lookup.R`

## Important flags

- `WRITE_OUTPUT=TRUE`
  Persist processed outputs and final maps.
- `USE_EE_OGIM=TRUE`
  Try Earth Engine OGIM only when no local OGIM file is present.
- `ENABLE_OSM_GAPFILL=TRUE`
  Allow OSM to enter as an explicit patch layer. Without this flag, OSM is excluded from ingestion.
- `DOWNLOAD_GDELT=TRUE`
  Refresh GDELT article discovery. Network-dependent.
- `DOWNLOAD_FIRMS=TRUE`
  Deprecated. `01_ingest_sources.R` now fails fast and points to `05_firms_anomaly.R`.
- `REFRESH_FIRMS=TRUE`
  Refresh FIRMS conflict, pre-conflict, and historical baseline data in `05_firms_anomaly.R`. Network-dependent.
- `INCIDENT_SITE_MATCH_KM=2`
  Incident-to-site linkage radius in `02_build_incidents.R`. Default is now `2 km`.
- `MAP_MODE=infrastructure`
  Default map. Other modes now include `sites`, `candidate_sites`, `firms_anomaly_grid`, `firms_anomaly_sites`, `validation`, and `validation_capacity_labels`.

## Processed Data Outputs (`data_processed/`)

### Infrastructure Processing
- `provider_intake_log.csv` - Log of all processed provider files and their status
- `ogim_sites_raw.csv` & `ogim_lines_raw.geojson` - Processed OGIM infrastructure
- `ggit_sites_raw.csv` - Processed GEM gas infrastructure sites
- `energy_sites.csv` - **Master sites layer** (merged from all infrastructure providers)
- `energy_lines.geojson` - **Master lines layer** (merged pipelines and transmission)

### Incident Analysis
- `tp4_incidents_raw.csv` - Processed TP4 OSINT incidents
- `scraped_incidents_raw.csv` - Web-scraped incident validation data
- `centcom_factsheets_parsed.csv` - Structured CENTCOM scrape reference table
- `acled_incidents_raw.csv` - ACLED conflict events (when enabled)
- `incidents_review.csv` - Incidents flagged for manual review
- `incidents_confirmed.csv` - **Validated incident dataset**
- `incident_site_links.csv` - **Spatial links** between incidents and infrastructure
- `incident_article_links.csv` - GDELT article-incident linkages

### GDELT Integration
- `gdelt_articles_raw.csv` - Downloaded GDELT news articles
- `gdelt_query_log.csv` - Record of GDELT API queries and results

### FIRMS Anomaly Detection
- `firms_hotspots_raw.csv` - Current FIRMS fire detections
- `firms_hotspots_preconflict_raw.csv` - Pre-conflict baseline fire data
- `firms_hotspots_historical_raw.csv` - Historical fire pattern data
- `firms_grid_daily.csv` - Daily fire detections on 2km grid
- `firms_grid_anomalies.csv` - **Statistical fire anomalies** by grid cell
- `firms_facility_anomalies.csv` - **Facility-level anomaly scores**

### Economic Analysis
- `un_comtrade_country_commodity_flows.csv` - Country-level commodity trade flows
- `un_comtrade_country_commodity_summary.csv` - Country-level trade summaries

### Capacity-at-Risk Analysis (`analysis_output/`)
- `site_capacity_base.csv` - Canonical per-site capacity base used for downstream risk analysis
- `site_capacity_match_diagnostics.csv` - Per-site diagnostics showing matched vs unmatched capacity rows
- `incident_site_capacity.csv` - Incident-site-capacity fact table
- `site_day_capacity_risk.csv` - Deduplicated unique `(site_id, incident_day)` capacity-at-risk table
- `country_capacity_summary_oil.csv` - Oil country summary
- `country_capacity_summary_gas.csv` - Gas country summary
- `daily_capacity_risk_summary.csv` - Daily country/product capacity-at-risk summary
- `fig_capacity_risk_by_country_combined_01.png` - Combined oil/gas country figure
- `fig_capacity_risk_timeseries_02.png` - Capacity-at-risk time series

### Supporting Data
- `data_raw/web_scraped/incidents.csv` - Raw incident scrape output from `05_web_scrape_incidents.R`
- `osm_energy_assets_raw.csv` - OpenStreetMap gap-fill infrastructure (when enabled)
- `osm_sector_tile_log.csv` - OSM data download log

## Final outputs

- `output/map_inputs.gpkg`
- `output/energy_infrastructure_map.pdf`
- optional `output/energy_facilities_map.pdf`
- optional `output/energy_candidate_sites_map.pdf`
- optional `output/energy_firms_anomaly_grid_map.pdf`
- optional `output/energy_firms_anomaly_sites_map.pdf`
- optional `output/incident_validation_map.pdf`
- optional `output/incident_validation_capacity_labels_map.pdf`

Capacity-at-risk figures are written to `analysis_output/`, not `output/`.

## GeoPackage layers

Always expected:

- `energy_sites`
- `energy_lines`
- `incidents`
- `firms_hotspots`
- `firms_grid_anomalies`
- `firms_anomalous_hotspots`
- `firms_facility_anomalies`
- `gdelt_articles`
- `incident_site_links`
- `incident_article_links`
- `site_validation_summary`
- `provider_intake_log`

Provider-native layers are written when their source data exist, for example:

- `ogim_sites`
- `ogim_lines`
- `goit_lines`
- `ggit_lines`
- `goget_sites`
- `gchi_sites`
- `osm_gapfill_sites`

## Notes

- `03_make_maps.R` now reads from `map_inputs.gpkg`, not directly from raw provider files.
- The default render is the infrastructure map, which shows linework and sites together.
- The legend only shows infrastructure classes that are actually present in the compiled package.
- Typed output fields are carried on provider site rows, merged `energy_sites`, and `incident_site_links`; oil/liquid rollups are exposed as `capacity_kbd` and `production_kbd` where valid.
- The reproducible local baseline is defined by the existing local files plus the active reproducible scripts above. Any network refresh or scraping step is optional and should be documented separately when used.
- Incident scraping is reproducible **downstream** once the scraped outputs are saved locally. Fresh live scrapes should be treated as refresh steps, not as guaranteed stable inputs.
