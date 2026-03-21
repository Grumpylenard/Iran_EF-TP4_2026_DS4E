# Iran War Visualisation

This repo builds a provider-backed infrastructure and incident map package for the Gulf / Iran study area.

## Provider stack

The infrastructure base is now:

1. `OGIM`
2. `GEM` core trackers:
   - `GOIT`
   - `GGIT`
   - `GOGET`
   - `GChI`
3. Optional `OSM` gap-fill only when explicitly enabled

`NETL GOGI` is scaffolded as an optional later provider, but it is not required for the main path.

## Input layout

The intake now auto-detects provider files from either of these roots:

- `data_raw/providers/`
- `data_raw/Geodata/`

Provider-specific subfolders are still supported under `data_raw/providers/`, but a flat drop-folder under `data_raw/Geodata/` also works.

Typical files:

- `OGIM_v1.1.gpkg`
- `GOIT.*`
- `GGIT.*`
- `GOGET.*`
- `GChI.*`

Supported local formats are `gpkg`, `geojson`, `json`, `shp`, `gdb`, `csv`, `parquet`, `xlsx`, and `xls`.

## Scripts

- `01_ingest_sources.R`
  Reads local TP4 OSINT data, optionally downloads GDELT and FIRMS, and normalizes OGIM / GEM / optional OSM provider files into provider-native site and line layers.
- `02_build_incidents.R`
  Builds merged `energy_sites` and `energy_lines`, then links incidents to the merged site layer plus FIRMS and GDELT evidence.
- `03_make_maps.R`
  Compiles `output/map_inputs.gpkg` and renders maps from the compiled package.

## Workflow

1. Put provider files under `data_raw/providers/` or `data_raw/Geodata/`
2. Run `Rscript 01_ingest_sources.R`
3. Run `Rscript 02_build_incidents.R`
4. Run `Rscript 03_make_maps.R`

## Important flags

- `WRITE_OUTPUT=TRUE`
  Persist processed outputs and final maps.
- `USE_EE_OGIM=TRUE`
  Try Earth Engine OGIM only when no local OGIM file is present.
- `ENABLE_OSM_GAPFILL=TRUE`
  Allow OSM to enter as an explicit patch layer. Without this flag, OSM is excluded from ingestion.
- `DOWNLOAD_GDELT=TRUE`
  Refresh GDELT article discovery.
- `DOWNLOAD_FIRMS=TRUE`
  Refresh FIRMS hotspot data.
- `MAP_MODE=infrastructure`
  Default map. Other modes are `sites` and `validation`.

## Processed outputs

- `data_processed/provider_intake_log.csv`
- `data_processed/<provider>_sites_raw.csv`
- `data_processed/<provider>_lines_raw.geojson`
- `data_processed/energy_sites.csv`
- `data_processed/energy_lines.geojson`
- `data_processed/incidents_review.csv`
- `data_processed/incidents_confirmed.csv`
- `data_processed/incident_site_links.csv`
- `data_processed/incident_firms_links.csv`
- `data_processed/incident_article_links.csv`

## Final outputs

- `output/map_inputs.gpkg`
- `output/energy_infrastructure_map.pdf`
- optional `output/energy_facilities_map.pdf`
- optional `output/incident_validation_map.pdf`

## GeoPackage layers

Always expected:

- `energy_sites`
- `energy_lines`
- `incidents`
- `firms_hotspots`
- `gdelt_articles`
- `incident_site_links`
- `incident_firms_links`
- `incident_article_links`
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
