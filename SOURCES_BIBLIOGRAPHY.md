# Iran War Visualization Project - Complete Source Bibliography

## Primary Infrastructure Data Sources

### Oil & Gas Infrastructure Mapping (OGIM)
Oil and Gas Infrastructure Mapping. (2025). *OGIM v2.5.1 Database*. Available at: https://ogim.org/ [Dataset accessed March 2026].

### Global Energy Monitor (GEM)
Global Energy Monitor. (2025). *Global Oil Infrastructure Tracker (GOIT): Oil & NGL Pipelines 2025-03*. Available at: https://globalenergymonitor.org/projects/global-oil-infrastructure-tracker/ [Dataset accessed March 2026].

Global Energy Monitor. (2025). *Global Gas Infrastructure Tracker (GGIT): Gas Pipelines 2025-11*. Available at: https://globalenergymonitor.org/projects/global-gas-infrastructure-tracker/ [Dataset accessed March 2026].

Global Energy Monitor. (2025). *Global Gas Infrastructure Tracker (GGIT): LNG Terminals 2025-09*. Available at: https://globalenergymonitor.org/projects/global-gas-infrastructure-tracker/ [Dataset accessed March 2026].

Global Energy Monitor. (2026). *Global Oil and Gas Plant Tracker (GOGPT) January 2026*. Available at: https://globalenergymonitor.org/projects/global-oil-gas-plant-tracker/ [Dataset accessed March 2026].

## News & Media Intelligence Sources

### GDELT Project
Leetaru, K. and Schrodt, P.A. (2013). *GDELT: Global Data on Events, Location, and Tone, 1979–2012*. ISA Annual Convention 2013, San Francisco, CA. Available at: https://api.gdeltproject.org/api/v2/doc/doc [API accessed March 2026].

### Web-Scraped News Sources
U.S. Central Command. (2026). *Press Releases*. Available at: https://www.centcom.mil/MEDIA/PRESS-RELEASES/ [Web-scraped March 2026].

Critical Threats Project & Institute for the Study of War. (2026). *Iran Updates*. Available at: https://www.criticalthreats.org [Web-scraped March 2026].

Wikipedia Contributors. (2026). *Operation True Promise 4 Timeline Articles*. Available at: https://en.wikipedia.org [Web-scraped March 2026].

iranstrikemap.com. (2026). *Strike Incident Database*. Available at: https://iranstrikemap.com [Web-scraped March 2026].

## Satellite & Remote Sensing Data

### FIRMS (Fire Information for Resource Management System)
NASA. (2026). *FIRMS: Fire Information for Resource Management System*. Available at: https://firms.modaps.eosdis.nasa.gov/api/ [API key: a8199de2124bd9a01c91965155fd45fe, accessed March 2026].

### OpenStreetMap (Gapfill Infrastructure)
OpenStreetMap Contributors. (2026). *OpenStreetMap*. Available at: https://www.openstreetmap.org [Overpass API accessed March 2026].

## Economic & Trade Data

### United Nations Comtrade
United Nations. (2026). *UN Comtrade Database: Oil and Gas Trade Flows*. Available at: https://comtrade.un.org/ [Dataset accessed March 23, 2026].
- UNdata Export: Light Oils Refined Petroleum Products
- Bilateral Trade Flow Data by Country and Commodity

## Conflict & Incident Data

### ACLED (Armed Conflict Location & Event Data Project)
Raleigh, C., Linke, A., Hegre, H. and Karlsen, J. (2010). Introducing ACLED: An Armed Conflict Location and Event Data Project. *Journal of Peace Research*, 47(5), pp.651-660.
- Middle East Aggregated Data up to March 7, 2026

### Operation True Promise 4 (TP4) OSINT Collection
Primary OSINT data collection covering 19 attack waves, including:
- **Iranian Media Reports**: Social media posts, official statements, and Persian-language coverage
- **International Reactions**: Diplomatic responses and international media coverage  
- **Social Media Intelligence**: X (Twitter) posts and snippets in multiple languages
- **Incident Wave Classifications**: Structured incident data with geospatial coordinates

Search methodology documented in: `data_raw/osint_tp4/search_queries.md`

## Geospatial & Technical Infrastructure

### R Programming Language & Libraries
R Core Team. (2024). *R: A Language and Environment for Statistical Computing*. R Foundation for Statistical Computing, Vienna, Austria. Available at: https://www.R-project.org/

Key R packages used:
- **sf**: Pebesma, E. (2018). Simple Features for R: Standardized Support for Spatial Vector Data. *The R Journal*, 10(1), pp.439-446.
- **dplyr**: Wickham, H., François, R., Henry, L. and Müller, K. (2023). *dplyr: A Grammar of Data Manipulation*. R package.
- **httr2**: Wickham, H. (2023). *httr2: Perform HTTP Requests and Process the Responses*. R package.
- **rvest**: Wickham, H. (2022). *rvest: Easily Harvest (Scrape) Web Pages*. R package.
- **tidygeocoder**: Cambon, J., Hernangómez, D. and Belanger, C. (2021). *tidygeocoder: An R package for geocoding*. Journal of Open Source Software, 6(65), 3544.

### LaTeX & Document Preparation
Lamport, L. (1994). *LaTeX: A Document Preparation System*. 2nd ed. Reading, MA: Addison-Wesley.

TinyTeX Distribution: Xie, Y. (2019). TinyTeX: A lightweight, cross-platform, and easy-to-maintain LaTeX distribution based on TeX Live. *TUGboat*, 40(1), pp.30-32.

### Geographic Coordinate Reference Systems
- **WGS84 (EPSG:4326)**: World Geodetic System 1984
- **Asia South Albers (EPSG:6933)**: Used for 2km grid calculations and spatial analysis

## Project Methodology References

### Incident-Infrastructure Spatial Linking
Spatial proximity analysis using 2km buffer zones for linking conflict incidents to energy infrastructure, based on:
- Buff distance methodology following standard GIS practices for point-to-point proximity analysis
- Grid-based anomaly detection using 2km × 2km cells for fire hotspot analysis

### Fire Anomaly Detection
Statistical baseline comparison methodology:
- Pre-conflict baseline: 90 days prior to conflict start
- Historical reference: 2024-2025 seasonal patterns
- Z-score threshold: 3 standard deviations above historical mean
- Minimum anomaly threshold: 4+ hotspots per facility buffer zone

## Web Scraping Ethics & Rate Limiting
All web scraping conducted with appropriate delays (1.0 second minimum between requests) and respectful of robots.txt policies where available. User-Agent strings identify academic research purpose.

---

**Data Collection Period**: February 28, 2026 - March 23, 2026  
**Spatial Scope**: Persian Gulf region (25.5°N-30.5°N, 48.0°E-57.0°E)  
**Temporal Scope**: Conflict period with pre-conflict baselines and historical comparisons

**Note**: This bibliography represents all primary and secondary data sources integrated into the analytical pipeline. Web-scraped sources accessed during March 2026 may not be fully reproducible due to dynamic content changes.