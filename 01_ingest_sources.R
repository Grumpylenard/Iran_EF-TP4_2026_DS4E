source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(httr2)
  library(jsonlite)
  library(readr)
  library(sf)
  library(stringr)
})

# Manual controls. Keep WRITE_OUTPUT=FALSE until you want files written.
use_local_tp4 <- env_flag("USE_LOCAL_TP4", TRUE)
download_gdelt <- env_flag("DOWNLOAD_GDELT", FALSE)
download_firms <- env_flag("DOWNLOAD_FIRMS", FALSE)
write_output <- env_flag("WRITE_OUTPUT", TRUE)
use_ee_ogim <- env_flag("USE_EE_OGIM", FALSE)
enable_osm_gapfill <- env_flag("ENABLE_OSM_GAPFILL", FALSE)
preview_n <- 5

settings <- project_settings()
provider_registry <- build_provider_registry()
asset_registry <- build_asset_class_registry()

paths <- list(
  provider_roots = settings$provider_roots,
  tp4_json = project_path("data_raw", "osint_tp4", "waves.json"),
  tp4_csv = project_path("data_processed", "tp4_incidents_raw.csv"),
  acled_raw_csv = project_path("data_raw", "Middle-East_aggregated_data_up_to-2026-03-07.csv"),
  acled_csv = project_path("data_processed", "acled_incidents_raw.csv"),
  gdelt_csv = project_path("data_processed", "gdelt_articles_raw.csv"),
  gdelt_log = project_path("data_processed", "gdelt_query_log.csv"),
  provider_log_csv = project_path("data_processed", "provider_intake_log.csv"),
  legacy_osm_csv = project_path("data_processed", "osm_energy_assets_raw.csv")
)

study_bbox_sf <- sf::st_as_sfc(sf::st_bbox(c(
  xmin = settings$bbox$west,
  ymin = settings$bbox$south,
  xmax = settings$bbox$east,
  ymax = settings$bbox$north
), crs = 4326))

study_bbox_wkt <- sf::st_as_text(study_bbox_sf)

clip_sites_to_bbox <- function(sites) {
  if (nrow(sites) == 0) return(sites)

  sites %>%
    filter(
      is.finite(lat),
      is.finite(lon),
      lat >= settings$bbox$south,
      lat <= settings$bbox$north,
      lon >= settings$bbox$west,
      lon <= settings$bbox$east
    )
}

clip_lines_to_bbox <- function(lines) {
  if (nrow(lines) == 0) return(lines)

  hits <- lengths(sf::st_intersects(lines, study_bbox_sf)) > 0
  lines[hits, ]
}

clip_normalized_results <- function(result) {
  result$sites <- clip_sites_to_bbox(result$sites)
  result$lines <- clip_lines_to_bbox(result$lines)
  result
}

flatten_tp4_incidents <- function(path) {
  raw <- jsonlite::fromJSON(path, flatten = TRUE)
  incidents <- tibble::as_tibble(raw$incidents)

  incidents %>%
    transmute(
      incident_id = coalesce(uuid, paste0("tp4_", sequence)),
      source_system = "tp4_osint_repo",
      operation = operation,
      sequence = sequence,
      wave_number = wave_number,
      announced_utc = timing.announced_utc,
      probable_launch_time = timing.probable_launch_time,
      incident_day = as.Date(substr(coalesce(timing.announced_utc, timing.probable_launch_time), 1, 10)),
      target_text = targets.targets,
      landing_countries = collapse_list_column(targets.landings_countries),
      target_country_guess = mapply(
        function(a, b) extract_country_mentions(paste(a %||% "", b %||% "")),
        targets.targets,
        collapse_list_column(targets.landings_countries),
        USE.NAMES = FALSE
      ),
      weapon_payload = weapons.payload,
      ballistic_flag = coalesce(weapons.ballistic_missiles_used, FALSE),
      drone_flag = coalesce(weapons.drones_used, FALSE),
      cruise_flag = coalesce(weapons.cruise_missiles_used, FALSE),
      cluster_flag = vapply(
        weapons.cluster_warhead,
        function(x) isTRUE(x) || any(unlist(x) == TRUE, na.rm = TRUE),
        logical(1)
      ),
      target_energy_flag = coalesce(targets.critical_infrastructure.targeted_energy, FALSE),
      target_port_flag = coalesce(targets.critical_infrastructure.targeted_port, FALSE),
      target_industrial_flag = coalesce(targets.critical_infrastructure.targeted_industrial, FALSE),
      target_military_flag = coalesce(targets.critical_infrastructure.targeted_military_base, FALSE),
      target_lat = targets.target_coordinates.lat,
      target_lon = targets.target_coordinates.lon,
      source_urls = collapse_list_column(sources),
      description = description
    )
}

flatten_acled_incidents <- function(path) {
  if (!file.exists(path)) {
    return(tibble())
  }

  readr::read_csv(path, show_col_types = FALSE) %>%
    transmute(
      incident_day = as.Date(WEEK, format = "%d-%B-%Y"),
      source_admin_id = as.character(ID),
      source_country = str_squish(as.character(COUNTRY)),
      source_admin1 = str_squish(as.character(ADMIN1)),
      event_type = as.character(EVENT_TYPE),
      sub_event_type = as.character(SUB_EVENT_TYPE),
      events = as.integer(EVENTS),
      fatalities = as.integer(FATALITIES),
      population_exposure = as.character(POPULATION_EXPOSURE),
      target_lat = as.numeric(CENTROID_LATITUDE),
      target_lon = as.numeric(CENTROID_LONGITUDE)
    ) %>%
    filter(
      !is.na(incident_day),
      incident_day >= settings$study_start,
      incident_day <= settings$study_end,
      sub_event_type == "Air/drone strike",
      !is.na(target_lat),
      !is.na(target_lon)
    ) %>%
    mutate(
      incident_id = paste0("acled_", source_admin_id, "_", format(incident_day, "%Y%m%d")),
      source_system = "acled_aggregated_csv",
      operation = "ACLED",
      sequence = NA_integer_,
      wave_number = NA_integer_,
      announced_utc = as.character(incident_day),
      probable_launch_time = NA_character_,
      target_text = str_trim(paste(source_admin1, source_country, "ACLED air/drone strike aggregate", sep = ", ")),
      landing_countries = source_country,
      target_country_guess = source_country,
      weapon_payload = sub_event_type,
      ballistic_flag = FALSE,
      drone_flag = TRUE,
      cruise_flag = FALSE,
      cluster_flag = FALSE,
      target_energy_flag = FALSE,
      target_port_flag = FALSE,
      target_industrial_flag = FALSE,
      target_military_flag = FALSE,
      source_urls = NA_character_,
      description = paste(
        "ACLED weekly aggregate:",
        dplyr::coalesce(as.character(events), "NA"), "events,",
        dplyr::coalesce(as.character(fatalities), "NA"), "fatalities,",
        "admin1 =", source_admin1,
        ifelse(!is.na(population_exposure) & nzchar(population_exposure), paste0(", population exposure = ", population_exposure), "")
      )
    ) %>%
    select(
      incident_id,
      source_system,
      operation,
      sequence,
      wave_number,
      announced_utc,
      probable_launch_time,
      incident_day,
      target_text,
      landing_countries,
      target_country_guess,
      weapon_payload,
      ballistic_flag,
      drone_flag,
      cruise_flag,
      cluster_flag,
      target_energy_flag,
      target_port_flag,
      target_industrial_flag,
      target_military_flag,
      target_lat,
      target_lon,
      source_urls,
      description
    ) %>%
    distinct()
}

fetch_gdelt_articles <- function(query_id, query, window_start, window_end) {
  req <- httr2::request("https://api.gdeltproject.org/api/v2/doc/doc") |>
    httr2::req_url_query(
      query = query,
      mode = "ArtList",
      format = "json",
      maxrecords = 250,
      startdatetime = to_gdelt_datetime(window_start, end_of_day = FALSE),
      enddatetime = to_gdelt_datetime(window_end, end_of_day = TRUE)
    ) |>
    httr2::req_timeout(90)

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp, simplifyVector = TRUE)

  articles <- tibble::as_tibble(body$articles %||% tibble())
  if (nrow(articles) == 0) {
    return(list(
      articles = tibble(),
      log = tibble(query_id = query_id, window_start = window_start, window_end = window_end, article_count = 0)
    ))
  }

  list(
    articles = articles %>%
      transmute(
        query_id = query_id,
        query_window_start = as.character(window_start),
        query_window_end = as.character(window_end),
        seendate = seendate %||% NA_character_,
        title = title %||% NA_character_,
        url = url %||% NA_character_,
        domain = domain %||% NA_character_,
        language = language %||% NA_character_,
        sourcecountry = sourcecountry %||% NA_character_,
        socialimage = socialimage %||% NA_character_
      ),
    log = tibble(query_id = query_id, window_start = window_start, window_end = window_end, article_count = nrow(articles))
  )
}

empty_provider_sites <- function() {
  tibble(
    provider = character(),
    source_dataset = character(),
    source_file = character(),
    source_layer = character(),
    source_id = character(),
    asset_class = character(),
    asset_label = character(),
    provider_class = character(),
    subclass = character(),
    name = character(),
    status = character(),
    country = character(),
    commodity_group = character(),
    liquid_capacity_bpd = double(),
    liquid_throughput_bpd = double(),
    gas_capacity_mmcfd = double(),
    gas_throughput_mmcfd = double(),
    lng_capacity_mtpa = double(),
    lng_capacity_bcmy = double(),
    oil_production_bpd = double(),
    capacity_kbd = double(),
    production_kbd = double(),
    primary_output_kind = character(),
    primary_output_value = double(),
    primary_output_unit = character(),
    primary_output_basis = character(),
    geometry_role = character(),
    provider_priority = integer(),
    lat = double(),
    lon = double()
  )
}

empty_provider_lines <- function() {
  sf::st_sf(
    empty_provider_sites() %>% select(-lat, -lon),
    geometry = sf::st_sfc(crs = 4326)
  )
}

empty_provider_intake_log <- function() {
  tibble(
    source_dataset = character(),
    provider = character(),
    source_file = character(),
    source_layer = character(),
    status = character(),
    site_count = integer(),
    line_count = integer(),
    note = character()
  )
}

find_provider_files <- function(spec, provider_roots) {
  search_dirs <- unique(unlist(lapply(provider_roots, function(root) {
    c(file.path(root, spec$input_subdir), root)
  })))
  search_dirs <- search_dirs[dir.exists(search_dirs)]

  if (length(search_dirs) == 0) return(character())

  files <- unique(unlist(lapply(search_dirs, function(dir_path) {
    list.files(dir_path, recursive = TRUE, full.names = TRUE)
  })))
  files <- files[file.info(files)$isdir %in% FALSE]
  files[str_detect(basename(files), spec$file_regex)]
}

read_provider_file <- function(path, bbox_wkt = NA_character_) {
  ext <- str_to_lower(tools::file_ext(path))

  if (ext %in% c("gpkg", "gdb")) {
    layers <- tryCatch(sf::st_layers(path)$name, error = function(e) character())
    if (length(layers) == 0) {
      data <- tryCatch(
        sf::st_read(path, quiet = TRUE, wkt_filter = bbox_wkt),
        error = function(e) tryCatch(sf::st_read(path, quiet = TRUE), error = function(e2) NULL)
      )
      if (is.null(data)) return(list())
      return(list(list(layer = NA_character_, data = data)))
    }

    parts <- lapply(layers, function(layer_name) {
      data <- tryCatch(
        sf::st_read(path, layer = layer_name, quiet = TRUE, wkt_filter = bbox_wkt),
        error = function(e) tryCatch(sf::st_read(path, layer = layer_name, quiet = TRUE), error = function(e2) NULL)
      )
      if (is.null(data)) return(NULL)
      list(layer = layer_name, data = data)
    })

    return(Filter(Negate(is.null), parts))
  }

  if (ext %in% c("geojson", "json", "shp")) {
    data <- tryCatch(
      sf::st_read(path, quiet = TRUE, wkt_filter = bbox_wkt),
      error = function(e) sf::st_read(path, quiet = TRUE)
    )
    return(list(list(layer = NA_character_, data = data)))
  }

  if (ext == "csv") {
    return(list(list(layer = NA_character_, data = readr::read_csv(path, show_col_types = FALSE))))
  }

  if (ext == "parquet") {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Reading parquet provider files requires the arrow package.")
    }
    return(list(list(layer = NA_character_, data = arrow::read_parquet(path) %>% as.data.frame())))
  }

  if (ext %in% c("xlsx", "xls")) {
    if (!requireNamespace("readxl", quietly = TRUE)) {
      stop("Reading Excel provider files requires the readxl package.")
    }

    sheets <- readxl::excel_sheets(path)
    data_sheets <- sheets[!str_detect(str_to_lower(sheets), "about|data dictionary|dictionary|acronym|copyright|column key")]
    if (length(data_sheets) == 0) data_sheets <- sheets

    return(lapply(data_sheets, function(sheet_name) {
      list(layer = sheet_name, data = readxl::read_excel(path, sheet = sheet_name))
    }))
  }

  stop("Unsupported provider file type: ", path)
}

extract_site_points <- function(sf_data) {
  points <- sf::st_point_on_surface(sf::st_zm(sf::st_transform(sf_data, 4326)))
  coords <- sf::st_coordinates(points)
  list(
    data = sf::st_drop_geometry(points),
    lon = coords[, 1],
    lat = coords[, 2]
  )
}

normalize_output_unit <- function(x) {
  normalized <- normalize_key_text(x)

  dplyr::case_when(
    normalized %in% c("bpd", "barrels_per_day", "barrel_per_day", "bopd") ~ "bpd",
    normalized %in% c("kbd", "kbpd", "kb_d", "kbbl_d", "thousand_barrels_per_day") ~ "kbd",
    normalized %in% c("mmcfd", "mmscfd", "million_cubic_feet_per_day", "mmcf_d") ~ "mmcfd",
    normalized %in% c("mtpa", "million_tonnes_per_annum", "million_tons_per_annum", "mta") ~ "mtpa",
    normalized %in% c("bcmy", "bcm_y", "bcm_per_year", "bcm_year", "bcmy") ~ "bcmy",
    normalized %in% c("bcfd", "bcf_d", "billion_cubic_feet_per_day") ~ "bcfd",
    TRUE ~ ifelse(normalized == "missing", NA_character_, normalized)
  )
}

collapse_duplicate_metric <- function(x) {
  values <- as.numeric(x)
  values <- values[!is.na(values)]

  if (length(values) == 0) {
    return(NA_real_)
  }

  distinct_values <- unique(values)
  if (length(distinct_values) == 1) {
    return(distinct_values[[1]])
  }

  sum(values)
}

sanitize_output_metric <- function(x) {
  ifelse(!is.na(x) & x <= 0, NA_real_, x)
}

extract_provider_output_fields <- function(data) {
  generic_capacity_value <- extract_numeric_field(
    data,
    c("^capacity$", "^capacity_value$", "^design_capacity$", "^nameplate_capacity$", "^output_capacity$")
  )
  generic_capacity_unit <- normalize_output_unit(
    extract_text_field(data, c("^capacity_units?$", "^capacity_unit$", "^capacityunits?$"))
  )

  generic_throughput_value <- extract_numeric_field(
    data,
    c("^throughput$", "^throughput_value$")
  )
  generic_throughput_unit <- normalize_output_unit(
    extract_text_field(data, c("^throughput_units?$", "^throughput_unit$"))
  )

  generic_production_value <- extract_numeric_field(
    data,
    c("^production$", "^production_value$", "^prod$", "^prod_value$")
  )
  generic_production_unit <- normalize_output_unit(
    extract_text_field(data, c("^production_units?$", "^production_unit$", "^prod_units?$", "^prod_unit$"))
  )

  generic_output_value <- extract_numeric_field(
    data,
    c("^output$", "^output_value$")
  )
  generic_output_unit <- normalize_output_unit(
    extract_text_field(data, c("^output_units?$", "^output_unit$"))
  )

  liquid_capacity_bpd <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c("^liq_capacity_bpd$", "^liquid_capacity_bpd$", "^liquid_capacity_bopd$", "^capacity_bpd$", "^capacitybpd$", "^oil_capacity_bpd$", "^crude_capacity_bpd$")
    ),
    extract_numeric_field(
      data,
      c("^capacity_kbd$", "^capacity_kbpd$", "^capacitykbpd$", "^liquid_capacity_kbd$", "^oil_capacity_kbd$", "^crude_capacity_kbd$")
    ) * 1000,
    ifelse(generic_capacity_unit == "bpd", generic_capacity_value, NA_real_),
    ifelse(generic_capacity_unit == "kbd", generic_capacity_value * 1000, NA_real_)
  )

  liquid_throughput_bpd <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c("^liq_throughput_bpd$", "^liquid_throughput_bpd$", "^throughput_bpd$", "^throughputbpd$", "^liquid_throughput_bopd$")
    ),
    extract_numeric_field(
      data,
      c("^throughput_kbd$", "^throughput_kbpd$", "^throughputkbpd$", "^liquid_throughput_kbd$")
    ) * 1000,
    ifelse(generic_throughput_unit == "bpd", generic_throughput_value, NA_real_),
    ifelse(generic_throughput_unit == "kbd", generic_throughput_value * 1000, NA_real_)
  )

  gas_capacity_mmcfd <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c("^gas_capacity_mmcfd$", "^gascapacity_mmcfd$", "^capacity_mmcfd$", "^capacitymmcfd$")
    ),
    ifelse(generic_capacity_unit == "mmcfd", generic_capacity_value, NA_real_)
  )

  gas_throughput_mmcfd <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c("^gas_throughput_mmcfd$", "^gasthroughput_mmcfd$", "^throughput_mmcfd$", "^throughputmmcfd$")
    ),
    ifelse(generic_throughput_unit == "mmcfd", generic_throughput_value, NA_real_)
  )

  lng_capacity_mtpa <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c(
        "^tot_import_lng_terminal_capacityin_mtpa$",
        "^tot_export_lng_terminal_capacityin_mtpa$",
        "^tot_lng_terminal_capacityin_mtpa$",
        "^capacityin_mtpa$",
        "^lng_capacity_mtpa$",
        "^capacity_mtpa$"
      )
    ),
    ifelse(generic_capacity_unit == "mtpa", generic_capacity_value, NA_real_)
  )

  lng_capacity_bcmy <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c(
        "^tot_import_lng_terminal_capacityin_bcm_y$",
        "^tot_export_lng_terminal_capacityin_bcm_y$",
        "^tot_lng_terminal_capacityin_bcm_y$",
        "^capacityin_bcm_y$",
        "^lng_capacity_bcm_y$",
        "^capacity_bcm_y$",
        "^capacitybcmy$"
      )
    ),
    ifelse(generic_capacity_unit == "bcmy", generic_capacity_value, NA_real_)
  )

  oil_production_bpd <- dplyr::coalesce(
    extract_numeric_field(
      data,
      c(
        "^oil_production_bpd$",
        "^production_oil_bpd$",
        "^prod_oil_bpd$",
        "^field_production_bpd$",
        "^crude_production_bpd$",
        "^production_bpd$",
        "^prod_bpd$"
      )
    ),
    extract_numeric_field(
      data,
      c("^production_kbd$", "^production_kbpd$", "^prod_kbd$", "^prod_kbpd$", "^output_kbd$", "^output_kbpd$")
    ) * 1000,
    ifelse(generic_production_unit == "bpd", generic_production_value, NA_real_),
    ifelse(generic_production_unit == "kbd", generic_production_value * 1000, NA_real_),
    ifelse(generic_output_unit == "bpd", generic_output_value, NA_real_),
    ifelse(generic_output_unit == "kbd", generic_output_value * 1000, NA_real_)
  )

  liquid_capacity_bpd <- sanitize_output_metric(liquid_capacity_bpd)
  liquid_throughput_bpd <- sanitize_output_metric(liquid_throughput_bpd)
  gas_capacity_mmcfd <- sanitize_output_metric(gas_capacity_mmcfd)
  gas_throughput_mmcfd <- sanitize_output_metric(gas_throughput_mmcfd)
  lng_capacity_mtpa <- sanitize_output_metric(lng_capacity_mtpa)
  lng_capacity_bcmy <- sanitize_output_metric(lng_capacity_bcmy)
  oil_production_bpd <- sanitize_output_metric(oil_production_bpd)

  tibble(
    liquid_capacity_bpd = liquid_capacity_bpd,
    liquid_throughput_bpd = liquid_throughput_bpd,
    gas_capacity_mmcfd = gas_capacity_mmcfd,
    gas_throughput_mmcfd = gas_throughput_mmcfd,
    lng_capacity_mtpa = lng_capacity_mtpa,
    lng_capacity_bcmy = lng_capacity_bcmy,
    oil_production_bpd = oil_production_bpd,
    capacity_kbd = liquid_capacity_bpd / 1000,
    production_kbd = oil_production_bpd / 1000
  )
}

build_provider_base_table <- function(data, source_dataset, source_layer = NA_character_) {
  output_fields <- extract_provider_output_fields(data)

  tibble(
    source_id = infer_source_id(data, source_dataset),
    provider_class = dplyr::coalesce(
      extract_text_field(data, c("^category$", "^asset_type$", "^type$", "^class$", "^fac_type$", "facility_type", "project_type", "sector", "subsector", "^fuel$", "commodity", "query_asset_group", "route_type")),
      rep(source_layer %||% NA_character_, nrow(data))
    ),
    subclass = extract_text_field(data, c("^subclass$", "^subtype$", "subcategory", "subsector")),
    name = dplyr::coalesce(
      extract_text_field(data, c("^name$", "^fac_name$", "facility_name", "asset_name", "project_name", "terminal_name", "unit_name", "field_name", "refinery_name", "pipeline_name", "segment_name")),
      extract_text_field(data, c("operator", "company", "owner"))
    ),
    status = extract_text_field(data, c("^status$", "^fac_status$", "^ogim_status$", "project_status", "operating_status", "phase", "stage", "fid_status", "shelved_cancelled_status_type")),
    country = extract_text_field(data, c("^country$", "^country_area$", "^countries$", "^countries_or_areas$", "country_name", "admin0", "nation", "start_country_or_area", "end_country_or_area", "start_country", "end_country"))
  ) %>%
    bind_cols(output_fields)
}

derive_commodity_group <- function(asset_class, provider_class, subclass, name, site_metrics) {
  text <- str_to_lower(paste(asset_class, provider_class, subclass, name, sep = " "))

  has_oil_metric <- !is.na(site_metrics$liquid_capacity_bpd) |
    !is.na(site_metrics$liquid_throughput_bpd) |
    !is.na(site_metrics$oil_production_bpd)
  has_gas_metric <- !is.na(site_metrics$gas_capacity_mmcfd) |
    !is.na(site_metrics$gas_throughput_mmcfd) |
    !is.na(site_metrics$lng_capacity_mtpa) |
    !is.na(site_metrics$lng_capacity_bcmy)

  oil_text <- str_detect(text, "\\b(oil|crude|petroleum|refin|gasoline|diesel|condensate|asphalt|biofuel)\\b")
  gas_text <- str_detect(text, "natural gas|\\bgas\\b|lng|lpg|compress")

  oil_hint <- asset_class %in% c("refinery", "petroleum_terminal", "tank_battery")
  gas_hint <- asset_class %in% c("lng_facility", "compressor_station")

  dplyr::case_when(
    (has_oil_metric | oil_text | oil_hint) & (has_gas_metric | gas_text | gas_hint) ~ "mixed",
    has_gas_metric | gas_text | gas_hint ~ "gas",
    has_oil_metric | oil_text | oil_hint ~ "oil",
    TRUE ~ NA_character_
  )
}

finalize_provider_sites <- function(site_rows) {
  if (nrow(site_rows) == 0) {
    return(site_rows)
  }

  site_rows %>%
    mutate(
      commodity_group = derive_commodity_group(
        asset_class = asset_class,
        provider_class = provider_class,
        subclass = subclass,
        name = name,
        site_metrics = pick(
          liquid_capacity_bpd,
          liquid_throughput_bpd,
          gas_capacity_mmcfd,
          gas_throughput_mmcfd,
          lng_capacity_mtpa,
          lng_capacity_bcmy,
          oil_production_bpd
        )
      )
    ) %>%
    mutate(
      primary_output_kind = dplyr::case_when(
        asset_class %in% c("extraction_site", "offshore_platform") & !is.na(oil_production_bpd) ~ "oil_production",
        asset_class %in% c("extraction_site", "offshore_platform") & !is.na(liquid_capacity_bpd) ~ "liquid_capacity",
        asset_class %in% c("extraction_site", "offshore_platform") & !is.na(gas_capacity_mmcfd) ~ "gas_capacity",
        asset_class == "lng_facility" & !is.na(lng_capacity_mtpa) ~ "lng_capacity_mtpa",
        asset_class == "lng_facility" & !is.na(lng_capacity_bcmy) ~ "lng_capacity_bcmy",
        asset_class == "lng_facility" & !is.na(gas_capacity_mmcfd) ~ "gas_capacity",
        asset_class %in% c("refinery", "petroleum_terminal", "tank_battery", "processing_facility") & !is.na(liquid_capacity_bpd) ~ "liquid_capacity",
        asset_class %in% c("refinery", "petroleum_terminal", "tank_battery", "processing_facility") & !is.na(liquid_throughput_bpd) ~ "liquid_throughput",
        asset_class == "compressor_station" & !is.na(gas_capacity_mmcfd) ~ "gas_capacity",
        asset_class == "compressor_station" & !is.na(gas_throughput_mmcfd) ~ "gas_throughput",
        !is.na(liquid_capacity_bpd) ~ "liquid_capacity",
        !is.na(liquid_throughput_bpd) ~ "liquid_throughput",
        !is.na(oil_production_bpd) ~ "oil_production",
        !is.na(lng_capacity_mtpa) ~ "lng_capacity_mtpa",
        !is.na(lng_capacity_bcmy) ~ "lng_capacity_bcmy",
        !is.na(gas_capacity_mmcfd) ~ "gas_capacity",
        !is.na(gas_throughput_mmcfd) ~ "gas_throughput",
        TRUE ~ NA_character_
      ),
      primary_output_value = dplyr::case_when(
        primary_output_kind == "oil_production" ~ oil_production_bpd,
        primary_output_kind == "liquid_capacity" ~ liquid_capacity_bpd,
        primary_output_kind == "liquid_throughput" ~ liquid_throughput_bpd,
        primary_output_kind == "gas_capacity" ~ gas_capacity_mmcfd,
        primary_output_kind == "gas_throughput" ~ gas_throughput_mmcfd,
        primary_output_kind == "lng_capacity_mtpa" ~ lng_capacity_mtpa,
        primary_output_kind == "lng_capacity_bcmy" ~ lng_capacity_bcmy,
        TRUE ~ NA_real_
      ),
      primary_output_unit = dplyr::case_when(
        primary_output_kind %in% c("oil_production", "liquid_capacity", "liquid_throughput") ~ "bpd",
        primary_output_kind %in% c("gas_capacity", "gas_throughput") ~ "mmcfd",
        primary_output_kind == "lng_capacity_mtpa" ~ "mtpa",
        primary_output_kind == "lng_capacity_bcmy" ~ "bcmy",
        TRUE ~ NA_character_
      ),
      primary_output_basis = dplyr::case_when(
        primary_output_kind == "oil_production" ~ "production",
        str_detect(primary_output_kind %||% "", "throughput") ~ "throughput",
        !is.na(primary_output_kind) ~ "capacity",
        TRUE ~ NA_character_
      )
    )
}

collapse_provider_site_rows <- function(site_rows, source_dataset, source_layer = NA_character_) {
  if (nrow(site_rows) == 0) {
    return(site_rows)
  }

  is_ggit_lng <- identical(source_dataset, "ggit") &&
    str_detect(str_to_lower(source_layer %||% ""), "^lng terminals$")

  if (!is_ggit_lng) {
    return(site_rows)
  }

  site_rows %>%
    group_by(source_id) %>%
    summarise(
      provider = first_non_missing_chr(provider),
      source_dataset = first_non_missing_chr(source_dataset),
      source_file = first_non_missing_chr(source_file),
      source_layer = first_non_missing_chr(source_layer),
      asset_class = first_non_missing_chr(asset_class),
      asset_label = first_non_missing_chr(asset_label),
      provider_class = first_non_missing_chr(provider_class),
      subclass = first_non_missing_chr(subclass),
      name = first_non_missing_chr(name),
      status = first_non_missing_chr(status),
      country = first_non_missing_chr(country),
      commodity_group = first_non_missing_chr(commodity_group),
      liquid_capacity_bpd = collapse_duplicate_metric(liquid_capacity_bpd),
      liquid_throughput_bpd = collapse_duplicate_metric(liquid_throughput_bpd),
      gas_capacity_mmcfd = collapse_duplicate_metric(gas_capacity_mmcfd),
      gas_throughput_mmcfd = collapse_duplicate_metric(gas_throughput_mmcfd),
      lng_capacity_mtpa = collapse_duplicate_metric(lng_capacity_mtpa),
      lng_capacity_bcmy = collapse_duplicate_metric(lng_capacity_bcmy),
      oil_production_bpd = collapse_duplicate_metric(oil_production_bpd),
      capacity_kbd = collapse_duplicate_metric(capacity_kbd),
      production_kbd = collapse_duplicate_metric(production_kbd),
      primary_output_kind = first_non_missing_chr(primary_output_kind),
      primary_output_value = collapse_duplicate_metric(primary_output_value),
      primary_output_unit = first_non_missing_chr(primary_output_unit),
      primary_output_basis = first_non_missing_chr(primary_output_basis),
      geometry_role = first_non_missing_chr(geometry_role),
      provider_priority = as.integer(first_non_missing_num(provider_priority)),
      lat = first_non_missing_num(lat),
      lon = first_non_missing_num(lon),
      .groups = "drop"
    )
}

normalize_provider_frame <- function(data, source_dataset, provider, provider_priority, source_file, source_layer = NA_character_) {
  if (nrow(data) == 0) {
    return(list(sites = empty_provider_sites(), lines = empty_provider_lines(), note = "empty"))
  }

  if (inherits(data, "sf")) {
    x <- sf::st_transform(data, 4326)
    geom_types <- as.character(sf::st_geometry_type(x, by_geometry = TRUE))
    line_idx <- geom_types %in% c("LINESTRING", "MULTILINESTRING")
    site_idx <- !line_idx

    attr_tbl <- sf::st_drop_geometry(x)
    base_tbl <- build_provider_base_table(attr_tbl, source_dataset, source_layer)

    site_rows <- empty_provider_sites()
    if (any(site_idx)) {
      site_points <- extract_site_points(x[site_idx, ])
      site_tbl <- base_tbl[site_idx, ] %>%
        transmute(
          provider = provider,
          source_dataset = source_dataset,
          source_file = source_file,
          source_layer = source_layer,
          source_id = source_id,
          asset_class = classify_asset_class(paste(provider_class, subclass, name, source_layer), source_dataset, "site"),
          provider_class = provider_class,
          subclass = subclass,
          name = dplyr::coalesce(name, provider_class, source_id),
          status = status,
          country = country,
          commodity_group = NA_character_,
          liquid_capacity_bpd = liquid_capacity_bpd,
          liquid_throughput_bpd = liquid_throughput_bpd,
          gas_capacity_mmcfd = gas_capacity_mmcfd,
          gas_throughput_mmcfd = gas_throughput_mmcfd,
          lng_capacity_mtpa = lng_capacity_mtpa,
          lng_capacity_bcmy = lng_capacity_bcmy,
          oil_production_bpd = oil_production_bpd,
          capacity_kbd = capacity_kbd,
          production_kbd = production_kbd,
          primary_output_kind = NA_character_,
          primary_output_value = NA_real_,
          primary_output_unit = NA_character_,
          primary_output_basis = NA_character_,
          geometry_role = "site",
          provider_priority = provider_priority,
          lat = site_points$lat,
          lon = site_points$lon
        ) %>%
        left_join(asset_registry %>% select(asset_class, asset_label), by = "asset_class") %>%
        relocate(asset_label, .after = asset_class) %>%
        finalize_provider_sites() %>%
        collapse_provider_site_rows(source_dataset = source_dataset, source_layer = source_layer)
      site_rows <- bind_rows(empty_provider_sites(), site_tbl)
    }

    line_rows <- empty_provider_lines()
    if (any(line_idx)) {
      line_tbl <- base_tbl[line_idx, ] %>%
        transmute(
          provider = provider,
          source_dataset = source_dataset,
          source_file = source_file,
          source_layer = source_layer,
          source_id = source_id,
          asset_class = classify_asset_class(paste(provider_class, subclass, name, source_layer), source_dataset, "line"),
          provider_class = provider_class,
          subclass = subclass,
          name = dplyr::coalesce(name, provider_class, source_id),
          status = status,
          country = country,
          geometry_role = "line",
          provider_priority = provider_priority
        ) %>%
        left_join(asset_registry %>% select(asset_class, asset_label), by = "asset_class") %>%
        relocate(asset_label, .after = asset_class)

      line_rows <- sf::st_sf(line_tbl, geometry = sf::st_geometry(x[line_idx, ]), crs = 4326)
    }

    return(list(sites = site_rows, lines = line_rows, note = "success"))
  }

  wkt_col <- match_column_name(data, c("^wkt$", "^geom$", "^geometry$", "shape_wkt"))
  if (!is.na(wkt_col) && any(str_detect(str_to_upper(data[[wkt_col]] %||% ""), "POINT|LINESTRING|POLYGON"), na.rm = TRUE)) {
    sf_data <- sf::st_as_sf(data, wkt = wkt_col, crs = 4326)
    return(normalize_provider_frame(sf_data, source_dataset, provider, provider_priority, source_file, source_layer))
  }

  lat <- extract_numeric_field(data, c("^lat$", "latitude", "y_coord", "ycenter"))
  lon <- extract_numeric_field(data, c("^lon$", "^lng$", "longitude", "x_coord", "xcenter"))
  has_coords <- is.finite(lat) & is.finite(lon)

  if (!any(has_coords)) {
    return(list(sites = empty_provider_sites(), lines = empty_provider_lines(), note = "no_geometry"))
  }

  base_tbl <- build_provider_base_table(data, source_dataset, source_layer) %>%
    mutate(
      lat = lat,
      lon = lon
    )

  site_rows <- base_tbl %>%
    filter(has_coords) %>%
    transmute(
      provider = provider,
      source_dataset = source_dataset,
      source_file = source_file,
      source_layer = source_layer,
      source_id = source_id,
      asset_class = classify_asset_class(paste(provider_class, subclass, name, source_layer), source_dataset, "site"),
      provider_class = provider_class,
      subclass = subclass,
      name = dplyr::coalesce(name, provider_class, source_id),
      status = status,
      country = country,
      commodity_group = NA_character_,
      liquid_capacity_bpd = liquid_capacity_bpd,
      liquid_throughput_bpd = liquid_throughput_bpd,
      gas_capacity_mmcfd = gas_capacity_mmcfd,
      gas_throughput_mmcfd = gas_throughput_mmcfd,
      lng_capacity_mtpa = lng_capacity_mtpa,
      lng_capacity_bcmy = lng_capacity_bcmy,
      oil_production_bpd = oil_production_bpd,
      capacity_kbd = capacity_kbd,
      production_kbd = production_kbd,
      primary_output_kind = NA_character_,
      primary_output_value = NA_real_,
      primary_output_unit = NA_character_,
      primary_output_basis = NA_character_,
      geometry_role = "site",
      provider_priority = provider_priority,
      lat = lat,
      lon = lon
    ) %>%
    left_join(asset_registry %>% select(asset_class, asset_label), by = "asset_class") %>%
    relocate(asset_label, .after = asset_class) %>%
    finalize_provider_sites() %>%
    collapse_provider_site_rows(source_dataset = source_dataset, source_layer = source_layer)

  list(sites = bind_rows(empty_provider_sites(), site_rows), lines = empty_provider_lines(), note = "success")
}

fetch_ogim_earth_engine <- function(settings) {
  if (!requireNamespace("rgee", quietly = TRUE)) {
    stop("USE_EE_OGIM=TRUE requires the rgee package.")
  }

  bbox <- sf::st_as_sfc(sf::st_bbox(c(
    xmin = settings$bbox$west,
    ymin = settings$bbox$south,
    xmax = settings$bbox$east,
    ymax = settings$bbox$north
  ), crs = 4326))

  rgee::ee_Initialize()
  aoi_sf <- sf::st_sf(id = 1, geometry = bbox)
  aoi_ee <- rgee::sf_as_ee(aoi_sf)
  ogim <- rgee::ee$FeatureCollection(settings$ogim_earth_engine_collection)$filterBounds(aoi_ee)
  rgee::ee_as_sf(ogim)
}

normalize_spec_sources <- function(spec, paths, use_ee_ogim = FALSE, enable_osm_gapfill = FALSE) {
  files <- find_provider_files(spec, paths$provider_roots)

  if (spec$source_dataset == "osm_gapfill" && enable_osm_gapfill && file.exists(paths$legacy_osm_csv)) {
    files <- unique(c(files, paths$legacy_osm_csv))
  }

  site_results <- list()
  line_results <- list()
  log_results <- list()

  if (length(files) == 0 && spec$source_dataset == "ogim" && use_ee_ogim) {
    ee_data <- fetch_ogim_earth_engine(settings)
    normalized <- normalize_provider_frame(
      ee_data,
      source_dataset = spec$source_dataset,
      provider = spec$provider,
      provider_priority = spec$provider_priority,
      source_file = "earth_engine://EDF/OGIM/current",
      source_layer = NA_character_
    )

    site_results[[1]] <- normalized$sites
    line_results[[1]] <- normalized$lines
    log_results[[1]] <- tibble(
      source_dataset = spec$source_dataset,
      provider = spec$provider,
      source_file = "earth_engine://EDF/OGIM/current",
      source_layer = NA_character_,
      status = normalized$note,
      site_count = nrow(normalized$sites),
      line_count = nrow(normalized$lines),
      note = "earth_engine"
    )
  } else if (length(files) == 0) {
    log_results[[1]] <- tibble(
      source_dataset = spec$source_dataset,
      provider = spec$provider,
      source_file = NA_character_,
      source_layer = NA_character_,
      status = "missing",
      site_count = 0L,
      line_count = 0L,
      note = "no files discovered"
    )
  } else {
    for (file_path in files) {
      source_parts <- read_provider_file(file_path, bbox_wkt = study_bbox_wkt)
      for (part in source_parts) {
        normalized <- normalize_provider_frame(
          part$data,
          source_dataset = spec$source_dataset,
          provider = spec$provider,
          provider_priority = spec$provider_priority,
          source_file = file_path,
          source_layer = part$layer %||% NA_character_
        )
        normalized <- clip_normalized_results(normalized)

        site_results[[length(site_results) + 1L]] <- normalized$sites
        line_results[[length(line_results) + 1L]] <- normalized$lines
        log_results[[length(log_results) + 1L]] <- tibble(
          source_dataset = spec$source_dataset,
          provider = spec$provider,
          source_file = file_path,
          source_layer = part$layer %||% NA_character_,
          status = normalized$note,
          site_count = nrow(normalized$sites),
          line_count = nrow(normalized$lines),
          note = NA_character_
        )
      }
    }
  }

  list(
    sites = bind_rows(c(list(empty_provider_sites()), site_results)),
    lines = do.call(rbind, c(list(empty_provider_lines()), line_results)),
    log = bind_rows(c(list(empty_provider_intake_log()), log_results))
  )
}

write_provider_outputs <- function(source_dataset, sites, lines) {
  output_paths <- provider_output_paths(source_dataset)

  if (nrow(sites) > 0) {
    write_csv_safe(sites, output_paths$site_csv)
  } else if (file.exists(output_paths$site_csv)) {
    unlink(output_paths$site_csv)
  }

  if (nrow(lines) > 0) {
    write_sf_safe(lines, output_paths$line_geojson)
  } else if (file.exists(output_paths$line_geojson)) {
    unlink(output_paths$line_geojson)
  }
}

tp4_incidents <- tibble()
acled_incidents <- tibble()
gdelt_articles <- read_csv_if_exists(paths$gdelt_csv)
gdelt_log <- read_csv_if_exists(paths$gdelt_log)
provider_log <- empty_provider_intake_log()

if (download_firms) {
  stop("FIRMS download has moved to 05_firms_anomaly.R. Run REFRESH_FIRMS=TRUE Rscript --vanilla 05_firms_anomaly.R instead.")
}

if (use_local_tp4) {
  tp4_incidents <- flatten_tp4_incidents(paths$tp4_json)
  message("TP4 incidents loaded: ", nrow(tp4_incidents))
  print(utils::head(tp4_incidents, preview_n))
}

acled_incidents <- flatten_acled_incidents(paths$acled_raw_csv)
if (nrow(acled_incidents) > 0) {
  message("ACLED incidents loaded: ", nrow(acled_incidents))
  print(utils::head(acled_incidents, preview_n))
}

if (download_gdelt) {
  windows <- date_windows(settings$study_start, settings$study_end, 7)
  gdelt_results <- lapply(seq_len(nrow(settings$gdelt_queries)), function(i) {
    q <- settings$gdelt_queries[i, ]
    lapply(seq_len(nrow(windows)), function(j) {
      fetch_gdelt_articles(q$query_id, q$query, windows$window_start[[j]], windows$window_end[[j]])
    })
  })
  gdelt_articles <- bind_rows(lapply(gdelt_results, function(x) bind_rows(lapply(x, `[[`, "articles"))))
  gdelt_log <- bind_rows(lapply(gdelt_results, function(x) bind_rows(lapply(x, `[[`, "log"))))
  message("GDELT articles loaded: ", nrow(gdelt_articles))
}

provider_specs <- provider_registry

provider_results <- lapply(seq_len(nrow(provider_specs)), function(i) {
  spec <- provider_specs[i, ]
  normalize_spec_sources(
    spec = spec,
    paths = paths,
    use_ee_ogim = use_ee_ogim,
    enable_osm_gapfill = enable_osm_gapfill
  )
})

names(provider_results) <- provider_specs$source_dataset
provider_log <- bind_rows(lapply(provider_results, `[[`, "log"))

total_sites <- sum(vapply(provider_results, function(x) nrow(x$sites), integer(1)))
total_lines <- sum(vapply(provider_results, function(x) nrow(x$lines), integer(1)))

if (write_output) {
  write_csv_safe(tp4_incidents, paths$tp4_csv)
  write_csv_safe(acled_incidents, paths$acled_csv)
  write_csv_safe(gdelt_articles, paths$gdelt_csv)
  write_csv_safe(gdelt_log, paths$gdelt_log)
  write_csv_safe(provider_log, paths$provider_log_csv)

  for (dataset_id in names(provider_results)) {
    write_provider_outputs(
      dataset_id,
      provider_results[[dataset_id]]$sites,
      provider_results[[dataset_id]]$lines
    )
  }
}

message("Provider sites loaded: ", total_sites)
message("Provider lines loaded: ", total_lines)

if (total_sites + total_lines == 0) {
  stop(
    "No OGIM/GEM provider files were found under data_raw/providers/, and no usable OGIM Earth Engine or OSM gap-fill input was enabled."
  )
}

site_frames <- Filter(
  function(x) !is.null(x) && all(c("source_dataset", "asset_label") %in% names(x)),
  lapply(provider_results, `[[`, "sites")
)
site_summary <- if (length(site_frames) == 0) {
  tibble(source_dataset = character(), asset_label = character(), n = integer())
} else {
  bind_rows(site_frames) %>%
    count(source_dataset, asset_label, sort = TRUE)
}
if (nrow(site_summary) > 0) {
  print(site_summary)
}

line_frames <- Filter(
  function(x) !is.null(x) && all(c("source_dataset", "asset_label") %in% names(x)),
  lapply(provider_results, function(x) {
    if (nrow(x$lines) == 0) return(tibble())
    sf::st_drop_geometry(x$lines)
  })
)
line_summary <- if (length(line_frames) == 0) {
  tibble(source_dataset = character(), asset_label = character(), n = integer())
} else {
  bind_rows(line_frames) %>%
    count(source_dataset, asset_label, sort = TRUE)
}
if (nrow(line_summary) > 0) {
  print(line_summary)
}

if (write_output) {
  message("Ingestion outputs written to data_processed/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save provider-normalized outputs.")
}
