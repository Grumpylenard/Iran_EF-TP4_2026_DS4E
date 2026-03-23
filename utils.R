suppressPackageStartupMessages({
  library(dplyr)
  library(jsonlite)
  library(readr)
  library(stringr)
})

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || (length(x) == 1 && is.na(x))) y else x
}

project_path <- function(...) {
  file.path(getwd(), ...)
}

ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

write_csv_safe <- function(data, path) {
  ensure_dir(dirname(path))
  readr::write_csv(data, path, na = "")
  invisible(path)
}

write_json_safe <- function(data, path) {
  ensure_dir(dirname(path))
  jsonlite::write_json(data, path, pretty = TRUE, auto_unbox = TRUE, null = "null")
  invisible(path)
}

write_sf_safe <- function(data, path) {
  ensure_dir(dirname(path))
  if (file.exists(path)) unlink(path)
  sf::st_write(data, path, delete_dsn = TRUE, quiet = TRUE)
  invisible(path)
}

read_csv_if_exists <- function(path) {
  if (file.exists(path)) {
    readr::read_csv(path, show_col_types = FALSE)
  } else {
    tibble()
  }
}

read_sf_if_exists <- function(path, layer = NULL) {
  if (!file.exists(path)) return(NULL)
  if (is.null(layer)) {
    sf::st_read(path, quiet = TRUE)
  } else {
    sf::st_read(path, layer = layer, quiet = TRUE)
  }
}

env_flag <- function(name, default = FALSE) {
  value <- Sys.getenv(name, unset = "")
  if (value == "") return(default)
  tolower(value) %in% c("true", "1", "yes", "y")
}

env_csv <- function(name, default = character()) {
  value <- Sys.getenv(name, unset = "")
  if (value == "") return(default)

  values <- trimws(unlist(strsplit(value, ",")))
  values[values != ""]
}

collapse_nested <- function(x, sep = " | ") {
  values <- unlist(x, recursive = TRUE, use.names = FALSE)
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) {
    NA_character_
  } else {
    paste(unique(as.character(values)), collapse = sep)
  }
}

collapse_list_column <- function(x, sep = " | ") {
  vapply(x, collapse_nested, character(1), sep = sep)
}

collapse_values <- function(x, sep = " | ") {
  values <- unique(as.character(x[!is.na(x)]))
  values <- values[values != ""]
  if (length(values) == 0) NA_character_ else paste(values, collapse = sep)
}

first_non_missing_chr <- function(x) {
  values <- as.character(x)
  values <- values[!is.na(values) & values != ""]
  if (length(values) == 0) NA_character_ else values[[1]]
}

first_non_missing_num <- function(x) {
  values <- as.numeric(x)
  values <- values[!is.na(values)]
  if (length(values) == 0) NA_real_ else values[[1]]
}

coerce_numeric_vector <- function(x) {
  if (is.numeric(x)) {
    return(as.numeric(x))
  }

  suppressWarnings(
    readr::parse_number(
      as.character(x),
      locale = readr::locale(grouping_mark = ",")
    )
  )
}

to_gdelt_datetime <- function(x, end_of_day = FALSE) {
  stamp <- if (isTRUE(end_of_day)) "235959" else "000000"
  paste0(format(as.Date(x), "%Y%m%d"), stamp)
}

date_windows <- function(start_date, end_date, window_days = 7) {
  starts <- seq(as.Date(start_date), as.Date(end_date), by = sprintf("%s days", window_days))
  tibble(
    window_start = starts,
    window_end = pmin(starts + (window_days - 1), as.Date(end_date))
  )
}

last_day_of_month <- function(year, month) {
  next_month_year <- ifelse(month == 12, year + 1L, year)
  next_month <- ifelse(month == 12, 1L, month + 1L)
  as.integer(format(as.Date(sprintf("%04d-%02d-01", next_month_year, next_month)) - 1, "%d"))
}

shift_date_to_year <- function(x, year) {
  dates <- as.Date(x)
  years <- rep(as.integer(year), length.out = length(dates))
  months <- as.integer(format(dates, "%m"))
  days <- as.integer(format(dates, "%d"))
  max_days <- mapply(last_day_of_month, years, months)

  as.Date(sprintf(
    "%04d-%02d-%02d",
    years,
    months,
    pmin(days, max_days)
  ))
}

normalize_key_text <- function(x) {
  value <- as.character(x %||% "")
  value <- str_replace_all(value, "([a-z0-9])([A-Z])", "\\1_\\2")
  value <- str_to_lower(value)
  value <- str_replace(value, "^https?://", "")
  value <- str_replace_all(value, "[^a-z0-9]+", "_")
  value <- str_replace_all(value, "^_+|_+$", "")
  value <- str_sub(value, 1, 120)
  ifelse(value == "", "missing", value)
}

make_article_key <- function(query_id, seendate, url, title) {
  base_value <- ifelse(!is.na(url) & url != "", url, paste(query_id, seendate, title, sep = "_"))
  paste0("gdelt_", vapply(base_value, normalize_key_text, character(1)))
}

make_firms_hotspot_id <- function(acq_date, latitude, longitude) {
  date_value <- ifelse(is.na(acq_date) | acq_date == "", "unknown", gsub("-", "", as.character(acq_date)))
  lat_value <- ifelse(is.na(latitude), "na", formatC(as.numeric(latitude), format = "f", digits = 4))
  lon_value <- ifelse(is.na(longitude), "na", formatC(as.numeric(longitude), format = "f", digits = 4))
  paste0("firms_", date_value, "_", lat_value, "_", lon_value)
}

attach_firms_grid <- function(data, lon_col = "longitude", lat_col = "latitude", grid_km = 2, grid_crs = 6933) {
  grid_columns <- tibble::tibble(
    cell_id = character(),
    cell_x = integer(),
    cell_y = integer(),
    xmin_m = double(),
    ymin_m = double(),
    xmax_m = double(),
    ymax_m = double(),
    cell_center_lon = double(),
    cell_center_lat = double()
  )

  if (nrow(data) == 0) {
    return(dplyr::bind_cols(tibble::as_tibble(data), grid_columns))
  }

  pts <- sf::st_as_sf(tibble::as_tibble(data), coords = c(lon_col, lat_col), crs = 4326, remove = FALSE)
  xy <- sf::st_coordinates(sf::st_transform(pts, grid_crs))
  grid_size_m <- as.numeric(grid_km) * 1000

  cell_x <- floor(xy[, 1] / grid_size_m)
  cell_y <- floor(xy[, 2] / grid_size_m)
  xmin_m <- cell_x * grid_size_m
  ymin_m <- cell_y * grid_size_m
  xmax_m <- xmin_m + grid_size_m
  ymax_m <- ymin_m + grid_size_m

  center_pts <- sf::st_as_sf(
    tibble::tibble(
      x = xmin_m + (grid_size_m / 2),
      y = ymin_m + (grid_size_m / 2)
    ),
    coords = c("x", "y"),
    crs = grid_crs
  )
  center_xy <- sf::st_coordinates(sf::st_transform(center_pts, 4326))

  dplyr::bind_cols(
    tibble::as_tibble(data),
    tibble::tibble(
      cell_id = sprintf("cell_%sm_%s_%s", as.integer(grid_size_m), cell_x, cell_y),
      cell_x = as.integer(cell_x),
      cell_y = as.integer(cell_y),
      xmin_m = as.numeric(xmin_m),
      ymin_m = as.numeric(ymin_m),
      xmax_m = as.numeric(xmax_m),
      ymax_m = as.numeric(ymax_m),
      cell_center_lon = as.numeric(center_xy[, 1]),
      cell_center_lat = as.numeric(center_xy[, 2])
    )
  )
}

firms_grid_cells_as_sf <- function(data, grid_crs = 6933, target_crs = 4326) {
  if (nrow(data) == 0) {
    return(sf::st_sf(tibble::as_tibble(data), geometry = sf::st_sfc(crs = target_crs)))
  }

  polygons <- lapply(seq_len(nrow(data)), function(i) {
    coords <- matrix(
      c(
        data$xmin_m[[i]], data$ymin_m[[i]],
        data$xmax_m[[i]], data$ymin_m[[i]],
        data$xmax_m[[i]], data$ymax_m[[i]],
        data$xmin_m[[i]], data$ymax_m[[i]],
        data$xmin_m[[i]], data$ymin_m[[i]]
      ),
      ncol = 2,
      byrow = TRUE
    )
    sf::st_polygon(list(coords))
  })

  sf::st_sf(
    tibble::as_tibble(data),
    geometry = sf::st_sfc(polygons, crs = grid_crs)
  ) %>%
    sf::st_transform(target_crs)
}

build_asset_class_registry <- function() {
  tibble::tribble(
    ~asset_class, ~asset_label, ~asset_rank, ~map_color, ~geometry_role_default,
    "pipeline", "Pipeline / Linear Infrastructure", 1, "#6a51a3", "line",
    "refinery", "Refinery", 2, "#8c510a", "site",
    "petrochemical", "Petrochemical", 3, "#bf812d", "site",
    "lng_facility", "LNG / LPG Facility", 4, "#35978f", "site",
    "petroleum_terminal", "Petroleum Terminal", 5, "#80cdc1", "site",
    "processing_facility", "Processing Facility", 6, "#01665e", "site",
    "compressor_station", "Compressor Station", 7, "#1b7837", "site",
    "extraction_site", "Extraction Site", 8, "#b2182b", "site",
    "offshore_platform", "Offshore Platform", 9, "#2b8cbe", "site",
    "tank_battery", "Tank Battery", 10, "#dfc27d", "site",
    "station_other", "Station / Other", 11, "#636363", "site",
    "other_energy_site", "Other Energy Site", 12, "#969696", "site"
  )
}

build_provider_registry <- function() {
  tibble::tribble(
    ~source_dataset, ~provider, ~provider_priority, ~input_subdir, ~file_regex, ~default_asset_class, ~default_geometry_role,
    "ogim", "OGIM", 1L, "ogim", "(?i)ogim.*\\.(gpkg|geojson|json|shp|gdb)$", NA_character_, NA_character_,
    "goit", "GEM", 2L, "gem", "(?i)goit.*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", "pipeline", NA_character_,
    "ggit", "GEM", 2L, "gem", "(?i)ggit.*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", "pipeline", NA_character_,
    "goget", "GEM", 2L, "gem", "(?i)goget.*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", "extraction_site", "site",
    "gchi", "GEM", 2L, "gem", "(?i)(gchi|global[_-]?chem|chemicals).*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", "petrochemical", "site",
    "kapsarc", "KAPSARC", 2L, "kapsarc", "(?i)(kapsarc|gcc.*oil.*field|oil.*field).*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", "extraction_site", "site",
    "gogi", "NETL", 3L, "gogi", "(?i)gogi.*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", NA_character_, NA_character_,
    "osm_gapfill", "OSM", 4L, "osm", "(?i)osm.*\\.(gpkg|geojson|json|shp|gdb|csv|parquet|xlsx|xls)$", NA_character_, "site"
  )
}

provider_output_paths <- function(source_dataset) {
  list(
    site_csv = project_path("data_processed", paste0(source_dataset, "_sites_raw.csv")),
    line_geojson = project_path("data_processed", paste0(source_dataset, "_lines_raw.geojson"))
  )
}

normalize_names_vector <- function(x) {
  normalize_key_text(x)
}

match_column_name <- function(data, patterns) {
  if (ncol(data) == 0) return(NA_character_)

  normalized <- normalize_names_vector(names(data))
  for (pattern in patterns) {
    idx <- which(str_detect(normalized, pattern))
    if (length(idx) > 0) return(names(data)[idx[[1]]])
  }

  NA_character_
}

extract_text_field <- function(data, patterns, default = NA_character_) {
  col <- match_column_name(data, patterns)
  if (is.na(col)) return(rep(default, nrow(data)))
  values <- as.character(data[[col]])
  values[values == ""] <- default
  values
}

extract_numeric_field <- function(data, patterns, default = NA_real_) {
  col <- match_column_name(data, patterns)
  if (is.na(col)) return(rep(default, nrow(data)))
  coerce_numeric_vector(data[[col]])
}

infer_source_id <- function(data, source_dataset) {
  candidate <- extract_text_field(
    data,
    c(
      "^source_id$", "^asset_id$", "^project_id$", "^unit_id$", "^facility_id$", "^field_id$", "^ogim_id$",
      "^fac_id$", "^id$", "^objectid$", "^globalid$", "^gid$", "uuid"
    )
  )

  missing_idx <- is.na(candidate) | candidate == ""
  candidate[missing_idx] <- paste0(source_dataset, "_", seq_len(nrow(data))[missing_idx])
  candidate
}

classify_asset_class <- function(provider_text, source_dataset, geometry_role = "site") {
  text <- str_to_lower(provider_text %||% "")
  source_dataset <- rep(source_dataset, length.out = length(text))
  geometry_role <- rep(geometry_role, length.out = length(text))

  defaults <- dplyr::case_when(
    source_dataset %in% c("goit", "ggit") & geometry_role == "line" ~ "pipeline",
    source_dataset == "goget" ~ "extraction_site",
    source_dataset == "kapsarc" ~ "extraction_site",
    source_dataset == "gchi" ~ "petrochemical",
    TRUE ~ NA_character_
  )

  matched <- dplyr::case_when(
    str_detect(text, "refiner") ~ "refinery",
    str_detect(text, "petrochem|chemical") ~ "petrochemical",
    str_detect(text, "lng|lpg") ~ "lng_facility",
    str_detect(text, "terminal|depot|storage terminal|export terminal") ~ "petroleum_terminal",
    str_detect(text, "gather|process|processing") ~ "processing_facility",
    str_detect(text, "compress") ~ "compressor_station",
    str_detect(text, "well|field|extract|production|drill") ~ "extraction_site",
    str_detect(text, "offshore|platform|rig") ~ "offshore_platform",
    str_detect(text, "tank") ~ "tank_battery",
    geometry_role == "line" | str_detect(text, "pipeline|transmission line|trunkline") ~ "pipeline",
    str_detect(text, "station") ~ "station_other",
    TRUE ~ defaults
  )

  dplyr::coalesce(matched, ifelse(geometry_role == "line", "pipeline", "other_energy_site"))
}

project_settings <- function() {
  list(
    study_start = as.Date("2026-01-01"),
    study_end = Sys.Date(),
    bbox = list(west = 24, south = 12, east = 63, north = 38),
    provider_roots = c(
      project_path("data_raw", "providers"),
      project_path("data_raw", "Geodata")
    ),
    provider_root = project_path("data_raw", "providers"),
    ogim_earth_engine_collection = "EDF/OGIM/current",
    gdelt_queries = tibble::tribble(
      ~query_id, ~query,
      "tp4_core", "\"True Promise 4\" OR \"Operation True Promise 4\" OR \"وعده صادق ۴\"",
      "regional_strikes", "(Iran OR Iranian) AND (missile OR ballistic OR drone) AND (strike OR attack) AND (Israel OR Bahrain OR Iraq OR Kuwait OR Oman OR Qatar OR Saudi OR UAE OR \"United Arab Emirates\" OR Jordan OR Cyprus OR Turkey)",
      "energy_targeting", "(Iran OR Iranian) AND (missile OR drone OR strike OR attack) AND (oil OR gas OR refinery OR terminal OR pipeline OR port OR tanker OR lng OR lpg OR petrochemical OR petroleum)"
    ),
    target_countries = c(
      "Bahrain", "Cyprus", "Iran", "Iraq", "Israel", "Jordan",
      "Kuwait", "Oman", "Qatar", "Saudi Arabia", "Turkey", "United Arab Emirates"
    ),
    country_patterns = c(
      "Bahrain" = "bahrain|manama",
      "Cyprus" = "cyprus|akrotiri",
      "Iran" = "iran|iranian",
      "Iraq" = "iraq|erbil|duhok|harir",
      "Israel" = "israel|tel aviv|haifa|eilat|jerusalem|nevatim|tel nof",
      "Jordan" = "jordan",
      "Kuwait" = "kuwait|ali al-salem|abdullah al-mubarak|mohammed al-ahmad",
      "Oman" = "oman|duqm",
      "Qatar" = "qatar",
      "Saudi Arabia" = "saudi|aramco|prince sultan|riyadh|jeddah|king khalid",
      "Turkey" = "turkey",
      "United Arab Emirates" = "united arab emirates|uae|abu dhabi|dubai|jebel ali|etihad"
    ),
    energy_patterns = c(
      "energy", "oil", "gas", "refinery", "terminal", "pipeline",
      "petrochem", "petroleum", "tanker", "port", "aramco",
      "lng", "lpg", "storage tank", "depot", "export"
    ),
    firms_conflict_start = as.Date(Sys.getenv("FIRMS_CONFLICT_START", unset = "2026-02-28")),
    firms_reference_years = as.integer(env_csv("FIRMS_REFERENCE_YEARS", c("2024", "2025"))),
    firms_grid_km = as.numeric(Sys.getenv("FIRMS_GRID_KM", unset = "2")),
    firms_anomaly_z = as.numeric(Sys.getenv("FIRMS_ANOMALY_Z", unset = "3")),
    firms_anomaly_lift = as.numeric(Sys.getenv("FIRMS_ANOMALY_LIFT", unset = "3")),
    firms_anomaly_min_hotspots = as.numeric(Sys.getenv("FIRMS_ANOMALY_MIN_HOTSPOTS", unset = "4")),
    firms_preconflict_days = as.integer(Sys.getenv("FIRMS_PRECONFLICT_DAYS", unset = "30")),
    firms_seasonal_window_days = as.integer(Sys.getenv("FIRMS_SEASONAL_WINDOW_DAYS", unset = "3")),
    firms_sensor = Sys.getenv("FIRMS_SENSOR", unset = "VIIRS_SNPP_NRT"),
    firms_chunk_days = 10,
    firms_match_days = 2,
    firms_match_km = 15,
    firms_site_match_km = as.numeric(Sys.getenv("FIRMS_SITE_MATCH_KM", unset = "5")),
    incident_site_match_km = as.numeric(Sys.getenv("INCIDENT_SITE_MATCH_KM", unset = "2")),
    overpass_endpoints = env_csv(
      "OVERPASS_ENDPOINTS",
      c("https://overpass-api.de/api/interpreter", "https://lz4.overpass-api.de/api/interpreter")
    ),
    osm_request_timeout_seconds = 180
  )
}

extract_country_mentions <- function(text, settings = project_settings()) {
  text <- str_to_lower(text %||% "")
  matches <- names(settings$country_patterns)[
    vapply(settings$country_patterns, function(pattern) str_detect(text, pattern), logical(1))
  ]
  if (length(matches) == 0) NA_character_ else paste(matches, collapse = " | ")
}

flag_energy_text <- function(text, settings = project_settings()) {
  str_detect(str_to_lower(text %||% ""), paste(settings$energy_patterns, collapse = "|"))
}

haversine_km <- function(lat1, lon1, lat2, lon2) {
  to_rad <- pi / 180
  dlat <- (lat2 - lat1) * to_rad
  dlon <- (lon2 - lon1) * to_rad
  a <- sin(dlat / 2)^2 + cos(lat1 * to_rad) * cos(lat2 * to_rad) * sin(dlon / 2)^2
  6371 * 2 * atan2(sqrt(a), sqrt(1 - a))
}
