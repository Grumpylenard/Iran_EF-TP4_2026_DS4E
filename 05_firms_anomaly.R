source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(httr2)
  library(readr)
  library(sf)
})

refresh_firms <- env_flag("REFRESH_FIRMS", FALSE)
write_output <- env_flag("WRITE_OUTPUT", FALSE)
preview_n <- 10
firms_map_key <- Sys.getenv("FIRMS_MAP_KEY", unset = "a8199de2124bd9a01c91965155fd45fe")

settings <- project_settings()

paths <- list(
  current_csv = project_path("data_processed", "firms_hotspots_raw.csv"),
  preconflict_csv = project_path("data_processed", "firms_hotspots_preconflict_raw.csv"),
  historical_csv = project_path("data_processed", "firms_hotspots_historical_raw.csv"),
  grid_daily_csv = project_path("data_processed", "firms_grid_daily.csv"),
  grid_anomalies_csv = project_path("data_processed", "firms_grid_anomalies.csv"),
  facility_anomalies_csv = project_path("data_processed", "firms_facility_anomalies.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv")
)

periods <- list(
  conflict_start = as.Date(settings$firms_conflict_start),
  conflict_end = as.Date(settings$study_end),
  pre_start = as.Date(settings$firms_conflict_start) - settings$firms_preconflict_days,
  pre_end = as.Date(settings$firms_conflict_start) - 1,
  seasonal_start = as.Date(settings$firms_conflict_start) - settings$firms_seasonal_window_days,
  seasonal_end = as.Date(settings$study_end) + settings$firms_seasonal_window_days
)

empty_firms_hotspots <- function() {
  tibble(
    hotspot_id = character(),
    acq_date = character(),
    latitude = double(),
    longitude = double(),
    bright_ti4 = double(),
    scan = double(),
    track = double(),
    acq_time = double(),
    satellite = character(),
    instrument = character(),
    confidence = character(),
    version = character(),
    bright_ti5 = double(),
    frp = double(),
    daynight = character(),
    source_sensor = character(),
    query_window_start = character(),
    query_window_end = character(),
    source_period = character(),
    reference_year = integer()
  )
}

empty_firms_daily_counts <- function() {
  tibble(
    acq_date = as.Date(character()),
    cell_id = character(),
    cell_x = integer(),
    cell_y = integer(),
    xmin_m = double(),
    ymin_m = double(),
    xmax_m = double(),
    ymax_m = double(),
    cell_center_lon = double(),
    cell_center_lat = double(),
    hotspot_count = integer()
  )
}

empty_firms_grid_daily <- function() {
  tibble(
    acq_date = character(),
    cell_id = character(),
    cell_x = integer(),
    cell_y = integer(),
    xmin_m = double(),
    ymin_m = double(),
    xmax_m = double(),
    ymax_m = double(),
    cell_center_lon = double(),
    cell_center_lat = double(),
    current_hotspot_count = integer(),
    baseline_mode = character(),
    baseline_n_preconflict = integer(),
    baseline_median_preconflict = double(),
    baseline_mad_preconflict = double(),
    baseline_n_seasonal = integer(),
    baseline_median_seasonal = double(),
    baseline_mad_seasonal = double(),
    baseline_median_combined = double(),
    absolute_lift = double(),
    robust_z_preconflict = double(),
    robust_z_seasonal = double(),
    anomaly_flag = logical()
  )
}

empty_firms_facility_anomalies <- function() {
  tibble(
    site_id = character(),
    asset_class = character(),
    asset_label = character(),
    name = character(),
    primary_provider = character(),
    anomaly_day_count = integer(),
    anomaly_cell_count = integer(),
    anomaly_hotspot_count = integer(),
    first_anomaly_date = character(),
    latest_anomaly_date = character(),
    nearest_anomaly_km = double(),
    max_current_hotspot_count = integer(),
    max_absolute_lift = double(),
    max_robust_z_preconflict = double(),
    max_robust_z_seasonal = double(),
    baseline_mode = character(),
    attribution_mode = character(),
    lat = double(),
    lon = double()
  )
}

empty_energy_sites <- function() {
  tibble(
    site_id = character(),
    asset_class = character(),
    asset_label = character(),
    asset_rank = integer(),
    map_color = character(),
    name = character(),
    status = character(),
    country = character(),
    lat = double(),
    lon = double(),
    geometry_role = character(),
    primary_provider = character(),
    primary_dataset = character(),
    provider_priority = integer(),
    provider_sources = character(),
    source_datasets = character(),
    source_ids = character(),
    provider_classes = character(),
    subclasses = character(),
    record_count = integer()
  )
}

prepare_firms_hotspots <- function(path) {
  template <- empty_firms_hotspots()
  raw <- read_csv_if_exists(path)

  for (column_name in setdiff(names(template), names(raw))) {
    raw[[column_name]] <- NA
  }

  bind_rows(
    template,
    raw %>%
      select(all_of(names(template))) %>%
      mutate(
        hotspot_id = dplyr::coalesce(as.character(hotspot_id), make_firms_hotspot_id(acq_date, latitude, longitude)),
        acq_date = as.character(acq_date),
        latitude = as.numeric(latitude),
        longitude = as.numeric(longitude),
        bright_ti4 = as.numeric(bright_ti4),
        scan = as.numeric(scan),
        track = as.numeric(track),
        acq_time = as.numeric(acq_time),
        satellite = as.character(satellite),
        instrument = as.character(instrument),
        confidence = as.character(confidence),
        version = as.character(version),
        bright_ti5 = as.numeric(bright_ti5),
        frp = as.numeric(frp),
        daynight = as.character(daynight),
        source_sensor = as.character(source_sensor),
        query_window_start = as.character(query_window_start),
        query_window_end = as.character(query_window_end),
        source_period = as.character(source_period),
        reference_year = as.integer(reference_year)
      )
  ) %>%
    mutate(
      acq_date = as.Date(acq_date),
      query_window_start = as.Date(query_window_start),
      query_window_end = as.Date(query_window_end),
      hotspot_id = dplyr::coalesce(hotspot_id, make_firms_hotspot_id(acq_date, latitude, longitude))
    ) %>%
    distinct(hotspot_id, acq_date, latitude, longitude, .keep_all = TRUE)
}

prepare_energy_sites <- function(path) {
  template <- empty_energy_sites()
  raw <- read_csv_if_exists(path)

  for (column_name in setdiff(names(template), names(raw))) {
    raw[[column_name]] <- NA
  }

  bind_rows(
    template,
    raw %>%
      select(all_of(names(template))) %>%
      mutate(
        asset_rank = as.integer(asset_rank),
        provider_priority = as.integer(provider_priority),
        lat = as.numeric(lat),
        lon = as.numeric(lon)
      )
  )
}

fetch_firms_day <- function(map_key, sensor, bbox, day_value, source_period, reference_year = NA_integer_) {
  url <- sprintf(
    "https://firms.modaps.eosdis.nasa.gov/api/area/csv/%s/%s/%s/%s/%s",
    map_key,
    sensor,
    bbox,
    1,
    as.character(day_value)
  )

  for (attempt in seq_len(4)) {
    day_tbl <- tryCatch({
      resp <- httr2::request(url) |>
        httr2::req_timeout(120) |>
        httr2::req_perform()
      body <- httr2::resp_body_string(resp)
      readr::read_csv(I(body), show_col_types = FALSE)
    }, error = function(e) {
      if (attempt == 4) {
        warning(sprintf("FIRMS request failed for %s (%s) after %s attempts: %s", day_value, sensor, attempt, conditionMessage(e)))
      } else {
        Sys.sleep(attempt * 2)
      }
      NULL
    })

    if (!is.null(day_tbl)) {
      return(day_tbl %>%
        mutate(
          hotspot_id = make_firms_hotspot_id(day_value, latitude, longitude),
          source_sensor = sensor,
          query_window_start = as.character(day_value),
          query_window_end = as.character(day_value),
          source_period = source_period,
          reference_year = as.integer(reference_year)
        ))
    }
  }

  empty_firms_hotspots()
}

fetch_firms_range <- function(start_date, end_date, sensor, source_period, reference_year = NA_integer_) {
  if (firms_map_key == "") {
    stop("Set FIRMS_MAP_KEY before refreshing FIRMS data.")
  }

  bbox <- with(settings$bbox, sprintf("%s,%s,%s,%s", west, south, east, north))
  windows <- date_windows(start_date, end_date, 1)

  bind_rows(lapply(seq_len(nrow(windows)), function(i) {
    fetch_firms_day(
      map_key = firms_map_key,
      sensor = sensor,
      bbox = bbox,
      day_value = windows$window_start[[i]],
      source_period = source_period,
      reference_year = reference_year
    )
  })) %>%
    mutate(
      acq_date = as.Date(acq_date),
      query_window_start = as.Date(query_window_start),
      query_window_end = as.Date(query_window_end)
    )
}

aggregate_firms_daily <- function(data, count_name = "hotspot_count") {
  if (nrow(data) == 0) {
    out <- empty_firms_daily_counts()
    names(out)[names(out) == "hotspot_count"] <- count_name
    return(out)
  }

  counts <- data %>%
    count(
      acq_date,
      cell_id,
      cell_x,
      cell_y,
      xmin_m,
      ymin_m,
      xmax_m,
      ymax_m,
      cell_center_lon,
      cell_center_lat,
      name = count_name
    ) %>%
    mutate(acq_date = as.Date(acq_date))

  bind_rows(
    {
      out <- empty_firms_daily_counts()
      names(out)[names(out) == "hotspot_count"] <- count_name
      out
    },
    counts
  )
}

safe_robust_z <- function(count_value, median_value, mad_value) {
  denominator <- pmax(1, 1.4826 * dplyr::coalesce(mad_value, 0))
  ifelse(is.na(median_value), NA_real_, (count_value - median_value) / denominator)
}

build_preconflict_summary <- function(preconflict_daily, current_daily) {
  if (nrow(current_daily) == 0) {
    return(tibble(
      cell_id = character(),
      baseline_n_preconflict = integer(),
      baseline_median_preconflict = double(),
      baseline_mad_preconflict = double()
    ))
  }

  pre_dates <- seq(periods$pre_start, periods$pre_end, by = "day")
  current_cells <- current_daily %>%
    distinct(cell_id)

  baseline_frame <- tibble::as_tibble(expand.grid(
    cell_id = current_cells$cell_id,
    baseline_date = pre_dates,
    KEEP.OUT.ATTRS = FALSE,
    stringsAsFactors = FALSE
  )) %>%
    left_join(
      preconflict_daily %>%
        transmute(cell_id, baseline_date = as.Date(acq_date), hotspot_count = as.integer(hotspot_count)),
      by = c("cell_id", "baseline_date")
    ) %>%
    mutate(hotspot_count = as.integer(dplyr::coalesce(hotspot_count, 0L)))

  baseline_frame %>%
    group_by(cell_id) %>%
    summarise(
      baseline_n_preconflict = n(),
      baseline_median_preconflict = median(hotspot_count, na.rm = TRUE),
      baseline_mad_preconflict = stats::mad(hotspot_count, constant = 1, na.rm = TRUE),
      .groups = "drop"
    )
}

build_seasonal_summary <- function(historical_daily, current_daily) {
  if (nrow(current_daily) == 0 || nrow(historical_daily) == 0 || length(settings$firms_reference_years) == 0) {
    return(tibble(
      cell_id = character(),
      acq_date = as.Date(character()),
      baseline_n_seasonal = integer(),
      baseline_median_seasonal = double(),
      baseline_mad_seasonal = double()
    ))
  }

  current_keys <- current_daily %>%
    distinct(cell_id, acq_date) %>%
    mutate(acq_date = as.Date(acq_date))
  offsets <- seq.int(-settings$firms_seasonal_window_days, settings$firms_seasonal_window_days)

  seasonal_candidates <- bind_rows(lapply(settings$firms_reference_years, function(reference_year) {
    bind_rows(lapply(offsets, function(offset_days) {
      tibble(
        cell_id = current_keys$cell_id,
        acq_date = current_keys$acq_date,
        sample_date = shift_date_to_year(current_keys$acq_date, reference_year) + offset_days,
        reference_year = as.integer(reference_year),
        seasonal_offset_days = as.integer(offset_days)
      )
    }))
  }))

  seasonal_candidates %>%
    left_join(
      historical_daily %>%
        transmute(cell_id, sample_date = as.Date(acq_date), hotspot_count = as.integer(hotspot_count)),
      by = c("cell_id", "sample_date")
    ) %>%
    mutate(hotspot_count = as.integer(dplyr::coalesce(hotspot_count, 0L))) %>%
    group_by(cell_id, acq_date) %>%
    summarise(
      baseline_n_seasonal = n(),
      baseline_median_seasonal = median(hotspot_count, na.rm = TRUE),
      baseline_mad_seasonal = stats::mad(hotspot_count, constant = 1, na.rm = TRUE),
      .groups = "drop"
    )
}

build_grid_metrics <- function(current_daily, preconflict_daily, historical_daily) {
  if (nrow(current_daily) == 0) {
    return(empty_firms_grid_daily())
  }

  preconflict_summary <- build_preconflict_summary(preconflict_daily, current_daily)
  seasonal_summary <- build_seasonal_summary(historical_daily, current_daily)

  current_daily %>%
    rename(current_hotspot_count = hotspot_count) %>%
    left_join(preconflict_summary, by = "cell_id") %>%
    left_join(seasonal_summary, by = c("cell_id", "acq_date")) %>%
    mutate(
      baseline_mode = if_else(!is.na(baseline_median_seasonal), "hybrid", "pre_conflict_only"),
      baseline_n_preconflict = as.integer(dplyr::coalesce(baseline_n_preconflict, 0L)),
      baseline_n_seasonal = as.integer(dplyr::coalesce(baseline_n_seasonal, 0L)),
      baseline_median_combined = if_else(
        baseline_mode == "hybrid",
        pmax(baseline_median_preconflict, baseline_median_seasonal),
        baseline_median_preconflict
      ),
      absolute_lift = current_hotspot_count - baseline_median_combined,
      robust_z_preconflict = safe_robust_z(current_hotspot_count, baseline_median_preconflict, baseline_mad_preconflict),
      robust_z_seasonal = safe_robust_z(current_hotspot_count, baseline_median_seasonal, baseline_mad_seasonal),
      anomaly_flag = current_hotspot_count >= settings$firms_anomaly_min_hotspots &
        absolute_lift >= settings$firms_anomaly_lift &
        robust_z_preconflict >= settings$firms_anomaly_z &
        (
          baseline_mode == "pre_conflict_only" |
            robust_z_seasonal >= settings$firms_anomaly_z
        ),
      acq_date = as.Date(acq_date)
    ) %>%
    mutate(across(c(acq_date), as.character)) %>%
    bind_rows(empty_firms_grid_daily(), .)
}

build_facility_anomalies <- function(grid_anomalies, energy_sites) {
  if (nrow(grid_anomalies) == 0 || nrow(energy_sites) == 0) {
    return(empty_firms_facility_anomalies())
  }

  candidate_sites <- energy_sites %>%
    filter(!is.na(lat), !is.na(lon))

  if (nrow(candidate_sites) == 0) {
    return(empty_firms_facility_anomalies())
  }

  anomaly_cells <- firms_grid_cells_as_sf(grid_anomalies, target_crs = 6933)
  anomaly_centroids <- grid_anomalies %>%
    st_as_sf(coords = c("cell_center_lon", "cell_center_lat"), crs = 4326, remove = FALSE) %>%
    st_transform(6933)
  sites_sf <- candidate_sites %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE) %>%
    st_transform(6933)

  build_relation_rows <- function(index_list, relation_label) {
    bind_rows(lapply(seq_along(index_list), function(site_row) {
      anomaly_rows <- index_list[[site_row]]
      if (length(anomaly_rows) == 0) return(NULL)

      tibble(
        site_row = as.integer(site_row),
        anomaly_row = as.integer(anomaly_rows),
        relation = relation_label
      )
    }))
  }

  inside_rows <- build_relation_rows(sf::st_intersects(sites_sf, anomaly_cells), "cell_contains_site")
  near_rows <- build_relation_rows(
    sf::st_is_within_distance(sites_sf, anomaly_centroids, dist = settings$firms_grid_km * 1000),
    "near_cell_centroid"
  )

  if (nrow(near_rows) > 0 && nrow(inside_rows) > 0) {
    near_rows <- anti_join(near_rows, inside_rows, by = c("site_row", "anomaly_row"))
  }

  relations <- bind_rows(inside_rows, near_rows)
  if (nrow(relations) == 0) {
    return(empty_firms_facility_anomalies())
  }

  centroid_distances_km <- as.numeric(sf::st_distance(
    sf::st_geometry(sites_sf)[relations$site_row],
    sf::st_geometry(anomaly_centroids)[relations$anomaly_row],
    by_element = TRUE
  )) / 1000

  relations <- relations %>%
    mutate(
      distance_km = if_else(relation == "cell_contains_site", 0, centroid_distances_km)
    ) %>%
    left_join(
      candidate_sites %>%
        mutate(site_row = row_number()) %>%
        select(site_row, site_id, asset_class, asset_label, name, primary_provider, lat, lon),
      by = "site_row"
    ) %>%
    left_join(
      grid_anomalies %>%
        mutate(anomaly_row = row_number()) %>%
        select(
          anomaly_row,
          cell_id,
          acq_date,
          current_hotspot_count,
          absolute_lift,
          robust_z_preconflict,
          robust_z_seasonal,
          baseline_mode
        ),
      by = "anomaly_row"
    )

  shared_cells <- relations %>%
    count(anomaly_row, name = "site_count")

  relations %>%
    left_join(shared_cells, by = "anomaly_row") %>%
    group_by(site_id) %>%
    summarise(
      asset_class = first(asset_class),
      asset_label = first(asset_label),
      name = first(name),
      primary_provider = first(primary_provider),
      anomaly_day_count = n_distinct(acq_date),
      anomaly_cell_count = n_distinct(cell_id),
      anomaly_hotspot_count = sum(current_hotspot_count, na.rm = TRUE),
      first_anomaly_date = min(acq_date, na.rm = TRUE),
      latest_anomaly_date = max(acq_date, na.rm = TRUE),
      nearest_anomaly_km = min(distance_km, na.rm = TRUE),
      max_current_hotspot_count = max(current_hotspot_count, na.rm = TRUE),
      max_absolute_lift = max(absolute_lift, na.rm = TRUE),
      max_robust_z_preconflict = max(robust_z_preconflict, na.rm = TRUE),
      max_robust_z_seasonal = suppressWarnings(max(robust_z_seasonal, na.rm = TRUE)),
      baseline_mode = if_else(any(baseline_mode == "hybrid", na.rm = TRUE), "hybrid", "pre_conflict_only"),
      attribution_mode = case_when(
        any(site_count > 1, na.rm = TRUE) ~ "shared_cell",
        any(relation == "cell_contains_site", na.rm = TRUE) ~ "cell_contains_site",
        TRUE ~ "near_cell_centroid"
      ),
      lat = first(lat),
      lon = first(lon),
      .groups = "drop"
    ) %>%
    mutate(
      max_robust_z_seasonal = ifelse(is.infinite(max_robust_z_seasonal), NA_real_, max_robust_z_seasonal)
    ) %>%
    bind_rows(empty_firms_facility_anomalies(), .)
}

if (refresh_firms && firms_map_key == "") {
  stop("Set FIRMS_MAP_KEY before running REFRESH_FIRMS=TRUE.")
}

energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
if (nrow(energy_sites) == 0) {
  stop("No compiled energy sites found. Run 02_build_incidents.R before 05_firms_anomaly.R.")
}

current_hotspots_existing <- prepare_firms_hotspots(paths$current_csv)
preconflict_hotspots_existing <- prepare_firms_hotspots(paths$preconflict_csv)
historical_hotspots_existing <- prepare_firms_hotspots(paths$historical_csv)

if (refresh_firms) {
  current_hotspots <- fetch_firms_range(
    start_date = periods$conflict_start,
    end_date = periods$conflict_end,
    sensor = settings$firms_sensor,
    source_period = "conflict_current"
  )
  preconflict_hotspots <- fetch_firms_range(
    start_date = periods$pre_start,
    end_date = periods$pre_end,
    sensor = settings$firms_sensor,
    source_period = "preconflict"
  )
  historical_hotspots <- bind_rows(lapply(settings$firms_reference_years, function(reference_year) {
    fetch_firms_range(
      start_date = shift_date_to_year(periods$seasonal_start, reference_year),
      end_date = shift_date_to_year(periods$seasonal_end, reference_year),
      sensor = "VIIRS_SNPP_SP",
      source_period = "seasonal_reference",
      reference_year = reference_year
    )
  }))
} else {
  current_hotspots <- current_hotspots_existing
  preconflict_hotspots <- preconflict_hotspots_existing
  historical_hotspots <- historical_hotspots_existing
}

legacy_preconflict <- current_hotspots %>%
  filter(acq_date >= periods$pre_start, acq_date <= periods$pre_end)
if (nrow(preconflict_hotspots) == 0 && nrow(legacy_preconflict) > 0) {
  preconflict_hotspots <- legacy_preconflict
}

current_hotspots <- current_hotspots %>%
  filter(acq_date >= periods$conflict_start, acq_date <= periods$conflict_end) %>%
  mutate(source_period = dplyr::coalesce(source_period, "conflict_current"))
preconflict_hotspots <- preconflict_hotspots %>%
  filter(acq_date >= periods$pre_start, acq_date <= periods$pre_end) %>%
  mutate(source_period = dplyr::coalesce(source_period, "preconflict"))
historical_hotspots <- historical_hotspots %>%
  filter(reference_year %in% settings$firms_reference_years) %>%
  mutate(source_period = dplyr::coalesce(source_period, "seasonal_reference"))

if (nrow(current_hotspots) == 0) {
  stop("No conflict-period FIRMS hotspots available. Run REFRESH_FIRMS=TRUE or provide data_processed/firms_hotspots_raw.csv.")
}
if (nrow(preconflict_hotspots) == 0) {
  stop("No pre-conflict FIRMS baseline available. Run REFRESH_FIRMS=TRUE or provide data_processed/firms_hotspots_preconflict_raw.csv.")
}

current_hotspots_gridded <- attach_firms_grid(
  current_hotspots,
  lon_col = "longitude",
  lat_col = "latitude",
  grid_km = settings$firms_grid_km
)
preconflict_hotspots_gridded <- attach_firms_grid(
  preconflict_hotspots,
  lon_col = "longitude",
  lat_col = "latitude",
  grid_km = settings$firms_grid_km
)
historical_hotspots_gridded <- attach_firms_grid(
  historical_hotspots,
  lon_col = "longitude",
  lat_col = "latitude",
  grid_km = settings$firms_grid_km
)

current_daily <- aggregate_firms_daily(current_hotspots_gridded)
preconflict_daily <- aggregate_firms_daily(preconflict_hotspots_gridded)
historical_daily <- aggregate_firms_daily(historical_hotspots_gridded)

grid_daily <- build_grid_metrics(current_daily, preconflict_daily, historical_daily)
grid_anomalies <- grid_daily %>%
  filter(!is.na(anomaly_flag), anomaly_flag)
facility_anomalies <- build_facility_anomalies(grid_anomalies, energy_sites)

message("Conflict-period FIRMS hotspots: ", nrow(current_hotspots))
message("Pre-conflict FIRMS hotspots: ", nrow(preconflict_hotspots))
message("Historical FIRMS hotspots: ", nrow(historical_hotspots))
message("Current FIRMS grid cell-days: ", nrow(grid_daily))
message("Anomalous FIRMS grid cell-days: ", nrow(grid_anomalies))
message("Facilities near anomalous FIRMS cells: ", nrow(facility_anomalies))

if (nrow(grid_anomalies) > 0) {
  print(utils::head(grid_anomalies, preview_n))
}
if (nrow(facility_anomalies) > 0) {
  print(utils::head(facility_anomalies, preview_n))
}

if (write_output) {
  write_csv_safe(current_hotspots, paths$current_csv)
  write_csv_safe(preconflict_hotspots, paths$preconflict_csv)
  write_csv_safe(historical_hotspots, paths$historical_csv)
  write_csv_safe(grid_daily, paths$grid_daily_csv)
  write_csv_safe(grid_anomalies, paths$grid_anomalies_csv)
  write_csv_safe(facility_anomalies, paths$facility_anomalies_csv)
  message("FIRMS anomaly outputs written to data_processed/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save FIRMS raw, grid, and facility anomaly outputs.")
}
