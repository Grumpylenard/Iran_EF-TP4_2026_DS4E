source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
})
ggrepel_available <- requireNamespace("ggrepel", quietly = TRUE)

# Manual controls.
map_mode <- Sys.getenv("MAP_MODE", unset = "infrastructure")   # "infrastructure", "sites", "candidate_sites", "firms_anomaly_grid", "firms_anomaly_sites", "firms_compare", "validation", or "validation_capacity_labels"
map_source <- Sys.getenv("MAP_SOURCE", unset = "review")       # "review" or "confirmed"
write_output <- env_flag("WRITE_OUTPUT", FALSE)

settings <- project_settings()
asset_registry <- build_asset_class_registry()
provider_registry <- build_provider_registry()

# Gulf-focused framing for the first map:
# keep the western edge tight to the eastern Saudi field belt
# and exclude Levant / eastern Mediterranean assets.
focus_bbox <- list(
  west = 46.4,
  south = 20.5,
  east = 60.0,
  north = 31.8
)

paths <- list(
  review_csv = project_path("data_processed", "incidents_review.csv"),
  confirmed_csv = project_path("data_processed", "incidents_confirmed.csv"),
  firms_csv = project_path("data_processed", "firms_hotspots_raw.csv"),
  firms_grid_daily_csv = project_path("data_processed", "firms_grid_daily.csv"),
  firms_grid_anomalies_csv = project_path("data_processed", "firms_grid_anomalies.csv"),
  firms_facility_anomalies_csv = project_path("data_processed", "firms_facility_anomalies.csv"),
  gdelt_csv = project_path("data_processed", "gdelt_articles_raw.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  energy_lines_geojson = project_path("data_processed", "energy_lines.geojson"),
  site_links_csv = project_path("data_processed", "incident_site_links.csv"),
  article_links_csv = project_path("data_processed", "incident_article_links.csv"),
  provider_log_csv = project_path("data_processed", "provider_intake_log.csv"),
  map_gpkg = project_path("output", "map_inputs.gpkg"),
  infrastructure_map_pdf = project_path("output", "energy_infrastructure_map.pdf"),
  facilities_map_pdf = project_path("output", "energy_facilities_map.pdf"),
  candidate_sites_map_pdf = project_path("output", "energy_candidate_sites_map.pdf"),
  firms_anomaly_grid_map_pdf = project_path("output", "energy_firms_anomaly_grid_map.pdf"),
  firms_anomaly_sites_map_pdf = project_path("output", "energy_firms_anomaly_sites_map.pdf"),
  firms_comparison_map_pdf = project_path("output", "energy_firms_comparison_map.pdf"),
  validation_map_pdf = project_path("output", "incident_validation_map.pdf"),
  validation_capacity_labels_map_pdf = project_path("output", "incident_validation_capacity_labels_map.pdf")
)

empty_article_links <- function() {
  tibble(
    incident_id = character(),
    article_key = character(),
    article_date = character(),
    query_id = character(),
    title = character(),
    url = character(),
    domain = character(),
    link_rank = integer()
  )
}

empty_site_links <- function() {
  tibble(
    incident_id = character(),
    site_id = character(),
    site_name = character(),
    asset_class = character(),
    asset_label = character(),
    primary_provider = character(),
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
    distance_km = double(),
    link_rank = integer()
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

empty_energy_lines <- function() {
  sf::st_sf(
    tibble(
      line_id = character(),
      asset_class = character(),
      asset_label = character(),
      asset_rank = integer(),
      map_color = character(),
      name = character(),
      status = character(),
      country = character(),
      geometry_role = character(),
      primary_provider = character(),
      primary_dataset = character(),
      provider_priority = integer(),
      provider_sources = character(),
      source_datasets = character(),
      source_ids = character(),
      provider_classes = character(),
      subclasses = character()
    ),
    geometry = sf::st_sfc(crs = 4326)
  )
}

empty_candidate_sites <- function() {
  tibble(
    site_id = character(),
    asset_class = character(),
    asset_label = character(),
    name = character(),
    primary_provider = character(),
    incident_count = integer(),
    nearest_km = double(),
    any_rank1 = logical(),
    within_10km = logical(),
    max_confidence = character(),
    candidate_status = character(),
    lat = double(),
    lon = double()
  )
}

empty_legacy_firms_site_matches <- function() {
  tibble(
    hotspot_id = character(),
    acq_date = character(),
    latitude = double(),
    longitude = double(),
    site_id = character(),
    site_name = character(),
    asset_class = character(),
    asset_label = character(),
    primary_provider = character(),
    distance_km = double()
  )
}

empty_legacy_firms_matched_sites <- function() {
  tibble(
    site_id = character(),
    asset_class = character(),
    asset_label = character(),
    name = character(),
    primary_provider = character(),
    hotspot_count = integer(),
    first_acq_date = character(),
    latest_acq_date = character(),
    nearest_km = double(),
    lat = double(),
    lon = double()
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

empty_site_validation_summary <- function() {
  tibble(
    site_id = character(),
    asset_class = character(),
    asset_label = character(),
    name = character(),
    country = character(),
    primary_provider = character(),
    commodity_group = character(),
    display_commodity_family = character(),
    display_type_label = character(),
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
    display_output_value = double(),
    display_output_unit = character(),
    display_output_basis = character(),
    display_output_text = character(),
    output_size_bucket = character(),
    validation_color_group = character(),
    incident_count = integer(),
    nearest_incident_km = double(),
    any_rank1_link = logical(),
    any_high_confidence_incident = logical(),
    first_high_confidence_incident_day = character(),
    latest_high_confidence_incident_day = character(),
    firms_anomaly_flag = logical(),
    first_anomaly_date = character(),
    latest_anomaly_date = character(),
    nearest_anomaly_km = double(),
    anomaly_day_count = integer(),
    anomaly_cell_count = integer(),
    anomaly_hotspot_count = integer(),
    confirmation_source_count = integer(),
    confirmation_sources_display = character(),
    confirmation_status = character(),
    provisional_triple_confirmed = logical(),
    display_hit_date = character(),
    label_rank_group = character(),
    label_rank_value = double(),
    label_rank_priority = integer(),
    lat = double(),
    lon = double()
  )
}

write_gpkg_layer <- function(data, dsn, layer, append = TRUE, aspatial = FALSE) {
  if (aspatial) {
    sf::st_write(
      data,
      dsn = dsn,
      layer = layer,
      append = append,
      quiet = TRUE,
      layer_options = "ASPATIAL_VARIANT=GPKG_ATTRIBUTES"
    )
  } else {
    sf::st_write(data, dsn = dsn, layer = layer, append = append, quiet = TRUE)
  }
}

gpkg_has_layer <- function(dsn, layer_name) {
  if (!file.exists(dsn)) return(FALSE)
  layer_name %in% sf::st_layers(dsn)$name
}

draw_side_by_side_maps <- function(left_plot, right_plot, title = NULL) {
  grid::grid.newpage()

  if (!is.null(title) && nzchar(title)) {
    grid::grid.text(
      title,
      x = 0.5,
      y = 0.985,
      gp = grid::gpar(fontsize = 16, fontface = "bold", col = "#20313c")
    )
  }

  print(
    left_plot,
    vp = grid::viewport(x = 0.25, y = 0.46, width = 0.49, height = 0.88)
  )
  print(
    right_plot,
    vp = grid::viewport(x = 0.75, y = 0.46, width = 0.49, height = 0.88)
  )
}

save_side_by_side_map_pdf <- function(left_plot, right_plot, path, title = NULL, width = 15, height = 7.5) {
  grDevices::pdf(path, width = width, height = height, onefile = TRUE)
  on.exit(grDevices::dev.off(), add = TRUE)
  draw_side_by_side_maps(left_plot, right_plot, title = title)
}

next_numbered_output_path <- function(path) {
  dir_path <- dirname(path)
  stem <- tools::file_path_sans_ext(basename(path))
  ext <- tools::file_ext(path)
  suffix <- if (nzchar(ext)) paste0(".", ext) else ""
  prefix <- paste0(stem, "_")

  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  existing_files <- list.files(dir_path, all.files = FALSE, no.. = TRUE)
  existing_numbers <- integer()

  for (file_name in existing_files) {
    if (!startsWith(file_name, prefix) || !endsWith(file_name, suffix)) next
    middle <- substr(file_name, nchar(prefix) + 1, nchar(file_name) - nchar(suffix))
    if (grepl("^[0-9]{2}$", middle)) {
      existing_numbers <- c(existing_numbers, as.integer(middle))
    }
  }

  next_number <- if (length(existing_numbers) == 0) 1L else max(existing_numbers) + 1L
  file.path(dir_path, paste0(stem, "_", sprintf("%02d", next_number), suffix))
}

prepare_incidents <- function(path) {
  bind_rows(
    tibble(
      incident_id = character(),
      incident_day = character(),
      target_country = character(),
      target_text = character(),
      confidence_tier = character(),
      lat = double(),
      lon = double()
    ),
    read_csv_if_exists(path) %>%
      mutate(incident_day = as.character(incident_day))
  )
}

prepare_firms <- function(path) {
  template <- tibble(
    hotspot_id = character(),
    acq_date = character(),
    latitude = double(),
    longitude = double()
  )
  raw <- read_csv_if_exists(path)

  for (column_name in setdiff(names(template), names(raw))) {
    raw[[column_name]] <- NA
  }

  bind_rows(
    template,
    raw %>%
      select(all_of(names(template))) %>%
      mutate(
        hotspot_id = as.character(hotspot_id),
        acq_date = as.character(acq_date),
        latitude = as.numeric(latitude),
        longitude = as.numeric(longitude)
      )
  ) %>%
    mutate(
      hotspot_id = dplyr::coalesce(hotspot_id, make_firms_hotspot_id(acq_date, latitude, longitude))
    )
}

prepare_gdelt <- function(path) {
  bind_rows(
    tibble(
      query_id = character(),
      seendate = character(),
      title = character(),
      url = character(),
      domain = character()
    ),
    read_csv_if_exists(path)
  ) %>%
    mutate(
      article_key = make_article_key(query_id, seendate, url, title),
      article_date = as.character(as.Date(substr(seendate, 1, 8), format = "%Y%m%d"))
    )
}

prepare_site_links <- function(path) {
  bind_rows(
    empty_site_links(),
    read_csv_if_exists(path) %>%
      mutate(
        commodity_group = as.character(commodity_group),
        liquid_capacity_bpd = as.numeric(liquid_capacity_bpd),
        liquid_throughput_bpd = as.numeric(liquid_throughput_bpd),
        gas_capacity_mmcfd = as.numeric(gas_capacity_mmcfd),
        gas_throughput_mmcfd = as.numeric(gas_throughput_mmcfd),
        lng_capacity_mtpa = as.numeric(lng_capacity_mtpa),
        lng_capacity_bcmy = as.numeric(lng_capacity_bcmy),
        oil_production_bpd = as.numeric(oil_production_bpd),
        capacity_kbd = as.numeric(capacity_kbd),
        production_kbd = as.numeric(production_kbd),
        primary_output_kind = as.character(primary_output_kind),
        primary_output_value = as.numeric(primary_output_value),
        primary_output_unit = as.character(primary_output_unit),
        primary_output_basis = as.character(primary_output_basis),
        distance_km = as.numeric(distance_km),
        link_rank = as.integer(link_rank)
      )
  )
}

prepare_article_links <- function(path) {
  bind_rows(
    empty_article_links(),
    read_csv_if_exists(path) %>%
      mutate(
        article_date = as.character(article_date),
        link_rank = as.integer(link_rank)
      )
  )
}

prepare_energy_sites <- function(path) {
  bind_rows(
    empty_energy_sites(),
    read_csv_if_exists(path)
  ) %>%
    mutate(
      asset_rank = as.integer(asset_rank),
      provider_priority = as.integer(provider_priority),
      commodity_group = as.character(commodity_group),
      liquid_capacity_bpd = as.numeric(liquid_capacity_bpd),
      liquid_throughput_bpd = as.numeric(liquid_throughput_bpd),
      gas_capacity_mmcfd = as.numeric(gas_capacity_mmcfd),
      gas_throughput_mmcfd = as.numeric(gas_throughput_mmcfd),
      lng_capacity_mtpa = as.numeric(lng_capacity_mtpa),
      lng_capacity_bcmy = as.numeric(lng_capacity_bcmy),
      oil_production_bpd = as.numeric(oil_production_bpd),
      capacity_kbd = as.numeric(capacity_kbd),
      production_kbd = as.numeric(production_kbd),
      primary_output_kind = as.character(primary_output_kind),
      primary_output_value = as.numeric(primary_output_value),
      primary_output_unit = as.character(primary_output_unit),
      primary_output_basis = as.character(primary_output_basis),
      record_count = as.integer(record_count),
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    )
}

prepare_energy_lines <- function(path) {
  if (!file.exists(path)) {
    return(empty_energy_lines())
  }

  lines <- read_sf_if_exists(path)
  if (is.null(lines)) {
    return(empty_energy_lines())
  }

  lines %>%
    mutate(
      asset_rank = as.integer(asset_rank),
      provider_priority = as.integer(provider_priority)
    )
}

prepare_provider_sites <- function(dataset_id) {
  path <- provider_output_paths(dataset_id)$site_csv
  if (!file.exists(path)) return(NULL)

  read_csv_if_exists(path) %>%
    mutate(
      provider_priority = as.integer(provider_priority),
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    )
}

prepare_provider_lines <- function(dataset_id) {
  path <- provider_output_paths(dataset_id)$line_geojson
  if (!file.exists(path)) return(NULL)
  read_sf_if_exists(path)
}

prepare_provider_log <- function(path) {
  bind_rows(
    tibble(
      source_dataset = character(),
      provider = character(),
      source_file = character(),
      source_layer = character(),
      status = character(),
      site_count = integer(),
      line_count = integer(),
      note = character()
    ),
    read_csv_if_exists(path) %>%
      mutate(
        site_count = as.integer(site_count),
        line_count = as.integer(line_count)
      )
  )
}

prepare_firms_grid_daily <- function(path) {
  template <- empty_firms_grid_daily()
  raw <- read_csv_if_exists(path)

  for (column_name in setdiff(names(template), names(raw))) {
    raw[[column_name]] <- NA
  }

  bind_rows(
    template,
    raw %>%
      select(all_of(names(template))) %>%
      mutate(
        acq_date = as.character(acq_date),
        cell_x = as.integer(cell_x),
        cell_y = as.integer(cell_y),
        xmin_m = as.numeric(xmin_m),
        ymin_m = as.numeric(ymin_m),
        xmax_m = as.numeric(xmax_m),
        ymax_m = as.numeric(ymax_m),
        cell_center_lon = as.numeric(cell_center_lon),
        cell_center_lat = as.numeric(cell_center_lat),
        current_hotspot_count = as.integer(current_hotspot_count),
        baseline_mode = as.character(baseline_mode),
        baseline_n_preconflict = as.integer(baseline_n_preconflict),
        baseline_median_preconflict = as.numeric(baseline_median_preconflict),
        baseline_mad_preconflict = as.numeric(baseline_mad_preconflict),
        baseline_n_seasonal = as.integer(baseline_n_seasonal),
        baseline_median_seasonal = as.numeric(baseline_median_seasonal),
        baseline_mad_seasonal = as.numeric(baseline_mad_seasonal),
        baseline_median_combined = as.numeric(baseline_median_combined),
        absolute_lift = as.numeric(absolute_lift),
        robust_z_preconflict = as.numeric(robust_z_preconflict),
        robust_z_seasonal = as.numeric(robust_z_seasonal),
        anomaly_flag = as.logical(anomaly_flag)
      )
  )
}

prepare_firms_facility_anomalies <- function(path) {
  template <- empty_firms_facility_anomalies()
  raw <- read_csv_if_exists(path)

  for (column_name in setdiff(names(template), names(raw))) {
    raw[[column_name]] <- NA
  }

  bind_rows(
    template,
    raw %>%
      select(all_of(names(template))) %>%
      mutate(
        asset_class = as.character(asset_class),
        asset_label = as.character(asset_label),
        name = as.character(name),
        primary_provider = as.character(primary_provider),
        anomaly_day_count = as.integer(anomaly_day_count),
        anomaly_cell_count = as.integer(anomaly_cell_count),
        anomaly_hotspot_count = as.integer(anomaly_hotspot_count),
        first_anomaly_date = as.character(first_anomaly_date),
        latest_anomaly_date = as.character(latest_anomaly_date),
        nearest_anomaly_km = as.numeric(nearest_anomaly_km),
        max_current_hotspot_count = as.integer(max_current_hotspot_count),
        max_absolute_lift = as.numeric(max_absolute_lift),
        max_robust_z_preconflict = as.numeric(max_robust_z_preconflict),
        max_robust_z_seasonal = as.numeric(max_robust_z_seasonal),
        baseline_mode = as.character(baseline_mode),
        attribution_mode = as.character(attribution_mode),
        lat = as.numeric(lat),
        lon = as.numeric(lon)
      )
  )
}

build_firms_anomalous_hotspots <- function(firms, grid_anomalies, settings) {
  if (nrow(firms) == 0 || nrow(grid_anomalies) == 0) {
    return(bind_cols(prepare_firms("missing"), empty_firms_grid_daily()[0, c("cell_id", "current_hotspot_count", "absolute_lift", "robust_z_preconflict", "robust_z_seasonal")]))
  }

  anomaly_keys <- grid_anomalies %>%
    select(cell_id, acq_date, current_hotspot_count, absolute_lift, robust_z_preconflict, robust_z_seasonal)

  attach_firms_grid(
    firms,
    lon_col = "longitude",
    lat_col = "latitude",
    grid_km = settings$firms_grid_km
  ) %>%
    mutate(acq_date = as.character(acq_date)) %>%
    inner_join(anomaly_keys, by = c("cell_id", "acq_date"))
}

build_legacy_firms_overlay <- function(firms, energy_sites, settings) {
  if (nrow(firms) == 0 || nrow(energy_sites) == 0) {
    return(list(
      matches = empty_legacy_firms_site_matches(),
      sites = empty_legacy_firms_matched_sites()
    ))
  }

  firms_points <- firms %>%
    filter(!is.na(latitude), !is.na(longitude)) %>%
    mutate(acq_date = as.character(acq_date))
  sites_points <- energy_sites %>%
    filter(!is.na(lat), !is.na(lon))

  if (nrow(firms_points) == 0 || nrow(sites_points) == 0) {
    return(list(
      matches = empty_legacy_firms_site_matches(),
      sites = empty_legacy_firms_matched_sites()
    ))
  }

  firms_sf_local <- firms_points %>%
    sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
  sites_sf_local <- sites_points %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

  nearby_idx <- sf::st_is_within_distance(
    firms_sf_local,
    sites_sf_local,
    dist = settings$firms_site_match_km * 1000
  )

  match_rows <- bind_rows(lapply(seq_along(nearby_idx), function(i) {
    idx <- nearby_idx[[i]]
    if (length(idx) == 0) return(NULL)

    distances <- haversine_km(
      firms_points$latitude[[i]],
      firms_points$longitude[[i]],
      sites_points$lat[idx],
      sites_points$lon[idx]
    )
    keep <- idx[[which.min(distances)]]

    tibble(
      hotspot_id = firms_points$hotspot_id[[i]],
      acq_date = firms_points$acq_date[[i]],
      latitude = firms_points$latitude[[i]],
      longitude = firms_points$longitude[[i]],
      site_id = sites_points$site_id[[keep]],
      site_name = sites_points$name[[keep]],
      asset_class = sites_points$asset_class[[keep]],
      asset_label = sites_points$asset_label[[keep]],
      primary_provider = sites_points$primary_provider[[keep]],
      distance_km = min(distances, na.rm = TRUE)
    )
  }))

  if (nrow(match_rows) == 0) {
    return(list(
      matches = empty_legacy_firms_site_matches(),
      sites = empty_legacy_firms_matched_sites()
    ))
  }

  matched_sites <- match_rows %>%
    group_by(site_id) %>%
    summarise(
      hotspot_count = n(),
      first_acq_date = min(acq_date, na.rm = TRUE),
      latest_acq_date = max(acq_date, na.rm = TRUE),
      nearest_km = min(distance_km, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    left_join(
      sites_points %>%
        select(site_id, asset_class, asset_label, name, primary_provider, lat, lon),
      by = "site_id"
    ) %>%
    select(
      site_id,
      asset_class,
      asset_label,
      name,
      primary_provider,
      hotspot_count,
      first_acq_date,
      latest_acq_date,
      nearest_km,
      lat,
      lon
    )

  list(
    matches = bind_rows(empty_legacy_firms_site_matches(), match_rows),
    sites = bind_rows(empty_legacy_firms_matched_sites(), matched_sites)
  )
}

build_candidate_sites <- function(site_links, incidents, energy_sites) {
  if (nrow(site_links) == 0 || nrow(energy_sites) == 0) {
    return(empty_candidate_sites())
  }

  summaries <- site_links %>%
    left_join(
      incidents %>% select(incident_id, confidence_tier),
      by = "incident_id"
    ) %>%
    group_by(site_id) %>%
    summarise(
      incident_count = n_distinct(incident_id),
      nearest_km = min(distance_km, na.rm = TRUE),
      any_rank1 = any(link_rank == 1, na.rm = TRUE),
      within_10km = any(distance_km <= 10, na.rm = TRUE),
      max_confidence = case_when(
        any(confidence_tier == "high", na.rm = TRUE) ~ "high",
        any(confidence_tier == "medium", na.rm = TRUE) ~ "medium",
        any(confidence_tier == "low", na.rm = TRUE) ~ "low",
        TRUE ~ NA_character_
      ),
      .groups = "drop"
    ) %>%
    filter(any_rank1 | within_10km) %>%
    mutate(
      candidate_status = case_when(
        any_rank1 & within_10km ~ "Primary / within 10 km",
        any_rank1 ~ "Primary candidate",
        within_10km ~ "Within 10 km",
        TRUE ~ "Linked site"
      )
    ) %>%
    left_join(
      energy_sites %>%
        select(site_id, asset_class, asset_label, name, primary_provider, lat, lon),
      by = "site_id"
    ) %>%
    select(
      site_id,
      asset_class,
      asset_label,
      name,
      primary_provider,
      incident_count,
      nearest_km,
      any_rank1,
      within_10km,
      max_confidence,
      candidate_status,
      lat,
      lon
    )

  bind_rows(empty_candidate_sites(), summaries)
}

safe_any <- function(x) {
  if (length(x) == 0 || all(is.na(x))) return(FALSE)
  any(x, na.rm = TRUE)
}

safe_min_num <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_real_)
  min(x)
}

safe_min_date_chr <- function(x) {
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) return(NA_character_)
  as.character(min(as.Date(x)))
}

safe_max_date_chr <- function(x) {
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) return(NA_character_)
  as.character(max(as.Date(x)))
}

normalize_site_name <- function(x) {
  y <- trimws(dplyr::coalesce(as.character(x), ""))
  dplyr::if_else(!nzchar(y) | toupper(y) == "N/A", "Unnamed site", y)
}

derive_commodity_family <- function(commodity_group, asset_class) {
  commodity_norm <- tolower(trimws(dplyr::coalesce(as.character(commodity_group), "")))
  asset_norm <- tolower(trimws(dplyr::coalesce(as.character(asset_class), "")))

  case_when(
    commodity_norm == "oil" ~ "Oil / Liquids",
    commodity_norm == "gas" ~ "Gas / LNG",
    commodity_norm == "mixed" ~ "Mixed",
    asset_norm %in% c("refinery", "petroleum_terminal", "tank_battery") ~ "Oil / Liquids",
    asset_norm %in% c("lng_facility", "processing_facility", "compressor_station") ~ "Gas / LNG",
    TRUE ~ "Other / Unknown"
  )
}

format_output_text <- function(value, unit, basis) {
  ifelse(
    is.na(value) | !nzchar(dplyr::coalesce(unit, "")),
    NA_character_,
    paste0(
      scales::label_number(accuracy = 0.1, trim = TRUE)(value),
      " ",
      unit,
      ifelse(
        nzchar(dplyr::coalesce(basis, "")),
        paste0(" ", basis),
        ""
      )
    )
  )
}

derive_output_size_bucket <- function(display_commodity_family, display_output_value, display_output_unit) {
  unit_norm <- tolower(trimws(dplyr::coalesce(display_output_unit, "")))
  bucket <- case_when(
    is.na(display_output_value) | display_output_value <= 0 ~ "No output data",
    display_commodity_family == "Oil / Liquids" & display_output_value >= 200 ~ "High output",
    display_commodity_family == "Oil / Liquids" & display_output_value >= 50 ~ "Medium output",
    display_commodity_family == "Oil / Liquids" ~ "Low output",
    unit_norm == "mtpa" & display_output_value >= 5 ~ "High output",
    unit_norm == "mtpa" & display_output_value >= 1 ~ "Medium output",
    unit_norm == "mtpa" ~ "Low output",
    unit_norm %in% c("bcm/y", "bcmy") & display_output_value >= 7 ~ "High output",
    unit_norm %in% c("bcm/y", "bcmy") & display_output_value >= 2 ~ "Medium output",
    unit_norm %in% c("bcm/y", "bcmy") ~ "Low output",
    unit_norm == "mmcfd" & display_output_value >= 500 ~ "High output",
    unit_norm == "mmcfd" & display_output_value >= 100 ~ "Medium output",
    unit_norm == "mmcfd" ~ "Low output",
    TRUE ~ "Low output"
  )

  factor(bucket, levels = c("No output data", "Low output", "Medium output", "High output"))
}

build_site_validation_summary <- function(site_links, incidents, energy_sites, firms_facility_anomalies, settings) {
  base_sites <- energy_sites %>%
    mutate(
      name = normalize_site_name(name),
      display_commodity_family = derive_commodity_family(commodity_group, asset_class),
      display_type_label = case_when(
        display_commodity_family == "Oil / Liquids" ~ paste(asset_label, "Oil", sep = " | "),
        display_commodity_family == "Gas / LNG" ~ paste(asset_label, "Gas/LNG", sep = " | "),
        display_commodity_family == "Mixed" ~ paste(asset_label, "Mixed", sep = " | "),
        TRUE ~ paste(asset_label, "Other", sep = " | ")
      ),
      display_output_value = case_when(
        display_commodity_family == "Oil / Liquids" & !is.na(capacity_kbd) ~ capacity_kbd,
        display_commodity_family == "Oil / Liquids" & !is.na(production_kbd) ~ production_kbd,
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_mtpa) ~ lng_capacity_mtpa,
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_bcmy) ~ lng_capacity_bcmy,
        display_commodity_family == "Gas / LNG" & !is.na(gas_capacity_mmcfd) ~ gas_capacity_mmcfd,
        display_commodity_family == "Gas / LNG" & !is.na(gas_throughput_mmcfd) ~ gas_throughput_mmcfd,
        !is.na(primary_output_value) ~ primary_output_value,
        TRUE ~ NA_real_
      ),
      display_output_unit = case_when(
        display_commodity_family == "Oil / Liquids" & !is.na(capacity_kbd) ~ "kbd",
        display_commodity_family == "Oil / Liquids" & !is.na(production_kbd) ~ "kbd",
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_mtpa) ~ "mtpa",
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_bcmy) ~ "bcm/y",
        display_commodity_family == "Gas / LNG" & !is.na(gas_capacity_mmcfd) ~ "mmcfd",
        display_commodity_family == "Gas / LNG" & !is.na(gas_throughput_mmcfd) ~ "mmcfd",
        TRUE ~ primary_output_unit
      ),
      display_output_basis = case_when(
        display_commodity_family == "Oil / Liquids" & !is.na(capacity_kbd) ~ "capacity",
        display_commodity_family == "Oil / Liquids" & !is.na(production_kbd) ~ "production",
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_mtpa) ~ "capacity",
        display_commodity_family == "Gas / LNG" & !is.na(lng_capacity_bcmy) ~ "capacity",
        display_commodity_family == "Gas / LNG" & !is.na(gas_capacity_mmcfd) ~ "capacity",
        display_commodity_family == "Gas / LNG" & !is.na(gas_throughput_mmcfd) ~ "throughput",
        TRUE ~ primary_output_basis
      ),
      validation_color_group = case_when(
        display_commodity_family == "Oil / Liquids" ~ "Oil / Liquids",
        display_commodity_family == "Gas / LNG" ~ "Gas / LNG",
        display_commodity_family == "Mixed" ~ "Mixed",
        TRUE ~ "Other / Unknown"
      )
    )

  incident_lookup <- incidents %>%
    select(incident_id, incident_day, confidence_tier) %>%
    filter(
      !is.na(incident_day),
      as.Date(incident_day) >= settings$study_start,
      as.Date(incident_day) <= settings$study_end
    ) %>%
    distinct()

  incident_evidence <- if (nrow(site_links) > 0 && nrow(incident_lookup) > 0) {
    site_links %>%
      left_join(incident_lookup, by = "incident_id") %>%
      group_by(site_id) %>%
      summarise(
        incident_count = n_distinct(incident_id),
        nearest_incident_km = safe_min_num(distance_km),
        any_rank1_link = safe_any(link_rank == 1),
        any_high_confidence_incident = safe_any(confidence_tier == "high"),
        first_high_confidence_incident_day = safe_min_date_chr(incident_day[confidence_tier == "high"]),
        latest_high_confidence_incident_day = safe_max_date_chr(incident_day[confidence_tier == "high"]),
        .groups = "drop"
      )
  } else {
    empty_site_validation_summary()[0, c(
      "site_id", "incident_count", "nearest_incident_km", "any_rank1_link",
      "any_high_confidence_incident", "first_high_confidence_incident_day",
      "latest_high_confidence_incident_day"
    )]
  }

  firms_evidence <- if (nrow(firms_facility_anomalies) > 0) {
    firms_facility_anomalies %>%
      transmute(
        site_id,
        firms_anomaly_flag = TRUE,
        first_anomaly_date,
        latest_anomaly_date,
        nearest_anomaly_km,
        anomaly_day_count,
        anomaly_cell_count,
        anomaly_hotspot_count
      )
  } else {
    empty_site_validation_summary()[0, c(
      "site_id", "firms_anomaly_flag", "first_anomaly_date", "latest_anomaly_date",
      "nearest_anomaly_km", "anomaly_day_count", "anomaly_cell_count", "anomaly_hotspot_count"
    )]
  }

  summary_tbl <- base_sites %>%
    left_join(incident_evidence, by = "site_id") %>%
    left_join(firms_evidence, by = "site_id") %>%
    mutate(
      incident_count = dplyr::coalesce(incident_count, 0L),
      any_rank1_link = dplyr::coalesce(any_rank1_link, FALSE),
      any_high_confidence_incident = dplyr::coalesce(any_high_confidence_incident, FALSE),
      firms_anomaly_flag = dplyr::coalesce(firms_anomaly_flag, FALSE),
      anomaly_day_count = dplyr::coalesce(anomaly_day_count, 0L),
      anomaly_cell_count = dplyr::coalesce(anomaly_cell_count, 0L),
      anomaly_hotspot_count = dplyr::coalesce(anomaly_hotspot_count, 0L),
      display_output_text = format_output_text(display_output_value, display_output_unit, display_output_basis),
      output_size_bucket = derive_output_size_bucket(display_commodity_family, display_output_value, display_output_unit),
      confirmation_source_count = as.integer(incident_count > 0) + as.integer(any_high_confidence_incident) + as.integer(firms_anomaly_flag),
      provisional_triple_confirmed = incident_count > 0 & any_high_confidence_incident & firms_anomaly_flag,
      confirmation_sources_display = case_when(
        provisional_triple_confirmed ~ "TP4 incident link; high-confidence incident; FIRMS anomaly",
        any_high_confidence_incident & incident_count > 0 ~ "TP4 incident link; high-confidence incident",
        incident_count > 0 & firms_anomaly_flag ~ "TP4 incident link; FIRMS anomaly",
        incident_count > 0 ~ "TP4 incident link",
        firms_anomaly_flag ~ "FIRMS anomaly",
        TRUE ~ "No confirmation signals"
      ),
      confirmation_status = case_when(
        provisional_triple_confirmed ~ "Provisional triple-confirmed",
        incident_count > 0 ~ "Incident-linked",
        firms_anomaly_flag ~ "FIRMS anomaly near site",
        TRUE ~ "Background site"
      ),
      display_hit_date = if_else(any_high_confidence_incident, first_high_confidence_incident_day, NA_character_),
      label_rank_group = case_when(
        display_commodity_family == "Oil / Liquids" ~ "oil",
        display_commodity_family == "Gas / LNG" ~ "gas",
        TRUE ~ "other"
      ),
      label_rank_value = case_when(
        label_rank_group == "oil" & !is.na(capacity_kbd) ~ capacity_kbd,
        label_rank_group == "oil" & !is.na(production_kbd) ~ production_kbd,
        TRUE ~ display_output_value
      ),
      label_rank_priority = case_when(
        label_rank_group == "oil" ~ 1L,
        tolower(trimws(dplyr::coalesce(display_output_unit, ""))) == "mtpa" ~ 1L,
        tolower(trimws(dplyr::coalesce(display_output_unit, ""))) %in% c("bcm/y", "bcmy") ~ 2L,
        tolower(trimws(dplyr::coalesce(display_output_unit, ""))) == "mmcfd" ~ 3L,
        TRUE ~ 4L
      )
    ) %>%
    select(all_of(names(empty_site_validation_summary())))

  bind_rows(empty_site_validation_summary(), summary_tbl)
}

select_validation_label_sites <- function(site_validation_summary, label_limit = 25L) {
  eligible <- site_validation_summary %>%
    filter(
      incident_count > 0,
      !is.na(lat),
      !is.na(lon),
      !is.na(display_output_value),
      name != "Unnamed site"
    ) %>%
    mutate(
      label_metric_priority = case_when(
        !is.na(capacity_kbd) ~ 1L,
        !is.na(production_kbd) ~ 2L,
        TRUE ~ 3L
      ),
      label_metric_value = case_when(
        !is.na(capacity_kbd) ~ capacity_kbd,
        !is.na(production_kbd) ~ production_kbd,
        TRUE ~ label_rank_value
      )
    )

  if (nrow(eligible) == 0) {
    return(empty_site_validation_summary())
  }

  eligible %>%
    arrange(label_metric_priority, desc(label_metric_value), nearest_incident_km, name) %>%
    slice_head(n = label_limit)
}

prepare_label_points <- function(label_sites_sf, focus_bbox) {
  if (nrow(label_sites_sf) == 0) {
    return(tibble())
  }

  coords <- sf::st_coordinates(label_sites_sf)
  mid_lon <- mean(c(focus_bbox$west, focus_bbox$east))
  bind_cols(
    sf::st_drop_geometry(label_sites_sf),
    tibble(X = coords[, "X"], Y = coords[, "Y"])
  ) %>%
    mutate(
      label_side = if_else(X <= mid_lon, "left", "right"),
      label_nudge_x = if_else(label_side == "left", -1.1, 1.1),
      label_hjust = if_else(label_side == "left", 1, 0),
      label_text = paste(
        name,
        paste(country, dplyr::coalesce(display_output_text, "No capacity data"), sep = " | "),
        sep = "\n"
      )
    )
}

package_path <- if (write_output) next_numbered_output_path(paths$map_gpkg) else tempfile(fileext = ".gpkg")

incidents <- if (map_source == "confirmed") {
  prepare_incidents(paths$confirmed_csv)
} else {
  prepare_incidents(paths$review_csv)
}
firms <- prepare_firms(paths$firms_csv)
firms_grid_daily <- prepare_firms_grid_daily(paths$firms_grid_daily_csv)
firms_grid_anomalies <- prepare_firms_grid_daily(paths$firms_grid_anomalies_csv)
if (nrow(firms_grid_anomalies) == 0) {
  firms_grid_anomalies <- firms_grid_daily %>%
    filter(!is.na(anomaly_flag), anomaly_flag)
}
firms_facility_anomalies <- prepare_firms_facility_anomalies(paths$firms_facility_anomalies_csv)
gdelt <- tibble()
energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
energy_lines <- prepare_energy_lines(paths$energy_lines_geojson)
incident_site_links <- prepare_site_links(paths$site_links_csv)
incident_article_links <- prepare_article_links(paths$article_links_csv)
provider_log <- prepare_provider_log(paths$provider_log_csv)
candidate_sites <- build_candidate_sites(incident_site_links, incidents, energy_sites)
firms_anomalous_hotspots <- build_firms_anomalous_hotspots(firms, firms_grid_anomalies, settings)
site_validation_summary <- build_site_validation_summary(incident_site_links, incidents, energy_sites, firms_facility_anomalies, settings)

if (nrow(energy_sites) + nrow(energy_lines) == 0) {
  stop("No compiled energy infrastructure layers found. Run 01_ingest_sources.R and 02_build_incidents.R first.")
}

energy_sites_sf <- if (nrow(energy_sites) > 0) {
  energy_sites %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(empty_energy_sites(), geometry = sf::st_sfc(crs = 4326))
}

incidents_sf <- if (nrow(incidents) > 0) {
  incidents %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(prepare_incidents("missing"), geometry = sf::st_sfc(crs = 4326))
}

firms_sf <- if (nrow(firms) > 0) {
  firms %>%
    filter(!is.na(latitude), !is.na(longitude)) %>%
    sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(prepare_firms("missing"), geometry = sf::st_sfc(crs = 4326))
}

candidate_sites_sf <- if (nrow(candidate_sites) > 0) {
  candidate_sites %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(empty_candidate_sites(), geometry = sf::st_sfc(crs = 4326))
}

firms_grid_anomalies_sf <- if (nrow(firms_grid_anomalies) > 0) {
  firms_grid_cells_as_sf(firms_grid_anomalies)
} else {
  sf::st_sf(empty_firms_grid_daily(), geometry = sf::st_sfc(crs = 4326))
}

firms_anomalous_hotspots_sf <- if (nrow(firms_anomalous_hotspots) > 0) {
  firms_anomalous_hotspots %>%
    filter(!is.na(latitude), !is.na(longitude)) %>%
    sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(bind_cols(prepare_firms("missing"), empty_firms_grid_daily()[0, c("cell_id", "current_hotspot_count", "absolute_lift", "robust_z_preconflict", "robust_z_seasonal")]), geometry = sf::st_sfc(crs = 4326))
}

firms_facility_anomalies_sf <- if (nrow(firms_facility_anomalies) > 0) {
  firms_facility_anomalies %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(empty_firms_facility_anomalies(), geometry = sf::st_sfc(crs = 4326))
}

site_validation_summary_sf <- if (nrow(site_validation_summary) > 0) {
  site_validation_summary %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
} else {
  sf::st_sf(empty_site_validation_summary(), geometry = sf::st_sfc(crs = 4326))
}

if (write_output) {
  ensure_dir(project_path("output"))
}
if (file.exists(package_path)) {
  unlink(package_path)
}

write_gpkg_layer(energy_sites_sf, package_path, "energy_sites", append = FALSE, aspatial = FALSE)
write_gpkg_layer(energy_lines, package_path, "energy_lines", append = TRUE, aspatial = FALSE)
write_gpkg_layer(incidents_sf, package_path, "incidents", append = TRUE, aspatial = FALSE)
write_gpkg_layer(firms_sf, package_path, "firms_hotspots", append = TRUE, aspatial = FALSE)
write_gpkg_layer(firms_grid_daily, package_path, "firms_grid_daily", append = TRUE, aspatial = TRUE)
write_gpkg_layer(firms_grid_anomalies_sf, package_path, "firms_grid_anomalies", append = TRUE, aspatial = FALSE)
write_gpkg_layer(firms_anomalous_hotspots_sf, package_path, "firms_anomalous_hotspots", append = TRUE, aspatial = FALSE)
write_gpkg_layer(firms_facility_anomalies_sf, package_path, "firms_facility_anomalies", append = TRUE, aspatial = FALSE)
write_gpkg_layer(candidate_sites_sf, package_path, "candidate_sites", append = TRUE, aspatial = FALSE)
write_gpkg_layer(site_validation_summary, package_path, "site_validation_summary", append = TRUE, aspatial = TRUE)
write_gpkg_layer(gdelt, package_path, "gdelt_articles", append = TRUE, aspatial = TRUE)
write_gpkg_layer(incident_site_links, package_path, "incident_site_links", append = TRUE, aspatial = TRUE)
write_gpkg_layer(incident_article_links, package_path, "incident_article_links", append = TRUE, aspatial = TRUE)
write_gpkg_layer(provider_log, package_path, "provider_intake_log", append = TRUE, aspatial = TRUE)

for (dataset_id in provider_registry$source_dataset) {
  provider_sites <- prepare_provider_sites(dataset_id)
  if (!is.null(provider_sites) && nrow(provider_sites) > 0) {
    provider_sites_sf <- provider_sites %>%
      filter(!is.na(lat), !is.na(lon)) %>%
      sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
    write_gpkg_layer(provider_sites_sf, package_path, paste0(dataset_id, "_sites"), append = TRUE, aspatial = FALSE)
  }

  provider_lines <- prepare_provider_lines(dataset_id)
  if (!is.null(provider_lines) && nrow(provider_lines) > 0) {
    write_gpkg_layer(provider_lines, package_path, paste0(dataset_id, "_lines"), append = TRUE, aspatial = FALSE)
  }
}

background_land <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
label_bbox <- sf::st_bbox(c(
  xmin = focus_bbox$west,
  ymin = focus_bbox$south,
  xmax = focus_bbox$east,
  ymax = focus_bbox$north
), crs = sf::st_crs(background_land))

label_countries <- background_land %>%
  sf::st_crop(label_bbox)

country_labels <- label_countries %>%
  sf::st_transform(3857) %>%
  sf::st_point_on_surface() %>%
  sf::st_transform(4326)

energy_sites_from_package <- sf::st_read(package_path, layer = "energy_sites", quiet = TRUE)
energy_lines_from_package <- if (gpkg_has_layer(package_path, "energy_lines")) {
  sf::st_read(package_path, layer = "energy_lines", quiet = TRUE)
} else {
  empty_energy_lines()
}
candidate_sites_from_package <- if (gpkg_has_layer(package_path, "candidate_sites")) {
  sf::st_read(package_path, layer = "candidate_sites", quiet = TRUE)
} else {
  sf::st_sf(empty_candidate_sites(), geometry = sf::st_sfc(crs = 4326))
}
firms_grid_anomalies_from_package <- if (gpkg_has_layer(package_path, "firms_grid_anomalies")) {
  sf::st_read(package_path, layer = "firms_grid_anomalies", quiet = TRUE)
} else {
  sf::st_sf(empty_firms_grid_daily(), geometry = sf::st_sfc(crs = 4326))
}
firms_anomalous_hotspots_from_package <- if (gpkg_has_layer(package_path, "firms_anomalous_hotspots")) {
  sf::st_read(package_path, layer = "firms_anomalous_hotspots", quiet = TRUE)
} else {
  sf::st_sf(bind_cols(prepare_firms("missing"), empty_firms_grid_daily()[0, c("cell_id", "current_hotspot_count", "absolute_lift", "robust_z_preconflict", "robust_z_seasonal")]), geometry = sf::st_sfc(crs = 4326))
}
firms_facility_anomalies_from_package <- if (gpkg_has_layer(package_path, "firms_facility_anomalies")) {
  sf::st_read(package_path, layer = "firms_facility_anomalies", quiet = TRUE)
} else {
  sf::st_sf(empty_firms_facility_anomalies(), geometry = sf::st_sfc(crs = 4326))
}
firms_hotspots_from_package <- if (gpkg_has_layer(package_path, "firms_hotspots")) {
  sf::st_read(package_path, layer = "firms_hotspots", quiet = TRUE)
} else {
  sf::st_sf(prepare_firms("missing"), geometry = sf::st_sfc(crs = 4326))
}

focus_window <- sf::st_as_sfc(label_bbox)
background_land <- label_countries
energy_sites_from_package <- sf::st_crop(energy_sites_from_package, label_bbox)
energy_lines_from_package <- if (nrow(energy_lines_from_package) > 0) {
  suppressWarnings(sf::st_crop(energy_lines_from_package, label_bbox))
} else {
  energy_lines_from_package
}
candidate_sites_from_package <- if (nrow(candidate_sites_from_package) > 0) {
  sf::st_crop(candidate_sites_from_package, label_bbox)
} else {
  candidate_sites_from_package
}
firms_grid_anomalies_from_package <- if (nrow(firms_grid_anomalies_from_package) > 0) {
  sf::st_crop(firms_grid_anomalies_from_package, label_bbox)
} else {
  firms_grid_anomalies_from_package
}
firms_anomalous_hotspots_from_package <- if (nrow(firms_anomalous_hotspots_from_package) > 0) {
  sf::st_crop(firms_anomalous_hotspots_from_package, label_bbox)
} else {
  firms_anomalous_hotspots_from_package
}
firms_facility_anomalies_from_package <- if (nrow(firms_facility_anomalies_from_package) > 0) {
  sf::st_crop(firms_facility_anomalies_from_package, label_bbox)
} else {
  firms_facility_anomalies_from_package
}
firms_hotspots_from_package <- if (nrow(firms_hotspots_from_package) > 0) {
  sf::st_crop(firms_hotspots_from_package, label_bbox)
} else {
  firms_hotspots_from_package
}
incidents_from_package <- if (nrow(incidents_sf) > 0) {
  sf::st_crop(incidents_sf, label_bbox)
} else {
  incidents_sf
}
site_validation_summary_from_package <- if (nrow(site_validation_summary_sf) > 0) {
  sf::st_crop(site_validation_summary_sf, label_bbox)
} else {
  site_validation_summary_sf
}

line_legend_label <- "Pipeline Network"
site_colors <- asset_registry %>%
  filter(geometry_role_default == "site") %>%
  select(asset_label, map_color)
map_colors <- c(setNames(site_colors$map_color, site_colors$asset_label), setNames("#6f8093", line_legend_label))

map_theme <- theme_minimal() +
  theme(
    panel.background = element_rect(fill = "#b7d9ee", color = NA),
    plot.background = element_rect(fill = "#f6f1e7", color = NA),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    axis.title = element_blank(),
    axis.text = element_text(color = "#5e6b74", size = 8),
    legend.background = element_rect(fill = scales::alpha("#f9f7f1", 0.92), color = "#d7d1c3"),
    legend.key = element_rect(fill = scales::alpha("#f9f7f1", 0), color = NA),
    plot.title = element_text(face = "bold", size = 14, color = "#20313c"),
    plot.subtitle = element_text(size = 10, color = "#36505f")
  )

validation_family_colors <- c(
  "Oil / Liquids" = "#8c510a",
  "Gas / LNG" = "#1b7837",
  "Mixed" = "#5ab4ac",
  "Other / Unknown" = "#7f7f7f"
)
validation_size_values <- c(
  "No output data" = 1.0,
  "Low output" = 1.45,
  "Medium output" = 2.15,
  "High output" = 3.0
)
validation_overlay_fills <- c(
  "Incident-linked site" = "#f7f0e1",
  "Double-Confirmation through FIRMS Thermal Anomaly detection" = "#f26b1d"
)
validation_status_shapes <- c(
  "Incident-linked" = 1,
  "Provisional triple-confirmed" = 4
)

if (map_mode == "infrastructure") {
  if (nrow(energy_lines_from_package) > 0) {
    energy_lines_from_package$map_group <- line_legend_label
  }
  if (nrow(energy_sites_from_package) > 0) {
    energy_sites_from_package$map_group <- energy_sites_from_package$asset_label
  }

  infrastructure_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_lines_from_package) > 0) {
    infrastructure_map <- infrastructure_map +
      geom_sf(
        data = energy_lines_from_package,
        aes(color = map_group),
        linewidth = 0.18,
        alpha = 0.18,
        lineend = "round",
        inherit.aes = FALSE
      )
  }

  if (nrow(energy_sites_from_package) > 0) {
    infrastructure_map <- infrastructure_map +
      geom_sf(
        data = energy_sites_from_package,
        aes(color = map_group),
        size = 1.35,
        alpha = 0.82,
        inherit.aes = FALSE
      )
  }

  infrastructure_map <- infrastructure_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = map_colors, drop = TRUE, na.value = "#666666") +
    labs(
      title = "Gulf Energy Infrastructure",
      subtitle = "Gulf-centered extent with western cutoff at the eastern Saudi field belt",
      color = "Infrastructure class"
    ) +
    map_theme

  print(infrastructure_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$infrastructure_map_pdf), infrastructure_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and infrastructure map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_infrastructure_map.pdf.")
  }
} else if (map_mode == "sites") {
  sites_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE) +
    geom_sf(
      data = energy_sites_from_package,
      aes(color = asset_label),
      size = 1.45,
      alpha = 0.84,
      inherit.aes = FALSE
    ) +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "Gulf Energy Sites",
      subtitle = "Facility-centered Gulf extent with the Saudi eastern field belt at the west edge",
      color = "Infrastructure class"
    ) +
    map_theme

  print(sites_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$facilities_map_pdf), sites_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and site map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_facilities_map.pdf.")
  }
} else if (map_mode == "candidate_sites") {
  candidate_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_sites_from_package) > 0) {
    candidate_map <- candidate_map +
      geom_sf(
        data = energy_sites_from_package,
        color = "#7c878f",
        size = 0.95,
        alpha = 0.18,
        inherit.aes = FALSE
      )
  }

  if (nrow(candidate_sites_from_package) > 0) {
    candidate_map <- candidate_map +
      geom_sf(
        data = candidate_sites_from_package,
        color = "#1f1f1f",
        size = 3.0,
        alpha = 0.9,
        inherit.aes = FALSE
      ) +
      geom_sf(
        data = candidate_sites_from_package,
        aes(color = asset_label),
        size = 1.95,
        alpha = 0.98,
        inherit.aes = FALSE
      )
  }

  candidate_map <- candidate_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "Potentially Targeted Gulf Energy Sites",
      subtitle = "Colored sites are incident-linked candidates: nearest linked site or within 10 km of an incident",
      color = "Candidate class"
    ) +
    map_theme

  print(candidate_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$candidate_sites_map_pdf), candidate_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and candidate-sites map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_candidate_sites_map.pdf.")
  }
} else if (map_mode == "firms_anomaly_grid") {
  firms_anomaly_grid_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_sites_from_package) > 0) {
    firms_anomaly_grid_map <- firms_anomaly_grid_map +
      geom_sf(
        data = energy_sites_from_package,
        color = "#7c878f",
        size = 0.85,
        alpha = 0.14,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_grid_anomalies_from_package) > 0) {
    firms_anomaly_grid_map <- firms_anomaly_grid_map +
      geom_sf(
        data = firms_grid_anomalies_from_package,
        aes(fill = absolute_lift),
        color = "#7f2704",
        linewidth = 0.12,
        alpha = 0.58,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_anomalous_hotspots_from_package) > 0) {
    firms_anomaly_grid_map <- firms_anomaly_grid_map +
      geom_sf(
        data = firms_anomalous_hotspots_from_package,
        color = "#3f0d12",
        size = 0.42,
        alpha = 0.28,
        inherit.aes = FALSE
      )
  }

  firms_anomaly_grid_map <- firms_anomaly_grid_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_fill_gradient(low = "#fdcc8a", high = "#b30000", na.value = "#fdcc8a") +
    labs(
      title = "FIRMS Thermal Anomaly Grid",
      subtitle = paste0(
        settings$firms_grid_km,
        " km cells flagged only when hotspot counts exceed pre-conflict baseline",
        if (nrow(firms_grid_anomalies_from_package) > 0 && any(firms_grid_anomalies_from_package$baseline_mode == "hybrid", na.rm = TRUE)) " and seasonal history" else ""
      ),
      fill = "Hotspot lift"
    ) +
    map_theme

  print(firms_anomaly_grid_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$firms_anomaly_grid_map_pdf), firms_anomaly_grid_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and FIRMS anomaly-grid map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_firms_anomaly_grid_map.pdf.")
  }
} else if (map_mode %in% c("firms_anomaly_sites", "firms_overlay")) {
  firms_anomaly_sites_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_sites_from_package) > 0) {
    firms_anomaly_sites_map <- firms_anomaly_sites_map +
      geom_sf(
        data = energy_sites_from_package,
        color = "#7c878f",
        size = 0.9,
        alpha = 0.16,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_grid_anomalies_from_package) > 0) {
    firms_anomaly_sites_map <- firms_anomaly_sites_map +
      geom_sf(
        data = firms_grid_anomalies_from_package,
        fill = "#f26b1d",
        color = NA,
        alpha = 0.18,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_facility_anomalies_from_package) > 0) {
    firms_anomaly_sites_map <- firms_anomaly_sites_map +
      geom_sf(
        data = firms_facility_anomalies_from_package,
        color = "#1f1f1f",
        size = 3.0,
        alpha = 0.92,
        inherit.aes = FALSE
      ) +
      geom_sf(
        data = firms_facility_anomalies_from_package,
        aes(color = asset_label),
        size = 1.95,
        alpha = 0.98,
        inherit.aes = FALSE
      )
  }

  firms_anomaly_sites_map <- firms_anomaly_sites_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "FIRMS Thermal Anomalies Near Gulf Energy Facilities",
      subtitle = paste0(
        "Facilities are highlighted only when they intersect or fall within ",
        settings$firms_grid_km,
        " km of anomalous FIRMS cells"
      ),
      color = "Facility class"
    ) +
    map_theme

  print(firms_anomaly_sites_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$firms_anomaly_sites_map_pdf), firms_anomaly_sites_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and FIRMS anomaly-sites map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_firms_anomaly_sites_map.pdf.")
  }
} else if (map_mode == "firms_compare") {
  legacy_firms_overlay <- build_legacy_firms_overlay(
    firms = sf::st_drop_geometry(firms_hotspots_from_package),
    energy_sites = sf::st_drop_geometry(energy_sites_from_package),
    settings = settings
  )

  legacy_firms_matches_sf <- if (nrow(legacy_firms_overlay$matches) > 0) {
    legacy_firms_overlay$matches %>%
      filter(!is.na(latitude), !is.na(longitude)) %>%
      sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
  } else {
    sf::st_sf(empty_legacy_firms_site_matches(), geometry = sf::st_sfc(crs = 4326))
  }

  legacy_firms_sites_sf <- if (nrow(legacy_firms_overlay$sites) > 0) {
    legacy_firms_overlay$sites %>%
      filter(!is.na(lat), !is.na(lon)) %>%
      sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  } else {
    sf::st_sf(empty_legacy_firms_matched_sites(), geometry = sf::st_sfc(crs = 4326))
  }

  comparison_theme <- map_theme +
    theme(
      legend.position = "bottom",
      legend.title = element_text(size = 8),
      legend.text = element_text(size = 7),
      plot.title = element_text(face = "bold", size = 12, color = "#20313c"),
      plot.subtitle = element_text(size = 9, color = "#36505f")
    )

  legacy_overlay_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.4, check_overlap = TRUE)

  if (nrow(energy_sites_from_package) > 0) {
    legacy_overlay_map <- legacy_overlay_map +
      geom_sf(
        data = energy_sites_from_package,
        color = "#7c878f",
        size = 0.85,
        alpha = 0.14,
        inherit.aes = FALSE
      )
  }

  if (nrow(legacy_firms_matches_sf) > 0) {
    legacy_overlay_map <- legacy_overlay_map +
      geom_sf(
        data = legacy_firms_matches_sf,
        color = "#f26b1d",
        size = 0.9,
        alpha = 0.58,
        inherit.aes = FALSE
      )
  }

  if (nrow(legacy_firms_sites_sf) > 0) {
    legacy_overlay_map <- legacy_overlay_map +
      geom_sf(
        data = legacy_firms_sites_sf,
        color = "#1f1f1f",
        size = 2.6,
        alpha = 0.9,
        inherit.aes = FALSE
      ) +
      geom_sf(
        data = legacy_firms_sites_sf,
        aes(color = asset_label),
        size = 1.65,
        alpha = 0.98,
        inherit.aes = FALSE
      )
  }

  legacy_overlay_map <- legacy_overlay_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "Legacy Raw FIRMS Overlay",
      subtitle = paste0(
        scales::comma(nrow(legacy_firms_matches_sf)),
        " matched hotspots; ",
        scales::comma(nrow(legacy_firms_sites_sf)),
        " facilities within ",
        settings$firms_site_match_km,
        " km"
      ),
      color = "Facility class"
    ) +
    comparison_theme

  anomaly_compare_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.4, check_overlap = TRUE)

  if (nrow(energy_sites_from_package) > 0) {
    anomaly_compare_map <- anomaly_compare_map +
      geom_sf(
        data = energy_sites_from_package,
        color = "#7c878f",
        size = 0.85,
        alpha = 0.14,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_grid_anomalies_from_package) > 0) {
    anomaly_compare_map <- anomaly_compare_map +
      geom_sf(
        data = firms_grid_anomalies_from_package,
        fill = "#f26b1d",
        color = NA,
        alpha = 0.18,
        inherit.aes = FALSE
      )
  }

  if (nrow(firms_facility_anomalies_from_package) > 0) {
    anomaly_compare_map <- anomaly_compare_map +
      geom_sf(
        data = firms_facility_anomalies_from_package,
        color = "#1f1f1f",
        size = 2.6,
        alpha = 0.9,
        inherit.aes = FALSE
      ) +
      geom_sf(
        data = firms_facility_anomalies_from_package,
        aes(color = asset_label),
        size = 1.65,
        alpha = 0.98,
        inherit.aes = FALSE
      )
  }

  anomaly_compare_map <- anomaly_compare_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "Anomaly-Filtered FIRMS Overlay",
      subtitle = paste0(
        scales::comma(nrow(firms_grid_anomalies_from_package)),
        " anomalous cells; ",
        scales::comma(nrow(firms_facility_anomalies_from_package)),
        " flagged facilities"
      ),
      color = "Facility class"
    ) +
    comparison_theme +
    theme(legend.position = "none")

  if (write_output) {
    save_side_by_side_map_pdf(
      legacy_overlay_map,
      anomaly_compare_map,
      next_numbered_output_path(paths$firms_comparison_map_pdf),
      title = "FIRMS Comparison: Raw Proximity Overlay vs Anomaly Filter"
    )
    message("GeoPackage and FIRMS comparison map written to output/")
  } else {
    draw_side_by_side_maps(
      legacy_overlay_map,
      anomaly_compare_map,
      title = "FIRMS Comparison: Raw Proximity Overlay vs Anomaly Filter"
    )
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_firms_comparison_map.pdf.")
  }
} else if (map_mode == "validation_capacity_labels") {
  label_sites <- select_validation_label_sites(sf::st_drop_geometry(site_validation_summary_from_package))
  label_sites_sf <- if (nrow(label_sites) > 0) {
    label_sites %>%
      sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  } else {
    sf::st_sf(empty_site_validation_summary(), geometry = sf::st_sfc(crs = 4326))
  }
  label_points <- prepare_label_points(label_sites_sf, focus_bbox)
  incident_overlay_sites <- site_validation_summary_from_package %>%
    filter(incident_count > 0) %>%
    mutate(
      overlay_status = if_else(
        firms_anomaly_flag,
        "Double-Confirmation through FIRMS Thermal Anomaly detection",
        "Incident-linked site"
      )
    )

  validation_labels_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.6, check_overlap = TRUE)

  if (nrow(site_validation_summary_from_package) > 0) {
    validation_labels_map <- validation_labels_map +
      geom_sf(
        data = site_validation_summary_from_package,
        aes(color = validation_color_group, size = output_size_bucket),
        alpha = 0.28,
        inherit.aes = FALSE
      )
  }

  if (nrow(incident_overlay_sites) > 0) {
    validation_labels_map <- validation_labels_map +
      geom_sf(
        data = incident_overlay_sites,
        aes(fill = overlay_status),
        shape = 21,
        color = "#1f1f1f",
        size = 3.25,
        stroke = 0.45,
        alpha = 0.68,
        inherit.aes = FALSE
      )
  }

  if (nrow(label_sites_sf) > 0) {
    validation_labels_map <- validation_labels_map +
      geom_sf(
        data = label_sites_sf,
        aes(color = validation_color_group, size = output_size_bucket),
        shape = 16,
        alpha = 0.72,
        inherit.aes = FALSE,
        show.legend = FALSE
      )
  }

  if (nrow(label_points) > 0) {
    if (ggrepel_available) {
      validation_labels_map <- validation_labels_map +
        ggrepel::geom_label_repel(
          data = label_points,
          aes(x = X, y = Y, label = label_text),
          size = 2.5,
          fill = scales::alpha("#fffaf0", 0.96),
          color = "#20313c",
          label.size = 0.18,
          label.padding = grid::unit(0.12, "lines"),
          box.padding = grid::unit(0.25, "lines"),
          point.padding = grid::unit(0.16, "lines"),
          nudge_x = label_points$label_nudge_x,
          hjust = label_points$label_hjust,
          direction = "y",
          segment.color = "#5e6b74",
          segment.alpha = 0.7,
          min.segment.length = 0,
          seed = 42,
          show.legend = FALSE
        )
    } else {
      validation_labels_map <- validation_labels_map +
        geom_label(
          data = label_points,
          aes(x = X, y = Y, label = label_text),
          size = 2.4,
          hjust = 0,
          vjust = 0,
          fill = scales::alpha("#fffaf0", 0.96),
          color = "#20313c",
          label.size = 0.18,
          show.legend = FALSE
        )
    }
  }

  validation_labels_map <- validation_labels_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE,
      clip = "off"
    ) +
    scale_color_manual(values = validation_family_colors, drop = TRUE, na.value = "#7f7f7f") +
    scale_fill_manual(values = validation_overlay_fills, drop = FALSE) +
    scale_size_manual(values = validation_size_values, drop = FALSE) +
    labs(
      title = "Highest-Capacity Incident-Linked Energy Sites",
      subtitle = paste0(
        "Top 25 incident-linked sites by available capacity metric; orange-filled circles indicate Double-Confirmation through FIRMS Thermal Anomaly detection"
      ),
      color = "Commodity family",
      fill = "Incident-site status",
      size = "Output bucket"
    ) +
    map_theme +
    theme(
      legend.position = "bottom",
      legend.box = "vertical",
      plot.margin = margin(12, 120, 12, 120)
    )

  print(validation_labels_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$validation_capacity_labels_map_pdf), validation_labels_map, width = 13, height = 8.5, device = grDevices::pdf)
    message("GeoPackage and validation capacity-labels map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and incident_validation_capacity_labels_map.pdf.")
  }
} else {
  suppressPackageStartupMessages(library(ggrepel))

  # Tighter Gulf corridor bbox
  val_bbox <- list(west = 44, south = 21, east = 60, north = 32)

  # Re-crop data to the tighter validation bbox
  val_label_bbox <- sf::st_bbox(c(
    xmin = val_bbox$west, ymin = val_bbox$south,
    xmax = val_bbox$east, ymax = val_bbox$north
  ), crs = sf::st_crs(4326))

  val_land <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") %>%
    sf::st_crop(val_label_bbox)
  val_country_labels <- val_land %>%
    sf::st_transform(3857) %>%
    sf::st_point_on_surface() %>%
    sf::st_transform(4326)
  val_sites <- if (nrow(site_validation_summary_sf) > 0) {
    sf::st_crop(site_validation_summary_sf, val_label_bbox)
  } else {
    site_validation_summary_sf
  }
  val_incidents <- if (nrow(incidents_sf) > 0) {
    sf::st_crop(incidents_sf, val_label_bbox)
  } else {
    incidents_sf
  }

  # Two-channel encoding: fill = verification status, size = capacity_kbd (continuous)
  if (nrow(val_sites) > 0) {
    val_sites <- val_sites %>%
      mutate(
        verification_status = case_when(
          incident_count > 0 & firms_anomaly_flag ~ "FIRMS double-confirmed",
          incident_count > 0 ~ "Incident-linked",
          TRUE ~ "Unlinked infrastructure"
        ),
        size_value = dplyr::coalesce(capacity_kbd, production_kbd, 0)
      )
  }

  n_incident_links <- if (nrow(val_sites) > 0) sum(val_sites$incident_count > 0, na.rm = TRUE) else 0L
  n_firms_confirmed <- if (nrow(val_sites) > 0) sum(val_sites$incident_count > 0 & val_sites$firms_anomaly_flag, na.rm = TRUE) else 0L

  verification_fills <- c(
    "FIRMS double-confirmed" = "#a50f15",
    "Incident-linked" = "#e6550d",
    "Unlinked infrastructure" = "#bdbdbd"
  )

  validation_map <- ggplot() +
    geom_sf(data = val_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = val_country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(val_sites) > 0) {
    # Background: unlinked sites (grey, small)
    unlinked <- val_sites %>% filter(verification_status == "Unlinked infrastructure")
    if (nrow(unlinked) > 0) {
      validation_map <- validation_map +
        geom_sf(
          data = unlinked,
          fill = "#bdbdbd", color = "#999999",
          shape = 21, size = 0.6, alpha = 0.25, stroke = 0.15,
          inherit.aes = FALSE, show.legend = FALSE
        )
    }

    # Foreground: incident-linked and FIRMS-confirmed sites
    linked <- val_sites %>% filter(verification_status != "Unlinked infrastructure")
    if (nrow(linked) > 0) {
      validation_map <- validation_map +
        geom_sf(
          data = linked,
          aes(fill = verification_status, size = size_value),
          shape = 21, color = "#202020", stroke = 0.4, alpha = 0.78,
          inherit.aes = FALSE
        )
    }
  }

  # Incident points as small dark crosses
  if (nrow(val_incidents) > 0) {
    validation_map <- validation_map +
      geom_sf(
        data = val_incidents %>% mutate(incident_layer = "Strike location"),
        aes(shape = incident_layer),
        color = "#1e252b", size = 0.7, alpha = 0.4, stroke = 0.4,
        inherit.aes = FALSE
      ) +
      scale_shape_manual(values = c("Strike location" = 3), name = NULL)
  }

  # Label ALL incident-linked targets
  if (nrow(val_sites) > 0) {
    label_candidates <- val_sites %>%
      filter(
        incident_count > 0,
        !is.na(name), name != "Unnamed site", name != ""
      )

    if (nrow(label_candidates) > 0) {
      label_coords <- sf::st_coordinates(label_candidates)
      label_df <- bind_cols(
        sf::st_drop_geometry(label_candidates),
        tibble(X = label_coords[, "X"], Y = label_coords[, "Y"])
      )

      validation_map <- validation_map +
        geom_text_repel(
          data = label_df,
          aes(x = X, y = Y, label = name),
          size = 1.8, color = "#20313c", fontface = "bold",
          max.overlaps = 40,
          segment.size = 0.2, segment.color = "#888888",
          box.padding = 0.25, point.padding = 0.15,
          min.segment.length = 0.1,
          force = 2, force_pull = 0.5,
          inherit.aes = FALSE
        )
    }
  }

  validation_map <- validation_map +
    coord_sf(
      xlim = c(val_bbox$west, val_bbox$east),
      ylim = c(val_bbox$south, val_bbox$north),
      expand = FALSE
    ) +
    scale_fill_manual(values = verification_fills, drop = FALSE) +
    scale_size_continuous(
      range = c(1.5, 8), breaks = c(100, 300, 500, 900),
      labels = function(x) paste0(x, " kbd"),
      guide = guide_legend(override.aes = list(fill = "#e6550d", shape = 21))
    ) +
    labs(
      title = "Gulf Energy Infrastructure: Incident Validation",
      subtitle = paste0(
        "Orange = incident-linked | Dark red = FIRMS corroboration | Size ~ capacity (kbd) | ",
        n_incident_links, " incident-site links, ", n_firms_confirmed, " FIRMS-confirmed"
      ),
      fill = "Verification status",
      size = "Capacity"
    ) +
    map_theme +
    theme(
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.margin = margin(t = 4, r = 8, b = 4, l = 8),
      legend.spacing.x = unit(8, "pt"),
      legend.text = element_text(size = 7.5, margin = margin(l = 2, r = 4)),
      legend.title = element_text(size = 8, face = "bold"),
      legend.key.size = unit(14, "pt"),
      plot.subtitle = element_text(size = 8.5, color = "#36505f")
    )

  print(validation_map)

  if (write_output) {
    ggsave(next_numbered_output_path(paths$validation_map_pdf), validation_map, width = 12, height = 8, device = grDevices::pdf)
    message("GeoPackage and validation map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and incident_validation_map.pdf.")
  }
}
