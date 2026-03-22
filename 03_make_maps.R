source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
})

# Manual controls.
map_mode <- Sys.getenv("MAP_MODE", unset = "infrastructure")   # "infrastructure", "sites", "candidate_sites", "firms_anomaly_grid", "firms_anomaly_sites", "firms_compare", or "validation"
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
  validation_map_pdf = project_path("output", "incident_validation_map.pdf")
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

package_path <- if (write_output) paths$map_gpkg else tempfile(fileext = ".gpkg")

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
gdelt <- prepare_gdelt(paths$gdelt_csv)
energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
energy_lines <- prepare_energy_lines(paths$energy_lines_geojson)
incident_site_links <- prepare_site_links(paths$site_links_csv)
incident_article_links <- prepare_article_links(paths$article_links_csv)
provider_log <- prepare_provider_log(paths$provider_log_csv)
candidate_sites <- build_candidate_sites(incident_site_links, incidents, energy_sites)
firms_anomalous_hotspots <- build_firms_anomalous_hotspots(firms, firms_grid_anomalies, settings)

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
    ggsave(paths$infrastructure_map_pdf, infrastructure_map, width = 11, height = 7, device = grDevices::pdf)
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
    ggsave(paths$facilities_map_pdf, sites_map, width = 11, height = 7, device = grDevices::pdf)
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
    ggsave(paths$candidate_sites_map_pdf, candidate_map, width = 11, height = 7, device = grDevices::pdf)
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
    ggsave(paths$firms_anomaly_grid_map_pdf, firms_anomaly_grid_map, width = 11, height = 7, device = grDevices::pdf)
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
    ggsave(paths$firms_anomaly_sites_map_pdf, firms_anomaly_sites_map, width = 11, height = 7, device = grDevices::pdf)
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
      paths$firms_comparison_map_pdf,
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
} else {
  incidents_from_package <- sf::st_read(package_path, layer = "incidents", quiet = TRUE)
  firms_from_package <- sf::st_read(package_path, layer = "firms_hotspots", quiet = TRUE)
  incidents_from_package <- sf::st_crop(incidents_from_package, label_bbox)
  firms_from_package <- sf::st_crop(firms_from_package, label_bbox)

  validation_map <- ggplot() +
    geom_sf(data = background_land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_lines_from_package) > 0) {
    validation_map <- validation_map +
      geom_sf(data = energy_lines_from_package, color = "#6f8093", linewidth = 0.18, alpha = 0.16, lineend = "round")
  }
  if (nrow(energy_sites_from_package) > 0) {
    validation_map <- validation_map +
      geom_sf(data = energy_sites_from_package, color = "#2b8cbe", size = 0.8, alpha = 0.35)
  }
  if (nrow(firms_from_package) > 0) {
    validation_map <- validation_map +
      geom_sf(data = firms_from_package, color = "#d95f02", size = 1.1, alpha = 0.55)
  }
  if (nrow(incidents_from_package) > 0) {
    validation_map <- validation_map +
      geom_sf(data = incidents_from_package, aes(shape = confidence_tier), color = "#1b9e77", size = 2.3)
  }

  validation_map <- validation_map +
    coord_sf(
      xlim = c(focus_bbox$west, focus_bbox$east),
      ylim = c(focus_bbox$south, focus_bbox$north),
      expand = FALSE
    ) +
    labs(
      title = "Incident Validation Map",
      subtitle = paste("Source:", map_source),
      shape = "Confidence tier"
    ) +
    map_theme

  print(validation_map)

  if (write_output) {
    ggsave(paths$validation_map_pdf, validation_map, width = 10, height = 7, device = grDevices::pdf)
    message("GeoPackage and validation map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and incident_validation_map.pdf.")
  }
}
