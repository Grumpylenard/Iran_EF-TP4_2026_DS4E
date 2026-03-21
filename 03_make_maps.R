source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
})

# Manual controls.
map_mode <- Sys.getenv("MAP_MODE", unset = "infrastructure")   # "infrastructure", "sites", or "validation"
map_source <- Sys.getenv("MAP_SOURCE", unset = "review")       # "review" or "confirmed"
write_output <- env_flag("WRITE_OUTPUT", FALSE)

settings <- project_settings()
asset_registry <- build_asset_class_registry()
provider_registry <- build_provider_registry()

paths <- list(
  review_csv = project_path("data_processed", "incidents_review.csv"),
  confirmed_csv = project_path("data_processed", "incidents_confirmed.csv"),
  firms_csv = project_path("data_processed", "firms_hotspots_raw.csv"),
  gdelt_csv = project_path("data_processed", "gdelt_articles_raw.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  energy_lines_geojson = project_path("data_processed", "energy_lines.geojson"),
  site_links_csv = project_path("data_processed", "incident_site_links.csv"),
  firms_links_csv = project_path("data_processed", "incident_firms_links.csv"),
  article_links_csv = project_path("data_processed", "incident_article_links.csv"),
  provider_log_csv = project_path("data_processed", "provider_intake_log.csv"),
  map_gpkg = project_path("output", "map_inputs.gpkg"),
  infrastructure_map_pdf = project_path("output", "energy_infrastructure_map.pdf"),
  facilities_map_pdf = project_path("output", "energy_facilities_map.pdf"),
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

empty_firms_links <- function() {
  tibble(
    incident_id = character(),
    hotspot_id = character(),
    acq_date = character(),
    latitude = double(),
    longitude = double(),
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
  bind_rows(
    tibble(
      hotspot_id = character(),
      acq_date = character(),
      latitude = double(),
      longitude = double()
    ),
    read_csv_if_exists(path)
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

prepare_firms_links <- function(path) {
  bind_rows(
    empty_firms_links(),
    read_csv_if_exists(path) %>%
      mutate(
        acq_date = as.character(acq_date),
        latitude = as.numeric(latitude),
        longitude = as.numeric(longitude),
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

package_path <- if (write_output) paths$map_gpkg else tempfile(fileext = ".gpkg")

incidents <- if (map_source == "confirmed") {
  prepare_incidents(paths$confirmed_csv)
} else {
  prepare_incidents(paths$review_csv)
}
firms <- prepare_firms(paths$firms_csv)
gdelt <- prepare_gdelt(paths$gdelt_csv)
energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
energy_lines <- prepare_energy_lines(paths$energy_lines_geojson)
incident_site_links <- prepare_site_links(paths$site_links_csv)
incident_firms_links <- prepare_firms_links(paths$firms_links_csv)
incident_article_links <- prepare_article_links(paths$article_links_csv)
provider_log <- prepare_provider_log(paths$provider_log_csv)

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
write_gpkg_layer(gdelt, package_path, "gdelt_articles", append = TRUE, aspatial = TRUE)
write_gpkg_layer(incident_site_links, package_path, "incident_site_links", append = TRUE, aspatial = TRUE)
write_gpkg_layer(incident_firms_links, package_path, "incident_firms_links", append = TRUE, aspatial = TRUE)
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
  xmin = settings$bbox$west,
  ymin = settings$bbox$south,
  xmax = settings$bbox$east,
  ymax = settings$bbox$north
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

line_legend_label <- "Pipeline / Linear Infrastructure"
site_colors <- asset_registry %>%
  filter(geometry_role_default == "site") %>%
  select(asset_label, map_color)
map_colors <- c(setNames(site_colors$map_color, site_colors$asset_label), setNames("#6a51a3", line_legend_label))

if (map_mode == "infrastructure") {
  if (nrow(energy_lines_from_package) > 0) {
    energy_lines_from_package$map_group <- line_legend_label
  }
  if (nrow(energy_sites_from_package) > 0) {
    energy_sites_from_package$map_group <- energy_sites_from_package$asset_label
  }

  infrastructure_map <- ggplot() +
    geom_sf(data = background_land, fill = "#f5f2ea", color = "#d7d1c3", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#6c6657", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_lines_from_package) > 0) {
    infrastructure_map <- infrastructure_map +
      geom_sf(
        data = energy_lines_from_package,
        aes(color = map_group),
        linewidth = 0.35,
        alpha = 0.45,
        inherit.aes = FALSE
      )
  }

  if (nrow(energy_sites_from_package) > 0) {
    infrastructure_map <- infrastructure_map +
      geom_sf(
        data = energy_sites_from_package,
        aes(color = map_group),
        size = 1.5,
        alpha = 0.85,
        inherit.aes = FALSE
      )
  }

  infrastructure_map <- infrastructure_map +
    coord_sf(
      xlim = c(settings$bbox$west, settings$bbox$east),
      ylim = c(settings$bbox$south, settings$bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = map_colors, drop = TRUE, na.value = "#666666") +
    labs(
      title = "Export-Relevant Energy Infrastructure",
      subtitle = "OGIM / GEM compiled GeoPackage",
      color = "Infrastructure class"
    ) +
    theme_minimal()

  print(infrastructure_map)

  if (write_output) {
    ggsave(paths$infrastructure_map_pdf, infrastructure_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and infrastructure map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_infrastructure_map.pdf.")
  }
} else if (map_mode == "sites") {
  sites_map <- ggplot() +
    geom_sf(data = background_land, fill = "#f5f2ea", color = "#d7d1c3", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#6c6657", size = 2.8, check_overlap = TRUE) +
    geom_sf(
      data = energy_sites_from_package,
      aes(color = asset_label),
      size = 1.6,
      alpha = 0.8,
      inherit.aes = FALSE
    ) +
    coord_sf(
      xlim = c(settings$bbox$west, settings$bbox$east),
      ylim = c(settings$bbox$south, settings$bbox$north),
      expand = FALSE
    ) +
    scale_color_manual(values = setNames(site_colors$map_color, site_colors$asset_label), drop = TRUE, na.value = "#666666") +
    labs(
      title = "Export-Relevant Energy Sites",
      subtitle = "Point / centroid infrastructure layer from the compiled GeoPackage",
      color = "Infrastructure class"
    ) +
    theme_minimal()

  print(sites_map)

  if (write_output) {
    ggsave(paths$facilities_map_pdf, sites_map, width = 11, height = 7, device = grDevices::pdf)
    message("GeoPackage and site map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and energy_facilities_map.pdf.")
  }
} else {
  incidents_from_package <- sf::st_read(package_path, layer = "incidents", quiet = TRUE)
  firms_from_package <- sf::st_read(package_path, layer = "firms_hotspots", quiet = TRUE)

  validation_map <- ggplot() +
    geom_sf(data = background_land, fill = "#f5f2ea", color = "#d7d1c3", linewidth = 0.2) +
    geom_sf_text(data = country_labels, aes(label = admin), color = "#6c6657", size = 2.8, check_overlap = TRUE)

  if (nrow(energy_lines_from_package) > 0) {
    validation_map <- validation_map +
      geom_sf(data = energy_lines_from_package, color = "#6a51a3", linewidth = 0.25, alpha = 0.25)
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
      xlim = c(settings$bbox$west, settings$bbox$east),
      ylim = c(settings$bbox$south, settings$bbox$north),
      expand = FALSE
    ) +
    labs(
      title = "Incident Validation Map",
      subtitle = paste("Source:", map_source),
      shape = "Confidence tier"
    ) +
    theme_minimal()

  print(validation_map)

  if (write_output) {
    ggsave(paths$validation_map_pdf, validation_map, width = 10, height = 7, device = grDevices::pdf)
    message("GeoPackage and validation map written to output/")
  } else {
    message("Preview only. Set WRITE_OUTPUT=TRUE to save map_inputs.gpkg and incident_validation_map.pdf.")
  }
}
