source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(sf)
  library(stringr)
})

# Manual controls.
write_output <- env_flag("WRITE_OUTPUT", FALSE)
preview_n <- 10

settings <- project_settings()
asset_registry <- build_asset_class_registry()
provider_registry <- build_provider_registry()

paths <- list(
  tp4_csv = project_path("data_processed", "tp4_incidents_raw.csv"),
  gdelt_csv = project_path("data_processed", "gdelt_articles_raw.csv"),
  review_csv = project_path("data_processed", "incidents_review.csv"),
  confirmed_csv = project_path("data_processed", "incidents_confirmed.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  energy_lines_geojson = project_path("data_processed", "energy_lines.geojson"),
  site_links_csv = project_path("data_processed", "incident_site_links.csv"),
  article_links_csv = project_path("data_processed", "incident_article_links.csv"),
  legacy_firms_links_csv = project_path("data_processed", "incident_firms_links.csv")
)

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

coerce_provider_sites_schema <- function(data) {
  if (nrow(data) == 0) return(empty_provider_sites())

  data %>%
    mutate(
      across(
        c(
          provider, source_dataset, source_file, source_layer, source_id,
          asset_class, asset_label, provider_class, subclass, name,
          status, country, geometry_role
        ),
        as.character
      ),
      provider_priority = as.integer(provider_priority),
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    )
}

coerce_provider_lines_schema <- function(data) {
  if (nrow(data) == 0) return(empty_provider_lines())

  data %>%
    mutate(
      across(
        c(
          provider, source_dataset, source_file, source_layer, source_id,
          asset_class, asset_label, provider_class, subclass, name,
          status, country, geometry_role
        ),
        as.character
      ),
      provider_priority = as.integer(provider_priority)
    )
}

load_provider_sites <- function(registry) {
  datasets <- registry$source_dataset
  bind_rows(c(
    list(empty_provider_sites()),
    lapply(datasets, function(dataset_id) {
      path <- provider_output_paths(dataset_id)$site_csv
      if (!file.exists(path)) return(tibble())
      coerce_provider_sites_schema(read_csv_if_exists(path))
    })
  ))
}

load_provider_lines <- function(registry) {
  layers <- lapply(registry$source_dataset, function(dataset_id) {
    path <- provider_output_paths(dataset_id)$line_geojson
    if (!file.exists(path)) return(NULL)
    coerce_provider_lines_schema(read_sf_if_exists(path))
  })
  layers <- Filter(Negate(is.null), layers)

  if (length(layers) == 0) {
    return(empty_energy_lines())
  }

  do.call(rbind, c(list(empty_provider_lines()), layers))
}

merge_energy_sites <- function(provider_sites, asset_registry) {
  if (nrow(provider_sites) == 0) {
    return(empty_energy_sites())
  }

  merged <- provider_sites %>%
    mutate(
      dedupe_name = normalize_key_text(name),
      lat_bucket = ifelse(is.na(lat), NA_character_, sprintf("%.2f", round(lat, 2))),
      lon_bucket = ifelse(is.na(lon), NA_character_, sprintf("%.2f", round(lon, 2))),
      merge_key = ifelse(
        dedupe_name == "missing" | is.na(lat_bucket) | is.na(lon_bucket),
        paste(provider, source_dataset, source_id, sep = "|"),
        paste(asset_class, dedupe_name, lat_bucket, lon_bucket, sep = "|")
      )
    ) %>%
    arrange(provider_priority, source_dataset, source_id) %>%
    group_by(merge_key) %>%
    summarise(
      asset_class = first_non_missing_chr(asset_class),
      name = first_non_missing_chr(name),
      status = first_non_missing_chr(status),
      country = first_non_missing_chr(country),
      lat = first_non_missing_num(lat),
      lon = first_non_missing_num(lon),
      geometry_role = "site",
      primary_provider = first_non_missing_chr(provider),
      primary_dataset = first_non_missing_chr(source_dataset),
      provider_priority = as.integer(first_non_missing_num(provider_priority)),
      provider_sources = collapse_values(provider),
      source_datasets = collapse_values(source_dataset),
      source_ids = collapse_values(source_id),
      provider_classes = collapse_values(provider_class),
      subclasses = collapse_values(subclass),
      record_count = dplyr::n(),
      .groups = "drop"
    ) %>%
    left_join(asset_registry %>% select(asset_class, asset_label, asset_rank, map_color), by = "asset_class") %>%
    arrange(asset_rank, provider_priority, name) %>%
    mutate(
      site_id = paste0("site_", row_number()),
      name = dplyr::coalesce(name, paste(asset_label, site_id))
    ) %>%
    select(
      site_id,
      asset_class,
      asset_label,
      asset_rank,
      map_color,
      name,
      status,
      country,
      lat,
      lon,
      geometry_role,
      primary_provider,
      primary_dataset,
      provider_priority,
      provider_sources,
      source_datasets,
      source_ids,
      provider_classes,
      subclasses,
      record_count
    )

  bind_rows(empty_energy_sites(), merged)
}

merge_energy_lines <- function(provider_lines, asset_registry) {
  if (nrow(provider_lines) == 0) {
    return(empty_energy_lines())
  }

  line_tbl <- provider_lines %>%
    sf::st_drop_geometry() %>%
    mutate(
      asset_class = dplyr::coalesce(asset_class, "pipeline")
    ) %>%
    left_join(
      asset_registry %>% select(asset_class, registry_asset_label = asset_label, asset_rank, map_color),
      by = "asset_class"
    ) %>%
    mutate(
      asset_label = dplyr::coalesce(asset_label, registry_asset_label)
    ) %>%
    select(-registry_asset_label) %>%
    arrange(provider_priority, source_dataset, source_id) %>%
    distinct(provider, source_dataset, source_id, .keep_all = TRUE) %>%
    mutate(
      line_id = paste0("line_", row_number()),
      geometry_role = "line",
      primary_provider = provider,
      primary_dataset = source_dataset,
      provider_sources = provider,
      source_datasets = source_dataset,
      source_ids = source_id,
      provider_classes = provider_class,
      subclasses = subclass,
      name = dplyr::coalesce(name, paste(asset_label, line_id))
    ) %>%
    select(
      line_id,
      asset_class,
      asset_label,
      asset_rank,
      map_color,
      name,
      status,
      country,
      geometry_role,
      primary_provider,
      primary_dataset,
      provider_priority,
      provider_sources,
      source_datasets,
      source_ids,
      provider_classes,
      subclasses
    )

  sf::st_sf(line_tbl, geometry = sf::st_geometry(provider_lines), crs = 4326)
}

split_search_terms <- function(text) {
  values <- trimws(unlist(strsplit(text %||% "", "\\s*\\|\\s*")))
  values[values != ""]
}

title_matches_terms <- function(title_text, terms) {
  if (length(terms) == 0) return(TRUE)
  any(vapply(terms, function(term) str_detect(title_text, fixed(str_to_lower(term))), logical(1)))
}

match_gdelt_articles <- function(incident_row, gdelt_articles) {
  if (nrow(gdelt_articles) == 0) {
    return(empty_article_links())
  }

  incident_date <- as.Date(incident_row$incident_day)
  country_terms <- split_search_terms(incident_row$target_country_guess)
  target_terms <- split_search_terms(str_sub(incident_row$target_text %||% "", 1, 40))

  matches <- gdelt_articles %>%
    filter(!is.na(seen_date), abs(as.numeric(seen_date - incident_date)) <= 1)

  if (length(country_terms) > 0) {
    matches <- matches[vapply(matches$title_text, title_matches_terms, logical(1), terms = country_terms), ]
  }
  if (length(target_terms) > 0) {
    matches <- matches[vapply(matches$title_text, title_matches_terms, logical(1), terms = target_terms), ]
  }

  matches %>%
    distinct(article_key, .keep_all = TRUE) %>%
    arrange(seen_date, title) %>%
    mutate(
      incident_id = incident_row$incident_id,
      article_date = as.character(seen_date),
      link_rank = row_number()
    ) %>%
    transmute(
      incident_id,
      article_key,
      article_date,
      query_id,
      title,
      url,
      domain,
      link_rank
    )
}

match_energy_sites <- function(incident_row, energy_sites, settings) {
  if (nrow(energy_sites) == 0 || is.na(incident_row$target_lat) || is.na(incident_row$target_lon)) {
    return(empty_site_links())
  }

  distances <- haversine_km(incident_row$target_lat, incident_row$target_lon, energy_sites$lat, energy_sites$lon)

  energy_sites %>%
    mutate(distance_km = distances) %>%
    filter(is.finite(distance_km), distance_km <= settings$incident_site_match_km) %>%
    arrange(distance_km, asset_rank, name) %>%
    mutate(
      incident_id = incident_row$incident_id,
      link_rank = row_number()
    ) %>%
    transmute(
      incident_id,
      site_id,
      site_name = name,
      asset_class,
      asset_label,
      primary_provider,
      distance_km,
      link_rank
    )
}

tp4 <- read_csv_if_exists(paths$tp4_csv)
gdelt <- bind_rows(
  tibble(
    query_id = character(),
    seendate = character(),
    title = character(),
    url = character(),
    domain = character()
  ),
  read_csv_if_exists(paths$gdelt_csv)
  ) %>%
  mutate(
    article_key = make_article_key(query_id, seendate, url, title),
    seen_date = as.Date(substr(seendate, 1, 8), format = "%Y%m%d"),
    title_text = str_to_lower(title %||% "")
  )

provider_sites <- load_provider_sites(provider_registry)
provider_lines <- load_provider_lines(provider_registry)
energy_sites <- merge_energy_sites(provider_sites, asset_registry)
energy_lines <- merge_energy_lines(provider_lines, asset_registry)

if (nrow(tp4) == 0) {
  stop("No TP4 incident staging file found. Run 01_ingest_sources.R with USE_LOCAL_TP4=TRUE and WRITE_OUTPUT=TRUE.")
}

if (nrow(energy_sites) + nrow(energy_lines) == 0) {
  stop("No provider-normalized infrastructure layers were found. Run 01_ingest_sources.R with OGIM/GEM data or ENABLE_OSM_GAPFILL=TRUE.")
}

bundles <- lapply(seq_len(nrow(tp4)), function(i) {
  row <- tp4[i, ]
  article_links <- match_gdelt_articles(row, gdelt)
  site_links <- match_energy_sites(row, energy_sites, settings)

  nearest_site <- site_links %>% slice_head(n = 1)
  nearest_site_name <- if (nrow(nearest_site) == 0) NA_character_ else nearest_site$site_name[[1]]
  nearest_site_class <- if (nrow(nearest_site) == 0) NA_character_ else nearest_site$asset_label[[1]]
  nearest_site_provider <- if (nrow(nearest_site) == 0) NA_character_ else nearest_site$primary_provider[[1]]
  nearest_site_distance <- if (nrow(nearest_site) == 0) NA_real_ else nearest_site$distance_km[[1]]

  article_urls <- unique(article_links$url[!is.na(article_links$url) & article_links$url != ""])
  energy_candidate <- isTRUE(row$target_energy_flag) ||
    isTRUE(row$target_port_flag) ||
    isTRUE(row$target_industrial_flag) ||
    flag_energy_text(paste(row$target_text, row$description, nearest_site_class))

  confidence_tier <- case_when(
    energy_candidate && nrow(site_links) > 0 && nrow(article_links) > 0 ~ "high",
    energy_candidate ~ "medium",
    TRUE ~ "low"
  )

  list(
    review = tibble(
      incident_id = row$incident_id,
      incident_day = row$incident_day,
      wave_number = row$wave_number,
      target_country = row$target_country_guess,
      target_text = row$target_text,
      weapon_payload = row$weapon_payload,
      ballistic_flag = row$ballistic_flag,
      drone_flag = row$drone_flag,
      target_energy_flag = row$target_energy_flag,
      energy_candidate_flag = energy_candidate,
      lat = row$target_lat,
      lon = row$target_lon,
      source_osint = TRUE,
      source_gdelt_count = nrow(article_links),
      gdelt_urls = if (length(article_urls) == 0) NA_character_ else paste(article_urls, collapse = " | "),
      firms_match_flag = NA,
      firms_distance_km = NA_real_,
      firms_hotspot_date = NA_character_,
      energy_site_name = nearest_site_name,
      energy_site_class = nearest_site_class,
      energy_site_provider = nearest_site_provider,
      energy_site_distance_km = nearest_site_distance,
      confidence_tier = confidence_tier,
      review_status = "needs_review"
    ),
    site_links = site_links,
    article_links = article_links
  )
})

incidents_review <- bind_rows(lapply(bundles, `[[`, "review"))
incidents_confirmed <- incidents_review %>%
  filter(review_status %in% c("approved", "approved_high"))

incident_site_links <- bind_rows(c(list(empty_site_links()), lapply(bundles, `[[`, "site_links"))) %>%
  filter(!is.na(incident_id), !is.na(site_id))

incident_article_links <- bind_rows(c(list(empty_article_links()), lapply(bundles, `[[`, "article_links"))) %>%
  filter(!is.na(incident_id), !is.na(article_key))

message("Energy sites: ", nrow(energy_sites))
message("Energy lines: ", nrow(energy_lines))
message("Incident review rows: ", nrow(incidents_review))
print(utils::head(incidents_review, preview_n))
message("Incident-site links: ", nrow(incident_site_links))
message("Incident-GDELT links: ", nrow(incident_article_links))

if (write_output) {
  write_csv_safe(incidents_review, paths$review_csv)
  write_csv_safe(incidents_confirmed, paths$confirmed_csv)
  write_csv_safe(energy_sites, paths$energy_sites_csv)
  if (nrow(energy_lines) > 0) {
    write_sf_safe(energy_lines, paths$energy_lines_geojson)
  } else if (file.exists(paths$energy_lines_geojson)) {
    unlink(paths$energy_lines_geojson)
  }
  write_csv_safe(incident_site_links, paths$site_links_csv)
  write_csv_safe(incident_article_links, paths$article_links_csv)
  if (file.exists(paths$legacy_firms_links_csv)) {
    unlink(paths$legacy_firms_links_csv)
  }
  message("Incident and infrastructure outputs written to data_processed/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save energy_sites, energy_lines, and incident link tables.")
}
