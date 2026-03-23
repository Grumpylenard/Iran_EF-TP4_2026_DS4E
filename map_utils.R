# map_utils.R
# Shared mapping utilities for the three FINAL map scripts.
# Extracted from 03_make_maps.R to avoid duplication.
# Each FINAL script does: source("utils.R"); source("map_utils.R")

# ── Empty data templates ─────────────────────────────────────────────────────

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

# ── Utility map functions ────────────────────────────────────────────────────

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

# ── Data preparation functions ───────────────────────────────────────────────

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

# ── Helper functions ─────────────────────────────────────────────────────────

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

# ── Core analysis: site validation summary ───────────────────────────────────

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

# ── Theme and color factories ────────────────────────────────────────────────
# Wrapped in functions to avoid ggplot2 load-order issues when sourcing.

make_map_theme <- function() {
  theme_minimal() +
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
}

make_verification_fills <- function() {
  c(
    "FIRMS double-confirmed" = "#a50f15",
    "Incident-linked" = "#e6550d",
    "Unlinked infrastructure" = "#bdbdbd"
  )
}

# ── Background map helpers ──────────────────────────────────────────────────

prepare_background_map <- function(bbox) {
  label_bbox <- sf::st_bbox(c(
    xmin = bbox$west, ymin = bbox$south,
    xmax = bbox$east, ymax = bbox$north
  ), crs = sf::st_crs(4326))

  land <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") %>%
    sf::st_crop(label_bbox)

  country_labels <- land %>%
    sf::st_transform(3857) %>%
    sf::st_point_on_surface() %>%
    sf::st_transform(4326)

  list(land = land, country_labels = country_labels, bbox = label_bbox)
}
