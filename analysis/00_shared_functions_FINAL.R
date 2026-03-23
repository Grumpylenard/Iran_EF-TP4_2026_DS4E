suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(lubridate)
  library(ggplot2)
  library(forcats)
  library(scales)
})

# Shared helpers for the stand-alone capacity-at-risk analysis workflow.
# Assumption: scripts are executed with Rscript from anywhere, and locate the
# repo root from the script path rather than the working directory.

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

get_script_path <- function() {
  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg) == 0) {
    stop("Unable to determine the running script path. Run these files with Rscript.")
  }
  normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/", mustWork = TRUE)
}

get_repo_root <- function() {
  normalizePath(file.path(dirname(get_script_path()), ".."), winslash = "/", mustWork = TRUE)
}

get_analysis_paths <- function(repo_root = get_repo_root()) {
  analysis_output <- file.path(repo_root, "analysis_output")

  list(
    repo_root = repo_root,
    analysis_output = analysis_output,
    incidents_review = file.path(repo_root, "data_processed", "incidents_review.csv"),
    incident_site_links = file.path(repo_root, "data_processed", "incident_site_links.csv"),
    energy_sites = file.path(repo_root, "data_processed", "energy_sites.csv"),
    ogim_sites_raw = file.path(repo_root, "data_processed", "ogim_sites_raw.csv"),
    ggit_sites_raw = file.path(repo_root, "data_processed", "ggit_sites_raw.csv"),
    goget_sites_raw = file.path(repo_root, "data_processed", "goget_sites_raw.csv"),
    site_capacity_lookup = file.path(repo_root, "data_manual", "site_capacity_lookup.csv"),
    site_capacity_base = file.path(analysis_output, "site_capacity_base.csv"),
    site_capacity_match_diagnostics = file.path(analysis_output, "site_capacity_match_diagnostics.csv"),
    incident_site_capacity = file.path(analysis_output, "incident_site_capacity.csv"),
    site_day_capacity_risk = file.path(analysis_output, "site_day_capacity_risk.csv"),
    country_capacity_summary_oil = file.path(analysis_output, "country_capacity_summary_oil.csv"),
    country_capacity_summary_gas = file.path(analysis_output, "country_capacity_summary_gas.csv"),
    daily_capacity_risk_summary = file.path(analysis_output, "daily_capacity_risk_summary.csv"),
    fig_capacity_risk_by_country_combined = file.path(analysis_output, "fig_capacity_risk_by_country_combined_01.png"),
    fig_capacity_risk_timeseries = file.path(analysis_output, "fig_capacity_risk_timeseries_02.png")
  )
}

ensure_output_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  invisible(path)
}

empty_tbl <- function(...) {
  tibble(...)
}

ensure_columns <- function(tbl, columns) {
  missing_cols <- setdiff(columns, names(tbl))
  if (length(missing_cols) > 0) {
    for (col in missing_cols) {
      tbl[[col]] <- NA_character_
    }
  }
  tbl
}

normalize_country <- function(x) {
  x %>%
    as.character() %>%
    stringr::str_replace_all("[\r\n\t]+", " ") %>%
    stringr::str_squish() %>%
    toupper() %>%
    na_if("")
}

normalize_exact_text <- function(x) {
  x %>%
    as.character() %>%
    iconv(from = "", to = "ASCII//TRANSLIT", sub = "") %>%
    stringr::str_replace_all("[\r\n\t]+", " ") %>%
    stringr::str_squish() %>%
    toupper() %>%
    na_if("")
}

normalize_names <- function(x) {
  x %>%
    as.character() %>%
    iconv(from = "", to = "ASCII//TRANSLIT", sub = "") %>%
    stringr::str_to_lower() %>%
    stringr::str_replace_all("[^a-z0-9]+", " ") %>%
    stringr::str_squish() %>%
    na_if("")
}

parse_bool <- function(x) {
  value <- toupper(trimws(as.character(x)))
  case_when(
    value %in% c("TRUE", "T", "1", "YES", "Y") ~ TRUE,
    value %in% c("FALSE", "F", "0", "NO", "N") ~ FALSE,
    TRUE ~ NA
  )
}

safe_num <- function(x) {
  suppressWarnings(as.numeric(x))
}

first_nonmissing <- function(x) {
  idx <- which(!is.na(x) & x != "")
  if (length(idx) == 0) {
    return(NA)
  }
  x[[idx[[1]]]]
}

confidence_rank <- function(x) {
  recode(
    stringr::str_to_lower(as.character(x)),
    "high" = 3L,
    "medium" = 2L,
    "low" = 1L,
    .default = 0L
  )
}

confidence_from_rank <- function(x) {
  case_when(
    is.na(x) ~ NA_character_,
    x >= 3L ~ "high",
    x == 2L ~ "medium",
    x == 1L ~ "low",
    TRUE ~ NA_character_
  )
}

preferred_capacity_order <- c(
  "capacity_kbd",
  "production_kbd",
  "liquid_capacity_bpd",
  "liquid_throughput_bpd",
  "oil_production_bpd",
  "lng_capacity_mtpa",
  "lng_capacity_bcmy",
  "gas_capacity_mmcfd",
  "gas_throughput_mmcfd"
)

capacity_field_metadata <- tibble(
  field = c(
    "capacity_kbd",
    "production_kbd",
    "liquid_capacity_bpd",
    "liquid_throughput_bpd",
    "oil_production_bpd",
    "lng_capacity_mtpa",
    "lng_capacity_bcmy",
    "gas_capacity_mmcfd",
    "gas_throughput_mmcfd"
  ),
  capacity_unit = c("kbd", "kbd", "bpd", "bpd", "bpd", "mtpa", "bcmy", "mmcfd", "mmcfd"),
  conversion_factor = c(1, 1, 1 / 1000, 1 / 1000, 1 / 1000, 23.8, 17.2, 1 / 5.8, 1 / 5.8),
  default_product_group = c(NA, NA, "oil", "oil", "oil", "gas", "gas", "gas", "gas")
)

primary_kind_to_field <- c(
  "capacity_kbd" = "capacity_kbd",
  "production_kbd" = "production_kbd",
  "liquid_capacity" = "liquid_capacity_bpd",
  "liquid_capacity_bpd" = "liquid_capacity_bpd",
  "liquid_throughput" = "liquid_throughput_bpd",
  "liquid_throughput_bpd" = "liquid_throughput_bpd",
  "oil_production" = "oil_production_bpd",
  "oil_production_bpd" = "oil_production_bpd",
  "lng_capacity_mtpa" = "lng_capacity_mtpa",
  "lng_capacity_bcmy" = "lng_capacity_bcmy",
  "gas_capacity" = "gas_capacity_mmcfd",
  "gas_capacity_mmcfd" = "gas_capacity_mmcfd",
  "gas_throughput" = "gas_throughput_mmcfd",
  "gas_throughput_mmcfd" = "gas_throughput_mmcfd"
)

empty_manual_lookup <- function() {
  tibble(
    site_name = character(),
    capacity_value = character(),
    capacity_unit = character(),
    capacity_kbd_equiv = character(),
    country = character(),
    asset_class = character(),
    source_note = character()
  )
}

empty_provider_tbl <- function() {
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
    liquid_capacity_bpd = character(),
    liquid_throughput_bpd = character(),
    gas_capacity_mmcfd = character(),
    gas_throughput_mmcfd = character(),
    lng_capacity_mtpa = character(),
    lng_capacity_bcmy = character(),
    oil_production_bpd = character(),
    capacity_kbd = character(),
    production_kbd = character(),
    primary_output_kind = character(),
    primary_output_value = character(),
    primary_output_unit = character(),
    primary_output_basis = character(),
    geometry_role = character(),
    provider_priority = character(),
    lat = character(),
    lon = character()
  )
}

read_optional_csv <- function(path, empty_template = tibble()) {
  if (!file.exists(path)) {
    return(empty_template)
  }

  readr::read_csv(path, show_col_types = FALSE)
}

read_inputs <- function(repo_root = get_repo_root()) {
  paths <- get_analysis_paths(repo_root)

  required <- c(paths$incidents_review, paths$incident_site_links, paths$energy_sites)
  missing_required <- required[!file.exists(required)]
  if (length(missing_required) > 0) {
    stop("Missing required input file(s): ", paste(missing_required, collapse = ", "))
  }

  incidents_review <- readr::read_csv(paths$incidents_review, show_col_types = FALSE)
  incident_site_links <- readr::read_csv(paths$incident_site_links, show_col_types = FALSE)
  energy_sites <- readr::read_csv(paths$energy_sites, show_col_types = FALSE)
  ogim_sites_raw <- read_optional_csv(paths$ogim_sites_raw, empty_provider_tbl())
  ggit_sites_raw <- read_optional_csv(paths$ggit_sites_raw, empty_provider_tbl())
  goget_sites_raw <- read_optional_csv(paths$goget_sites_raw, empty_provider_tbl())
  site_capacity_lookup <- read_optional_csv(paths$site_capacity_lookup, empty_manual_lookup())

  list(
    paths = paths,
    incidents_review = incidents_review,
    incident_site_links = incident_site_links,
    energy_sites = energy_sites,
    ogim_sites_raw = ogim_sites_raw,
    ggit_sites_raw = ggit_sites_raw,
    goget_sites_raw = goget_sites_raw,
    site_capacity_lookup = site_capacity_lookup
  )
}

prepare_matchable_table <- function(tbl, name_col, country_col, asset_col) {
  if (nrow(tbl) == 0) {
    return(tbl)
  }

  tbl %>%
    mutate(
      .row_id = row_number(),
      .name_exact = normalize_exact_text(.data[[name_col]]),
      .country_exact = normalize_country(.data[[country_col]]),
      .asset_exact = normalize_names(.data[[asset_col]]),
      .name_norm = normalize_names(.data[[name_col]]),
      .country_norm = normalize_country(.data[[country_col]]),
      .asset_norm = normalize_names(.data[[asset_col]])
    )
}

build_key_maps <- function(tbl) {
  if (nrow(tbl) == 0) {
    return(list(
      exact_asset = list(),
      exact_country = list(),
      norm_asset = list(),
      norm_country = list()
    ))
  }

  split_index <- function(key) {
    values <- tbl[[key]]
    idx <- split(tbl$.row_id, values)
    idx[!is.na(names(idx)) & names(idx) != ""]
  }

  tbl <- tbl %>%
    mutate(
      .key_exact_asset = if_else(
        !is.na(.name_exact) & !is.na(.country_exact) & !is.na(.asset_exact),
        paste(.name_exact, .country_exact, .asset_exact, sep = "||"),
        NA_character_
      ),
      .key_exact_country = if_else(
        !is.na(.name_exact) & !is.na(.country_exact),
        paste(.name_exact, .country_exact, sep = "||"),
        NA_character_
      ),
      .key_norm_asset = if_else(
        !is.na(.name_norm) & !is.na(.country_norm) & !is.na(.asset_norm),
        paste(.name_norm, .country_norm, .asset_norm, sep = "||"),
        NA_character_
      ),
      .key_norm_country = if_else(
        !is.na(.name_norm) & !is.na(.country_norm),
        paste(.name_norm, .country_norm, sep = "||"),
        NA_character_
      )
    )

  list(
    exact_asset = split_index(".key_exact_asset"),
    exact_country = split_index(".key_exact_country"),
    norm_asset = split_index(".key_norm_asset"),
    norm_country = split_index(".key_norm_country")
  )
}

match_lookup_row <- function(site_row, lookup_tbl, lookup_maps, prefix) {
  if (nrow(lookup_tbl) == 0) {
    return(list(status = "no_match", row = NULL))
  }

  site_name_exact <- normalize_exact_text(site_row[["site_name"]])
  site_country <- normalize_country(site_row[["country"]])
  site_asset <- normalize_names(site_row[["asset_class"]])
  site_name_norm <- normalize_names(site_row[["site_name"]])

  stages <- list(
    list(
      key = if (!is.na(site_name_exact) && !is.na(site_country) && !is.na(site_asset)) {
        paste(site_name_exact, site_country, site_asset, sep = "||")
      } else {
        NA_character_
      },
      map = "exact_asset",
      quality = paste0(prefix, "_exact")
    ),
    list(
      key = if (!is.na(site_name_exact) && !is.na(site_country)) {
        paste(site_name_exact, site_country, sep = "||")
      } else {
        NA_character_
      },
      map = "exact_country",
      quality = paste0(prefix, "_exact")
    ),
    list(
      key = if (!is.na(site_name_norm) && !is.na(site_country) && !is.na(site_asset)) {
        paste(site_name_norm, site_country, site_asset, sep = "||")
      } else {
        NA_character_
      },
      map = "norm_asset",
      quality = paste0(prefix, "_normalized")
    ),
    list(
      key = if (!is.na(site_name_norm) && !is.na(site_country)) {
        paste(site_name_norm, site_country, sep = "||")
      } else {
        NA_character_
      },
      map = "norm_country",
      quality = paste0(prefix, "_normalized")
    )
  )

  for (stage in stages) {
    if (is.na(stage$key) || stage$key == "") {
      next
    }

    matches <- lookup_maps[[stage$map]][[stage$key]]
    if (is.null(matches)) {
      next
    }

    if (length(matches) == 1) {
      row <- lookup_tbl %>%
        filter(.row_id == matches[[1]]) %>%
        slice(1)
      return(list(status = stage$quality, row = row))
    }

    return(list(status = "ambiguous", row = NULL))
  }

  list(status = "no_match", row = NULL)
}

derive_product_group <- function(asset_class, commodity_group, selected_field = NA_character_, unit = NA_character_, site_name = NA_character_) {
  asset_class <- normalize_names(asset_class)
  commodity_group <- normalize_names(commodity_group)
  unit <- normalize_names(unit)
  site_name <- normalize_names(site_name)

  if (!is.na(selected_field) && selected_field %in% c("liquid_capacity_bpd", "liquid_throughput_bpd", "oil_production_bpd")) {
    return("oil")
  }

  if (!is.na(selected_field) && selected_field %in% c("lng_capacity_mtpa", "lng_capacity_bcmy", "gas_capacity_mmcfd", "gas_throughput_mmcfd")) {
    return("gas")
  }

  if (!is.na(commodity_group) && commodity_group %in% c("oil", "gas")) {
    return(commodity_group)
  }

  if (!is.na(asset_class) && asset_class == "refinery") {
    return("oil")
  }

  if (!is.na(asset_class) && asset_class == "lng facility") {
    return("gas")
  }

  if (!is.na(unit) && unit %in% c("mtpa", "bcmy", "mmcfd")) {
    return("gas")
  }

  if (!is.na(unit) && unit %in% c("kbd", "kbpd", "bpd")) {
    if (!is.na(asset_class) && asset_class == "lng facility") {
      return("gas")
    }
    if (!is.na(site_name) && str_detect(site_name, "lng|gas|flng|fsru")) {
      return("gas")
    }
    if (!is.na(site_name) && str_detect(site_name, "refin|oil|crude|petroleum")) {
      return("oil")
    }
  }

  NA_character_
}

extract_capacity_from_structured_row <- function(row, source_label, default_match_quality, source_name = NA_character_) {
  row <- as.list(row)
  selected_field <- NA_character_

  primary_kind <- row[["primary_output_kind"]] %||% NA_character_
  mapped_primary <- if (!is.na(primary_kind) && primary_kind %in% names(primary_kind_to_field)) {
    unname(primary_kind_to_field[primary_kind])
  } else {
    NA_character_
  }
  if (!is.na(mapped_primary)) {
    if (!is.na(safe_num(row[[mapped_primary]]))) {
      selected_field <- mapped_primary
    }
  }

  if (is.na(selected_field)) {
    for (field in preferred_capacity_order) {
      if (!is.na(safe_num(row[[field]]))) {
        selected_field <- field
        break
      }
    }
  }

  if (is.na(selected_field)) {
    return(tibble(
      capacity_value = NA_real_,
      capacity_unit = NA_character_,
      capacity_kboed_equiv = NA_real_,
      product_group = NA_character_,
      capacity_source = source_label,
      match_quality = default_match_quality,
      selected_field = NA_character_,
      source_name = source_name
    ))
  }

  field_meta <- capacity_field_metadata %>%
    filter(field == selected_field) %>%
    slice(1)

  raw_value <- safe_num(row[[selected_field]])
  unit <- field_meta$capacity_unit[[1]]
  capacity_kboed_equiv <- raw_value * field_meta$conversion_factor[[1]]
  product_group <- derive_product_group(
    asset_class = row[["asset_class"]],
    commodity_group = row[["commodity_group"]],
    selected_field = selected_field,
    unit = unit,
    site_name = row[["site_name"]] %||% row[["name"]]
  )

  tibble(
    capacity_value = raw_value,
    capacity_unit = unit,
    capacity_kboed_equiv = capacity_kboed_equiv,
    product_group = product_group,
    capacity_source = source_label,
    match_quality = default_match_quality,
    selected_field = selected_field,
    source_name = source_name
  )
}

extract_capacity_from_manual_row <- function(row, match_quality) {
  row <- as.list(row)
  capacity_value <- safe_num(row[["capacity_value"]])
  capacity_unit <- normalize_names(row[["capacity_unit"]])
  capacity_kboed_equiv <- safe_num(row[["capacity_kbd_equiv"]])

  if (is.na(capacity_kboed_equiv) && !is.na(capacity_value) && !is.na(capacity_unit)) {
    capacity_kboed_equiv <- case_when(
      capacity_unit %in% c("kbd", "kbpd", "kboed", "kboe d", "kboe day") ~ capacity_value,
      capacity_unit %in% c("bpd", "boepd", "boe d", "boe day") ~ capacity_value / 1000,
      capacity_unit == "mmcfd" ~ capacity_value / 5.8,
      capacity_unit == "bcmy" ~ capacity_value * 17.2,
      capacity_unit == "mtpa" ~ capacity_value * 23.8,
      TRUE ~ NA_real_
    )
  }

  product_group <- derive_product_group(
    asset_class = row[["asset_class"]],
    commodity_group = NA_character_,
    selected_field = NA_character_,
    unit = capacity_unit,
    site_name = row[["site_name"]]
  )

  tibble(
    capacity_value = capacity_value,
    capacity_unit = capacity_unit,
    capacity_kboed_equiv = capacity_kboed_equiv,
    product_group = product_group,
    capacity_source = "manual_lookup",
    match_quality = match_quality,
    selected_field = NA_character_,
    source_name = row[["source_note"]] %||% NA_character_
  )
}

build_site_capacity_base <- function(inputs) {
  energy_sites <- inputs$energy_sites %>%
    ensure_columns(c(
      "site_id", "name", "country", "asset_class", "commodity_group",
      "capacity_kbd", "production_kbd", "liquid_capacity_bpd",
      "liquid_throughput_bpd", "oil_production_bpd", "lng_capacity_mtpa",
      "lng_capacity_bcmy", "gas_capacity_mmcfd", "gas_throughput_mmcfd",
      "primary_output_kind", "primary_output_value", "primary_output_unit"
    )) %>%
    mutate(
      site_name = as.character(name),
      country = normalize_country(country),
      asset_class = as.character(asset_class),
      commodity_group = normalize_names(commodity_group)
    ) %>%
    select(
      site_id, site_name, country, asset_class, commodity_group,
      capacity_kbd, production_kbd, liquid_capacity_bpd, liquid_throughput_bpd,
      oil_production_bpd, lng_capacity_mtpa, lng_capacity_bcmy,
      gas_capacity_mmcfd, gas_throughput_mmcfd, primary_output_kind,
      primary_output_value, primary_output_unit
    )

  manual_lookup <- inputs$site_capacity_lookup %>%
    ensure_columns(c("site_name", "capacity_value", "capacity_unit", "capacity_kbd_equiv", "country", "asset_class", "source_note")) %>%
    mutate(
      site_name = as.character(site_name),
      country = normalize_country(country),
      asset_class = as.character(asset_class)
    )

  provider_frames <- list(inputs$ogim_sites_raw, inputs$ggit_sites_raw, inputs$goget_sites_raw)
  provider_frames <- lapply(provider_frames, function(tbl) {
    if (nrow(tbl) == 0) {
      return(empty_provider_tbl())
    }
    tbl %>% mutate(across(everything(), as.character))
  })

  provider_lookup <- bind_rows(provider_frames) %>%
    ensure_columns(c(
      "provider", "source_dataset", "source_id", "name", "country", "asset_class",
      "commodity_group", "capacity_kbd", "production_kbd", "liquid_capacity_bpd",
      "liquid_throughput_bpd", "oil_production_bpd", "lng_capacity_mtpa",
      "lng_capacity_bcmy", "gas_capacity_mmcfd", "gas_throughput_mmcfd",
      "primary_output_kind"
    )) %>%
    mutate(
      site_name = as.character(name),
      country = normalize_country(country),
      asset_class = as.character(asset_class),
      commodity_group = normalize_names(commodity_group)
    ) %>%
    select(
      provider, source_dataset, source_id, site_name, country, asset_class, commodity_group,
      capacity_kbd, production_kbd, liquid_capacity_bpd, liquid_throughput_bpd,
      oil_production_bpd, lng_capacity_mtpa, lng_capacity_bcmy,
      gas_capacity_mmcfd, gas_throughput_mmcfd, primary_output_kind
    )

  manual_prepared <- prepare_matchable_table(manual_lookup, "site_name", "country", "asset_class")
  manual_maps <- build_key_maps(manual_prepared)

  provider_prepared <- prepare_matchable_table(provider_lookup, "site_name", "country", "asset_class")
  provider_maps <- build_key_maps(provider_prepared)

  results <- lapply(seq_len(nrow(energy_sites)), function(i) {
    site_row <- energy_sites[i, ]

    manual_match <- match_lookup_row(site_row, manual_prepared, manual_maps, "manual")
    provider_match <- match_lookup_row(site_row, provider_prepared, provider_maps, "provider")
    direct_capacity <- extract_capacity_from_structured_row(
      site_row,
      source_label = "energy_sites_direct",
      default_match_quality = "energy_sites_direct"
    )

    manual_capacity <- if (!is.null(manual_match$row)) {
      extract_capacity_from_manual_row(manual_match$row, manual_match$status)
    } else {
      tibble(
        capacity_value = NA_real_,
        capacity_unit = NA_character_,
        capacity_kboed_equiv = NA_real_,
        product_group = NA_character_,
        capacity_source = NA_character_,
        match_quality = manual_match$status,
        selected_field = NA_character_,
        source_name = NA_character_
      )
    }

    provider_capacity <- if (!is.null(provider_match$row)) {
      extract_capacity_from_structured_row(
        provider_match$row,
        source_label = "provider_fallback",
        default_match_quality = provider_match$status,
        source_name = paste(
          provider_match$row$provider[[1]],
          provider_match$row$source_dataset[[1]],
          provider_match$row$source_id[[1]],
          sep = ":"
        )
      )
    } else {
      tibble(
        capacity_value = NA_real_,
        capacity_unit = NA_character_,
        capacity_kboed_equiv = NA_real_,
        product_group = NA_character_,
        capacity_source = NA_character_,
        match_quality = provider_match$status,
        selected_field = NA_character_,
        source_name = NA_character_
      )
    }

    selected_capacity <- if (!is.na(manual_capacity$capacity_kboed_equiv[[1]])) {
      manual_capacity
    } else if (!is.na(direct_capacity$capacity_kboed_equiv[[1]])) {
      direct_capacity
    } else if (!is.na(provider_capacity$capacity_kboed_equiv[[1]])) {
      provider_capacity
    } else {
      tibble(
        capacity_value = NA_real_,
        capacity_unit = NA_character_,
        capacity_kboed_equiv = NA_real_,
        product_group = NA_character_,
        capacity_source = "unmatched",
        match_quality = if (manual_match$status == "ambiguous" || provider_match$status == "ambiguous") {
          "ambiguous"
        } else {
          "unmatched"
        },
        selected_field = NA_character_,
        source_name = NA_character_
      )
    }

    base_row <- tibble(
      site_id = site_row$site_id[[1]],
      site_name = site_row$site_name[[1]],
      country = site_row$country[[1]],
      asset_class = site_row$asset_class[[1]],
      product_group = selected_capacity$product_group[[1]],
      capacity_value = selected_capacity$capacity_value[[1]],
      capacity_unit = selected_capacity$capacity_unit[[1]],
      capacity_kboed_equiv = selected_capacity$capacity_kboed_equiv[[1]],
      capacity_source = selected_capacity$capacity_source[[1]],
      match_quality = selected_capacity$match_quality[[1]]
    )

    diagnostics_row <- tibble(
      site_id = site_row$site_id[[1]],
      site_name = site_row$site_name[[1]],
      country = site_row$country[[1]],
      asset_class = site_row$asset_class[[1]],
      selected_capacity_source = selected_capacity$capacity_source[[1]],
      selected_match_quality = selected_capacity$match_quality[[1]],
      selected_field = selected_capacity$selected_field[[1]],
      manual_match_status = manual_match$status,
      manual_source_note = manual_capacity$source_name[[1]],
      provider_match_status = provider_match$status,
      provider_source_name = provider_capacity$source_name[[1]]
    )

    list(base = base_row, diagnostics = diagnostics_row)
  })

  site_capacity_base <- bind_rows(lapply(results, `[[`, "base")) %>%
    mutate(
      product_group = if_else(product_group %in% c("oil", "gas"), product_group, NA_character_),
      capacity_value = as.numeric(capacity_value),
      capacity_kboed_equiv = as.numeric(capacity_kboed_equiv)
    ) %>%
    arrange(country, asset_class, site_name)

  diagnostics <- bind_rows(lapply(results, `[[`, "diagnostics")) %>%
    arrange(selected_match_quality, country, asset_class, site_name)

  list(
    site_capacity_base = site_capacity_base,
    diagnostics = diagnostics
  )
}

build_incident_site_capacity <- function(incident_site_links, incidents_review, site_capacity_base) {
  incidents_review <- incidents_review %>%
    ensure_columns(c("incident_id", "incident_day", "wave_number", "energy_candidate_flag", "confidence_tier")) %>%
    mutate(
      incident_day = lubridate::ymd(incident_day),
      wave_number = safe_num(wave_number),
      energy_candidate_flag = parse_bool(energy_candidate_flag),
      confidence_tier = stringr::str_to_lower(confidence_tier)
    ) %>%
    mutate(
      energy_relevant_flag = case_when(
        !is.na(energy_candidate_flag) ~ energy_candidate_flag,
        confidence_tier %in% c("high", "medium") ~ TRUE,
        TRUE ~ FALSE
      )
    ) %>%
    select(incident_id, incident_day, wave_number, energy_relevant_flag, confidence_tier)

  incident_site_links <- incident_site_links %>%
    ensure_columns(c("incident_id", "site_id", "site_name", "asset_class", "distance_km")) %>%
    mutate(
      distance_km = safe_num(distance_km),
      site_name = as.character(site_name),
      asset_class = as.character(asset_class)
    )

  site_capacity_base <- site_capacity_base %>%
    ensure_columns(c("site_id", "site_name", "country", "asset_class", "product_group", "capacity_kboed_equiv", "capacity_source", "match_quality")) %>%
    mutate(
      asset_class = as.character(asset_class),
      country = normalize_country(country)
    ) %>%
    rename(
      base_site_name = site_name,
      base_country = country,
      base_asset_class = asset_class
    )

  incident_site_capacity <- incident_site_links %>%
    inner_join(incidents_review, by = "incident_id") %>%
    left_join(site_capacity_base, by = "site_id") %>%
    mutate(
      site_name = coalesce(site_name, base_site_name),
      country = base_country,
      asset_class = coalesce(asset_class, base_asset_class),
      capacity_available_flag = !is.na(capacity_kboed_equiv)
    ) %>%
    filter(energy_relevant_flag) %>%
    transmute(
      incident_id,
      incident_day,
      wave_number,
      site_id,
      site_name,
      country,
      asset_class,
      product_group,
      capacity_kboed_equiv,
      confidence_tier,
      distance_km,
      energy_relevant_flag,
      capacity_available_flag,
      capacity_source,
      match_quality,
      link_rank = safe_num(link_rank)
    ) %>%
    arrange(incident_day, site_id, incident_id)

  incident_site_capacity
}

build_site_day_capacity_risk <- function(incident_site_capacity) {
  incident_site_capacity %>%
    ensure_columns(c(
      "incident_day", "site_id", "site_name", "country", "asset_class",
      "product_group", "capacity_kboed_equiv", "incident_id", "confidence_tier"
    )) %>%
    mutate(
      incident_day = lubridate::ymd(incident_day),
      confidence_tier = stringr::str_to_lower(confidence_tier)
    ) %>%
    group_by(incident_day, site_id) %>%
    summarise(
      site_name = first_nonmissing(site_name),
      country = first_nonmissing(country),
      asset_class = first_nonmissing(asset_class),
      product_group = first_nonmissing(product_group),
      capacity_kboed_equiv = first_nonmissing(capacity_kboed_equiv),
      n_incidents_linked = n_distinct(incident_id),
      max_confidence_tier = confidence_from_rank(max(confidence_rank(confidence_tier), na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    arrange(incident_day, country, site_name)
}

summarise_country_capacity <- function(site_capacity_base, site_day_capacity_risk, product_group_value) {
  site_capacity_base <- site_capacity_base %>%
    mutate(product_group = as.character(product_group))
  site_day_capacity_risk <- site_day_capacity_risk %>%
    mutate(product_group = as.character(product_group))

  base_capacity <- site_capacity_base %>%
    filter(product_group == product_group_value, !is.na(capacity_kboed_equiv), !is.na(country)) %>%
    group_by(country) %>%
    summarise(
      total_capacity_kboed_equiv = sum(capacity_kboed_equiv, na.rm = TRUE),
      .groups = "drop"
    )

  if (nrow(base_capacity) == 0) {
    return(tibble(
      country = character(),
      product_group = character(),
      total_capacity_kboed_equiv = double(),
      latest_capacity_at_risk_kboed_equiv = double(),
      max_capacity_at_risk_kboed_equiv = double(),
      share_latest_at_risk = double(),
      share_max_at_risk = double()
    ))
  }

  daily_country_risk <- site_day_capacity_risk %>%
    filter(product_group == product_group_value, !is.na(capacity_kboed_equiv), !is.na(country)) %>%
    group_by(incident_day, country) %>%
    summarise(
      capacity_at_risk_kboed_equiv = sum(capacity_kboed_equiv, na.rm = TRUE),
      .groups = "drop"
    )

  latest_day <- if (nrow(site_day_capacity_risk) > 0) {
    max(site_day_capacity_risk$incident_day, na.rm = TRUE)
  } else {
    as.Date(NA)
  }

  latest_risk <- daily_country_risk %>%
    filter(incident_day == latest_day) %>%
    transmute(
      country,
      latest_capacity_at_risk_kboed_equiv = capacity_at_risk_kboed_equiv
    )

  max_risk <- daily_country_risk %>%
    group_by(country) %>%
    summarise(
      max_capacity_at_risk_kboed_equiv = max(capacity_at_risk_kboed_equiv, na.rm = TRUE),
      .groups = "drop"
    )

  base_capacity %>%
    left_join(latest_risk, by = "country") %>%
    left_join(max_risk, by = "country") %>%
    mutate(
      product_group = product_group_value,
      latest_capacity_at_risk_kboed_equiv = coalesce(latest_capacity_at_risk_kboed_equiv, 0),
      max_capacity_at_risk_kboed_equiv = coalesce(max_capacity_at_risk_kboed_equiv, 0),
      share_latest_at_risk = if_else(
        total_capacity_kboed_equiv > 0,
        latest_capacity_at_risk_kboed_equiv / total_capacity_kboed_equiv,
        NA_real_
      ),
      share_max_at_risk = if_else(
        total_capacity_kboed_equiv > 0,
        max_capacity_at_risk_kboed_equiv / total_capacity_kboed_equiv,
        NA_real_
      )
    ) %>%
    arrange(desc(total_capacity_kboed_equiv), country)
}

summarise_daily_capacity_risk <- function(site_day_capacity_risk) {
  site_day_capacity_risk %>%
    filter(!is.na(incident_day), !is.na(product_group), !is.na(country), !is.na(capacity_kboed_equiv)) %>%
    group_by(incident_day, product_group, country) %>%
    summarise(
      capacity_at_risk_kboed_equiv = sum(capacity_kboed_equiv, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(incident_day, product_group, country)
}

build_placeholder_plot <- function(title, subtitle, message) {
  ggplot() +
    annotate("text", x = 0.5, y = 0.55, label = title, size = 7, fontface = "bold") +
    annotate("text", x = 0.5, y = 0.45, label = subtitle, size = 4.2) +
    annotate("text", x = 0.5, y = 0.34, label = message, size = 4) +
    coord_cartesian(xlim = c(0, 1), ylim = c(0, 1), clip = "off") +
    theme_void()
}

capacity_plot_palette <- function() {
  list(
    oil_total = "#dbc6ad",
    oil_risk = "#8a4f17",
    gas_total = "#cfe8cf",
    gas_risk = "#2f7d4f",
    total_line = "#2b2b2b"
  )
}

capacity_plot_theme <- function() {
  theme_minimal(base_size = 13) +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      plot.title = element_text(face = "bold", size = 18, margin = margin(b = 4)),
      plot.subtitle = element_text(size = 11.5, color = "#4a4a4a", margin = margin(b = 12)),
      plot.caption = element_text(size = 9.5, color = "#5b5b5b", margin = margin(t = 10)),
      strip.text = element_text(face = "bold", size = 12),
      legend.position = "top",
      legend.title = element_blank(),
      legend.text = element_text(size = 10.5),
      plot.margin = margin(16, 26, 16, 16)
    )
}

prepare_country_panel <- function(summary_tbl, product_group_value) {
  plot_tbl <- summary_tbl %>%
    filter(
      !is.na(country),
      max_capacity_at_risk_kboed_equiv > 0,
      country != "SUDAN"
    )

  if (product_group_value == "gas") {
    plot_tbl <- plot_tbl %>%
      filter(!country %in% c("JORDAN", "OMAN"))
  }

  if (nrow(plot_tbl) == 0) {
    return(plot_tbl)
  }

  panel_label <- if (product_group_value == "oil") "Oil" else "Gas"

  plot_tbl %>%
    arrange(desc(total_capacity_kboed_equiv), country) %>%
    mutate(
      panel = factor(panel_label, levels = c("Oil", "Gas")),
      country_panel = factor(
        paste(country, panel_label, sep = "___"),
        levels = rev(paste(country, panel_label, sep = "___"))
      ),
      label_share = if_else(
        max_capacity_at_risk_kboed_equiv > 0,
        scales::percent(share_max_at_risk, accuracy = 1),
        NA_character_
      )
    )
}

plot_capacity_risk_by_country_combined <- function(oil_summary, gas_summary, output_path) {
  palettes <- capacity_plot_palette()

  oil_tbl <- prepare_country_panel(oil_summary, "oil") %>%
    mutate(total_fill = palettes$oil_total, risk_fill = palettes$oil_risk)
  gas_tbl <- prepare_country_panel(gas_summary, "gas") %>%
    mutate(total_fill = palettes$gas_total, risk_fill = palettes$gas_risk)

  plot_tbl <- bind_rows(oil_tbl, gas_tbl)

  title <- "Country-Level Capacity at Risk"
  subtitle <- "Light bars show total mapped capacity; dark overlays show maximum daily capacity at risk. Exposure metric, not confirmed lost output."
  caption <- "Gas panel excludes Jordan and Oman by explicit plot-only rule."

  plot_obj <- if (nrow(plot_tbl) == 0) {
    build_placeholder_plot(title, subtitle, "No countries remain after plot-stage filtering.")
  } else {
    label_tbl <- plot_tbl %>%
      filter(!is.na(label_share))

    max_x <- max(plot_tbl$total_capacity_kboed_equiv, na.rm = TRUE)

    ggplot(plot_tbl, aes(y = country_panel)) +
      geom_col(aes(x = total_capacity_kboed_equiv, fill = total_fill), width = 0.74, show.legend = FALSE) +
      geom_col(aes(x = max_capacity_at_risk_kboed_equiv, fill = risk_fill), width = 0.42, show.legend = FALSE) +
      geom_text(
        data = label_tbl,
        aes(
          x = pmax(total_capacity_kboed_equiv, max_capacity_at_risk_kboed_equiv) + max_x * 0.03,
          label = label_share
        ),
        hjust = 0,
        size = 3.5,
        color = "#3f3f3f"
      ) +
      facet_grid(panel ~ ., scales = "free_y", space = "free_y", switch = "y") +
      scale_fill_identity() +
      scale_y_discrete(labels = function(x) sub("___.*$", "", x)) +
      scale_x_continuous(
        labels = label_number(accuracy = 1),
        expand = expansion(mult = c(0, 0.16))
      ) +
      labs(
        title = title,
        subtitle = subtitle,
        x = "Capacity (kboe/d equivalent)",
        y = NULL,
        caption = caption
      ) +
      capacity_plot_theme() +
      theme(
        panel.grid.major.x = element_line(color = "#e4e4e4", linewidth = 0.35),
        strip.placement = "outside",
        strip.background = element_rect(fill = "#f4f4f4", color = NA),
        plot.margin = margin(16, 56, 16, 16)
      ) +
      coord_cartesian(clip = "off")
  }

  ggplot2::ggsave(output_path, plot_obj, width = 12, height = 10.5, dpi = 300)
  plot_obj
}

plot_capacity_risk_timeseries <- function(daily_tbl, output_path) {
  palettes <- capacity_plot_palette()
  title <- "Daily Capacity at Risk"
  subtitle <- "Daily site-level exposure, deduplicated to unique site-day events. Exposure metric, not confirmed lost production."

  plot_tbl <- daily_tbl %>%
    filter(product_group %in% c("oil", "gas")) %>%
    group_by(incident_day, product_group) %>%
    summarise(
      capacity_at_risk_kboed_equiv = sum(capacity_at_risk_kboed_equiv, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(incident_day >= as.Date("2026-02-28"))

  plot_obj <- if (nrow(plot_tbl) == 0) {
    build_placeholder_plot(title, subtitle, "No daily capacity-at-risk rows are available to plot.")
  } else {
    start_date <- as.Date("2026-02-28")
    end_date <- max(plot_tbl$incident_day)
    date_range <- seq(start_date, end_date, by = "day")
    date_breaks <- seq(start_date, end_date, by = "7 days")

    plot_tbl <- plot_tbl %>%
      tidyr::complete(
        incident_day = date_range,
        product_group = c("oil", "gas"),
        fill = list(capacity_at_risk_kboed_equiv = 0)
      ) %>%
      mutate(product_group = factor(product_group, levels = c("oil", "gas")))

    total_tbl <- plot_tbl %>%
      group_by(incident_day) %>%
      summarise(total_capacity_at_risk_kboed_equiv = sum(capacity_at_risk_kboed_equiv, na.rm = TRUE), .groups = "drop")

    ggplot(plot_tbl, aes(x = incident_day, y = capacity_at_risk_kboed_equiv, fill = product_group)) +
      geom_col(width = 0.9, color = NA, alpha = 0.95) +
      geom_line(
        data = total_tbl,
        aes(x = incident_day, y = total_capacity_at_risk_kboed_equiv),
        inherit.aes = FALSE,
        color = palettes$total_line,
        linewidth = 0.75
      ) +
      geom_point(
        data = total_tbl,
        aes(x = incident_day, y = total_capacity_at_risk_kboed_equiv),
        inherit.aes = FALSE,
        color = palettes$total_line,
        fill = "white",
        size = 1.5,
        stroke = 0.2
      ) +
      scale_fill_manual(
        values = c(oil = palettes$oil_risk, gas = palettes$gas_risk),
        labels = c(oil = "Oil", gas = "Gas")
      ) +
      scale_y_continuous(
        labels = label_number(accuracy = 1),
        expand = expansion(mult = c(0, 0.06))
      ) +
      scale_x_date(
        breaks = date_breaks,
        date_labels = "%b %d",
        expand = expansion(mult = c(0.01, 0.01))
      ) +
      labs(
        title = title,
        subtitle = subtitle,
        x = NULL,
        y = "Capacity at risk (kboe/d equivalent)",
        fill = NULL,
        caption = "Columns are stacked by product group; the line shows the daily total."
      ) +
      capacity_plot_theme() +
      theme(
        panel.grid.major.x = element_blank(),
        axis.text.x = element_text(angle = 0, vjust = 0.5),
        legend.justification = "left"
      )
  }

  ggplot2::ggsave(output_path, plot_obj, width = 12, height = 7.2, dpi = 300)
  plot_obj
}
