source("utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
})

write_output <- identical(toupper(Sys.getenv("WRITE_OUTPUT", unset = "TRUE")), "TRUE")

paths <- list(
  crude_csv = project_path("data_raw", "UN_comtrade_oil_gas", "UNdata_Export_20260323_022753419.csv"),
  lng_csv = project_path("data_raw", "UN_comtrade_oil_gas", "UNdata_Export_20260323_022816315.csv"),
  light_oils_csv = project_path("data_raw", "UN_comtrade_oil_gas", "UNdata_Export_20260323_lightoils_refined.csv"),
  combined_csv = project_path("data_processed", "un_comtrade_country_commodity_flows.csv"),
  summary_csv = project_path("data_processed", "un_comtrade_country_commodity_summary.csv")
)

normalize_country <- function(x) {
  x %>%
    as.character() %>%
    str_squish() %>%
    str_to_upper() %>%
    na_if("")
}

read_trade_file <- function(path, commodity_key, product_group) {
  if (!file.exists(path)) {
    warning(sprintf("Missing trade input: %s", path), call. = FALSE)
    return(tibble())
  }

  raw <- readr::read_csv(path, show_col_types = FALSE)

  if (!"Comm. Code" %in% names(raw)) {
    raw[["Comm. Code"]] <- NA_character_
  }

  raw %>%
    transmute(
      source_dataset = "UN Comtrade",
      source_file = basename(path),
      commodity_key = commodity_key,
      product_group = product_group,
      country = normalize_country(`Country or Area`),
      year = suppressWarnings(as.integer(Year)),
      commodity_code = as.character(`Comm. Code`),
      commodity = as.character(Commodity),
      flow = str_to_lower(str_squish(as.character(Flow))),
      trade_usd = parse_double(as.character(`Trade (USD)`), na = c("", "NA", "N/A")),
      weight_kg = parse_double(as.character(`Weight (kg)`), na = c("", "NA", "N/A")),
      quantity_name = as.character(`Quantity Name`),
      quantity = parse_double(as.character(Quantity), na = c("", "NA", "N/A"))
    ) %>%
    mutate(
      weight_tonnes = weight_kg / 1000,
      quantity_available = !is.na(quantity),
      weight_available = !is.na(weight_kg)
    )
}

trade_files <- bind_rows(
  read_trade_file(paths$crude_csv, "crude_oil", "crude_oil"),
  read_trade_file(paths$lng_csv, "lng", "lng"),
  read_trade_file(paths$light_oils_csv, "light_oils_refined", "refined_products")
) %>%
  filter(!is.na(country), !is.na(year), !is.na(flow))

trade_summary <- trade_files %>%
  group_by(year, country, flow, product_group, commodity_key) %>%
  summarise(
    total_trade_usd = sum(trade_usd, na.rm = TRUE),
    total_weight_kg = sum(weight_kg, na.rm = TRUE),
    total_weight_tonnes = sum(weight_tonnes, na.rm = TRUE),
    total_quantity = sum(quantity, na.rm = TRUE),
    rows = n(),
    any_quantity_available = any(quantity_available, na.rm = TRUE),
    any_weight_available = any(weight_available, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(year, country, flow, product_group, commodity_key)

if (write_output) {
  write_csv(trade_files, paths$combined_csv)
  write_csv(trade_summary, paths$summary_csv)
  message("Combined UN Comtrade trade files written to data_processed/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save combined trade outputs.")
}

cat("\nUN Comtrade trade combination complete\n")
cat("-------------------------------------\n")
cat(sprintf("Combined rows: %s\n", nrow(trade_files)))
cat(sprintf("Summary rows: %s\n", nrow(trade_summary)))
cat("Rows by commodity:\n")
trade_files %>%
  count(commodity_key, sort = TRUE) %>%
  mutate(line = sprintf("  %s: %s", commodity_key, n)) %>%
  pull(line) -> commodity_lines
for (line in commodity_lines) {
  cat(sprintf("%s\n", line))
}
cat("Outputs:\n")
cat(sprintf("  %s\n", paths$combined_csv))
cat(sprintf("  %s\n", paths$summary_csv))
cat("\nNote: these files are country-level import/export totals by commodity, not bilateral exporter-importer partner flows.\n")
