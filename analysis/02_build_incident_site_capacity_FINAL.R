script_path <- normalizePath(sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1]), winslash = "/", mustWork = TRUE)
source(file.path(dirname(script_path), "00_shared_functions_FINAL.R"))

paths <- get_analysis_paths()
ensure_output_dir(paths$analysis_output)

if (!file.exists(paths$site_capacity_base)) {
  stop("Missing upstream file: ", paths$site_capacity_base, ". Run 01_build_site_capacity_base_FINAL.R first.")
}

inputs <- read_inputs()
site_capacity_base <- readr::read_csv(paths$site_capacity_base, show_col_types = FALSE)

incident_site_capacity <- build_incident_site_capacity(
  incident_site_links = inputs$incident_site_links,
  incidents_review = inputs$incidents_review,
  site_capacity_base = site_capacity_base
)

site_day_capacity_risk <- build_site_day_capacity_risk(incident_site_capacity)
country_capacity_summary_oil <- summarise_country_capacity(site_capacity_base, site_day_capacity_risk, "oil")
country_capacity_summary_gas <- summarise_country_capacity(site_capacity_base, site_day_capacity_risk, "gas")
daily_capacity_risk_summary <- summarise_daily_capacity_risk(site_day_capacity_risk)

readr::write_csv(incident_site_capacity, paths$incident_site_capacity, na = "")
readr::write_csv(site_day_capacity_risk, paths$site_day_capacity_risk, na = "")
readr::write_csv(country_capacity_summary_oil, paths$country_capacity_summary_oil, na = "")
readr::write_csv(country_capacity_summary_gas, paths$country_capacity_summary_gas, na = "")
readr::write_csv(daily_capacity_risk_summary, paths$daily_capacity_risk_summary, na = "")

latest_day <- if (nrow(site_day_capacity_risk) > 0) {
  max(site_day_capacity_risk$incident_day, na.rm = TRUE)
} else {
  as.Date(NA)
}

latest_total <- if (nrow(site_day_capacity_risk) > 0 && !is.na(latest_day)) {
  site_day_capacity_risk %>%
    filter(incident_day == latest_day, !is.na(capacity_kboed_equiv)) %>%
    summarise(total = sum(capacity_kboed_equiv, na.rm = TRUE)) %>%
    pull(total)
} else {
  0
}

cat("Incident-site fact table written to:", paths$incident_site_capacity, "\n")
cat("Site-day risk table written to:", paths$site_day_capacity_risk, "\n")
cat("Daily summary written to:", paths$daily_capacity_risk_summary, "\n")
cat("Incident-site rows retained:", nrow(incident_site_capacity), "\n")
cat("Unique incidents retained:", n_distinct(incident_site_capacity$incident_id), "\n")
cat("Unique site-days:", nrow(site_day_capacity_risk), "\n")
cat("Latest day:", as.character(latest_day), "\n")
cat("Latest total capacity at risk (kboe/d):", round(latest_total, 1), "\n")

top_oil <- country_capacity_summary_oil %>%
  slice_head(n = 5) %>%
  transmute(label = paste0(country, "=", round(max_capacity_at_risk_kboed_equiv, 1))) %>%
  pull(label)

top_gas <- country_capacity_summary_gas %>%
  slice_head(n = 5) %>%
  transmute(label = paste0(country, "=", round(max_capacity_at_risk_kboed_equiv, 1))) %>%
  pull(label)

cat("Top oil countries by max at-risk capacity:", paste(top_oil, collapse = ", "), "\n")
cat("Top gas countries by max at-risk capacity:", paste(top_gas, collapse = ", "), "\n")
