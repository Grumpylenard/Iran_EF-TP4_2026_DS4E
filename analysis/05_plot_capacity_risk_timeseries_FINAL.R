script_path <- normalizePath(sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1]), winslash = "/", mustWork = TRUE)
source(file.path(dirname(script_path), "00_shared_functions_FINAL.R"))

paths <- get_analysis_paths()
ensure_output_dir(paths$analysis_output)

if (!file.exists(paths$daily_capacity_risk_summary)) {
  stop("Missing upstream file: ", paths$daily_capacity_risk_summary, ". Run 02_build_incident_site_capacity_FINAL.R first.")
}

daily_summary <- readr::read_csv(paths$daily_capacity_risk_summary, show_col_types = FALSE) %>%
  mutate(incident_day = lubridate::ymd(incident_day))

plot_capacity_risk_timeseries(daily_summary, paths$fig_capacity_risk_timeseries)

cat("Capacity-at-risk time-series figure written to:", paths$fig_capacity_risk_timeseries, "\n")
cat("Daily rows plotted:", nrow(daily_summary), "\n")
