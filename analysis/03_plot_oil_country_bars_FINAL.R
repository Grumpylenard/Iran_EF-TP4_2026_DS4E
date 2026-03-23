script_path <- normalizePath(sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1]), winslash = "/", mustWork = TRUE)
source(file.path(dirname(script_path), "00_shared_functions_FINAL.R"))

paths <- get_analysis_paths()
ensure_output_dir(paths$analysis_output)

if (!file.exists(paths$country_capacity_summary_oil) || !file.exists(paths$country_capacity_summary_gas)) {
  stop(
    "Missing upstream country summary file(s): ",
    paste(c(paths$country_capacity_summary_oil, paths$country_capacity_summary_gas)[
      !file.exists(c(paths$country_capacity_summary_oil, paths$country_capacity_summary_gas))
    ], collapse = ", "),
    ". Run 02_build_incident_site_capacity_FINAL.R first."
  )
}

oil_summary <- readr::read_csv(paths$country_capacity_summary_oil, show_col_types = FALSE)
gas_summary <- readr::read_csv(paths$country_capacity_summary_gas, show_col_types = FALSE)

plot_capacity_risk_by_country_combined(
  oil_summary = oil_summary,
  gas_summary = gas_summary,
  output_path = paths$fig_capacity_risk_by_country_combined
)

oil_plotted <- oil_summary %>%
  filter(max_capacity_at_risk_kboed_equiv > 0, country != "SUDAN") %>%
  nrow()

gas_plotted <- gas_summary %>%
  filter(max_capacity_at_risk_kboed_equiv > 0, !country %in% c("JORDAN", "OMAN")) %>%
  nrow()

cat("Combined country figure written to:", paths$fig_capacity_risk_by_country_combined, "\n")
cat("Oil countries plotted:", oil_plotted, "\n")
cat("Gas countries plotted:", gas_plotted, "\n")
