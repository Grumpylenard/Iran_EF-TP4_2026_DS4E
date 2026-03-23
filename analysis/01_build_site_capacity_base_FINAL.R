script_path <- normalizePath(sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)[1]), winslash = "/", mustWork = TRUE)
source(file.path(dirname(script_path), "00_shared_functions_FINAL.R"))

paths <- get_analysis_paths()
ensure_output_dir(paths$analysis_output)

inputs <- read_inputs()
result <- build_site_capacity_base(inputs)

readr::write_csv(result$site_capacity_base, paths$site_capacity_base, na = "")
readr::write_csv(result$diagnostics, paths$site_capacity_match_diagnostics, na = "")

summary_tbl <- result$site_capacity_base %>%
  mutate(capacity_available = !is.na(capacity_kboed_equiv))

cat("Site capacity base written to:", paths$site_capacity_base, "\n")
cat("Diagnostics written to:", paths$site_capacity_match_diagnostics, "\n")
cat("Total sites:", nrow(summary_tbl), "\n")
cat("Sites with usable capacity:", sum(summary_tbl$capacity_available, na.rm = TRUE), "\n")
cat(
  "Product-group coverage:",
  paste(
    summary_tbl %>%
      count(product_group, name = "n") %>%
      mutate(product_group = coalesce(product_group, "NA")) %>%
      transmute(label = paste0(product_group, "=", n)) %>%
      pull(label),
    collapse = ", "
  ),
  "\n"
)
cat(
  "Capacity sources:",
  paste(
    summary_tbl %>%
      count(capacity_source, name = "n") %>%
      transmute(label = paste0(capacity_source, "=", n)) %>%
      pull(label),
    collapse = ", "
  ),
  "\n"
)
