# 03c_map_capacity_at_risk_FINAL.R
# Capacity at risk analysis and map.
# Reads the same inputs as 03b (incidents, energy sites, FIRMS anomalies),
# computes aggregate capacity at risk, and renders a summary map.
# Skeleton — analysis section is working, visualization is placeholder.

source("utils.R")
source("map_utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
  library(scales)
})

write_output <- env_flag("WRITE_OUTPUT", FALSE)
settings <- project_settings()
asset_registry <- build_asset_class_registry()

# Same Gulf bbox as validation map
val_bbox <- list(west = 44, south = 21, east = 60, north = 32)

paths <- list(
  incidents_csv    = project_path("data_processed", "incidents_review.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  site_links_csv   = project_path("data_processed", "incident_site_links.csv"),
  firms_csv        = project_path("data_processed", "firms_facility_anomalies.csv"),
  output_pdf       = project_path("output", "capacity_at_risk_map.pdf"),
  output_csv       = project_path("output", "capacity_at_risk_summary.csv")
)

# ── Load data ────────────────────────────────────────────────────────────────

incidents <- prepare_incidents(paths$incidents_csv)
energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
incident_site_links <- prepare_site_links(paths$site_links_csv)
firms_facility_anomalies <- prepare_firms_facility_anomalies(paths$firms_csv)

site_validation_summary <- build_site_validation_summary(
  incident_site_links, incidents, energy_sites, firms_facility_anomalies, settings
)

# ── Compute capacity at risk ────────────────────────────────────────────────

at_risk <- site_validation_summary %>%
  filter(incident_count > 0 | firms_anomaly_flag) %>%
  mutate(
    verification_status = case_when(
      incident_count > 0 & firms_anomaly_flag ~ "FIRMS double-confirmed",
      incident_count > 0 ~ "Incident-linked",
      firms_anomaly_flag ~ "FIRMS anomaly only",
      TRUE ~ "Unknown"
    )
  )

# Summary by country and commodity family
capacity_at_risk <- at_risk %>%
  group_by(country, display_commodity_family, verification_status) %>%
  summarise(
    n_sites = n(),
    total_capacity_kbd = sum(capacity_kbd, na.rm = TRUE),
    total_production_kbd = sum(production_kbd, na.rm = TRUE),
    total_gas_mmcfd = sum(gas_capacity_mmcfd, na.rm = TRUE),
    total_lng_mtpa = sum(lng_capacity_mtpa, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(total_capacity_kbd), desc(total_gas_mmcfd))

# Total regional capacity for denominator
total_regional <- site_validation_summary %>%
  summarise(
    total_sites = n(),
    total_capacity_kbd = sum(capacity_kbd, na.rm = TRUE),
    total_production_kbd = sum(production_kbd, na.rm = TRUE),
    total_gas_mmcfd = sum(gas_capacity_mmcfd, na.rm = TRUE),
    total_lng_mtpa = sum(lng_capacity_mtpa, na.rm = TRUE)
  )

at_risk_totals <- at_risk %>%
  summarise(
    at_risk_sites = n(),
    at_risk_capacity_kbd = sum(capacity_kbd, na.rm = TRUE),
    at_risk_production_kbd = sum(production_kbd, na.rm = TRUE),
    at_risk_gas_mmcfd = sum(gas_capacity_mmcfd, na.rm = TRUE),
    at_risk_lng_mtpa = sum(lng_capacity_mtpa, na.rm = TRUE)
  )

# Print summary
message("\n=== CAPACITY AT RISK SUMMARY ===")
message(sprintf(
  "At-risk sites: %d / %d (%.1f%%)",
  at_risk_totals$at_risk_sites,
  total_regional$total_sites,
  100 * at_risk_totals$at_risk_sites / max(total_regional$total_sites, 1)
))
message(sprintf(
  "Oil capacity at risk: %.0f / %.0f kbd (%.1f%%)",
  at_risk_totals$at_risk_capacity_kbd,
  total_regional$total_capacity_kbd,
  100 * at_risk_totals$at_risk_capacity_kbd / max(total_regional$total_capacity_kbd, 1)
))
message(sprintf(
  "Gas capacity at risk: %.0f / %.0f mmcfd (%.1f%%)",
  at_risk_totals$at_risk_gas_mmcfd,
  total_regional$total_gas_mmcfd,
  100 * at_risk_totals$at_risk_gas_mmcfd / max(total_regional$total_gas_mmcfd, 1)
))
message(sprintf(
  "LNG capacity at risk: %.1f / %.1f mtpa (%.1f%%)",
  at_risk_totals$at_risk_lng_mtpa,
  total_regional$total_lng_mtpa,
  100 * at_risk_totals$at_risk_lng_mtpa / max(total_regional$total_lng_mtpa, 1)
))

message("\nBy country and commodity:")
print(as.data.frame(capacity_at_risk), row.names = FALSE)

# ── TODO: Visualization ─────────────────────────────────────────────────────

# TODO: Bar chart of capacity at risk by country, faceted by commodity family
# TODO: Map with at-risk capacity bubbles (size ~ capacity, color ~ verification)
# TODO: Waterfall chart showing total capacity → at-risk portion
# TODO: Table output for appendix

# ── Placeholder map ─────────────────────────────────────────────────────────

bg <- prepare_background_map(val_bbox)

at_risk_sf <- at_risk %>%
  filter(!is.na(lat), !is.na(lon)) %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

val_label_bbox <- sf::st_bbox(c(
  xmin = val_bbox$west, ymin = val_bbox$south,
  xmax = val_bbox$east, ymax = val_bbox$north
), crs = sf::st_crs(4326))

at_risk_cropped <- if (nrow(at_risk_sf) > 0) {
  sf::st_crop(at_risk_sf, val_label_bbox)
} else {
  at_risk_sf
}

risk_map <- ggplot() +
  geom_sf(data = bg$land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
  geom_sf_text(data = bg$country_labels, aes(label = admin),
               color = "#5f594b", size = 2.8, check_overlap = TRUE)

if (nrow(at_risk_cropped) > 0) {
  at_risk_cropped <- at_risk_cropped %>%
    mutate(size_value = dplyr::coalesce(capacity_kbd, production_kbd, 0))

  risk_map <- risk_map +
    geom_sf(
      data = at_risk_cropped,
      aes(fill = verification_status, size = size_value),
      shape = 21, color = "#202020", stroke = 0.4, alpha = 0.78,
      inherit.aes = FALSE
    )
}

risk_fills <- c(
  "FIRMS double-confirmed" = "#a50f15",
  "Incident-linked" = "#e6550d",
  "FIRMS anomaly only" = "#fdae6b"
)

risk_map <- risk_map +
  coord_sf(
    xlim = c(val_bbox$west, val_bbox$east),
    ylim = c(val_bbox$south, val_bbox$north),
    expand = FALSE
  ) +
  scale_fill_manual(values = risk_fills, drop = FALSE) +
  scale_size_continuous(
    range = c(1.5, 10), breaks = c(100, 300, 500, 900),
    labels = function(x) paste0(x, " kbd"),
    guide = guide_legend(override.aes = list(fill = "#e6550d", shape = 21))
  ) +
  labs(
    title = "Gulf Energy Infrastructure: Capacity at Risk",
    subtitle = paste0(
      at_risk_totals$at_risk_sites, " facilities at risk | ",
      round(at_risk_totals$at_risk_capacity_kbd), " kbd oil capacity | ",
      round(at_risk_totals$at_risk_lng_mtpa, 1), " mtpa LNG capacity"
    ),
    fill = "Verification status",
    size = "Capacity"
  ) +
  make_map_theme() +
  theme(
    legend.position = "bottom",
    legend.box = "horizontal",
    legend.margin = margin(t = 4, r = 8, b = 4, l = 8),
    legend.spacing.x = unit(8, "pt"),
    legend.text = element_text(size = 7.5, margin = margin(l = 2, r = 4)),
    legend.title = element_text(size = 8, face = "bold"),
    legend.key.size = unit(14, "pt"),
    plot.subtitle = element_text(size = 8.5, color = "#36505f")
  )

print(risk_map)

# ── Map 2: from pre-computed site_day_capacity_risk.csv ───────────────────────
# Uses capacity_kboed_equiv (thousand barrels of oil equivalent/day) —
# a normalised unit across oil and gas, computed in the analysis pipeline.

site_day_risk_path <- project_path("analysis_output", "site_day_capacity_risk.csv")
site_day_risk <- read_csv_if_exists(site_day_risk_path)

if (nrow(site_day_risk) > 0) {
  # Aggregate to site level: peak capacity and cumulative incident count
  site_risk_agg <- site_day_risk %>%
    group_by(site_id, site_name, country, asset_class, product_group) %>%
    summarise(
      capacity_kboed = max(capacity_kboed_equiv, na.rm = TRUE),
      total_incidents = max(n_incidents_linked, na.rm = TRUE),
      max_confidence = first(max_confidence_tier),
      first_day = min(incident_day, na.rm = TRUE),
      last_day = max(incident_day, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    # Replace Inf/-Inf from empty groups
    mutate(capacity_kboed = if_else(is.infinite(capacity_kboed), NA_real_, capacity_kboed))

  # Join coordinates from energy_sites
  site_coords <- energy_sites %>%
    select(site_id, lat, lon) %>%
    filter(!is.na(lat), !is.na(lon))

  site_risk_geo <- site_risk_agg %>%
    inner_join(site_coords, by = "site_id") %>%
    filter(!is.na(lat), !is.na(lon)) %>%
    mutate(
      product_label = case_when(
        product_group == "oil" ~ "Oil",
        product_group == "gas" ~ "Gas / LNG",
        TRUE ~ "Other"
      )
    ) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

  site_risk_cropped <- if (nrow(site_risk_geo) > 0) {
    sf::st_crop(site_risk_geo, val_label_bbox)
  } else {
    site_risk_geo
  }

  # Summary stats for subtitle
  n_risk_sites <- nrow(site_risk_agg)
  total_kboed <- sum(site_risk_agg$capacity_kboed, na.rm = TRUE)

  product_fills <- c("Oil" = "#8c510a", "Gas / LNG" = "#1b7837", "Other" = "#7f7f7f")

  risk_map_2 <- ggplot() +
    geom_sf(data = bg$land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
    geom_sf_text(data = bg$country_labels, aes(label = admin),
                 color = "#5f594b", size = 2.8, check_overlap = TRUE)

  if (nrow(site_risk_cropped) > 0) {
    risk_map_2 <- risk_map_2 +
      geom_sf(
        data = site_risk_cropped,
        aes(fill = product_label, size = capacity_kboed),
        shape = 21, color = "#202020", stroke = 0.4, alpha = 0.78,
        inherit.aes = FALSE
      )
  }

  risk_map_2 <- risk_map_2 +
    coord_sf(
      xlim = c(val_bbox$west, val_bbox$east),
      ylim = c(val_bbox$south, val_bbox$north),
      expand = FALSE
    ) +
    scale_fill_manual(values = product_fills, drop = TRUE) +
    scale_size_continuous(
      range = c(1.5, 10), breaks = c(50, 150, 300, 500),
      labels = function(x) paste0(x, " kboe/d"),
      guide = guide_legend(override.aes = list(fill = "#e6550d", shape = 21))
    ) +
    labs(
      title = "Gulf Energy Infrastructure: Capacity at Risk (kboe/d)",
      subtitle = paste0(
        n_risk_sites, " incident-linked sites | ",
        round(total_kboed), " kboe/d total capacity at risk"
      ),
      fill = "Product group",
      size = "Capacity"
    ) +
    make_map_theme() +
    theme(
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.margin = margin(t = 4, r = 8, b = 4, l = 8),
      legend.spacing.x = unit(8, "pt"),
      legend.text = element_text(size = 7.5, margin = margin(l = 2, r = 4)),
      legend.title = element_text(size = 8, face = "bold"),
      legend.key.size = unit(14, "pt"),
      plot.subtitle = element_text(size = 8.5, color = "#36505f")
    )

  print(risk_map_2)

  message(sprintf(
    "\n=== SITE-DAY CAPACITY RISK (kboe/d) ===\n%d sites, %.0f kboe/d total",
    n_risk_sites, total_kboed
  ))
} else {
  message("No site_day_capacity_risk.csv found in analysis_output/ — skipping kboe/d map.")
}

# ── Output ───────────────────────────────────────────────────────────────────

if (write_output) {
  ensure_dir(project_path("output"))
  ggsave(
    next_numbered_output_path(paths$output_pdf),
    risk_map,
    width = 12, height = 8,
    device = grDevices::pdf
  )
  if (exists("risk_map_2")) {
    ggsave(
      next_numbered_output_path(project_path("output", "capacity_at_risk_kboed_map.pdf")),
      risk_map_2,
      width = 12, height = 8,
      device = grDevices::pdf
    )
  }
  readr::write_csv(capacity_at_risk, paths$output_csv)
  message("Capacity at risk maps and summary CSV written to output/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save maps and CSV.")
}
