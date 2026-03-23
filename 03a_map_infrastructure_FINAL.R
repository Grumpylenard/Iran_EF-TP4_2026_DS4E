# 03a_map_infrastructure_FINAL.R
# Baseline infrastructure map: all energy sites sized by capacity, colored by asset class.
# No energy lines, no incidents, no FIRMS.
# Extracted from 03_make_maps.R (MAP_MODE == "infrastructure").

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

# Gulf-focused framing: western edge at eastern Saudi field belt
focus_bbox <- list(west = 46.4, south = 20.5, east = 60.0, north = 31.8)

paths <- list(
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  output_pdf = project_path("output", "infrastructure_baseline_map.pdf")
)

# ── Load data ────────────────────────────────────────────────────────────────

energy_sites <- prepare_energy_sites(paths$energy_sites_csv)

if (nrow(energy_sites) == 0) {
  stop("No energy sites found. Run 01_ingest_sources.R first.")
}

energy_sites_sf <- energy_sites %>%
  filter(!is.na(lat), !is.na(lon)) %>%
  mutate(size_value = dplyr::coalesce(capacity_kbd, production_kbd, 0)) %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

# ── Background ───────────────────────────────────────────────────────────────

bg <- prepare_background_map(focus_bbox)

# ── Color palette from asset registry ────────────────────────────────────────

site_colors <- asset_registry %>%
  filter(geometry_role_default == "site") %>%
  select(asset_label, map_color)
map_colors <- setNames(site_colors$map_color, site_colors$asset_label)

# ── Build map ────────────────────────────────────────────────────────────────

n_sites <- nrow(energy_sites_sf)
n_countries <- n_distinct(energy_sites_sf$country, na.rm = TRUE)

infrastructure_map <- ggplot() +
  geom_sf(data = bg$land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
  geom_sf_text(data = bg$country_labels, aes(label = admin),
               color = "#5f594b", size = 2.8, check_overlap = TRUE) +
  geom_sf(
    data = energy_sites_sf,
    aes(color = asset_label, size = size_value),
    alpha = 0.75,
    inherit.aes = FALSE
  ) +
  coord_sf(
    xlim = c(focus_bbox$west, focus_bbox$east),
    ylim = c(focus_bbox$south, focus_bbox$north),
    expand = FALSE
  ) +
  scale_color_manual(values = map_colors, drop = TRUE, na.value = "#666666") +
  scale_size_continuous(
    range = c(0.8, 7),
    breaks = c(50, 200, 500, 1000),
    labels = function(x) paste0(x, " kbd"),
    guide = guide_legend(override.aes = list(alpha = 1))
  ) +
  labs(
    title = "Gulf Energy Infrastructure: Baseline",
    subtitle = paste0(n_sites, " facilities across ", n_countries, " countries | Size ~ capacity (kbd)"),
    color = "Infrastructure class",
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
    legend.key.size = unit(14, "pt")
  )

print(infrastructure_map)

if (write_output) {
  ensure_dir(project_path("output"))
  ggsave(
    next_numbered_output_path(paths$output_pdf),
    infrastructure_map,
    width = 11, height = 7,
    device = grDevices::pdf
  )
  message("Infrastructure baseline map written to output/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save infrastructure_baseline_map.pdf.")
}
