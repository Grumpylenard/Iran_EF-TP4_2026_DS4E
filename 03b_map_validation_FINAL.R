# 03b_map_validation_FINAL.R
# Incident validation map: two-channel encoding (fill = verification status, size = capacity).
# Shows incident-linked and FIRMS-confirmed sites on a tighter Gulf bbox.
# Labels all incident-linked targets with consolidated Ras Laffan / Mina al-Ahmadi.
# Extracted from 03_make_maps.R (default/validation branch).

source("utils.R")
source("map_utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(rnaturalearth)
  library(sf)
  library(scales)
  library(ggrepel)
})

write_output <- env_flag("WRITE_OUTPUT", FALSE)
settings <- project_settings()

# Tighter Gulf corridor bbox
val_bbox <- list(west = 44, south = 21, east = 60, north = 32)

paths <- list(
  incidents_csv    = project_path("data_processed", "incidents_review.csv"),
  energy_sites_csv = project_path("data_processed", "energy_sites.csv"),
  site_links_csv   = project_path("data_processed", "incident_site_links.csv"),
  firms_csv        = project_path("data_processed", "firms_facility_anomalies.csv"),
  output_pdf       = project_path("output", "incident_validation_map.pdf")
)

# ── Load data ────────────────────────────────────────────────────────────────

incidents <- prepare_incidents(paths$incidents_csv)
energy_sites <- prepare_energy_sites(paths$energy_sites_csv)
incident_site_links <- prepare_site_links(paths$site_links_csv)
firms_facility_anomalies <- prepare_firms_facility_anomalies(paths$firms_csv)

site_validation_summary <- build_site_validation_summary(
  incident_site_links, incidents, energy_sites, firms_facility_anomalies, settings
)

# ── Convert to SF and crop ───────────────────────────────────────────────────

val_label_bbox <- sf::st_bbox(c(
  xmin = val_bbox$west, ymin = val_bbox$south,
  xmax = val_bbox$east, ymax = val_bbox$north
), crs = sf::st_crs(4326))

site_validation_summary_sf <- site_validation_summary %>%
  filter(!is.na(lat), !is.na(lon)) %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

incidents_sf <- incidents %>%
  filter(!is.na(lat), !is.na(lon)) %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE)

val_sites <- if (nrow(site_validation_summary_sf) > 0) {
  sf::st_crop(site_validation_summary_sf, val_label_bbox)
} else {
  site_validation_summary_sf
}

val_incidents <- if (nrow(incidents_sf) > 0) {
  sf::st_crop(incidents_sf, val_label_bbox)
} else {
  incidents_sf
}

# ── Background ───────────────────────────────────────────────────────────────

bg <- prepare_background_map(val_bbox)

# ── Two-channel encoding ────────────────────────────────────────────────────

if (nrow(val_sites) > 0) {
  val_sites <- val_sites %>%
    mutate(
      verification_status = case_when(
        incident_count > 0 & firms_anomaly_flag ~ "FIRMS double-confirmed",
        incident_count > 0 ~ "Incident-linked",
        TRUE ~ "Unlinked infrastructure"
      ),
      size_value = dplyr::coalesce(capacity_kbd, production_kbd, 0)
    )
}

n_incident_links <- if (nrow(val_sites) > 0) sum(val_sites$incident_count > 0, na.rm = TRUE) else 0L
n_firms_confirmed <- if (nrow(val_sites) > 0) sum(val_sites$incident_count > 0 & val_sites$firms_anomaly_flag, na.rm = TRUE) else 0L

verification_fills <- make_verification_fills()

# ── Build map ────────────────────────────────────────────────────────────────

validation_map <- ggplot() +
  geom_sf(data = bg$land, fill = "#efe7d6", color = "#d0c5b4", linewidth = 0.2) +
  geom_sf_text(data = bg$country_labels, aes(label = admin),
               color = "#5f594b", size = 2.8, check_overlap = TRUE)

if (nrow(val_sites) > 0) {
  # Background: unlinked sites (grey, small)
  unlinked <- val_sites %>% filter(verification_status == "Unlinked infrastructure")
  if (nrow(unlinked) > 0) {
    validation_map <- validation_map +
      geom_sf(
        data = unlinked,
        fill = "#bdbdbd", color = "#999999",
        shape = 21, size = 0.6, alpha = 0.25, stroke = 0.15,
        inherit.aes = FALSE, show.legend = FALSE
      )
  }

  # Foreground: incident-linked and FIRMS-confirmed sites
  linked <- val_sites %>% filter(verification_status != "Unlinked infrastructure")
  if (nrow(linked) > 0) {
    validation_map <- validation_map +
      geom_sf(
        data = linked,
        aes(fill = verification_status, size = size_value),
        shape = 21, color = "#202020", stroke = 0.4, alpha = 0.78,
        inherit.aes = FALSE
      )
  }
}

# Incident points as small dark crosses
if (nrow(val_incidents) > 0) {
  validation_map <- validation_map +
    geom_sf(
      data = val_incidents %>% mutate(incident_layer = "Strike location"),
      aes(shape = incident_layer),
      color = "#1e252b", size = 0.7, alpha = 0.4, stroke = 0.4,
      inherit.aes = FALSE
    ) +
    scale_shape_manual(values = c("Strike location" = 3), name = NULL)
}

# ── Labels: all incident-linked targets with consolidation ───────────────────

if (nrow(val_sites) > 0) {
  label_candidates <- val_sites %>%
    filter(
      incident_count > 0,
      !is.na(name), name != "Unnamed site", name != ""
    )

  if (nrow(label_candidates) > 0) {
    label_coords <- sf::st_coordinates(label_candidates)
    label_df <- bind_cols(
      sf::st_drop_geometry(label_candidates),
      tibble(X = label_coords[, "X"], Y = label_coords[, "Y"])
    )

    # Consolidate clustered labels: Ras Laffan terminals → one label,
    # Mina al-Ahmadi variants → one label
    label_df <- label_df %>%
      mutate(
        label_group = case_when(
          grepl("RAS LAFFAN", name, ignore.case = TRUE) ~ "Ras Laffan",
          grepl("^MINA AL.AHMADI", name, ignore.case = TRUE) ~ "Mina al-Ahmadi",
          TRUE ~ name
        )
      ) %>%
      group_by(label_group) %>%
      summarise(
        X = mean(X),
        Y = mean(Y),
        name = first(label_group),
        .groups = "drop"
      )

    validation_map <- validation_map +
      geom_text_repel(
        data = label_df,
        aes(x = X, y = Y, label = name),
        size = 1.8, color = "#20313c", fontface = "bold",
        max.overlaps = 40,
        segment.size = 0.2, segment.color = "#888888",
        box.padding = 0.25, point.padding = 0.15,
        min.segment.length = 0.1,
        force = 2, force_pull = 0.5,
        inherit.aes = FALSE
      )
  }
}

# ── Scales, labs, theme ──────────────────────────────────────────────────────

validation_map <- validation_map +
  coord_sf(
    xlim = c(val_bbox$west, val_bbox$east),
    ylim = c(val_bbox$south, val_bbox$north),
    expand = FALSE
  ) +
  scale_fill_manual(values = verification_fills, drop = FALSE) +
  scale_size_continuous(
    range = c(1.5, 8), breaks = c(100, 300, 500, 900),
    labels = function(x) paste0(x, " kbd"),
    guide = guide_legend(override.aes = list(fill = "#e6550d", shape = 21))
  ) +
  labs(
    title = "Gulf Energy Infrastructure: Incident Validation",
    subtitle = paste0(
      "Orange = incident-linked | Dark red = FIRMS corroboration | Size ~ capacity (kbd) | ",
      n_incident_links, " incident-site links, ", n_firms_confirmed, " FIRMS-confirmed"
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

print(validation_map)

if (write_output) {
  ensure_dir(project_path("output"))
  ggsave(
    next_numbered_output_path(paths$output_pdf),
    validation_map,
    width = 12, height = 8,
    device = grDevices::pdf
  )
  message("Validation map written to output/")
} else {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save incident_validation_map.pdf.")
}
