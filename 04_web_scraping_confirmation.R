# strikes_scraper.R
#
# Scrapes strike/hit data from two sources:
#   1. CENTCOM press releases  (centcom.mil)
#   2. CTP-ISW Iran Updates    (criticalthreats.org) — note: NOT understandingwar.org
#
# Pipeline:
#   listing pages → keyword filter → fetch text → regex extraction
#   → Ollama fallback (multi-event) → OSM geocoding → unified CSV
#
# Output schema:
#   source, date, report_title, report_url,
#   location_name, country, target_type, munitions, actor, bda,
#   lat, lon
#
# Dependencies: rvest, tidyverse, httr2, jsonlite, tidygeocoder

library(rvest)
library(tidyverse)
library(httr2)
library(jsonlite)
library(tidygeocoder)

# ── Config ─────────────────────────────────────────────────────────────────────

CENTCOM_BASE  <- "https://www.centcom.mil"
CENTCOM_PATH  <- "/MEDIA/PRESS-RELEASES/"
CENTCOM_PAGES <- 10

ISW_BASE      <- "https://www.criticalthreats.org"
ISW_LIST_URL  <- "https://www.criticalthreats.org/briefs/iran-updates"
ISW_PAGES     <- 5        # listing pages; each has ~10 articles

OUTPUT_FILE   <- "strikes_data.csv"

OLLAMA_URL    <- "http://localhost:11434/api/generate"
OLLAMA_MODEL  <- "llama3"   # change to whichever model you have pulled

KEYWORDS <- c(
  "strike", "struck", "destroyed", "targeted", "killed",
  "iran", "irgc", "houthi", "missile", "drone", "precision",
  "airstrike", "combined force", "idf", "bomb"
)
KEYWORD_PATTERN <- paste(KEYWORDS, collapse = "|")

POLITE_DELAY <- 0.8   # seconds between requests

# ── Generic helpers ────────────────────────────────────────────────────────────

safe_read_html <- function(url) {
  Sys.sleep(POLITE_DELAY)
  tryCatch(
    read_html(url),
    error = function(e) { warning("fetch failed: ", url, "\n  ", e$message); NULL }
  )
}

# ── CENTCOM scraping ───────────────────────────────────────────────────────────

fetch_centcom_page <- function(page_num) {
  url  <- paste0(CENTCOM_BASE, CENTCOM_PATH, "?Page=", page_num)
  html <- safe_read_html(url)
  if (is.null(html)) return(tibble())
  
  # Verify selectors against live page — CENTCOM has restructured before
  titles <- html |> html_elements(".alist-title a") |> html_text(trim = TRUE)
  hrefs  <- html |> html_elements(".alist-title a") |> html_attr("href")
  dates  <- html |> html_elements(".alist-date")    |> html_text(trim = TRUE)
  
  n <- min(length(titles), length(hrefs), length(dates))
  if (n == 0) return(tibble())
  
  tibble(
    source = "CENTCOM",
    title  = titles[1:n],
    url    = paste0(CENTCOM_BASE, hrefs[1:n]),
    date   = dates[1:n]
  )
}

fetch_centcom_text <- function(url) {
  html <- safe_read_html(url)
  if (is.null(html)) return(NA_character_)
  # Common body selectors — try each in order
  selectors <- c(".atsBody", ".body-content", "article .field-items", ".press-release-body")
  for (sel in selectors) {
    node <- html |> html_element(sel)
    if (!is.na(node)) return(node |> html_text(trim = TRUE))
  }
  NA_character_
}

scrape_centcom <- function() {
  message("=== CENTCOM: fetching ", CENTCOM_PAGES, " listing pages ===")
  listings <- map_dfr(seq_len(CENTCOM_PAGES), fetch_centcom_page) |>
    distinct(url, .keep_all = TRUE)
  
  message("  total: ", nrow(listings), " | filtering by keyword...")
  listings |>
    filter(str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE))) |>
    mutate(text = map_chr(url, fetch_centcom_text))
}

# ── CTP-ISW scraping ───────────────────────────────────────────────────────────
# Articles live at criticalthreats.org/analysis/iran-update-*
# Listing at criticalthreats.org/briefs/iran-updates (paginated)

fetch_isw_listing_page <- function(page_num) {
  url  <- paste0(ISW_LIST_URL, if (page_num > 1) paste0("?page=", page_num) else "")
  html <- safe_read_html(url)
  if (is.null(html)) return(tibble())
  
  # Selectors — verify against live page
  # criticalthreats.org uses article cards; common patterns below
  anchors <- html |> html_elements("article a, .post-title a, h2 a, h3 a")
  titles  <- anchors |> html_text(trim = TRUE)
  hrefs   <- anchors |> html_attr("href")
  dates   <- html |> html_elements("time, .post-date, .entry-date") |>
    html_text(trim = TRUE)
  
  # Pad dates if count mismatches (some cards may omit them)
  if (length(dates) < length(titles)) {
    dates <- c(dates, rep(NA_character_, length(titles) - length(dates)))
  }
  
  n <- min(length(titles), length(hrefs))
  if (n == 0) return(tibble())
  
  tibble(
    source = "CTP-ISW",
    title  = titles[1:n],
    url    = if_else(
      str_starts(hrefs[1:n], "http"),
      hrefs[1:n],
      paste0(ISW_BASE, hrefs[1:n])
    ),
    date   = dates[1:n]
  ) |>
    filter(str_detect(url, "iran-update"))   # only Iran update articles
}

fetch_isw_text <- function(url) {
  html <- safe_read_html(url)
  if (is.null(html)) return(NA_character_)
  selectors <- c(".entry-content", ".post-content", "article .content", "main article")
  for (sel in selectors) {
    node <- html |> html_element(sel)
    if (!is.na(node)) return(node |> html_text(trim = TRUE))
  }
  NA_character_
}

scrape_isw <- function() {
  message("=== CTP-ISW: fetching ", ISW_PAGES, " listing pages ===")
  listings <- map_dfr(seq_len(ISW_PAGES), fetch_isw_listing_page) |>
    distinct(url, .keep_all = TRUE)
  
  message("  total: ", nrow(listings), " articles")
  # ISW articles are already Iran-focused; still apply keyword filter for
  # articles that contain actual strike events vs. purely political analysis
  listings |>
    filter(str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)) |
             str_detect(title, regex("special report|update", ignore_case = TRUE))) |>
    mutate(text = map_chr(url, fetch_isw_text))
}

# ── Regex location extraction (fast pass) ─────────────────────────────────────
# Returns first match only — Ollama handles multi-event extraction

LOCATION_PATTERNS <- list(
  approx    = "(?i)approximately\\s+[\\d,.]+\\s*(?:km|miles?)\\s+\\w+\\s+of\\s+([\\w\\s]+?)(?:\\.|,|;|$)",
  province  = "(?i)(?:in|near|at)\\s+(?:the\\s+)?([A-Z][\\w\\s]+?(?:Province|Governorate|District|Region))",
  city_in   = "(?i)(?:in|near|at)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?),\\s*(?:Iran|Iraq|Yemen|Syria|Lebanon)",
  city_plain = "(?i)(?:struck|targeted|destroyed|hit)\\s+(?:a|an|the)?\\s*[\\w\\s]*?(?:in|near|at)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?)"
)

extract_location_regex <- function(text) {
  if (is.na(text)) return(NA_character_)
  for (pattern in LOCATION_PATTERNS) {
    cap <- regmatches(text, regexec(pattern, text, perl = TRUE))[[1]]
    if (length(cap) >= 2 && nchar(trimws(cap[2])) > 2) return(trimws(cap[2]))
  }
  NA_character_
}

# ── Ollama extraction ──────────────────────────────────────────────────────────
# Returns a list of events (one article may contain many strikes)
# Each event: location_name, country, target_type, munitions, actor, bda

OLLAMA_PROMPT_TEMPLATE <- paste0(
  "You are a military analyst. Extract ALL distinct strike events from the text below.\n",
  "Return ONLY a JSON array. Each element must have these fields (use null if unknown):\n",
  "  location_name : string  (most specific place name, city or site)\n",
  "  country       : string\n",
  "  target_type   : string  (e.g. missile base, command center, radar site)\n",
  "  munitions     : string  (e.g. precision-guided, Tomahawk, F-35)\n",
  "  actor         : string  (e.g. US, Israel, combined force, IRGC)\n",
  "  bda           : string  (battle damage assessment, one sentence or null)\n\n",
  "Return only the JSON array with no markdown, no preamble.\n\nText:\n"
)

ollama_extract_events <- function(text, source_label) {
  empty <- tibble(
    location_name = NA_character_, country = NA_character_,
    target_type   = NA_character_, munitions = NA_character_,
    actor         = NA_character_, bda = NA_character_
  )
  if (is.na(text) || nchar(text) < 80) return(empty)
  
  # ISW articles are long — truncate to stay within context window
  max_chars <- if (source_label == "CTP-ISW") 6000 else 3000
  prompt    <- paste0(OLLAMA_PROMPT_TEMPLATE, substr(text, 1, max_chars))
  
  resp <- tryCatch(
    request(OLLAMA_URL) |>
      req_body_json(list(model = OLLAMA_MODEL, prompt = prompt, stream = FALSE)) |>
      req_timeout(120) |>
      req_perform() |>
      resp_body_json(),
    error = function(e) { warning("Ollama failed: ", e$message); NULL }
  )
  
  if (is.null(resp)) return(empty)
  
  raw  <- gsub("```json|```", "", resp$response)
  parsed <- tryCatch(fromJSON(raw, simplifyDataFrame = TRUE), error = function(e) NULL)
  
  if (is.null(parsed) || !is.data.frame(parsed) || nrow(parsed) == 0) return(empty)
  
  # Ensure all required columns exist
  required <- c("location_name","country","target_type","munitions","actor","bda")
  for (col in required) {
    if (!col %in% names(parsed)) parsed[[col]] <- NA_character_
  }
  
  parsed |>
    select(all_of(required)) |>
    mutate(across(everything(), as.character))
}

# ── Geocoding ──────────────────────────────────────────────────────────────────

geocode_df <- function(df) {
  needs <- df |> filter(!is.na(location_name) & is.na(lat))
  if (nrow(needs) == 0) return(df)
  
  message("  geocoding ", nrow(needs), " unique locations via OSM...")
  geocoded <- needs |>
    distinct(location_name) |>
    geocode(location_name, method = "osm", lat = lat_geo, long = lon_geo)
  
  df |>
    left_join(geocoded, by = "location_name") |>
    mutate(
      lat = coalesce(lat, lat_geo),
      lon = coalesce(lon, lon_geo)
    ) |>
    select(-lat_geo, -lon_geo)
}

# ── Per-article event extraction ───────────────────────────────────────────────
# Tries regex first; falls back to Ollama if no location found.
# Returns one row per event (Ollama may yield multiple per article).

extract_events <- function(row) {
  loc_regex <- extract_location_regex(row$text)
  
  if (!is.na(loc_regex)) {
    # Regex succeeded — single event row, no Ollama needed
    return(tibble(
      location_name = loc_regex,
      country       = NA_character_,
      target_type   = NA_character_,
      munitions     = NA_character_,
      actor         = NA_character_,
      bda           = NA_character_
    ))
  }
  
  # Regex failed — use Ollama
  ollama_extract_events(row$text, row$source)
}

# ── Main pipeline ──────────────────────────────────────────────────────────────

main <- function() {
  
  # 1. Scrape both sources
  centcom <- scrape_centcom()
  isw     <- scrape_isw()
  
  all_articles <- bind_rows(centcom, isw) |>
    distinct(url, .keep_all = TRUE)
  
  message("\nTotal articles after dedup: ", nrow(all_articles))
  
  if (nrow(all_articles) == 0) {
    message("No articles found — check CSS selectors and keyword list.")
    return(invisible(NULL))
  }
  
  # 2. Extract events per article
  message("\n--- Extracting events (regex + Ollama fallback) ---")
  
  events_list <- vector("list", nrow(all_articles))
  n_ollama <- 0
  
  for (i in seq_len(nrow(all_articles))) {
    row    <- all_articles[i, ]
    used_ollama <- is.na(extract_location_regex(row$text))
    if (used_ollama) n_ollama <- n_ollama + 1
    
    events <- extract_events(row)
    events_list[[i]] <- events |>
      mutate(
        source      = row$source,
        date        = row$date,
        report_title = row$title,
        report_url  = row$url,
        lat         = NA_real_,
        lon         = NA_real_
      )
  }
  
  message("  Ollama used for ", n_ollama, "/", nrow(all_articles), " articles")
  
  combined <- bind_rows(events_list) |>
    filter(!is.na(location_name)) |>
    select(source, date, report_title, report_url,
           location_name, country, target_type, munitions, actor, bda,
           lat, lon)
  
  message("  Total events extracted: ", nrow(combined))
  
  # 3. Geocode
  message("\n--- Geocoding ---")
  combined <- geocode_df(combined)
  
  n_coords <- sum(!is.na(combined$lat))
  message("  Events with coordinates: ", n_coords, "/", nrow(combined))
  
  # 4. Write output
  combined <- combined |>
    arrange(desc(date))
  
  write_csv(combined, OUTPUT_FILE, na = "")
  message("\n--- Done. Written to: ", OUTPUT_FILE)
  message("Rows: ", nrow(combined))
  
  invisible(combined)
}

main()

