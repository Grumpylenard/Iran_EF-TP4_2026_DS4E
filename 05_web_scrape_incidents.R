# 05_web_scrape_incidents.R
#
# Scrapes strike/hit data from CENTCOM and CTP-ISW sources
# Extracts incident locations using regex patterns
# Geocodes locations and outputs standalone CSV
#
# Output: data_raw/web_scraped/incidents.csv
# Schema: lat, lon, date, location_name, target_type, source, report_url

source("utils.R")
suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(tidygeocoder)
})

# ── Configuration ──────────────────────────────────────────────────────────────

CENTCOM_BASE  <- "https://www.centcom.mil"
CENTCOM_PATH  <- "/MEDIA/PRESS-RELEASES/"
CENTCOM_PAGES <- as.integer(Sys.getenv("CENTCOM_PAGES", "10"))

ISW_BASE      <- "https://www.criticalthreats.org"
ISW_LIST_URL  <- "https://www.criticalthreats.org/briefs/iran-updates"
ISW_PAGES     <- as.integer(Sys.getenv("ISW_PAGES", "5"))

OUTPUT_FILE   <- project_path("data_raw", "web_scraped", "incidents.csv")

scrape_centcom <- env_flag("SCRAPE_CENTCOM", TRUE)
scrape_isw <- env_flag("SCRAPE_ISW", TRUE)
write_output <- env_flag("WRITE_OUTPUT", TRUE)

KEYWORDS <- c(
  "strike", "struck", "destroyed", "targeted", "killed",
  "iran", "irgc", "houthi", "missile", "drone", "precision",
  "airstrike", "combined force", "idf", "bomb", "attack"
)
KEYWORD_PATTERN <- paste(KEYWORDS, collapse = "|")

POLITE_DELAY <- 0.8   # seconds between requests

# ── Helper functions ───────────────────────────────────────────────────────────

safe_read_html <- function(url) {
  Sys.sleep(POLITE_DELAY)
  tryCatch(
    read_html(url),
    error = function(e) { 
      warning("Fetch failed: ", url, "\n  ", e$message)
      NULL 
    }
  )
}

# ── CENTCOM scraping ───────────────────────────────────────────────────────────

fetch_centcom_page <- function(page_num) {
  url  <- paste0(CENTCOM_BASE, CENTCOM_PATH, "?Page=", page_num)
  html <- safe_read_html(url)
  if (is.null(html)) return(tibble())
  
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
  
  selectors <- c(".atsBody", ".body-content", "article .field-items", ".press-release-body")
  for (sel in selectors) {
    node <- html |> html_element(sel)
    if (!is.na(node)) return(node |> html_text(trim = TRUE))
  }
  NA_character_
}

scrape_centcom_articles <- function() {
  if (!scrape_centcom) {
    message("CENTCOM scraping disabled")
    return(tibble())
  }
  
  message("=== CENTCOM: fetching ", CENTCOM_PAGES, " listing pages ===")
  listings <- map_dfr(seq_len(CENTCOM_PAGES), fetch_centcom_page)
  
  if (nrow(listings) == 0) {
    message("  No CENTCOM articles found (check network connection)")
    return(tibble())
  }
  
  listings <- listings |> distinct(url, .keep_all = TRUE)
  
  message("  Total articles: ", nrow(listings))
  message("  Filtering by keywords...")
  
  filtered <- listings |>
    filter(str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)))
  
  message("  After keyword filter: ", nrow(filtered))
  
  if (nrow(filtered) == 0) return(tibble())
  
  message("  Fetching article text...")
  filtered |>
    mutate(text = map_chr(url, fetch_centcom_text))
}

# ── ISW scraping ───────────────────────────────────────────────────────────────

fetch_isw_listing_page <- function(page_num) {
  url  <- paste0(ISW_LIST_URL, if (page_num > 1) paste0("?page=", page_num) else "")
  html <- safe_read_html(url)
  if (is.null(html)) return(tibble())
  
  anchors <- html |> html_elements("article a, .post-title a, h2 a, h3 a")
  titles  <- anchors |> html_text(trim = TRUE)
  hrefs   <- anchors |> html_attr("href")
  dates   <- html |> html_elements("time, .post-date, .entry-date") |>
    html_text(trim = TRUE)
  
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
    filter(str_detect(url, "iran-update"))
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

scrape_isw_articles <- function() {
  if (!scrape_isw) {
    message("ISW scraping disabled")
    return(tibble())
  }
  
  message("=== CTP-ISW: fetching ", ISW_PAGES, " listing pages ===")
  listings <- map_dfr(seq_len(ISW_PAGES), fetch_isw_listing_page)
  
  if (nrow(listings) == 0) {
    message("  No ISW articles found (check network connection)")
    return(tibble())
  }
  
  listings <- listings |> distinct(url, .keep_all = TRUE)
  
  message("  Total Iran update articles: ", nrow(listings))
  message("  Filtering by keywords...")
  
  filtered <- listings |>
    filter(
      str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)) |
      str_detect(title, regex("special report|update", ignore_case = TRUE))
    )
  
  message("  After keyword filter: ", nrow(filtered))
  
  if (nrow(filtered) == 0) return(tibble())
  
  message("  Fetching article text...")
  filtered |>
    mutate(text = map_chr(url, fetch_isw_text))
}

# ── Regex extraction ───────────────────────────────────────────────────────────

LOCATION_PATTERNS <- list(
  # "approximately 50 km north of Tehran"
  approx = "(?i)approximately\\s+[\\d,.]+\\s*(?:km|kilometers?|miles?)\\s+(\\w+)\\s+of\\s+([A-Z][\\w\\s-]+?)(?:\\.|,|;|\\s+in\\s+|$)",
  # "in Tehran Province" or "near Hormozgan Governorate"
  province = "(?i)(?:in|near|at)\\s+(?:the\\s+)?([A-Z][\\w\\s-]+?(?:Province|Governorate|District|Region))(?:\\.|,|;|$)",
  # "in Tehran, Iran" or "near Baghdad, Iraq"
  city_country = "(?i)(?:in|near|at)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?),\\s*(?:Iran|Iraq|Yemen|Syria|Lebanon|Kuwait)",
  # "struck a facility in Bandar Abbas" or "targeted site near Khomeini"
  city_plain = "(?i)(?:struck|targeted|destroyed|hit|attack(?:ed|ing)?)\\s+(?:a|an|the)?\\s*[\\w\\s]*?(?:in|near|at)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?)",
  # "strike on Tehran" or "attack on Isfahan"
  strike_on = "(?i)(?:strike|attack|bombing)\\s+on\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?)"
)

TARGET_TYPE_PATTERNS <- list(
  # Common military/infrastructure targets
  base = "(?i)(missile|air|naval|military)\\s+(base|facility|installation)",
  site = "(?i)(radar|command|storage|munitions|weapons|launch)\\s+(site|facility|center)",
  infrastructure = "(?i)(oil|gas|refinery|port|pipeline|power|energy)\\s+(facility|infrastructure|site|terminal)",
  depot = "(?i)(weapons?|ammunition|fuel)\\s+(depot|storage)",
  headquarters = "(?i)(command|control|headquarters|HQ)",
  launcher = "(?i)(launcher|TEL|transporter)"
)

extract_locations <- function(text) {
  if (is.na(text) || nchar(text) < 20) return(character())
  
  locations <- character()
  
  for (pattern in LOCATION_PATTERNS) {
    matches <- gregexpr(pattern, text, perl = TRUE)
    match_data <- regmatches(text, matches)[[1]]
    
    if (length(match_data) > 0) {
      for (match in match_data) {
        cap <- regmatches(match, regexec(pattern, match, perl = TRUE))[[1]]
        if (length(cap) >= 2) {
          # Take the most specific capture group (usually the last one)
          location <- trimws(cap[length(cap)])
          if (nchar(location) > 2 && !grepl("^(the|this|that|these)$", location, ignore.case = TRUE)) {
            locations <- c(locations, location)
          }
        }
      }
    }
  }
  
  unique(locations)
}

extract_target_type <- function(text) {
  if (is.na(text) || nchar(text) < 20) return(NA_character_)
  
  for (pattern in TARGET_TYPE_PATTERNS) {
    match <- regmatches(text, regexec(pattern, text, perl = TRUE))[[1]]
    if (length(match) >= 1) {
      return(trimws(match[1]))
    }
  }
  
  NA_character_
}

extract_events <- function(article_row) {
  locations <- extract_locations(article_row$text)
  
  if (length(locations) == 0) {
    return(tibble())
  }
  
  target_type <- extract_target_type(article_row$text)
  
  tibble(
    location_name = locations,
    target_type = target_type,
    source = article_row$source,
    date = article_row$date,
    report_url = article_row$url
  )
}

# ── Geocoding ──────────────────────────────────────────────────────────────────

geocode_locations <- function(events_df) {
  if (nrow(events_df) == 0) {
    return(events_df |> mutate(lat = numeric(), lon = numeric()))
  }
  
  unique_locations <- events_df |>
    distinct(location_name) |>
    filter(!is.na(location_name), nchar(location_name) > 0)
  
  if (nrow(unique_locations) == 0) {
    return(events_df |> mutate(lat = NA_real_, lon = NA_real_))
  }
  
  message("  Geocoding ", nrow(unique_locations), " unique locations via OSM...")
  
  geocoded <- tryCatch({
    unique_locations |>
      geocode(location_name, method = "osm", lat = lat, long = lon)
  }, error = function(e) {
    warning("Geocoding failed: ", e$message)
    unique_locations |> mutate(lat = NA_real_, lon = NA_real_)
  })
  
  events_df |>
    left_join(geocoded, by = "location_name")
}

# ── Output formatting ──────────────────────────────────────────────────────────

format_output <- function(events_df) {
  events_df |>
    select(lat, lon, date, location_name, target_type, source, report_url) |>
    filter(!is.na(location_name)) |>
    arrange(desc(date), location_name)
}

# ── Main pipeline ──────────────────────────────────────────────────────────────

main <- function() {
  message("\n=== Web Scraping Pipeline ===\n")
  
  # 1. Scrape articles
  centcom_articles <- scrape_centcom_articles()
  isw_articles <- scrape_isw_articles()
  
  all_articles <- bind_rows(centcom_articles, isw_articles)
  
  if (nrow(all_articles) == 0) {
    message("\n--- No articles scraped ---")
    message("Both sources returned no results. Check network connection or keyword filters.")
    return(invisible(NULL))
  }
  
  all_articles <- all_articles |>
    distinct(url, .keep_all = TRUE) |>
    filter(!is.na(text), nchar(text) > 50)
  
  message("\n--- Article Summary ---")
  message("Total articles after dedup: ", nrow(all_articles))
  
  if (nrow(all_articles) == 0) {
    message("No articles found. Check CSS selectors and keyword list.")
    return(invisible(NULL))
  }
  
  # 2. Extract events
  message("\n--- Extracting Events (regex) ---")
  
  events_list <- map(seq_len(nrow(all_articles)), function(i) {
    extract_events(all_articles[i, ])
  })
  
  events <- bind_rows(events_list)
  message("  Total events extracted: ", nrow(events))
  
  if (nrow(events) == 0) {
    message("No events extracted. Check regex patterns.")
    return(invisible(NULL))
  }
  
  # 3. Geocode
  message("\n--- Geocoding Locations ---")
  events_geocoded <- geocode_locations(events)
  
  n_coords <- sum(!is.na(events_geocoded$lat))
  message("  Events with coordinates: ", n_coords, "/", nrow(events_geocoded))
  
  # 4. Format and write output
  output_data <- format_output(events_geocoded)
  
  message("\n--- Output Summary ---")
  message("Total output rows: ", nrow(output_data))
  message("Rows with coordinates: ", sum(!is.na(output_data$lat)))
  
  if (write_output) {
    write_csv_safe(output_data, OUTPUT_FILE)
    message("\n✓ Written to: ", OUTPUT_FILE)
  } else {
    message("\nPreview mode (WRITE_OUTPUT=FALSE). Output not saved.")
    if (nrow(output_data) > 0) {
      message("\nFirst few rows:")
      print(head(output_data, 5))
    }
  }
  
  invisible(output_data)
}

# Run main pipeline
if (!interactive()) {
  main()
}
