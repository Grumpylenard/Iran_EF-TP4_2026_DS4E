# 06_scrape_incidents.R
#
# Scrapes structured strike/attack data from Wikipedia, iranstrikemap.com,
# and CENTCOM fact sheets. Normalizes into a single tibble matching the
# TP4-compatible schema used by 02_build_incidents.R.
#
# Produces: data_processed/scraped_incidents_raw.csv
#           data_processed/centcom_factsheets_parsed.csv (validation reference)
#
# Usage:
#   Rscript 06_scrape_incidents.R
#   SCRAPE_WIKIPEDIA=TRUE SCRAPE_IRANSTRIKEMAP=TRUE SCRAPE_CENTCOM=TRUE Rscript 06_scrape_incidents.R

source("utils.R")
suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(readr)
  library(tidyr)
  library(httr2)
  library(jsonlite)
})

# ── Configuration ────────────────────────────────────────────────────────────

scrape_wikipedia     <- env_flag("SCRAPE_WIKIPEDIA", TRUE)
do_scrape_iranstrikemap <- env_flag("SCRAPE_IRANSTRIKEMAP", TRUE)
scrape_centcom       <- env_flag("SCRAPE_CENTCOM", TRUE)
write_output         <- env_flag("WRITE_OUTPUT", TRUE)

settings <- project_settings()

paths <- list(
  scraped_csv   = project_path("data_processed", "scraped_incidents_raw.csv"),
  centcom_csv   = project_path("data_processed", "centcom_factsheets_parsed.csv"),
  energy_csv    = project_path("data_processed", "energy_sites.csv")
)

POLITE_DELAY <- 1.0

# ── Known facility geocoding lookup ──────────────────────────────────────────
# Hardcoded coordinates for key facilities mentioned across Wikipedia articles.
# Used as geocoding fallback when the target name matches.

facility_coords <- tribble(
  ~pattern,                          ~facility_name,                    ~lat,     ~lon,    ~country,
  "ras laffan",                      "Ras Laffan Industrial City",      25.897,   51.533,  "Qatar",
  "mesaieed|mesaeed",                "Mesaieed Industrial City",        25.01,    51.55,   "Qatar",
  "pearl gtl",                       "Pearl GTL",                       25.91,    51.54,   "Qatar",
  "ruwais",                          "Ruwais Industrial Complex",       24.11,    52.73,   "UAE",
  "shah gas",                        "Shah Gas Field",                  23.56,    53.75,   "UAE",
  "habshan",                         "Habshan Gas Processing",          23.83,    53.62,   "UAE",
  "bab oilfield|bab oil field",      "Bab Oilfield",                   23.57,    54.05,   "UAE",
  "fujairah.*(?:oil|storage|terminal)", "Fujairah Oil Storage",        25.15,    56.35,   "UAE",
  "jebel ali",                       "Jebel Ali Port",                  25.004,   55.064,  "UAE",
  "abu dhabi.fujairah|fujairah.pipeline", "Abu Dhabi-Fujairah Pipeline", 24.6,   55.5,    "UAE",
  "mina al.ahmadi|mina ahmadi",      "Mina al-Ahmadi Refinery",        29.08,    48.15,   "Kuwait",
  "ras tanura",                      "Ras Tanura",                      26.64,    50.16,   "Saudi Arabia",
  "yanbu|samref",                    "Yanbu Refinery/SAMREF",           24.08,    38.06,   "Saudi Arabia",
  "jubail",                          "Jubail Petrochemical",            27.00,    49.66,   "Saudi Arabia",
  "bapco|sitra",                     "BAPCO Refinery",                  26.03,    50.55,   "Bahrain",
  "south pars|asaluyeh",             "South Pars / Asaluyeh",           27.48,    52.60,   "Iran",
  "kharg island|kharg",              "Kharg Island",                    29.23,    50.32,   "Iran",
  "bandar abbas",                    "Bandar Abbas",                    27.19,    56.27,   "Iran",
  "bushehr",                         "Bushehr",                         28.97,    50.84,   "Iran",
  "abadan",                          "Abadan Refinery",                 30.35,    48.28,   "Iran",
  "isfahan|esfahan",                 "Isfahan",                         32.65,    51.68,   "Iran",
  "tabriz",                          "Tabriz Refinery",                 38.08,    46.30,   "Iran",
  "tehran",                          "Tehran",                          35.69,    51.39,   "Iran",
  "al udeid|al.udeid",               "Al Udeid Air Base",               25.12,    51.31,   "Qatar",
  "dhahran",                         "Dhahran",                         26.27,    50.21,   "Saudi Arabia",
  "nevatim",                         "Nevatim Airbase",                 31.21,    34.93,   "Israel",
  "al dhafra|al.dhafra",             "Al Dhafra Air Base",              24.25,    54.55,   "UAE",
  "duqm|duqum",                      "Duqm Port",                      19.67,    57.70,   "Oman",
  "muscat",                          "Muscat",                          23.59,    58.55,   "Oman",
  "sohar",                           "Sohar Port/Refinery",             24.36,    56.73,   "Oman",
  "kuwait city",                     "Kuwait City",                     29.38,    47.99,   "Kuwait",
  "adnoc",                           "ADNOC Facility",                  24.45,    54.65,   "UAE",
  "aramco.*(?:ras|abqaiq|ghawar)",   "Aramco Facility",                25.94,    49.68,   "Saudi Arabia",
  "abqaiq",                          "Abqaiq Processing",               25.94,    49.68,   "Saudi Arabia",
  "ghawar",                          "Ghawar Oil Field",                25.37,    49.49,   "Saudi Arabia",
  "qatar.?energy",                   "QatarEnergy HQ",                  25.30,    51.53,   "Qatar"
)

# ── Helpers ──────────────────────────────────────────────────────────────────

geocode_target <- function(target_text, energy_sites = NULL) {
  text_lower <- str_to_lower(target_text %||% "")
  if (text_lower == "" || is.na(text_lower)) {
    return(list(lat = NA_real_, lon = NA_real_, matched_name = NA_character_))
  }

  # 1. Try facility_coords lookup

  for (i in seq_len(nrow(facility_coords))) {
    if (str_detect(text_lower, facility_coords$pattern[i])) {
      return(list(
        lat = facility_coords$lat[i],
        lon = facility_coords$lon[i],
        matched_name = facility_coords$facility_name[i]
      ))
    }
  }

  # 2. Try matching against energy_sites.csv if available
  if (!is.null(energy_sites) && nrow(energy_sites) > 0) {
    name_col <- if ("site_name" %in% names(energy_sites)) "site_name" else "name"
    sites_with_names <- energy_sites %>%
      filter(!is.na(.data[[name_col]]), !is.na(lat), !is.na(lon))

    if (nrow(sites_with_names) > 0) {
      # Try substring match
      match_idx <- which(str_detect(str_to_lower(sites_with_names[[name_col]]), fixed(text_lower)))
      if (length(match_idx) == 0) {
        # Try each word (>3 chars) from target_text against site names
        words <- str_extract_all(text_lower, "[a-z]{4,}")[[1]]
        for (w in words) {
          match_idx <- which(str_detect(str_to_lower(sites_with_names[[name_col]]), fixed(w)))
          if (length(match_idx) > 0) break
        }
      }

      if (length(match_idx) > 0) {
        best <- sites_with_names[match_idx[1], ]
        return(list(
          lat = best$lat,
          lon = best$lon,
          matched_name = best[[name_col]]
        ))
      }
    }
  }

  list(lat = NA_real_, lon = NA_real_, matched_name = NA_character_)
}

classify_strike_type <- function(text) {
  text_lower <- str_to_lower(text %||% "")
  case_when(
    str_detect(text_lower, "ballistic") ~ "ballistic_missile",
    str_detect(text_lower, "cruise") ~ "cruise_missile",
    str_detect(text_lower, "drone|uav|shahed|unmanned") ~ "drone",
    str_detect(text_lower, "airstrike|air strike|sortie|bomb") ~ "airstrike",
    str_detect(text_lower, "intercept|debris") ~ "intercepted_debris",
    str_detect(text_lower, "missile") ~ "missile",
    TRUE ~ "unknown"
  )
}

classify_actor <- function(text) {
  text_lower <- str_to_lower(text %||% "")
  case_when(
    str_detect(text_lower, "iran|irgc|islamic republic|tehran") ~ "Iran",
    str_detect(text_lower, "israel|idf|israeli") & str_detect(text_lower, "us|u\\.s\\.|america|centcom") ~ "US/Israel",
    str_detect(text_lower, "israel|idf|israeli") ~ "Israel",
    str_detect(text_lower, "us|u\\.s\\.|america|centcom|coalition") ~ "US",
    str_detect(text_lower, "houthi") ~ "Houthi",
    TRUE ~ "unknown"
  )
}

guess_country <- function(text) {
  text_lower <- str_to_lower(text %||% "")
  case_when(
    str_detect(text_lower, "qatar|doha|ras laffan|mesaieed|al udeid") ~ "Qatar",
    str_detect(text_lower, "uae|emirates|abu dhabi|dubai|ruwais|fujairah|jebel ali|al dhafra") ~ "United Arab Emirates",
    str_detect(text_lower, "bahrain|bapco|sitra|manama") ~ "Bahrain",
    str_detect(text_lower, "saudi|aramco|ras tanura|yanbu|jubail|abqaiq|ghawar|dhahran") ~ "Saudi Arabia",
    str_detect(text_lower, "kuwait|ahmadi") ~ "Kuwait",
    str_detect(text_lower, "oman|duqm|muscat|sohar") ~ "Oman",
    str_detect(text_lower, "iran|tehran|isfahan|tabriz|bushehr|kharg|asaluyeh|south pars|abadan|bandar abbas") ~ "Iran",
    str_detect(text_lower, "israel|tel aviv|nevatim|dimona") ~ "Israel",
    str_detect(text_lower, "iraq|baghdad|erbil|anbar") ~ "Iraq",
    TRUE ~ NA_character_
  )
}

make_incident_id <- function(source_system, date, target_text, seq_num) {
  slug <- str_to_lower(target_text %||% "unknown") %>%
    str_replace_all("[^a-z0-9]+", "_") %>%
    str_replace_all("^_|_$", "") %>%
    str_sub(1, 30)
  date_str <- str_replace_all(as.character(date %||% "unknown"), "-", "")
  paste0(source_system, "_", date_str, "_", slug, "_", seq_num)
}

# ── Wikipedia scraping ───────────────────────────────────────────────────────

wiki_articles <- c(
  "List_of_attacks_during_the_2026_Iran_war",
  "2026_Iranian_strikes_on_the_United_Arab_Emirates",
  "2026_Iranian_strikes_on_Qatar",
  "2026_Iranian_strikes_on_Bahrain",
  "2026_South_Pars_field_attack",
  "Timeline_of_the_2026_Iran_war"
)

fetch_wiki_wikitext <- function(page_title) {
  Sys.sleep(POLITE_DELAY)
  url <- paste0(
    "https://en.wikipedia.org/w/api.php?action=parse&page=",
    URLencode(page_title, reserved = TRUE),
    "&prop=wikitext&format=json"
  )

  resp <- tryCatch({
    request(url) %>%
      req_timeout(30) %>%
      req_headers(`User-Agent` = "IranWarSeminarProject/1.0 (university research)") %>%
      req_perform()
  }, error = function(e) {
    warning("Failed to fetch Wikipedia page: ", page_title, " — ", conditionMessage(e))
    return(NULL)
  })

  if (is.null(resp)) return(NA_character_)
  body <- resp_body_json(resp)

  if (!is.null(body$error)) {
    warning("Wikipedia API error for ", page_title, ": ", body$error$info)
    return(NA_character_)
  }

  body$parse$wikitext$`*` %||% NA_character_
}

extract_ref_urls_near <- function(wikitext, match_text, window = 500) {
  # Find position of match_text in wikitext, then extract <ref> URLs within window chars
  pos <- regexpr(fixed(str_sub(match_text, 1, 60)), wikitext, fixed = TRUE)
  if (pos == -1L) {
    # Fallback: try first 30 chars
    pos <- regexpr(fixed(str_sub(match_text, 1, 30)), wikitext, fixed = TRUE)
  }
  if (pos == -1L) return(NA_character_)

  start <- max(1L, as.integer(pos) - window)
  end <- min(nchar(wikitext), as.integer(pos) + nchar(match_text) + window)
  context <- substr(wikitext, start, end)

  # Extract URLs from <ref> tags: both <ref>url</ref> and <ref name="...">content with [url]</ref>
  # Pattern 1: bare URLs inside ref tags
  ref_blocks <- str_extract_all(context, "<ref[^>]*>.*?</ref>")[[1]]
  urls <- character()
  for (block in ref_blocks) {
    # Extract {{cite web|url=...}} patterns
    cite_urls <- str_match_all(block, "\\|\\s*url\\s*=\\s*([^|}<\\s]+)")[[1]]
    if (nrow(cite_urls) > 0) urls <- c(urls, cite_urls[, 2])
    # Extract bare URLs
    bare <- str_extract_all(block, "https?://[^\\s|}<\\]]+")[[1]]
    urls <- c(urls, bare)
  }

  urls <- unique(urls)
  # Clean trailing punctuation
  urls <- str_remove(urls, "[.,;)\\]]+$")
  # Drop wikipedia/wikidata internal links
  urls <- urls[!str_detect(urls, "(?i)wikipedia\\.org|wikidata\\.org|wikimedia\\.org")]

  if (length(urls) == 0) return(NA_character_)
  paste(urls, collapse = " | ")
}

parse_wiki_tables <- function(wikitext, source_article) {
  if (is.na(wikitext) || !nzchar(wikitext)) return(tibble())

  rows <- list()
  idx <- 0L

  # Split into lines
  lines <- str_split(wikitext, "\n")[[1]]

  # State tracking for wikitable parsing
  in_table <- FALSE
  header_cols <- character()
  current_date <- NA_character_
  current_actor <- NA_character_

  # Also track section headers for context
 current_section <- ""

  for (line in lines) {
    trimmed <- str_squish(line)

    # Track section headers (== Section == or === Subsection ===)
    if (str_detect(trimmed, "^={2,}\\s*(.+?)\\s*={2,}$")) {
      current_section <- str_match(trimmed, "^={2,}\\s*(.+?)\\s*={2,}$")[, 2]
      next
    }

    # Table start
    if (str_detect(trimmed, "^\\{\\|")) {
      in_table <- TRUE
      header_cols <- character()
      next
    }

    # Table end
    if (str_detect(trimmed, "^\\|\\}")) {
      in_table <- FALSE
      next
    }

    # Table header row
    if (in_table && str_detect(trimmed, "^!")) {
      header_cols <- str_split(str_replace(trimmed, "^!", ""), "\\|\\|")[[1]] %>%
        str_squish() %>%
        str_remove_all("!") %>%
        str_to_lower()
      next
    }

    # Table row separator
    if (in_table && str_detect(trimmed, "^\\|-")) {
      next
    }

    # Table data row
    if (in_table && str_detect(trimmed, "^\\|")) {
      cells <- str_split(str_replace(trimmed, "^\\|", ""), "\\|\\|")[[1]] %>%
        str_squish() %>%
        str_remove_all("\\[\\[|\\]\\]") %>%
        str_remove_all("\\{\\{[^}]*\\}\\}")

      if (length(cells) >= 2) {
        # Try to extract date, target, description from cells
        row_text <- paste(cells, collapse = " ")

        # Extract date from cells or carry forward
        date_match <- str_extract(row_text, "\\d{1,2}\\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}")
        if (is.na(date_match)) {
          date_match <- str_extract(row_text, "(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4}")
        }
        if (!is.na(date_match)) {
          parsed <- suppressWarnings(as.Date(date_match, tryFormats = c("%d %B %Y", "%B %d, %Y", "%B %d %Y")))
          if (!is.na(parsed)) current_date <- as.character(parsed)
        }

        # Extract target from row
        target <- cells[min(2, length(cells))]
        if (nchar(target) < 3 && length(cells) >= 3) target <- cells[3]

        ref_urls <- extract_ref_urls_near(wikitext, line)

        idx <- idx + 1L
        rows[[idx]] <- tibble(
          incident_day = current_date,
          target_text = target,
          description = row_text,
          section = current_section,
          source_article = source_article,
          ref_urls = ref_urls
        )
      }
      next
    }

    # Parse prose lines for strike mentions (outside tables)
    # Look for date patterns followed by target information
    if (!in_table && nchar(trimmed) > 20) {
      # Check for date at start of line or in bold
      date_match <- str_extract(trimmed, "\\d{1,2}\\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}")
      if (is.na(date_match)) {
        date_match <- str_extract(trimmed, "(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4}")
      }

      if (!is.na(date_match)) {
        parsed <- suppressWarnings(as.Date(date_match, tryFormats = c("%d %B %Y", "%B %d, %Y", "%B %d %Y")))
        if (!is.na(parsed)) current_date <- as.character(parsed)
      }

      # Check if line mentions a strike/attack/target
      strike_pattern <- "(?i)(struck|strike|attack|bomb|missile|drone|intercept|damage|destroy|hit|target|launch)"
      if (str_detect(trimmed, strike_pattern)) {
        # Clean wikitext markup
        clean_text <- trimmed %>%
          str_remove_all("\\[\\[([^|\\]]*\\|)?") %>%
          str_remove_all("\\]\\]") %>%
          str_remove_all("\\{\\{[^}]*\\}\\}") %>%
          str_remove_all("'''?") %>%
          str_remove_all("<ref[^>]*>.*?</ref>|<ref[^>]*/>") %>%
          str_squish()

        if (nchar(clean_text) > 15) {
          # Extract likely target name (facility/location names)
          target_candidates <- str_extract_all(clean_text, "(?i)(?:Ras Laffan|Ruwais|South Pars|Kharg|Bushehr|Bandar Abbas|Jebel Ali|Fujairah|BAPCO|Abqaiq|Ras Tanura|Yanbu|Jubail|Mesaieed|Pearl GTL|Shah Gas|Habshan|Mina al-Ahmadi|Nevatim|Al Udeid|Duqm|Abadan|Isfahan|Tabriz|Tehran|[A-Z][a-z]+(?:\\s+[A-Z][a-z]+){0,3}(?:\\s+(?:refinery|terminal|port|field|plant|facility|base|complex|pipeline|airport|airbase)))")[[1]]
          target <- if (length(target_candidates) > 0) target_candidates[1] else str_sub(clean_text, 1, 80)

          ref_urls <- extract_ref_urls_near(wikitext, trimmed)

          idx <- idx + 1L
          rows[[idx]] <- tibble(
            incident_day = current_date,
            target_text = target,
            description = clean_text,
            section = current_section,
            source_article = source_article,
            ref_urls = ref_urls
          )
        }
      }
    }
  }

  if (idx == 0L) return(tibble())
  bind_rows(rows)
}

scrape_wikipedia_incidents <- function(energy_sites) {
  if (!scrape_wikipedia) {
    message("Wikipedia scraping disabled")
    return(tibble())
  }

  message("=== Wikipedia: fetching ", length(wiki_articles), " articles ===")
  all_rows <- list()

  for (article in wiki_articles) {
    message("  Fetching: ", article)
    wikitext <- fetch_wiki_wikitext(article)

    if (is.na(wikitext)) {
      message("    -> Failed or empty")
      next
    }

    message("    -> Got ", nchar(wikitext), " chars of wikitext")
    parsed <- parse_wiki_tables(wikitext, article)
    message("    -> Extracted ", nrow(parsed), " raw rows")

    if (nrow(parsed) > 0) {
      all_rows[[length(all_rows) + 1]] <- parsed
    }
  }

  if (length(all_rows) == 0) {
    message("  No Wikipedia incidents extracted")
    return(tibble())
  }

  raw <- bind_rows(all_rows) %>%
    filter(!is.na(target_text), nchar(target_text) > 2) %>%
    # Remove duplicates within Wikipedia
    distinct(incident_day, target_text, .keep_all = TRUE)

  message("  Total unique Wikipedia rows: ", nrow(raw))

  # Normalize into output schema
  raw %>%
    mutate(
      row_num = row_number(),
      source_system = "wikipedia",
      actor = classify_actor(paste(description, section)),
      strike_type = classify_strike_type(description),
      damage_confirmed = str_detect(str_to_lower(description), "(?i)(destroy|damage|hit|fire|explosion|killed|casualt)"),
      energy_relevant = flag_energy_text(paste(target_text, description)),
      target_country_guess = coalesce(guess_country(target_text), guess_country(description)),
      wiki_page_url = paste0("https://en.wikipedia.org/wiki/", source_article),
      source_urls = coalesce(ref_urls, wiki_page_url)
    ) %>%
    rowwise() %>%
    mutate(
      geo = list(geocode_target(target_text, energy_sites)),
      target_lat = geo$lat,
      target_lon = geo$lon,
      incident_id = make_incident_id("wiki", incident_day, target_text, row_num)
    ) %>%
    ungroup() %>%
    select(
      incident_id, source_system, source_article = wiki_page_url,
      incident_day, target_text, target_country_guess,
      actor, strike_type, damage_confirmed, energy_relevant,
      target_lat, target_lon, description, source_urls
    )
}

# ── iranstrikemap.com scraping ───────────────────────────────────────────────

scrape_iranstrikemap_incidents <- function(energy_sites) {
  if (!do_scrape_iranstrikemap) {
    message("iranstrikemap.com scraping disabled")
    return(tibble())
  }

  message("=== iranstrikemap.com: probing for API endpoint ===")

  # Try common API endpoint patterns
  api_candidates <- c(
    "https://iranstrikemap.com/api/strikes",
    "https://iranstrikemap.com/api/v1/strikes",
    "https://iranstrikemap.com/api/data",
    "https://iranstrikemap.com/data/strikes.json",
    "https://iranstrikemap.com/strikes.json",
    "https://iranstrikemap.com/api/events"
  )

  strike_data <- NULL

  for (api_url in api_candidates) {
    resp <- tryCatch({
      request(api_url) %>%
        req_timeout(15) %>%
        req_headers(
          `User-Agent` = "IranWarSeminarProject/1.0 (university research)",
          Accept = "application/json"
        ) %>%
        req_perform()
    }, error = function(e) NULL)

    if (!is.null(resp) && resp_status(resp) == 200) {
      body_text <- resp_body_string(resp)
      # Check if it's JSON
      if (str_detect(body_text, "^\\s*[\\[\\{]")) {
        parsed <- tryCatch(fromJSON(body_text, simplifyVector = TRUE), error = function(e) NULL)
        if (!is.null(parsed)) {
          message("  Found API at: ", api_url)
          strike_data <- parsed
          break
        }
      }
    }
    Sys.sleep(0.5)
  }

  # Also try fetching the main page to find embedded data or JS bundle URLs
  if (is.null(strike_data)) {
    message("  No direct API found, checking main page for embedded data...")
    main_resp <- tryCatch({
      request("https://iranstrikemap.com") %>%
        req_timeout(15) %>%
        req_headers(`User-Agent` = "Mozilla/5.0") %>%
        req_perform()
    }, error = function(e) NULL)

    if (!is.null(main_resp) && resp_status(main_resp) == 200) {
      page_text <- resp_body_string(main_resp)

      # Look for inline JSON data
      json_match <- str_match(page_text, "(?:strikes|events|data)\\s*[=:]\\s*(\\[\\{.*?\\}\\])")
      if (!is.na(json_match[1, 2])) {
        parsed <- tryCatch(fromJSON(json_match[1, 2], simplifyVector = TRUE), error = function(e) NULL)
        if (!is.null(parsed)) {
          message("  Found embedded strike data")
          strike_data <- parsed
        }
      }

      # Look for Firebase/Supabase URLs
      if (is.null(strike_data)) {
        fb_match <- str_extract(page_text, "https://[a-z0-9-]+\\.(?:firebaseio\\.com|supabase\\.co)[^\"'\\s]*")
        if (!is.na(fb_match)) {
          message("  Found backend URL: ", fb_match)
          fb_resp <- tryCatch({
            request(paste0(fb_match, ".json")) %>%
              req_timeout(15) %>%
              req_perform()
          }, error = function(e) NULL)

          if (!is.null(fb_resp) && resp_status(fb_resp) == 200) {
            parsed <- tryCatch(fromJSON(resp_body_string(fb_resp), simplifyVector = TRUE), error = function(e) NULL)
            if (!is.null(parsed)) {
              strike_data <- parsed
              message("  Fetched data from Firebase/Supabase")
            }
          }
        }
      }
    }
  }

  if (is.null(strike_data)) {
    message("  iranstrikemap.com: no structured API found, skipping")
    return(tibble())
  }

  # The JSON has { strikes: [...], lastUpdated: "..." }
  # Each strike has nested fields: id, lat, lng, city, target, targetType,
  # striker, timestamp, confirmed, confidenceLevel, sources (list), description,
  # attackType, country, ...
  strikes_list <- if (is.list(strike_data) && "strikes" %in% names(strike_data)) {
    strike_data$strikes
  } else if (is.data.frame(strike_data)) {
    strike_data
  } else {
    strike_data
  }

  # Parse from list-of-lists into a flat tibble
  parse_one_strike <- function(s) {
    tibble(
      id = s$id %||% NA_character_,
      lat = as.numeric(s$lat %||% NA_real_),
      lng = as.numeric(s$lng %||% NA_real_),
      city = s$city %||% NA_character_,
      target = s$target %||% NA_character_,
      target_type = s$targetType %||% s$type %||% NA_character_,
      striker = s$striker %||% s$actor %||% NA_character_,
      timestamp = s$timestamp %||% NA_character_,
      confirmed = isTRUE(s$confirmed),
      confidence_level = s$confidenceLevel %||% s$confidence %||% NA_character_,
      sources_text = paste(unlist(s$sources %||% list()), collapse = " | "),
      description = s$description %||% NA_character_,
      attack_type = s$attackType %||% NA_character_,
      country = s$country %||% NA_character_,
      verified = isTRUE(s$verified)
    )
  }

  df <- if (is.data.frame(strikes_list)) {
    # Already a data.frame from simplifyVector=TRUE — flatten sources column
    strikes_list %>%
      as_tibble() %>%
      mutate(
        id = id %||% NA_character_,
        sources_text = vapply(sources, function(x) paste(unlist(x), collapse = " | "), character(1)),
        attack_type = attackType %||% NA_character_,
        target_type = targetType %||% NA_character_,
        confidence_level = confidenceLevel %||% confidence %||% NA_character_
      )
  } else {
    bind_rows(lapply(strikes_list, parse_one_strike))
  }

  message("  iranstrikemap.com: got ", nrow(df), " records")
  message("    confirmed: ", sum(df$confirmed, na.rm = TRUE),
          ", high-confidence: ", sum(df$confidence_level == "high", na.rm = TRUE))

  result <- tibble(
    incident_id = paste0("iransm_", df$id),
    source_system = "iranstrikemap",
    source_article = "https://iranstrikemap.com",
    incident_day = as.character(as.Date(df$timestamp)),
    target_text = coalesce(df$target, df$city),
    target_country_guess = coalesce(guess_country(df$country), guess_country(df$city)),
    actor = classify_actor(df$striker),
    strike_type = classify_strike_type(coalesce(df$attack_type, df$description, "")),
    damage_confirmed = df$confirmed,
    energy_relevant = flag_energy_text(paste(coalesce(df$target, ""), coalesce(df$description, ""), coalesce(df$target_type, ""))),
    target_lat = df$lat,
    target_lon = df$lng,
    description = coalesce(df$description, paste(df$target, df$city, sep = ", ")),
    source_urls = if_else(nchar(df$sources_text) > 0, df$sources_text, "iranstrikemap.com"),
    iransm_confidence = df$confidence_level,
    iransm_confirmed = df$confirmed
  )

  result
}

# ── CENTCOM fact sheet parsing ───────────────────────────────────────────────

centcom_pdf_urls <- c(
  "https://media.defense.gov/2026/Mar/03/2003882557/-1/-1/1/OPERATION-EPIC-FURY-FACT-SHEET-260303.PDF",
  "https://media.defense.gov/2026/Mar/16/2003899496/-1/-1/1/OPERATION-EPIC-FURY-FACT-SHEET.PDF",
  "https://media.defense.gov/2026/Mar/18/2003900300/-1/-1/1/OPERATION-EPIC-FURY-FACT-SHEET-MARCH-18.PDF"
)

scrape_centcom_factsheets <- function() {
  if (!scrape_centcom) {
    message("CENTCOM fact sheet scraping disabled")
    return(tibble())
  }

  # Check if pdftools is available
  if (!requireNamespace("pdftools", quietly = TRUE)) {
    message("  pdftools not installed — run install.packages('pdftools') to enable CENTCOM PDF parsing")
    return(tibble())
  }

  message("=== CENTCOM: fetching ", length(centcom_pdf_urls), " fact sheet PDFs ===")
  results <- list()

  for (pdf_url in centcom_pdf_urls) {
    message("  Fetching: ", basename(pdf_url))

    # Download to temp file
    tmp <- tempfile(fileext = ".pdf")
    resp <- tryCatch({
      request(pdf_url) %>%
        req_timeout(30) %>%
        req_headers(`User-Agent` = "IranWarSeminarProject/1.0 (university research)") %>%
        req_perform(path = tmp)
    }, error = function(e) {
      warning("Failed to download: ", pdf_url, " — ", conditionMessage(e))
      NULL
    })

    if (is.null(resp)) next

    text <- tryCatch(
      paste(pdftools::pdf_text(tmp), collapse = "\n"),
      error = function(e) {
        warning("Failed to parse PDF: ", basename(pdf_url))
        NA_character_
      }
    )
    unlink(tmp)

    if (is.na(text)) next

    message("    -> Got ", nchar(text), " chars of text")

    # Extract date from filename or text
    date_match <- str_extract(basename(pdf_url), "\\d{6}")
    factsheet_date <- if (!is.na(date_match)) {
      paste0("2026-", str_sub(date_match, 3, 4), "-", str_sub(date_match, 5, 6))
    } else {
      date_from_text <- str_extract(text, "(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4}")
      if (!is.na(date_from_text)) {
        as.character(suppressWarnings(as.Date(date_from_text, tryFormats = c("%B %d, %Y", "%B %d %Y"))))
      } else {
        NA_character_
      }
    }

    # Extract total targets struck
    total_targets <- str_extract(text, "(?i)(\\d[\\d,]*)\\s+(?:targets?\\s+(?:struck|destroyed|engaged|neutralized))")
    total_count <- if (!is.na(total_targets)) {
      as.integer(str_replace_all(str_extract(total_targets, "\\d[\\d,]*"), ",", ""))
    } else {
      NA_integer_
    }

    # Extract target categories
    categories <- str_extract_all(text, "(?i)(?:missile|launch|command|control|radar|air defense|weapons? storage|ammunition|drone|UAV|IRGC|naval|port|oil|gas|refinery|airfield|airbase|military|nuclear|enrichment|centrifuge|ballistic|cruise|depot|bunker|headquarters|communication)[\\w\\s]*(?:site|facility|base|center|system|launcher|battery|complex|infrastructure|target|node)")[[1]]
    categories <- unique(str_squish(categories))

    # Extract any specific facility/location names
    locations <- str_extract_all(text, paste(facility_coords$pattern, collapse = "|"))[[1]]
    locations <- unique(locations)

    results[[length(results) + 1]] <- tibble(
      factsheet_url = pdf_url,
      factsheet_date = factsheet_date,
      total_targets_mentioned = total_count,
      target_categories = paste(categories, collapse = "; "),
      locations_mentioned = paste(locations, collapse = "; "),
      raw_text_length = nchar(text)
    )
  }

  if (length(results) == 0) {
    message("  No CENTCOM fact sheets parsed")
    return(tibble())
  }

  centcom_summary <- bind_rows(results)
  message("  CENTCOM: parsed ", nrow(centcom_summary), " fact sheets")
  centcom_summary
}

# ── Main execution ───────────────────────────────────────────────────────────

message("Loading energy sites for geocoding...")
energy_sites <- read_csv_if_exists(paths$energy_csv)
message("  ", nrow(energy_sites), " energy sites available for matching")

# Scrape all sources
wiki_incidents <- scrape_wikipedia_incidents(energy_sites)
iransm_incidents <- scrape_iranstrikemap_incidents(energy_sites)
centcom_factsheets <- scrape_centcom_factsheets()

# ── Cross-reference: use iranstrikemap to validate unsourced Wikipedia rows ───

if (nrow(wiki_incidents) > 0 && nrow(iransm_incidents) > 0) {
  message("\n=== Cross-referencing iranstrikemap against Wikipedia ===")

  # Wiki rows that lack real ref URLs (only wikipedia.org fallback or NA)
  wiki_no_ref <- wiki_incidents %>%
    filter(is.na(source_urls) | str_detect(source_urls, "wikipedia\\.org"))
  wiki_has_ref <- wiki_incidents %>%
    filter(!is.na(source_urls) & !str_detect(source_urls, "wikipedia\\.org"))

  n_validated <- 0L
  if (nrow(wiki_no_ref) > 0) {
    # For each unsourced wiki row, find nearby iranstrikemap matches
    # Match by: same date + target name overlap OR coordinates within 20km
    for (i in seq_len(nrow(wiki_no_ref))) {
      w <- wiki_no_ref[i, ]

      # Try date + text match
      matches <- iransm_incidents %>%
        filter(incident_day == w$incident_day | is.na(w$incident_day)) %>%
        filter(
          str_detect(str_to_lower(coalesce(target_text, "")), fixed(str_to_lower(str_sub(w$target_text, 1, 15)))) |
          str_detect(str_to_lower(coalesce(w$target_text, "")), fixed(str_to_lower(str_sub(coalesce(target_text, ""), 1, 15))))
        )

      # Fallback: coordinate proximity (within 20km)
      if (nrow(matches) == 0 && !is.na(w$target_lat) && !is.na(w$target_lon)) {
        matches <- iransm_incidents %>%
          filter(!is.na(target_lat), !is.na(target_lon)) %>%
          filter(incident_day == w$incident_day | is.na(w$incident_day)) %>%
          mutate(dist = haversine_km(w$target_lat, w$target_lon, target_lat, target_lon)) %>%
          filter(dist < 20) %>%
          arrange(dist)
      }

      if (nrow(matches) > 0) {
        best <- matches[1, ]
        # Enrich the wiki row with iranstrikemap sources and coordinates
        wiki_no_ref$source_urls[i] <- paste(
          c(w$source_urls, best$source_urls),
          collapse = " | "
        )
        # Fill in coordinates if wiki row was missing them
        if (is.na(w$target_lat) && !is.na(best$target_lat)) {
          wiki_no_ref$target_lat[i] <- best$target_lat
          wiki_no_ref$target_lon[i] <- best$target_lon
        }
        n_validated <- n_validated + 1L
      }
    }

    wiki_incidents <- bind_rows(wiki_has_ref, wiki_no_ref)
    message("  Validated ", n_validated, " of ", nrow(wiki_no_ref), " unsourced Wikipedia rows via iranstrikemap")
  }
}

# Combine: keep all Wikipedia rows, add iranstrikemap-only rows (not already covered)
if (nrow(iransm_incidents) > 0 && nrow(wiki_incidents) > 0) {
  # Drop extra iranstrikemap columns before combining
  iransm_for_merge <- iransm_incidents %>%
    select(any_of(names(wiki_incidents)))

  # Find iranstrikemap rows NOT matching any Wikipedia row (by date + proximity)
  iransm_unique <- iransm_for_merge
  if (nrow(wiki_incidents) > 0) {
    matched_iransm <- logical(nrow(iransm_for_merge))
    for (j in seq_len(nrow(iransm_for_merge))) {
      s <- iransm_for_merge[j, ]
      wiki_same_day <- wiki_incidents %>%
        filter(incident_day == s$incident_day | is.na(s$incident_day))
      if (nrow(wiki_same_day) == 0) next

      # Check text overlap
      text_match <- any(str_detect(
        str_to_lower(wiki_same_day$target_text),
        fixed(str_to_lower(str_sub(s$target_text %||% "", 1, 12)))
      ), na.rm = TRUE)

      # Check coordinate proximity
      coord_match <- FALSE
      if (!is.na(s$target_lat) && !is.na(s$target_lon)) {
        wiki_with_coords <- wiki_same_day %>% filter(!is.na(target_lat), !is.na(target_lon))
        if (nrow(wiki_with_coords) > 0) {
          dists <- haversine_km(s$target_lat, s$target_lon, wiki_with_coords$target_lat, wiki_with_coords$target_lon)
          coord_match <- any(dists < 20, na.rm = TRUE)
        }
      }

      matched_iransm[j] <- text_match || coord_match
    }
    iransm_unique <- iransm_for_merge[!matched_iransm, ]
  }

  all_scraped <- bind_rows(wiki_incidents, iransm_unique)
  message("  iranstrikemap-only rows added: ", nrow(iransm_unique))
} else {
  all_scraped <- bind_rows(wiki_incidents, iransm_incidents %>% select(any_of(names(wiki_incidents))))
}

if (nrow(all_scraped) > 0) {
  # Drop junk rows: wiki categories, metadata fragments, overly long prose
  n_before <- nrow(all_scraped)
  all_scraped <- all_scraped %>%
    filter(
      !str_detect(target_text, "^Category:"),
      !str_detect(target_text, "^This is a list"),
      !str_detect(target_text, "^where specific incidents"),
      !str_detect(target_text, "^Attacks on "),
      !str_detect(description, "^Category:"),
      nchar(target_text) <= 120
    )
  message("  Dropped ", n_before - nrow(all_scraped), " junk rows")

  all_scraped <- all_scraped %>%
    distinct(incident_day, target_text, target_lat, target_lon, .keep_all = TRUE) %>%
    arrange(incident_day, target_text)

  n_geocoded <- sum(!is.na(all_scraped$target_lat))
  n_energy <- sum(all_scraped$energy_relevant, na.rm = TRUE)
  n_with_sources <- sum(!is.na(all_scraped$source_urls) & !str_detect(all_scraped$source_urls, "^https://en\\.wikipedia\\.org"), na.rm = TRUE)

  message("\n=== Scraping summary ===")
  message("  Total incidents: ", nrow(all_scraped))
  message("  Wikipedia:       ", sum(all_scraped$source_system == "wikipedia"))
  message("  iranstrikemap:   ", sum(all_scraped$source_system == "iranstrikemap"))
  message("  Geocoded:        ", n_geocoded, " (", round(100 * n_geocoded / nrow(all_scraped)), "%)")
  message("  Energy-relevant: ", n_energy)
  message("  With primary sources: ", n_with_sources)
} else {
  message("\nNo incidents scraped from any source.")
}

if (write_output && nrow(all_scraped) > 0) {
  write_csv_safe(all_scraped, paths$scraped_csv)
  message("Wrote: ", paths$scraped_csv)
}

if (write_output && nrow(centcom_factsheets) > 0) {
  write_csv_safe(centcom_factsheets, paths$centcom_csv)
  message("Wrote: ", paths$centcom_csv)
}

if (!write_output) {
  message("Preview only. Set WRITE_OUTPUT=TRUE to save outputs.")
}

message("Done.")
