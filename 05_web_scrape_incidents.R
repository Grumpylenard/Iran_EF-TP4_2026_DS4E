# 05_web_scrape_incidents.R
#
# Standalone incident scraper for CENTCOM, CTP-ISW, and Defense.gov releases.
# Produces: data_raw/web_scraped/incidents.csv
#
# Output schema:
#   lat, lon, date, location_name, target_type, source, report_url

source("utils.R")
suppressPackageStartupMessages({
  library(rvest)
  library(tidyverse)
  library(tidygeocoder)
})

# ── Configuration ──────────────────────────────────────────────────────────────

env_int <- function(name, default) {
  value <- suppressWarnings(as.integer(Sys.getenv(name, as.character(default))))
  if (is.na(value) || value < 1L) default else value
}

env_num <- function(name, default) {
  value <- suppressWarnings(as.numeric(Sys.getenv(name, as.character(default))))
  if (is.na(value)) default else value
}

CENTCOM_BASE  <- "https://www.centcom.mil"
CENTCOM_PATH  <- "/MEDIA/PRESS-RELEASES/"
CENTCOM_PAGES <- env_int("CENTCOM_PAGES", 10L)
CENTCOM_RSS_URL <- paste0(
  CENTCOM_BASE,
  "/DesktopModules/ArticleCS/RSS.ashx?ContentType=2&Site=808&isdashboardselected=0&max=20"
)

ISW_BASE         <- "https://www.criticalthreats.org"
ISW_LIST_URL     <- "https://www.criticalthreats.org/briefs/iran-updates"
ISW_SEARCH_URL   <- "https://www.criticalthreats.org/?s=iran+update"
ISW_ANALYSIS_URL <- "https://www.criticalthreats.org/analysis"
ISW_PAGES        <- env_int("ISW_PAGES", 5L)

DEFENSE_BASE     <- "https://www.defense.gov"
DEFENSE_LIST_URL <- "https://www.defense.gov/News/Releases/"
DEFENSE_PAGES    <- env_int("DEFENSE_PAGES", 5L)

MIN_PAGE1_ARTICLES <- env_int("MIN_PAGE1_ARTICLES", 5L)
LISTING_CAP_PER_SOURCE <- env_int("LISTING_CAP_PER_SOURCE", 120L)
MAX_ARTICLES_PER_SOURCE <- env_int("MAX_ARTICLES_PER_SOURCE", 25L)
GEO_LAT_MIN <- env_num("GEOCODE_LAT_MIN", -12)
GEO_LAT_MAX <- env_num("GEOCODE_LAT_MAX", 45)
GEO_LON_MIN <- env_num("GEOCODE_LON_MIN", 25)
GEO_LON_MAX <- env_num("GEOCODE_LON_MAX", 80)

OUTPUT_FILE <- project_path("data_raw", "web_scraped", "incidents.csv")

scrape_centcom <- env_flag("SCRAPE_CENTCOM", TRUE)
scrape_isw <- env_flag("SCRAPE_ISW", TRUE)
scrape_defense <- env_flag("SCRAPE_DEFENSE", TRUE)
geocode_enabled <- env_flag("GEOCODE_LOCATIONS", TRUE)
write_output <- env_flag("WRITE_OUTPUT", TRUE)

KEYWORDS <- c(
  "strike", "struck", "destroyed", "targeted", "killed", "attack",
  "iran", "irgc", "houthi", "missile", "drone", "precision",
  "airstrike", "combined force", "idf", "bomb", "epic fury",
  "air defense", "launch site", "facility", "base"
)
KEYWORD_PATTERN <- paste(KEYWORDS, collapse = "|")

POLITE_DELAY <- 0.8

MONTH_PATTERN <- paste0(
  "(?i)(Jan(?:uary)?\\.?|Feb(?:ruary)?\\.?|Mar(?:ch)?\\.?|Apr(?:il)?\\.?|",
  "May|Jun(?:e)?\\.?|Jul(?:y)?\\.?|Aug(?:ust)?\\.?|Sep(?:t|tember)?\\.?|",
  "Oct(?:ober)?\\.?|Nov(?:ember)?\\.?|Dec(?:ember)?\\.?)\\s+",
  "\\d{1,2},\\s+\\d{4}"
)

# ── Fetch / parsing helpers ────────────────────────────────────────────────────

fetch_url_text <- function(url) {
  out <- tryCatch(
    system2(
      "curl",
      c("-sSL", "--connect-timeout", "15", "--max-time", "45", "-A", "Mozilla/5.0", url),
      stdout = TRUE,
      stderr = TRUE
    ),
    error = function(e) character()
  )
  if (length(out) == 0) return(NA_character_)

  text <- paste(out, collapse = "\n")
  if (!nzchar(text)) NA_character_ else text
}

is_blocked_text <- function(text) {
  if (is.na(text) || !nzchar(text)) return(TRUE)

  str_detect(
    text,
    regex(
      "Access Denied|Just a moment|Enable JavaScript and cookies|Attention Required",
      ignore_case = TRUE
    )
  )
}

mirror_url <- function(url) {
  paste0("https://r.jina.ai/http://", str_replace(url, "^https?://", ""))
}

safe_fetch_text <- function(url, allow_mirror = TRUE, warn_fail = TRUE) {
  Sys.sleep(POLITE_DELAY)
  direct_text <- fetch_url_text(url)
  if (!is.na(direct_text) && !is_blocked_text(direct_text)) {
    return(direct_text)
  }

  if (allow_mirror) {
    Sys.sleep(POLITE_DELAY)
    mirrored_text <- fetch_url_text(mirror_url(url))
    if (!is.na(mirrored_text) && nzchar(mirrored_text)) {
      return(mirrored_text)
    }
  }

  if (warn_fail) {
    warning("Fetch failed: ", url)
  }
  NA_character_
}

safe_read_html <- function(url) {
  raw_text <- safe_fetch_text(url, allow_mirror = FALSE, warn_fail = FALSE)
  if (is.na(raw_text)) return(NULL)

  html <- tryCatch(read_html(raw_text), error = function(e) NULL)
  if (is.null(html)) return(NULL)

  page_title <- tryCatch(
    html |> html_element("title") |> html_text(trim = TRUE),
    error = function(e) ""
  )

  if (str_detect(page_title, regex("Access Denied|Just a moment", ignore_case = TRUE))) {
    return(NULL)
  }

  html
}

normalize_date <- function(x) {
  value <- str_squish(as.character(x %||% NA_character_))
  if (is.na(value) || value == "") return(NA_character_)

  normalized <- str_replace_all(value, "(?<=\\b[A-Za-z]{3})\\.", "")

  parsed <- suppressWarnings(as.Date(normalized, tryFormats = c(
    "%Y-%m-%d",
    "%B %d, %Y",
    "%b %d, %Y",
    "%a, %d %b %Y %H:%M:%S GMT"
  )))

  if (is.na(parsed)) {
    extracted <- str_extract(value, MONTH_PATTERN)
    if (!is.na(extracted)) {
      extracted <- str_replace_all(extracted, "(?<=\\b[A-Za-z]{3})\\.", "")
      parsed <- suppressWarnings(as.Date(extracted, tryFormats = c("%B %d, %Y", "%b %d, %Y")))
    }
  }

  if (is.na(parsed)) value else as.character(parsed)
}

normalize_listing_df <- function(df) {
  if (nrow(df) == 0) return(df)

  df |>
    mutate(
      title = str_squish(title),
      url = str_replace(url, "^http://", "https://"),
      url = str_replace(url, "^https://www\\.war\\.gov/", "https://www.defense.gov/"),
      date = vapply(date, normalize_date, character(1))
    ) |>
    filter(!is.na(title), title != "", !is.na(url), url != "") |>
    distinct(url, .keep_all = TRUE)
}

parse_markdown_links_with_dates <- function(text, source_label, url_regex) {
  if (is.na(text) || !nzchar(text)) return(tibble())

  lines <- str_split(text, "\n", simplify = FALSE)[[1]]
  rows <- list()
  idx <- 0L

  for (i in seq_along(lines)) {
    line <- str_squish(lines[[i]])
    matches <- str_match_all(line, paste0("\\[(.*?)\\]\\((", url_regex, ")\\)"))[[1]]
    if (nrow(matches) == 0) next

    date_candidate <- NA_character_
    if (i > 1) {
      for (j in seq(max(1L, i - 3L), i - 1L)) {
        candidate <- str_squish(str_replace(lines[[j]], "^[\\-*\\s]+", ""))
        date_found <- str_extract(candidate, MONTH_PATTERN)
        if (!is.na(date_found)) {
          date_candidate <- date_found
          break
        }
      }
    }

    for (k in seq_len(nrow(matches))) {
      title <- str_squish(matches[k, 2])
      title <- str_replace(title, "^(?i:article\\s+#+\\s*)", "")
      url <- matches[k, 3]
      date <- date_candidate
      if (is.na(date)) {
        date <- str_extract(title, MONTH_PATTERN)
      }

      idx <- idx + 1L
      rows[[idx]] <- tibble(
        source = source_label,
        title = title,
        url = url,
        date = date
      )
    }
  }

  if (idx == 0L) return(tibble())
  bind_rows(rows)
}

extract_article_text_from_markdown <- function(text) {
  if (is.na(text) || !nzchar(text)) return(NA_character_)

  body <- str_replace(text, "^[\\s\\S]*?Markdown Content:\\s*", "")
  body <- str_replace_all(body, "!\\[[^\\]]*\\]\\([^\\)]*\\)", " ")
  body <- str_replace_all(body, "\\[([^\\]]+)\\]\\([^\\)]*\\)", "\\1")

  lines <- str_split(body, "\n", simplify = FALSE)[[1]] |>
    str_squish()
  lines <- lines[lines != ""]
  if (length(lines) == 0) return(NA_character_)

  noise_pattern <- paste(
    "(?i)^(Title:|URL Source:|Search$|Back$|Home$|Place Holder$|Department of War$|",
    "Helpful Links$|Resources$|Popular$|Legal & Administrative$|Current RSS Feeds$|",
    "Skip to main content|An official website|Here's how you know|Official websites use|",
    "Secure \\.gov websites|Subscribe$|Live Events$)"
  )
  lines <- lines[!str_detect(lines, noise_pattern)]

  start_idx <- which(str_detect(
    lines,
    "(?i)(Immediate Release|Press Release|\\*\\*Iran Update|Key Takeaways|TAMPA, Fla\\.|Chief Pentagon Spokesman|combined force|U\\.S\\. Central Command|^#\\s+)"
  ))
  if (length(start_idx) > 0) {
    lines <- lines[min(start_idx):length(lines)]
  }

  end_idx <- which(str_detect(
    lines,
    "(?i)^(Subscribe to|###\\s+Subscribe|Department of War$|Hosted by|Privacy & Security|Previous Next Slideshow)"
  ))
  if (length(end_idx) > 0 && min(end_idx) > 1) {
    lines <- lines[seq_len(min(end_idx) - 1L)]
  }

  out <- str_squish(paste(lines, collapse = " "))
  if (nchar(out) < 80) NA_character_ else out
}

assert_min_articles <- function(page_df, source_label, min_required = MIN_PAGE1_ARTICLES) {
  n_articles <- page_df |> distinct(url) |> nrow()
  message("  Page 1 articles found: ", n_articles)

  if (n_articles < min_required) {
    stop(
      source_label,
      " page 1 returned ",
      n_articles,
      " articles (< ",
      min_required,
      "). Check selectors/fetch strategy."
    )
  }
}

# ── CENTCOM scraping ───────────────────────────────────────────────────────────

fetch_centcom_rss_page <- function() {
  rss_text <- safe_fetch_text(CENTCOM_RSS_URL, allow_mirror = FALSE, warn_fail = FALSE)
  if (is.na(rss_text)) return(tibble())

  rss_xml <- tryCatch(xml2::read_xml(rss_text), error = function(e) NULL)
  if (is.null(rss_xml)) return(tibble())

  items <- xml2::xml_find_all(rss_xml, ".//item")
  if (length(items) == 0) return(tibble())

  tibble(
    source = "CENTCOM",
    title = xml2::xml_text(xml2::xml_find_first(items, "./title")),
    url = xml2::xml_text(xml2::xml_find_first(items, "./link")),
    date = xml2::xml_text(xml2::xml_find_first(items, "./pubDate"))
  ) |>
    filter(str_detect(url, "https?://www\\.centcom\\.mil/MEDIA/PRESS-RELEASES/Press-Release-View/Article/\\d+/"))
}

parse_centcom_listing_html <- function(html) {
  items <- html |> html_elements(".alist.newsrelease .item")
  if (length(items) == 0) return(tibble())

  titles <- items |> html_element(".title a") |> html_text(trim = TRUE)
  hrefs <- items |> html_element(".title a") |> html_attr("href")
  dates <- items |> html_element(".info-bar .date") |> html_text(trim = TRUE)

  tibble(
    source = "CENTCOM",
    title = titles,
    url = if_else(str_starts(hrefs, "http"), hrefs, paste0(CENTCOM_BASE, hrefs)),
    date = dates
  )
}

fetch_centcom_page <- function(page_num) {
  url <- paste0(CENTCOM_BASE, CENTCOM_PATH, "?Page=", page_num)

  html <- safe_read_html(url)
  if (!is.null(html)) {
    parsed <- parse_centcom_listing_html(html)
    if (nrow(parsed) > 0) return(normalize_listing_df(parsed))
  }

  markdown_text <- safe_fetch_text(url, allow_mirror = TRUE, warn_fail = FALSE)
  parsed_markdown <- parse_markdown_links_with_dates(
    markdown_text,
    source_label = "CENTCOM",
    url_regex = "https?://www\\.centcom\\.mil/MEDIA/PRESS-RELEASES/Press-Release-View/Article/\\d+/[^\\)\\s]+"
  )

  if (nrow(parsed_markdown) > 0) {
    return(normalize_listing_df(parsed_markdown))
  }

  # Secondary fallback in heavily blocked environments.
  if (page_num == 1L) {
    rss_fallback <- fetch_centcom_rss_page()
    if (nrow(rss_fallback) > 0) return(normalize_listing_df(rss_fallback))
  }

  tibble()
}

fetch_centcom_text <- function(url) {
  html <- safe_read_html(url)

  if (!is.null(html)) {
    selectors <- c(
      "#news-content .body",
      ".article-body .body[itemprop='articleBody']",
      ".body[itemprop='articleBody']",
      ".atsBody",
      ".body-content",
      "article .field-items",
      ".press-release-body"
    )

    for (sel in selectors) {
      node <- html |> html_element(sel)
      if (!inherits(node, "xml_missing")) {
        text <- node |> html_text(trim = TRUE)
        if (!is.na(text) && nchar(text) > 80) return(text)
      }
    }
  }

  markdown_text <- safe_fetch_text(url, allow_mirror = TRUE, warn_fail = FALSE)
  extract_article_text_from_markdown(markdown_text)
}

scrape_centcom_articles <- function() {
  if (!scrape_centcom) {
    message("CENTCOM scraping disabled")
    return(tibble())
  }

  message("=== CENTCOM: fetching ", CENTCOM_PAGES, " listing pages ===")
  page1 <- fetch_centcom_page(1)

  if (nrow(page1) == 0) {
    message("  No CENTCOM articles found")
    return(tibble())
  }

  assert_min_articles(page1, "CENTCOM")

  listings <- bind_rows(
    page1,
    if (CENTCOM_PAGES > 1L) map_dfr(seq(2L, CENTCOM_PAGES), fetch_centcom_page) else tibble()
  ) |>
    normalize_listing_df() |>
    slice_head(n = LISTING_CAP_PER_SOURCE)

  message("  Total articles: ", nrow(listings))
  message("  Filtering by keywords...")

  filtered <- listings |>
    filter(str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)))

  message("  After keyword filter: ", nrow(filtered))
  if (nrow(filtered) == 0) return(tibble())

  filtered <- filtered |> slice_head(n = MAX_ARTICLES_PER_SOURCE)
  message("  Fetching up to ", nrow(filtered), " CENTCOM article texts...")

  filtered |>
    mutate(text = map_chr(url, fetch_centcom_text))
}

# ── CTP-ISW scraping ───────────────────────────────────────────────────────────

parse_isw_listing_html <- function(html) {
  anchors <- html |> html_elements("article h2 a, article h3 a, .post-title a, .entry-title a, h2 a, h3 a")
  if (length(anchors) == 0) return(tibble())

  titles <- anchors |> html_text(trim = TRUE)
  hrefs <- anchors |> html_attr("href")

  tibble(
    source = "CTP-ISW",
    title = titles,
    url = if_else(str_starts(hrefs, "http"), hrefs, paste0(ISW_BASE, hrefs)),
    date = str_extract(titles, MONTH_PATTERN)
  ) |>
    filter(str_detect(url, "criticalthreats\\.org/analysis/iran-update"))
}

fetch_isw_listing_page <- function(page_num) {
  list_url <- paste0(ISW_LIST_URL, if (page_num > 1L) paste0("?page=", page_num) else "")

  html <- safe_read_html(list_url)
  if (!is.null(html)) {
    parsed <- parse_isw_listing_html(html)
    if (nrow(parsed) > 0) return(normalize_listing_df(parsed))
  }

  search_url <- paste0(ISW_SEARCH_URL, if (page_num > 1L) paste0("&paged=", page_num) else "")
  search_text <- safe_fetch_text(search_url, allow_mirror = TRUE, warn_fail = FALSE)

  parsed_search <- parse_markdown_links_with_dates(
    search_text,
    source_label = "CTP-ISW",
    url_regex = "https?://www\\.criticalthreats\\.org/analysis/iran-update[^\\)\\s]+"
  )

  if (nrow(parsed_search) > 0) {
    return(normalize_listing_df(parsed_search))
  }

  analysis_url <- paste0(ISW_ANALYSIS_URL, if (page_num > 1L) paste0("/page/", page_num) else "")
  analysis_text <- safe_fetch_text(analysis_url, allow_mirror = TRUE, warn_fail = FALSE)

  parsed_analysis <- parse_markdown_links_with_dates(
    analysis_text,
    source_label = "CTP-ISW",
    url_regex = "https?://www\\.criticalthreats\\.org/analysis/iran-update[^\\)\\s]+"
  )

  normalize_listing_df(parsed_analysis)
}

fetch_isw_text <- function(url) {
  html <- safe_read_html(url)

  if (!is.null(html)) {
    selectors <- c(
      ".entry-content",
      ".post-content",
      "article .entry-content",
      "article .content",
      "main article"
    )

    for (sel in selectors) {
      node <- html |> html_element(sel)
      if (!inherits(node, "xml_missing")) {
        text <- node |> html_text(trim = TRUE)
        if (!is.na(text) && nchar(text) > 80) return(text)
      }
    }
  }

  markdown_text <- safe_fetch_text(url, allow_mirror = TRUE, warn_fail = FALSE)
  extract_article_text_from_markdown(markdown_text)
}

scrape_isw_articles <- function() {
  if (!scrape_isw) {
    message("ISW scraping disabled")
    return(tibble())
  }

  message("=== CTP-ISW: fetching ", ISW_PAGES, " listing pages ===")
  page1 <- fetch_isw_listing_page(1)

  if (nrow(page1) == 0) {
    message("  No ISW articles found")
    return(tibble())
  }

  assert_min_articles(page1, "CTP-ISW")

  listings <- bind_rows(
    page1,
    if (ISW_PAGES > 1L) map_dfr(seq(2L, ISW_PAGES), fetch_isw_listing_page) else tibble()
  ) |>
    normalize_listing_df() |>
    slice_head(n = LISTING_CAP_PER_SOURCE)

  message("  Total Iran update articles: ", nrow(listings))
  message("  Filtering by keywords...")

  filtered <- listings |>
    filter(
      str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)) |
        str_detect(title, regex("special report|update", ignore_case = TRUE))
    )

  message("  After keyword filter: ", nrow(filtered))
  if (nrow(filtered) == 0) return(tibble())

  filtered <- filtered |> slice_head(n = MAX_ARTICLES_PER_SOURCE)
  message("  Fetching up to ", nrow(filtered), " CTP-ISW article texts...")
  filtered |>
    mutate(text = map_chr(url, fetch_isw_text))
}

# ── Defense.gov scraping ───────────────────────────────────────────────────────

parse_defense_listing_html <- function(html) {
  cards <- html |> html_elements("#alist listing-titles-only, listing-titles-only")
  if (length(cards) == 0) return(tibble())

  titles <- cards |> html_attr("article-title")
  hrefs <- cards |> html_attr("article-url")
  dates <- cards |> html_attr("publish-date-ap")

  tibble(
    source = "DEFENSE",
    title = titles,
    url = if_else(str_starts(hrefs, "http"), hrefs, paste0(DEFENSE_BASE, hrefs)),
    date = dates
  ) |>
    filter(!is.na(url), url != "", !is.na(title), title != "")
}

fetch_defense_listing_page <- function(page_num) {
  url <- paste0(DEFENSE_LIST_URL, if (page_num > 1L) paste0("?Page=", page_num) else "")

  html <- safe_read_html(url)
  if (!is.null(html)) {
    parsed <- parse_defense_listing_html(html)
    if (nrow(parsed) > 0) return(normalize_listing_df(parsed))
  }

  markdown_text <- safe_fetch_text(url, allow_mirror = TRUE, warn_fail = FALSE)
  parsed_markdown <- parse_markdown_links_with_dates(
    markdown_text,
    source_label = "DEFENSE",
    url_regex = "https?://www\\.(?:war|defense)\\.gov/News/Releases/Release/Article/\\d+/[^\\)\\s]+"
  )

  normalize_listing_df(parsed_markdown)
}

fetch_defense_text <- function(url) {
  html <- safe_read_html(url)

  if (!is.null(html)) {
    selectors <- c(
      "#news-content .body",
      ".article-view #news-content .body",
      ".article-body .body[itemprop='articleBody']",
      ".body[itemprop='articleBody']",
      ".adetail .body"
    )

    for (sel in selectors) {
      node <- html |> html_element(sel)
      if (!inherits(node, "xml_missing")) {
        text <- node |> html_text(trim = TRUE)
        if (!is.na(text) && nchar(text) > 80) return(text)
      }
    }
  }

  markdown_text <- safe_fetch_text(url, allow_mirror = TRUE, warn_fail = FALSE)
  if (is.na(markdown_text) && str_detect(url, "www\\.war\\.gov")) {
    defense_url <- str_replace(url, "www\\.war\\.gov", "www.defense.gov")
    markdown_text <- safe_fetch_text(defense_url, allow_mirror = TRUE, warn_fail = FALSE)
  }

  extract_article_text_from_markdown(markdown_text)
}

scrape_defense_articles <- function() {
  if (!scrape_defense) {
    message("Defense.gov scraping disabled")
    return(tibble())
  }

  message("=== DEFENSE.GOV: fetching ", DEFENSE_PAGES, " listing pages ===")
  page1 <- fetch_defense_listing_page(1)

  if (nrow(page1) == 0) {
    message("  No Defense releases found")
    return(tibble())
  }

  assert_min_articles(page1, "Defense.gov")

  listings <- bind_rows(
    page1,
    if (DEFENSE_PAGES > 1L) map_dfr(seq(2L, DEFENSE_PAGES), fetch_defense_listing_page) else tibble()
  ) |>
    normalize_listing_df() |>
    slice_head(n = LISTING_CAP_PER_SOURCE)

  message("  Total releases: ", nrow(listings))
  message("  Filtering by keywords...")

  filtered <- listings |>
    filter(
      str_detect(title, regex(KEYWORD_PATTERN, ignore_case = TRUE)) |
        str_detect(title, regex("release|statement|operation epic fury|iran", ignore_case = TRUE))
    )

  message("  After keyword filter: ", nrow(filtered))
  if (nrow(filtered) == 0) return(tibble())

  filtered <- filtered |> slice_head(n = MAX_ARTICLES_PER_SOURCE)
  message("  Fetching up to ", nrow(filtered), " Defense article texts...")
  filtered |>
    mutate(text = map_chr(url, fetch_defense_text))
}

# ── Regex extraction ───────────────────────────────────────────────────────────

LOCATION_PATTERNS <- c(
  # CENTCOM-style phrasing: "approximately X km [dir] of [city]"
  "(?i:approximately\\s+[\\d,.]+\\s*(?:km|kilometers?|miles?)\\s+(?:to\\s+the\\s+)?(?:north|south|east|west|northeast|northwest|southeast|southwest)\\s+of)\\s+([A-Z][A-Za-z'\\-]+(?:\\s+(?:[A-Z][A-Za-z'\\-]+|al|ad|as|el|de|of|the)){0,5})",

  # ISW-style phrasing: "in [City], [Province] Province"
  "(?i:\\bin\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,3},\\s+[A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,3}\\s+Province)\\b",

  # Province-only mentions
  "(?i:\\b(?:in|near|around|at)\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,4}\\s+Province)\\b",

  # Combined-force pattern from ISW examples
  "(?i:(?:combined force|idf|u\\.s\\.|us|coalition|israeli forces?)\\s+(?:continued\\s+to\\s+)?(?:strike|struck|target(?:ed|ing)|hit)\\b[^\\.;]{0,140}?\\b(?:in|near|around|at)\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+(?:[A-Z][A-Za-z'\\-]+|al|ad|as|el|de|of|the)){0,6}(?:\\s+Province|\\s+City)?)\\b",

  # Generic strike phrasing
  "(?i:(?:strike|struck|target(?:ed|ing)|hit|attack(?:ed|ing)?)\\b[^\\.;]{0,120}?\\b(?:in|near|around|at)\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+(?:[A-Z][A-Za-z'\\-]+|al|ad|as|el|de|of|the)){0,6}(?:\\s+Province|\\s+City)?)\\b",

  # Around/near and directional-country phrases
  "(?i:\\baround\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,4})\\b",
  "(?i:\\bin\\s+(?:northern|southern|eastern|western|northwestern|northeastern|southwestern|southeastern|central)\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,3})\\b",

  # City + country
  "(?i:\\b(?:in|near|at)\\s+)\\b([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,4}),\\s*(?:Iran|Iraq|Syria|Lebanon|Yemen|Bahrain|Kuwait|Qatar|Israel|United\\s+Arab\\s+Emirates|UAE)\\b"
)

TARGET_TYPE_PATTERNS <- c(
  "(?i)(?:ballistic\\s+)?missile\\s+(?:production\\s+)?(?:site|facility|complex|base|depot)",
  "(?i)(?:drone|uav)\\s+(?:launch\\s+)?(?:site|facility|base)",
  "(?i)(?:air\\s*defense|air\\s*defence|radar)\\s+(?:system|site|facility|position)",
  "(?i)(?:command(?:\\s+and\\s+control)?|headquarters)\\s+(?:center|centre|facility|site|complex)",
  "(?i)(?:ammunition|munitions|weapons?|fuel)\\s+(?:depot|storage\\s+site|facility|complex)",
  "(?i)(?:nuclear|enrichment)\\s+(?:facility|site|plant)",
  "(?i)(?:airfield|airbase|military\\s+airfield|military\\s+base|base)"
)

LOCATION_CONNECTORS <- c("al", "ad", "as", "el", "de", "of", "the", "bin", "ibn", "ez")
LOCATION_BAD_WORDS <- c(
  "reportedly", "least", "people", "order", "pressure", "injured", "killed",
  "effort", "government", "campaign", "response", "special", "direction",
  "commander", "forces", "force", "targeting", "continued", "overnight",
  "district", "districts", "towns", "sites", "site", "base", "operation"
)

clean_location_name <- function(x) {
  if (is.na(x)) return(NA_character_)

  location <- str_squish(x)
  location <- str_remove(location, "^[Tt]he\\s+")
  location <- str_remove(location, "(?i)\\s+on\\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z\\.]*\\b(?:\\s+\\d{1,2})?.*$")
  location <- str_remove(location, "(?i)\\s+(?:between|during|since|before|after|overnight|throughout)\\s+.*$")
  location <- str_remove(location, "(?i)\\s+in\\s+the\\s+indian\\s+ocean$")
  location <- str_replace(location, "(?i),?\\s+City$", "")
  location <- str_replace(
    location,
    "(?i)^.*\\bin\\s+([A-Z][A-Za-z'\\-]+(?:\\s+[A-Z][A-Za-z'\\-]+){0,4}\\s+Province)$",
    "\\1"
  )
  location <- str_remove(location, "[\\.,;:]+$")
  location <- str_squish(location)

  if (str_detect(location, "\\d")) return(NA_character_)
  if (nchar(location) < 3 || nchar(location) > 70) return(NA_character_)
  if (str_detect(location, "(?i)\\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\\b")) return(NA_character_)
  if (str_detect(location, "(?i)\\band\\b")) return(NA_character_)

  if (str_detect(location, "(?i)\\b(?:iranian regime|civilian ports|military targets|operation epic fury|the region|the area|the department|chief pentagon spokesman|u\\.s\\. forces|combined force)\\b")) {
    return(NA_character_)
  }

  bad_word_pattern <- paste0("(?i)\\b(", paste(LOCATION_BAD_WORDS, collapse = "|"), ")\\b")
  if (str_detect(location, bad_word_pattern)) return(NA_character_)

  tokens <- str_split(str_replace_all(location, ",", " "), "\\s+", simplify = TRUE)
  tokens <- tokens[tokens != ""]
  if (length(tokens) == 0 || length(tokens) > 6) return(NA_character_)

  token_ok <- str_detect(tokens, "^[A-Z][A-Za-z'\\-]+$") | (tolower(tokens) %in% LOCATION_CONNECTORS)
  if (any(!token_ok)) return(NA_character_)
  if (sum(str_detect(tokens, "^[A-Z]")) < 1) return(NA_character_)

  location
}

extract_locations <- function(text) {
  if (is.na(text) || nchar(text) < 40) return(character())

  locations <- character()

  for (pattern in LOCATION_PATTERNS) {
    matches <- str_match_all(text, pattern)[[1]]
    if (nrow(matches) == 0) next

    captures <- matches[, 2]
    cleaned <- vapply(captures, clean_location_name, character(1))
    cleaned <- cleaned[!is.na(cleaned)]

    if (length(cleaned) > 0) {
      locations <- c(locations, cleaned)
    }
  }

  unique(locations)
}

extract_target_type <- function(text) {
  if (is.na(text) || nchar(text) < 30) return(NA_character_)

  for (pattern in TARGET_TYPE_PATTERNS) {
    match <- str_extract(text, pattern)
    if (!is.na(match) && nchar(match) > 2) {
      return(str_squish(match))
    }
  }

  NA_character_
}

extract_events <- function(article_row) {
  locations <- extract_locations(article_row$text)
  if (length(locations) == 0) return(tibble())

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

COUNTRY_HINT_PATTERNS <- list(
  Iran = "\\b(?:Tehran|Esfahan|Isfahan|Kerman|Bandar\\s+Abbas|Dezful|Natanz|Bushehr|Khuzestan|Hormozgan|Fars|Alborz|Jam|Tabriz|Mashhad|Shiraz|Qom)\\b",
  Iraq = "\\b(?:Baghdad|Anbar|Salah\\s+al\\s+Din|Basra|Mosul|Kirkuk|Erbil|Najaf|Karbala|Diyala)\\b",
  Lebanon = "\\b(?:Beirut|Tyre|Khiam|Bint\\s+Jbeil|Naqoura|Marjaayoun|Lebanon)\\b",
  Israel = "\\b(?:Israel|Dimona|Safed|Maalot|Kiryat|Rishon|Galilee)\\b",
  Bahrain = "\\bBahrain\\b",
  Kuwait = "\\bKuwait\\b",
  Qatar = "\\bQatar\\b",
  Syria = "\\b(?:Syria|Damascus|Homs|Aleppo|Deir\\s+ez\\s+Zor)\\b",
  Yemen = "\\b(?:Yemen|Sanaa|Aden|Hudaydah)\\b",
  `British Indian Ocean Territory` = "\\bDiego\\s+Garcia\\b"
)

infer_country_hint <- function(location_name) {
  if (str_detect(
    location_name,
    regex(
      "\\b(?:Iran|Iraq|Syria|Lebanon|Yemen|Bahrain|Kuwait|Qatar|Israel|United\\s+Arab\\s+Emirates|UAE|British\\s+Indian\\s+Ocean\\s+Territory)\\b",
      ignore_case = TRUE
    )
  )) {
    return(NA_character_)
  }

  for (country in names(COUNTRY_HINT_PATTERNS)) {
    if (str_detect(location_name, regex(COUNTRY_HINT_PATTERNS[[country]], ignore_case = TRUE))) {
      return(country)
    }
  }

  NA_character_
}

build_geocode_query <- function(location_name) {
  query <- str_squish(location_name)
  query <- str_remove(query, "(?i),?\\s+City$")
  query <- str_remove(query, "(?i)\\s+in\\s+the\\s+Indian\\s+Ocean$")
  query <- str_squish(query)

  hint <- infer_country_hint(query)
  if (!is.na(hint)) {
    query <- paste(query, hint, sep = ", ")
  }

  query
}

sanitize_geocode_results <- function(geocoded_df) {
  if (!all(c("location_name", "lat", "lon") %in% names(geocoded_df))) {
    return(geocoded_df)
  }

  has_coords <- !is.na(geocoded_df$lat) & !is.na(geocoded_df$lon)
  in_bbox <- has_coords &
    geocoded_df$lat >= GEO_LAT_MIN & geocoded_df$lat <= GEO_LAT_MAX &
    geocoded_df$lon >= GEO_LON_MIN & geocoded_df$lon <= GEO_LON_MAX

  diego_garcia_ok <- has_coords &
    str_detect(geocoded_df$location_name, regex("Diego\\s+Garcia", ignore_case = TRUE)) &
    geocoded_df$lat >= -15 & geocoded_df$lat <= 0 &
    geocoded_df$lon >= 60 & geocoded_df$lon <= 80

  keep <- in_bbox | diego_garcia_ok
  dropped <- sum(has_coords & !keep)

  if (dropped > 0) {
    message("  Dropped ", dropped, " implausible geocodes outside regional bounds")
  }

  geocoded_df |>
    mutate(
      lat = if_else(has_coords & !keep, NA_real_, lat),
      lon = if_else(has_coords & !keep, NA_real_, lon)
    )
}

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

  geocode_input <- unique_locations |>
    mutate(geocode_query = vapply(location_name, build_geocode_query, character(1)))

  geocoded <- tryCatch({
    geocode_input |>
      geocode(geocode_query, method = "osm", lat = lat, long = lon, full_results = TRUE) |>
      transmute(location_name, lat, lon)
  }, error = function(e) {
    warning("Geocoding failed: ", e$message)
    unique_locations |> mutate(lat = NA_real_, lon = NA_real_)
  })

  geocoded <- geocoded |>
    distinct(location_name, .keep_all = TRUE) |>
    sanitize_geocode_results() |>
    select(location_name, lat, lon)

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

  centcom_articles <- scrape_centcom_articles()
  isw_articles <- scrape_isw_articles()
  defense_articles <- scrape_defense_articles()

  all_articles <- bind_rows(centcom_articles, isw_articles, defense_articles)

  if (nrow(all_articles) == 0) {
    message("\n--- No articles scraped ---")
    message("All sources returned no results. Check selectors/network/fallback strategy.")
    return(invisible(NULL))
  }

  all_articles <- all_articles |>
    distinct(url, .keep_all = TRUE) |>
    filter(!is.na(text), nchar(text) > 80)

  message("\n--- Article Summary ---")
  message("Total articles after dedup/text filter: ", nrow(all_articles))

  if (nrow(all_articles) == 0) {
    message("No article text extracted. Check body selectors/fallback parsing.")
    return(invisible(NULL))
  }

  message("\n--- Extracting Events (regex) ---")

  events <- map_dfr(seq_len(nrow(all_articles)), function(i) {
    extract_events(all_articles[i, ])
  })

  message("  Total events extracted: ", nrow(events))
  if (nrow(events) == 0) {
    message("No events extracted. Check regex patterns.")
    return(invisible(NULL))
  }

  events_geocoded <- if (geocode_enabled) {
    message("\n--- Geocoding Locations ---")
    geocode_locations(events)
  } else {
    message("\n--- Geocoding disabled (GEOCODE_LOCATIONS=FALSE) ---")
    events |> mutate(lat = NA_real_, lon = NA_real_)
  }

  n_coords <- sum(!is.na(events_geocoded$lat))
  message("  Events with coordinates: ", n_coords, "/", nrow(events_geocoded))

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

if (!interactive()) {
  main()
}
