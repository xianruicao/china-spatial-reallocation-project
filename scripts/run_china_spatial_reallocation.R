#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  required_pkgs <- c(
    "sf", "terra", "exactextractr", "dplyr", "tidyr", "stringr", "purrr",
    "ggplot2", "patchwork", "scales", "readr", "tibble", "glue",
    "jsonlite", "httr2", "rvest", "geodata", "units", "lubridate"
  )
})

install_missing <- function(pkgs) {
  installed <- rownames(installed.packages())
  missing <- setdiff(pkgs, installed)
  if (length(missing) > 0) {
    install.packages(missing, repos = "https://cloud.r-project.org")
  }
}

install_missing(required_pkgs)
invisible(lapply(required_pkgs, library, character.only = TRUE))

options(scipen = 999)

START_YEAR <- 2000L
END_YEAR <- 2020L
YEARS_SNAPSHOT <- c(2000L, 2005L, 2010L, 2015L, 2020L)
# Denser rail-only chronology for visualizing post-2008 network changes.
YEARS_SNAPSHOT_RAIL_MAP <- c(2000L, 2005L, 2008L, 2010L, 2012L, 2015L, 2018L, 2020L)
HSR_MIN_YEAR <- 2008L
MAINLINE_RAILWAY_TYPES <- c("rail", "narrow_gauge")
CRS_PROJ <- 3857
QUANTILE_LOW <- 0.33
QUANTILE_HIGH <- 0.67
EXCLUDE_PROV_NAMES <- c("Hong Kong", "Macau")
OSM_MAJOR_ROAD_CLASSES <- c("motorway", "motorway_link", "trunk", "trunk_link")
OSM_AIRPORT_CLASSES <- c("airport", "airfield")
OSM_PORT_TRAFFIC_CLASSES <- c("pier", "marina")
OSM_FERRY_CLASS <- "ferry_terminal"
OSM_LOGISTICS_REGEX <- "logistics|freight|cargo|物流|货运|仓储|保税|集散|配送"
OSM_PORT_REGEX <- "港区|港口|码头|港务|港航|港埠|集装箱|port|harbour|harbor|terminal"
OSM_LANDUSE_HUB_CLASSES <- c("industrial", "commercial", "retail")
OSM_HUB_MIN_AREA_KM2 <- 0.05

required_dirs <- c(
  "data/raw",
  "data/raw/boundaries",
  "data/raw/sez",
  "data/raw/rail",
  "data/raw/multimodal",
  "data/raw/multimodal/geofabrik_zips",
  "data/raw/population",
  "data/raw/economy",
  "data/interim",
  "data/processed",
  "data/metadata",
  "scripts",
  "outputs/figures",
  "outputs/tables",
  "outputs/maps",
  "logs"
)

invisible(lapply(required_dirs, dir.create, recursive = TRUE, showWarnings = FALSE))

log_file <- "logs/pipeline_log.txt"
if (file.exists(log_file)) file.remove(log_file)

log_message <- function(msg) {
  stamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  line <- sprintf("[%s] %s", stamp, msg)
  cat(line, "\n", file = log_file, append = TRUE)
  message(line)
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

safe_download <- function(url, dest, retries = 3L, sleep_sec = 1) {
  for (i in seq_len(retries)) {
    ok <- tryCatch({
      download.file(url, destfile = dest, mode = "wb", quiet = TRUE)
      TRUE
    }, error = function(e) {
      log_message(glue::glue("Download failed (attempt {i}/{retries}): {url} | {e$message}"))
      FALSE
    })

    if (ok && file.exists(dest) && file.size(dest) > 0) {
      return(TRUE)
    }

    Sys.sleep(i * sleep_sec)
  }

  FALSE
}

read_geofabrik_shp <- function(zip_path, shp_base_name) {
  p <- sprintf("/vsizip/%s/%s.shp", normalizePath(zip_path, winslash = "/", mustWork = FALSE), shp_base_name)
  tryCatch(
    {
      x <- sf::st_read(p, quiet = TRUE)
      if (is.na(sf::st_crs(x))) {
        x <- sf::st_set_crs(x, 4326)
      } else if (sf::st_crs(x)$epsg != 4326) {
        x <- sf::st_transform(x, 4326)
      }
      x
    },
    error = function(e) {
      log_message(glue::glue("Failed to read layer {shp_base_name} from {basename(zip_path)}: {e$message}"))
      NULL
    }
  )
}

extract_geofabrik_china_subregion_paths <- function() {
  fallback <- c(
    "china/anhui-latest-free.shp.zip",
    "china/beijing-latest-free.shp.zip",
    "china/chongqing-latest-free.shp.zip",
    "china/fujian-latest-free.shp.zip",
    "china/gansu-latest-free.shp.zip",
    "china/guangdong-latest-free.shp.zip",
    "china/guangxi-latest-free.shp.zip",
    "china/guizhou-latest-free.shp.zip",
    "china/hainan-latest-free.shp.zip",
    "china/hebei-latest-free.shp.zip",
    "china/heilongjiang-latest-free.shp.zip",
    "china/henan-latest-free.shp.zip",
    "china/hubei-latest-free.shp.zip",
    "china/hunan-latest-free.shp.zip",
    "china/inner-mongolia-latest-free.shp.zip",
    "china/jiangsu-latest-free.shp.zip",
    "china/jiangxi-latest-free.shp.zip",
    "china/jilin-latest-free.shp.zip",
    "china/liaoning-latest-free.shp.zip",
    "china/ningxia-latest-free.shp.zip",
    "china/qinghai-latest-free.shp.zip",
    "china/shaanxi-latest-free.shp.zip",
    "china/shandong-latest-free.shp.zip",
    "china/shanghai-latest-free.shp.zip",
    "china/shanxi-latest-free.shp.zip",
    "china/sichuan-latest-free.shp.zip",
    "china/tianjin-latest-free.shp.zip",
    "china/tibet-latest-free.shp.zip",
    "china/xinjiang-latest-free.shp.zip",
    "china/yunnan-latest-free.shp.zip",
    "china/zhejiang-latest-free.shp.zip"
  )

  paths <- tryCatch({
    html <- httr2::request("https://download.geofabrik.de/asia/china.html") |>
      httr2::req_timeout(120) |>
      httr2::req_perform() |>
      httr2::resp_body_string()
    stringr::str_match_all(html, "china/[a-z\\-]+-latest-free\\.shp\\.zip")[[1]][, 1]
  }, error = function(e) {
    log_message(glue::glue("Could not parse Geofabrik China index page; using fallback province list. {e$message}"))
    character(0)
  })

  paths <- unique(paths)
  paths <- setdiff(paths, c("china/hong-kong-latest-free.shp.zip", "china/macau-latest-free.shp.zip"))
  if (length(paths) == 0) {
    return(fallback)
  }
  paths
}

query_wikidata <- function(query, timeout_sec = 240, retries = 4L) {
  last_error <- NULL

  for (i in seq_len(retries)) {
    ans <- tryCatch({
      resp <- httr2::request("https://query.wikidata.org/sparql") |>
        httr2::req_url_query(query = query, format = "json") |>
        httr2::req_user_agent("Codex-China-Spatial-Reallocation/1.0") |>
        httr2::req_timeout(timeout_sec) |>
        httr2::req_perform()

      jsonlite::fromJSON(httr2::resp_body_string(resp), simplifyDataFrame = TRUE)
    }, error = function(e) {
      last_error <<- e
      log_message(glue::glue("Wikidata query failed (attempt {i}/{retries}): {e$message}"))
      NULL
    })

    if (!is.null(ans)) {
      return(ans)
    }

    Sys.sleep(i * 2)
  }

  stop(glue::glue("Wikidata query failed after {retries} attempts: {last_error$message}"))
}

parse_point_wkt <- function(wkt) {
  # Handle POINT/Point casing, optional spaces, and scientific notation.
  m <- stringr::str_match(wkt, "(?i)point\\s*\\(([-0-9.eE+]+)\\s+([-0-9.eE+]+)\\)")
  tibble::tibble(
    lon = as.numeric(m[, 2]),
    lat = as.numeric(m[, 3])
  )
}

extract_binding_value <- function(bindings, field) {
  if (field %in% names(bindings)) {
    x <- bindings[[field]]
    if (is.data.frame(x) && "value" %in% names(x)) {
      return(as.character(x$value))
    }
    return(as.character(x))
  }

  value_col <- paste0(field, ".value")
  if (value_col %in% names(bindings)) {
    return(as.character(bindings[[value_col]]))
  }

  rep(NA_character_, nrow(bindings))
}

extract_binding_lang <- function(bindings, field) {
  if (field %in% names(bindings)) {
    x <- bindings[[field]]
    if (is.data.frame(x)) {
      lang_col <- names(x)[grepl("xml:lang", names(x), fixed = TRUE)]
      if (length(lang_col) > 0) {
        return(as.character(x[[lang_col[1]]]))
      }
    }
  }

  lang_col_flat <- paste0(field, ".xml:lang")
  if (lang_col_flat %in% names(bindings)) {
    return(as.character(bindings[[lang_col_flat]]))
  }

  rep(NA_character_, nrow(bindings))
}

first_non_na <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  x[1]
}

safe_min_year <- function(x) {
  x <- suppressWarnings(as.integer(x))
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_integer_)
  min(x)
}

is_hsr_line_name <- function(name_vec) {
  name_vec <- dplyr::coalesce(name_vec, "")
  # Conservative pattern: capture high-speed rail naming while avoiding generic highway strings.
  stringr::str_detect(
    stringr::str_to_lower(name_vec),
    "high[- ]?speed|\\bhsr\\b|gaosu"
  ) |
    stringr::str_detect(
      name_vec,
      "高速铁路|高速线|高铁|客专|客运专线|城际"
    )
}

normalize_name <- function(x) {
  x |>
    iconv(to = "ASCII//TRANSLIT") |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("[[:punct:]]", " ") |>
    stringr::str_replace_all("\\\\b(province|autonomous region|municipality|city|sheng|shi|region|uighur|uygur|zhuang|hui)\\\\b", " ") |>
    stringr::str_replace_all("\\\\s+", " ") |>
    stringr::str_trim()
}

agr <- function(x0, x1, n_years) {
  ifelse(is.na(x0) | is.na(x1) | x0 <= 0 | x1 <= 0, NA_real_, (x1 / x0)^(1 / n_years) - 1)
}

hh_index <- function(x) {
  x <- x[is.finite(x) & !is.na(x)]
  if (length(x) == 0 || sum(x) <= 0) return(NA_real_)
  s <- x / sum(x)
  sum(s^2)
}

quantile_band <- function(x, ql = QUANTILE_LOW, qh = QUANTILE_HIGH) {
  if (all(is.na(x))) return(rep(NA_character_, length(x)))
  qs <- as.numeric(stats::quantile(x, probs = c(ql, qh), na.rm = TRUE, names = FALSE))
  dplyr::case_when(
    is.na(x) ~ NA_character_,
    x <= qs[1] ~ "low",
    x > qs[2] ~ "high",
    TRUE ~ "mid"
  )
}

assign_trajectory_type <- function(zone_band, conn_band, pop_band, econ_band) {
  dplyr::case_when(
    zone_band == "high" & conn_band == "high" & pop_band == "high" & econ_band == "high" ~ "Zone-Rail Synergy Growth Pole",
    conn_band == "high" & pop_band == "high" & econ_band %in% c("mid", "high") & zone_band != "high" ~ "Rail-Led Corridor Urbanization",
    zone_band == "high" & econ_band == "high" & conn_band %in% c("low", "mid") ~ "Zone-Led Industrial/Policy Growth",
    pop_band == "high" & econ_band == "low" ~ "Population-Heavy / Lower Activity Growth",
    econ_band == "high" & pop_band == "low" ~ "Activity Growth Without Population Surge",
    zone_band == "low" & conn_band == "low" & pop_band == "low" & econ_band == "low" ~ "Bypassed / Lagging Region",
    TRUE ~ "Mixed Transitional Pattern"
  )
}

build_typology_variant <- function(
  df,
  variant_name,
  ql = QUANTILE_LOW,
  qh = QUANTILE_HIGH,
  w_zone = c(1, 0.5, 0.01),
  w_conn = c(1, 0.5, 2, 1)
) {
  out <- df |>
    dplyr::mutate(
      zone_intensity_v = w_zone[1] * dplyr::coalesce(active_zones, n_zones, 0) +
        w_zone[2] * dplyr::coalesce(n_zone_types, 0) +
        w_zone[3] * dplyr::coalesce(sez_area_km2, 0),
      connectivity_gain_v = w_conn[1] * dplyr::coalesce(hsr_km_change, 0) +
        w_conn[2] * dplyr::coalesce(rail_km_change, 0) +
        w_conn[3] * dplyr::coalesce(station_access_improve_km, 0) +
        w_conn[4] * dplyr::coalesce(hsr_station_gain, 0),
      pop_growth_v = pop_growth_ann,
      econ_growth_v = pref_gdp_growth_ann,
      zone_band_v = quantile_band(zone_intensity_v, ql = ql, qh = qh),
      conn_band_v = quantile_band(connectivity_gain_v, ql = ql, qh = qh),
      pop_band_v = quantile_band(pop_growth_v, ql = ql, qh = qh),
      econ_band_v = quantile_band(econ_growth_v, ql = ql, qh = qh),
      trajectory_type_v = assign_trajectory_type(zone_band_v, conn_band_v, pop_band_v, econ_band_v),
      variant = variant_name
    )

  out
}

adjusted_rand_index <- function(labels_a, labels_b) {
  ok <- !is.na(labels_a) & !is.na(labels_b)
  labels_a <- as.character(labels_a[ok])
  labels_b <- as.character(labels_b[ok])
  n <- length(labels_a)
  if (n < 2) return(NA_real_)

  tab <- table(labels_a, labels_b)
  a <- sum(choose(tab, 2))
  row_sum <- rowSums(tab)
  col_sum <- colSums(tab)
  b <- sum(choose(row_sum, 2))
  c <- sum(choose(col_sum, 2))
  d <- choose(n, 2)
  if (d == 0) return(NA_real_)

  expected <- (b * c) / d
  max_idx <- 0.5 * (b + c)
  denom <- max_idx - expected
  if (denom == 0) return(NA_real_)
  (a - expected) / denom
}

mean_diff_inference <- function(
  df,
  outcome_col,
  group_col,
  hi_value,
  lo_value,
  n_boot = 1500L,
  n_perm = 3000L,
  seed = 42L
) {
  y <- as.numeric(df[[outcome_col]])
  g <- as.character(df[[group_col]])
  keep <- is.finite(y) & !is.na(g) & g %in% c(hi_value, lo_value)

  y <- y[keep]
  g <- g[keep]

  n_hi <- sum(g == hi_value)
  n_lo <- sum(g == lo_value)

  if (length(y) == 0 || n_hi < 3 || n_lo < 3) {
    return(tibble::tibble(
      estimate = NA_real_,
      ci_low = NA_real_,
      ci_high = NA_real_,
      p_perm = NA_real_,
      n_hi = n_hi,
      n_lo = n_lo
    ))
  }

  estimate <- mean(y[g == hi_value], na.rm = TRUE) - mean(y[g == lo_value], na.rm = TRUE)

  set.seed(seed)
  boot_draws <- replicate(n_boot, {
    idx <- sample.int(length(y), replace = TRUE)
    yy <- y[idx]
    gg <- g[idx]
    if (sum(gg == hi_value) < 2 || sum(gg == lo_value) < 2) return(NA_real_)
    mean(yy[gg == hi_value], na.rm = TRUE) - mean(yy[gg == lo_value], na.rm = TRUE)
  })

  ci <- stats::quantile(boot_draws, probs = c(0.025, 0.975), na.rm = TRUE, names = FALSE)

  perm_draws <- replicate(n_perm, {
    gp <- sample(g, replace = FALSE)
    mean(y[gp == hi_value], na.rm = TRUE) - mean(y[gp == lo_value], na.rm = TRUE)
  })
  p_perm <- mean(abs(perm_draws) >= abs(estimate), na.rm = TRUE)

  tibble::tibble(
    estimate = estimate,
    ci_low = ci[1],
    ci_high = ci[2],
    p_perm = p_perm,
    n_hi = n_hi,
    n_lo = n_lo
  )
}

bootstrap_lm_terms <- function(
  df,
  formula_obj,
  terms_keep,
  n_boot = 1200L,
  seed = 42L
) {
  if (nrow(df) < 30) {
    return(tibble::tibble(
      term = terms_keep,
      estimate = NA_real_,
      ci_low = NA_real_,
      ci_high = NA_real_,
      p_value_ols = NA_real_,
      n_obs = nrow(df)
    ))
  }

  model <- stats::lm(formula_obj, data = df)
  coef_vec <- stats::coef(model)
  coef_smry <- summary(model)$coefficients

  get_term <- function(x, term) {
    if (term %in% names(x)) as.numeric(x[[term]]) else NA_real_
  }

  set.seed(seed)
  boot_mat <- replicate(n_boot, {
    idx <- sample.int(nrow(df), replace = TRUE)
    m <- tryCatch(stats::lm(formula_obj, data = df[idx, , drop = FALSE]), error = function(e) NULL)
    if (is.null(m)) return(rep(NA_real_, length(terms_keep)))
    co <- stats::coef(m)
    vapply(terms_keep, function(tt) get_term(co, tt), numeric(1))
  })

  if (is.vector(boot_mat)) {
    boot_mat <- matrix(boot_mat, nrow = length(terms_keep))
  }

  tibble::tibble(
    term = terms_keep,
    estimate = vapply(terms_keep, function(tt) get_term(coef_vec, tt), numeric(1)),
    ci_low = apply(boot_mat, 1, function(v) stats::quantile(v, probs = 0.025, na.rm = TRUE, names = FALSE)),
    ci_high = apply(boot_mat, 1, function(v) stats::quantile(v, probs = 0.975, na.rm = TRUE, names = FALSE)),
    p_value_ols = vapply(terms_keep, function(tt) {
      if (tt %in% rownames(coef_smry)) as.numeric(coef_smry[tt, "Pr(>|t|)"]) else NA_real_
    }, numeric(1)),
    n_obs = nrow(df)
  )
}

write_fig <- function(plot_obj, name, width = 12, height = 8, dpi = 300) {
  out <- file.path("outputs/figures", name)
  ggplot2::ggsave(out, plot_obj, width = width, height = height, dpi = dpi)
  if (stringr::str_detect(name, "^map_")) {
    file.copy(out, file.path("outputs/maps", name), overwrite = TRUE)
  }
  out
}

log_message("Step 1: Downloading and preparing administrative boundaries (GADM via geodata)")

gadm_path <- "data/raw/boundaries"
prov_vect <- geodata::gadm(country = "CHN", level = 1, path = gadm_path)
pref_vect <- geodata::gadm(country = "CHN", level = 2, path = gadm_path)

provinces <- sf::st_as_sf(prov_vect) |>
  dplyr::transmute(
    prov_id = as.character(GID_1),
    prov_name = as.character(NAME_1),
    geometry
  ) |>
  dplyr::filter(
    stringr::str_detect(prov_id, "^CHN\\."),
    !prov_name %in% EXCLUDE_PROV_NAMES
  )

prefectures <- sf::st_as_sf(pref_vect) |>
  dplyr::transmute(
    pref_id = as.character(GID_2),
    pref_name = as.character(NAME_2),
    prov_id = as.character(GID_1),
    prov_name = as.character(NAME_1),
    geometry
  ) |>
  dplyr::filter(
    stringr::str_detect(prov_id, "^CHN\\."),
    !prov_name %in% EXCLUDE_PROV_NAMES
  )

sf::st_write(provinces, "data/raw/boundaries/china_province.gpkg", delete_dsn = TRUE, quiet = TRUE)
sf::st_write(prefectures, "data/raw/boundaries/china_prefecture.gpkg", delete_dsn = TRUE, quiet = TRUE)

log_message(glue::glue("Boundaries prepared (mainland frame): {nrow(provinces)} provinces, {nrow(prefectures)} prefecture-level units (GADM level 2)."))

log_message("Step 2: Loading annual rail network layers from NBER China Surface Transport Database")
nber_base <- file.path("data/raw/rail/nber_selected", "GIS data", "GIS Transport China Shapefile")
nber_regular_dir <- file.path(nber_base, "RegularRailway")
nber_hsr_dir <- file.path(nber_base, "HighSpeedRailway")
nber_station_dir <- file.path(nber_base, "RailwayStations")

required_regular <- file.path(nber_regular_dir, sprintf("RegularRailway%d.shp", YEARS_SNAPSHOT_RAIL_MAP))
required_station <- file.path(nber_station_dir, sprintf("RailwayStations%d.shp", YEARS_SNAPSHOT))
missing_required <- c(
  required_regular[!file.exists(required_regular)],
  required_station[!file.exists(required_station)]
)

if (length(missing_required) > 0) {
  stop(glue::glue(
    "Missing required NBER rail/station files. Extract from NBER appendix archive into data/raw/rail/nber_selected first: ",
    "{paste(missing_required, collapse = '; ')}"
  ))
}

china_mainland_union <- sf::st_union(sf::st_make_valid(provinces))
china_union <- china_mainland_union

empty_rail_sf <- function() {
  sf::st_sf(
    name = character(),
    railway = character(),
    electrified = character(),
    usage = character(),
    rail_type_static = character(),
    rail_type = character(),
    source_year = integer(),
    start_year = integer(),
    geometry = sf::st_sfc(crs = CRS_PROJ)
  )
}

empty_station_sf <- function() {
  sf::st_sf(
    station_id = character(),
    station_name = character(),
    station_type = character(),
    opening_year = integer(),
    opening_year_imputed = logical(),
    year = integer(),
    geometry = sf::st_sfc(crs = CRS_PROJ)
  )
}

read_rail_layer_nber <- function(path, rail_type, snapshot_year) {
  if (!file.exists(path)) return(empty_rail_sf())

  x <- sf::st_read(path, quiet = TRUE)
  if (nrow(x) == 0) return(empty_rail_sf())
  start_year_vec <- if ("StartYear" %in% names(x)) {
    suppressWarnings(as.integer(x$StartYear))
  } else {
    rep(NA_integer_, nrow(x))
  }

  x <- x |>
    dplyr::transmute(
      name = as.character(name),
      railway = rail_type,
      electrified = NA_character_,
      usage = NA_character_,
      rail_type_static = rail_type,
      rail_type = rail_type,
      source_year = snapshot_year,
      start_year = start_year_vec,
      geometry
    ) |>
    sf::st_transform(CRS_PROJ)

  x <- sf::st_make_valid(x)
  x <- x[!sf::st_is_empty(x), ]
  if (nrow(x) == 0) return(empty_rail_sf())

  dup <- duplicated(sf::st_as_binary(sf::st_geometry(x)))
  x <- x[!dup, ]
  if (nrow(x) == 0) return(empty_rail_sf())

  seg_km <- as.numeric(sf::st_length(x)) / 1000
  x <- x[is.finite(seg_km) & seg_km > 0, ]
  x <- x |>
    dplyr::mutate(
      start_year = dplyr::case_when(
        is.na(start_year) ~ NA_integer_,
        start_year < 1850 ~ NA_integer_,
        start_year > END_YEAR ~ END_YEAR,
        TRUE ~ as.integer(start_year)
      )
    )
  x
}

rail_snap <- purrr::map_dfr(YEARS_SNAPSHOT_RAIL_MAP, function(y) {
  regular_path <- file.path(nber_regular_dir, sprintf("RegularRailway%d.shp", y))
  hsr_path <- file.path(nber_hsr_dir, sprintf("HighSpeedRailway%d.shp", y))

  rail_y <- dplyr::bind_rows(
    read_rail_layer_nber(regular_path, "rail", y),
    read_rail_layer_nber(hsr_path, "hsr", y)
  )

  if (nrow(rail_y) == 0) {
    stop(glue::glue("No rail segments available for year {y} after cleaning."))
  }

  rail_y |>
    dplyr::mutate(
      segment_id = sprintf("NBERSEG_%d_%07d", y, dplyr::row_number()),
      year = y
    ) |>
    dplyr::select(segment_id, name, railway, electrified, usage, rail_type_static, rail_type, source_year, start_year, year, geometry)
})

rail_quality <- rail_snap |>
  sf::st_drop_geometry() |>
  dplyr::count(year, rail_type, name = "n_segments") |>
  dplyr::arrange(year, rail_type)
log_message(glue::glue(
  "Rail snapshots loaded from NBER for years {paste(YEARS_SNAPSHOT_RAIL_MAP, collapse = ', ')} | ",
  "segments by type/year: ",
  "{paste0(rail_quality$year, '-', rail_quality$rail_type, ':', rail_quality$n_segments, collapse = '; ')}"
))

rail_lines <- rail_snap |>
  dplyr::filter(year == END_YEAR) |>
  dplyr::select(segment_id, name, railway, electrified, usage, rail_type_static, rail_type, source_year, start_year, geometry)

sf::st_write(rail_lines, "data/raw/rail/china_rail_lines_current.gpkg", delete_dsn = TRUE, quiet = TRUE)
sf::st_write(rail_snap, "data/raw/rail/china_rail_lines_timesliced.gpkg", delete_dsn = TRUE, quiet = TRUE)
rail_proj <- sf::st_transform(rail_lines, CRS_PROJ)

log_message("Step 3: Building station snapshots from annual NBER station layers")
station_snap <- purrr::map_dfr(YEARS_SNAPSHOT, function(y) {
  station_path <- file.path(nber_station_dir, sprintf("RailwayStations%d.shp", y))
  if (!file.exists(station_path)) return(empty_station_sf())

  st_y <- sf::st_read(station_path, quiet = TRUE)
  if (nrow(st_y) == 0) return(empty_station_sf())

  st_y <- sf::st_transform(st_y, CRS_PROJ)
  hsr_flag <- if ("HghSpdS" %in% names(st_y)) {
    stringr::str_to_lower(as.character(st_y$HghSpdS)) %in% c("yes", "y", "1", "true")
  } else {
    rep(FALSE, nrow(st_y))
  }
  station_name <- dplyr::coalesce(as.character(st_y$name), as.character(st_y$Station))

  coords <- sf::st_coordinates(st_y)
  st_y <- st_y |>
    dplyr::mutate(
      station_name = station_name,
      station_type = ifelse(hsr_flag, "hsr", "rail"),
      opening_year = as.integer(y),
      opening_year_imputed = FALSE,
      coord_x = round(coords[, 1], 3),
      coord_y = round(coords[, 2], 3)
    ) |>
    dplyr::distinct(station_type, coord_x, coord_y, .keep_all = TRUE) |>
    dplyr::mutate(
      year = as.integer(y),
      station_id = sprintf("STN_%d_%06d", y, dplyr::row_number())
    ) |>
    dplyr::select(station_id, station_name, station_type, opening_year, opening_year_imputed, year, geometry)

  st_y
})

stations_sf <- station_snap |>
  dplyr::filter(year == END_YEAR) |>
  dplyr::select(station_id, station_name, station_type, opening_year, opening_year_imputed, geometry)

sf::st_write(stations_sf, "data/raw/rail/china_rail_stations.gpkg", delete_dsn = TRUE, quiet = TRUE)
sf::st_write(station_snap, "data/interim/china_rail_station_snapshots.gpkg", delete_dsn = TRUE, quiet = TRUE)

station_quality <- station_snap |>
  sf::st_drop_geometry() |>
  dplyr::count(year, station_type, name = "n_stations") |>
  dplyr::arrange(year, station_type)
log_message(glue::glue(
  "Station snapshots loaded from NBER for years {paste(YEARS_SNAPSHOT, collapse = ', ')} | ",
  "stations by type/year: {paste0(station_quality$year, '-', station_quality$station_type, ':', station_quality$n_stations, collapse = '; ')}"
))

log_message("Step 4: Time-sliced rail and station layers ready (NBER annual snapshots)")

log_message("Step 5: Querying development-zone points from Wikidata")
zone_query <- paste(
  "SELECT DISTINCT ?item ?coord ?inception ?lab WHERE {",
  "?item wdt:P17 wd:Q148 ; wdt:P625 ?coord .",
  "OPTIONAL { ?item wdt:P571 ?inception . }",
  "?item rdfs:label ?lab .",
  "FILTER(LANG(?lab) IN (\"en\",\"zh\"))",
  "FILTER(REGEX(LCASE(STR(?lab)), \"development zone|economic and technological development zone|high-tech zone|special economic zone|free trade zone|bonded zone|开发区|高新区|保税区|自贸区|自由贸易试验区\"))",
  "MINUS { ?item wdt:P31/wdt:P279* wd:Q55488 }",
  "} LIMIT 1000"
)

zone_json <- query_wikidata(zone_query)
zone_bind <- zone_json$results$bindings
zone_bind <- jsonlite::flatten(zone_bind)

if (is.data.frame(zone_bind) && nrow(zone_bind) > 0) {
  zones_tbl <- tibble::tibble(
    item_uri = extract_binding_value(zone_bind, "item"),
    coord_wkt = extract_binding_value(zone_bind, "coord"),
    inception = extract_binding_value(zone_bind, "inception"),
    label = extract_binding_value(zone_bind, "lab"),
    lang = extract_binding_lang(zone_bind, "lab")
  )

  zone_coords <- parse_point_wkt(zones_tbl$coord_wkt)
  zones_tbl <- dplyr::bind_cols(zones_tbl, zone_coords) |>
    dplyr::mutate(
      item_id = stringr::str_replace(item_uri, "^.*/", ""),
      establishment_year = suppressWarnings(as.integer(stringr::str_sub(inception, 1, 4))),
      label_low = stringr::str_to_lower(label)
    ) |>
    dplyr::filter(is.finite(lon), is.finite(lat)) |>
    dplyr::filter(
      !stringr::str_detect(label_low, "station|metro|railway|tram|line|road|bridge|站|铁路|地铁|有轨|线路|公路|桥")
    )

  zones_tbl <- zones_tbl |>
    dplyr::group_by(item_id, lon, lat) |>
    dplyr::summarise(
      zone_name_en = first_non_na(label[lang == "en"]),
      zone_name_zh = first_non_na(label[lang == "zh"]),
      zone_name = dplyr::coalesce(zone_name_en, zone_name_zh),
      establishment_year = safe_min_year(establishment_year),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      zone_name = dplyr::coalesce(zone_name, item_id),
      zone_type = dplyr::case_when(
        stringr::str_detect(stringr::str_to_lower(zone_name), "special economic zone|\\\\bsez\\\\b|经济特区") ~ "SEZ",
        stringr::str_detect(stringr::str_to_lower(zone_name), "economic and technological|经济技术") ~ "ETDZ",
        stringr::str_detect(stringr::str_to_lower(zone_name), "high[ -]?tech|高新") ~ "HIDZ",
        stringr::str_detect(stringr::str_to_lower(zone_name), "free trade|bonded|自贸|保税") ~ "FTZ/Bonded",
        TRUE ~ "Development Zone"
      ),
      zone_id = sprintf("ZONE_%05d", dplyr::row_number())
    )

  sez_sf <- sf::st_as_sf(zones_tbl, coords = c("lon", "lat"), crs = 4326, remove = FALSE)
  inside_china_zone <- lengths(sf::st_intersects(sez_sf, china_union)) > 0
  sez_sf <- sez_sf[inside_china_zone, ]

  sf::st_write(sez_sf, "data/raw/sez/china_development_zones.gpkg", delete_dsn = TRUE, quiet = TRUE)
  log_message(glue::glue("Development zones prepared: {nrow(sez_sf)} points."))
} else {
  sez_sf <- sf::st_as_sf(
    tibble::tibble(
      zone_id = character(), zone_name = character(), zone_type = character(),
      establishment_year = integer(), lon = numeric(), lat = numeric()
    ),
    coords = c("lon", "lat"), crs = 4326
  )
  sf::st_write(sez_sf, "data/raw/sez/china_development_zones.gpkg", delete_dsn = TRUE, quiet = TRUE)
  log_message("No development-zone points returned from Wikidata; continuing with empty layer.")
}

log_message("Step 5b: Building multimodal infrastructure layers (expressways/national roads, airports, ports/logistics hubs) from Geofabrik OSM")
geofabrik_paths <- extract_geofabrik_china_subregion_paths()
geofabrik_base <- "https://download.geofabrik.de/asia"
geofabrik_zip_dir <- "data/raw/multimodal/geofabrik_zips"

geofabrik_tbl <- tibble::tibble(
  rel_path = geofabrik_paths,
  url = paste0(geofabrik_base, "/", rel_path),
  zip_file = file.path(geofabrik_zip_dir, basename(rel_path))
)

for (i in seq_len(nrow(geofabrik_tbl))) {
  zf <- geofabrik_tbl$zip_file[i]
  if (file.exists(zf) && file.size(zf) > 0) next
  ok <- safe_download(geofabrik_tbl$url[i], zf, retries = 3, sleep_sec = 2)
  if (!ok) {
    log_message(glue::glue("Failed to download Geofabrik region file: {geofabrik_tbl$url[i]}"))
  }
}

available_zips <- geofabrik_tbl$zip_file[file.exists(geofabrik_tbl$zip_file) & file.size(geofabrik_tbl$zip_file) > 0]
log_message(glue::glue("Geofabrik regional archives available: {length(available_zips)}/{nrow(geofabrik_tbl)}"))

empty_roads_mm <- sf::st_sf(
  source_region = character(),
  road_class = character(),
  road_name = character(),
  road_ref = character(),
  geometry = sf::st_sfc(crs = 4326)
)

empty_airports_mm <- sf::st_sf(
  source_region = character(),
  airport_class = character(),
  airport_name = character(),
  geometry = sf::st_sfc(crs = 4326)
)

empty_ports_mm <- sf::st_sf(
  source_region = character(),
  hub_class = character(),
  hub_name = character(),
  source_layer = character(),
  geometry = sf::st_sfc(crs = 4326)
)

extract_region_code <- function(zip_file) {
  stringr::str_replace(basename(zip_file), "-latest-free\\.shp\\.zip$", "")
}

if (length(available_zips) > 0) {
  roads_mm <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    rd <- read_geofabrik_shp(zf, "gis_osm_roads_free_1")
    if (is.null(rd) || nrow(rd) == 0) return(empty_roads_mm)
    rd |>
      dplyr::filter(fclass %in% OSM_MAJOR_ROAD_CLASSES) |>
      dplyr::transmute(
        source_region = region_code,
        road_class = fclass,
        road_name = name,
        road_ref = ref,
        geometry
      )
  })

  if (nrow(roads_mm) == 0) roads_mm <- empty_roads_mm
  roads_mm <- roads_mm[!sf::st_is_empty(roads_mm), ]
  if (nrow(roads_mm) > 0) roads_mm <- sf::st_transform(roads_mm, CRS_PROJ)

  airport_pts <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    tr_pt <- read_geofabrik_shp(zf, "gis_osm_transport_free_1")
    if (is.null(tr_pt) || nrow(tr_pt) == 0) return(empty_airports_mm)
    tr_pt |>
      dplyr::filter(fclass %in% OSM_AIRPORT_CLASSES) |>
      dplyr::transmute(
        source_region = region_code,
        airport_class = fclass,
        airport_name = name,
        geometry
      )
  })

  airport_area <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    tr_a <- read_geofabrik_shp(zf, "gis_osm_transport_a_free_1")
    if (is.null(tr_a) || nrow(tr_a) == 0) return(empty_airports_mm)
    tr_a <- tr_a |>
      dplyr::filter(fclass %in% OSM_AIRPORT_CLASSES)
    if (nrow(tr_a) == 0) return(empty_airports_mm)
    sf::st_point_on_surface(tr_a) |>
      dplyr::transmute(
        source_region = region_code,
        airport_class = fclass,
        airport_name = name,
        geometry
      )
  })

  airports_mm <- dplyr::bind_rows(airport_pts, airport_area)
  if (nrow(airports_mm) == 0) {
    airports_mm <- empty_airports_mm
  } else {
    airports_mm <- airports_mm[!sf::st_is_empty(airports_mm), ]
    airports_mm <- sf::st_transform(airports_mm, CRS_PROJ)
    coords <- sf::st_coordinates(airports_mm)
    airports_mm <- airports_mm |>
      dplyr::mutate(
        x_round = round(coords[, 1], 1),
        y_round = round(coords[, 2], 1),
        name_key = stringr::str_to_lower(dplyr::coalesce(airport_name, ""))
      ) |>
      dplyr::distinct(name_key, x_round, y_round, .keep_all = TRUE) |>
      dplyr::select(-x_round, -y_round, -name_key)
  }

  ports_terminal_pt <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    tr_pt <- read_geofabrik_shp(zf, "gis_osm_transport_free_1")
    if (is.null(tr_pt) || nrow(tr_pt) == 0) return(empty_ports_mm)
    tr_pt |>
      dplyr::filter(fclass == OSM_FERRY_CLASS) |>
      dplyr::filter(!is.na(name) & name != "") |>
      dplyr::transmute(
        source_region = region_code,
        hub_class = "Port terminal",
        hub_name = name,
        source_layer = "transport_point_ferry_terminal",
        geometry
      )
  })

  ports_terminal_area <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    tr_a <- read_geofabrik_shp(zf, "gis_osm_transport_a_free_1")
    if (is.null(tr_a) || nrow(tr_a) == 0) return(empty_ports_mm)
    tr_a <- tr_a |>
      dplyr::filter(fclass == OSM_FERRY_CLASS) |>
      dplyr::filter(!is.na(name) & name != "")
    if (nrow(tr_a) == 0) return(empty_ports_mm)
    sf::st_point_on_surface(tr_a) |>
      dplyr::transmute(
        source_region = region_code,
        hub_class = "Port terminal",
        hub_name = name,
        source_layer = "transport_area_ferry_terminal",
        geometry
      )
  })

  ports_traffic_area <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    ta <- read_geofabrik_shp(zf, "gis_osm_traffic_a_free_1")
    if (is.null(ta) || nrow(ta) == 0) return(empty_ports_mm)
    ta <- ta |>
      dplyr::filter(fclass %in% OSM_PORT_TRAFFIC_CLASSES) |>
      dplyr::filter(!is.na(name) & name != "")
    if (nrow(ta) == 0) return(empty_ports_mm)
    sf::st_point_on_surface(ta) |>
      dplyr::transmute(
        source_region = region_code,
        hub_class = "Port terminal",
        hub_name = name,
        source_layer = "traffic_area_pier_marina",
        geometry
      )
  })

  ports_landuse <- purrr::map_dfr(available_zips, function(zf) {
    region_code <- extract_region_code(zf)
    lu <- read_geofabrik_shp(zf, "gis_osm_landuse_a_free_1")
    if (is.null(lu) || nrow(lu) == 0) return(empty_ports_mm)
    lu <- lu |>
      dplyr::filter(fclass %in% OSM_LANDUSE_HUB_CLASSES) |>
      dplyr::filter(!is.na(name) & name != "")
    if (nrow(lu) == 0) return(empty_ports_mm)

    lu <- sf::st_transform(lu, CRS_PROJ) |>
      dplyr::mutate(
        area_km2 = as.numeric(sf::st_area(geometry)) / 1e6,
        name_low = stringr::str_to_lower(name),
        is_logistics = stringr::str_detect(name_low, OSM_LOGISTICS_REGEX),
        is_port = stringr::str_detect(name_low, OSM_PORT_REGEX)
      ) |>
      dplyr::filter(area_km2 >= OSM_HUB_MIN_AREA_KM2, is_logistics | is_port)

    if (nrow(lu) == 0) return(empty_ports_mm)
    sf::st_point_on_surface(lu) |>
      dplyr::transmute(
        source_region = region_code,
        hub_class = dplyr::case_when(
          is_logistics & is_port ~ "Port + logistics hub",
          is_logistics ~ "Logistics hub",
          TRUE ~ "Port complex"
        ),
        hub_name = name,
        source_layer = "landuse_area_named_hub",
        geometry
      ) |>
      sf::st_transform(4326)
  })

  ports_hubs_mm <- dplyr::bind_rows(
    ports_terminal_pt,
    ports_terminal_area,
    ports_traffic_area,
    ports_landuse
  )
  if (nrow(ports_hubs_mm) == 0) {
    ports_hubs_mm <- empty_ports_mm
  } else {
    ports_hubs_mm <- ports_hubs_mm[!sf::st_is_empty(ports_hubs_mm), ]
    if (sf::st_crs(ports_hubs_mm)$epsg != CRS_PROJ) {
      ports_hubs_mm <- sf::st_transform(ports_hubs_mm, CRS_PROJ)
    }
    coords <- sf::st_coordinates(ports_hubs_mm)
    ports_hubs_mm <- ports_hubs_mm |>
      dplyr::mutate(
        x_round = round(coords[, 1], 1),
        y_round = round(coords[, 2], 1),
        name_key = stringr::str_to_lower(dplyr::coalesce(hub_name, "")),
        class_key = dplyr::coalesce(hub_class, "")
      ) |>
      dplyr::distinct(name_key, class_key, x_round, y_round, .keep_all = TRUE) |>
      dplyr::select(-x_round, -y_round, -name_key, -class_key)
  }
} else {
  roads_mm <- sf::st_transform(empty_roads_mm, CRS_PROJ)
  airports_mm <- sf::st_transform(empty_airports_mm, CRS_PROJ)
  ports_hubs_mm <- sf::st_transform(empty_ports_mm, CRS_PROJ)
}

sf::st_write(roads_mm, "data/raw/multimodal/china_expressways_national_roads.gpkg", delete_dsn = TRUE, quiet = TRUE)
sf::st_write(airports_mm, "data/raw/multimodal/china_airports.gpkg", delete_dsn = TRUE, quiet = TRUE)
sf::st_write(ports_hubs_mm, "data/raw/multimodal/china_ports_logistics_hubs.gpkg", delete_dsn = TRUE, quiet = TRUE)

log_message(glue::glue(
  "Multimodal layers ready | roads: {nrow(roads_mm)}, airports: {nrow(airports_mm)}, ports/logistics hubs: {nrow(ports_hubs_mm)}"
))

log_message("Step 6: Downloading and processing population rasters (GPW via geodata::population)")
pop_records <- list()
pop_files <- character(0)

for (y in YEARS_SNAPSHOT) {
  r <- geodata::population(year = y, res = 2.5, path = "data/raw/population")
  out_file <- file.path("data/raw/population", sprintf("gpw_population_density_%d_2.5m.tif", y))

  # Keep a year-stamped copy in project raw folder.
  if (!file.exists(out_file)) {
    file.copy(terra::sources(r), out_file, overwrite = TRUE)
  }

  pop_records[[as.character(y)]] <- r
  pop_files <- c(pop_files, out_file)
}

log_message(glue::glue("Population rasters ready for {length(pop_records)} snapshot years."))

log_message("Step 7: Scraping provincial GDP snapshots (2000, 2010, 2015, 2020) from Wikipedia")
gdp_url <- "https://en.wikipedia.org/wiki/List_of_Chinese_administrative_divisions_by_GDP"
gdp_page <- rvest::read_html(gdp_url)
gdp_tables <- rvest::html_elements(gdp_page, "table") |>
  rvest::html_table(fill = TRUE)

candidate_idx <- which(purrr::map_lgl(gdp_tables, function(tb) {
  all(c("year", "2000", "2010", "2015", "2020") %in% names(tb))
}))

if (length(candidate_idx) == 0) {
  stop("Could not find expected GDP table on Wikipedia page.")
}

gdp_raw <- gdp_tables[[candidate_idx[1]]] |>
  dplyr::rename(prov_name_source = 1) |>
  dplyr::select(prov_name_source, `2000`, `2010`, `2015`, `2020`) |>
  dplyr::mutate(
    prov_name_source = stringr::str_replace_all(prov_name_source, "\\[[0-9]+\\]", ""),
    prov_name_source = stringr::str_squish(prov_name_source)
  ) |>
  dplyr::mutate(prov_name_clean = stringr::str_to_lower(prov_name_source)) |>
  dplyr::filter(
    !stringr::str_detect(prov_name_clean, "china\\\\(mainland\\\\)|cn¥|hong\\\\s*kong|hongkong|macau|macao"),
    prov_name_source != ""
  ) |>
  dplyr::select(-prov_name_clean) |>
  tidyr::pivot_longer(cols = c(`2000`, `2010`, `2015`, `2020`), names_to = "year", values_to = "gdp_value") |>
  dplyr::mutate(
    year = as.integer(year),
    gdp_value = readr::parse_number(gdp_value, locale = readr::locale(grouping_mark = ","))
  ) |>
  dplyr::filter(!is.na(gdp_value))

prov_lookup <- provinces |>
  sf::st_drop_geometry() |>
  dplyr::mutate(
    prov_key = normalize_name(prov_name),
    prov_key = dplyr::case_when(
      prov_key == "nei mongol" ~ "inner mongolia",
      prov_key == "xizang" ~ "tibet",
      prov_key == "xinjiang uygur" ~ "xinjiang",
      prov_key == "xinjiang uyghur" ~ "xinjiang",
      prov_key == "ningxia hui" ~ "ningxia",
      prov_key == "guangxi zhuang" ~ "guangxi",
      TRUE ~ prov_key
    )
  ) |>
  dplyr::select(prov_id, prov_name, prov_key) |>
  dplyr::distinct(prov_key, .keep_all = TRUE)

gdp_raw <- gdp_raw |>
  dplyr::mutate(
    prov_key = normalize_name(prov_name_source),
    prov_key = dplyr::case_when(
      prov_key == "xizang" ~ "tibet",
      prov_key == "nei mongol" ~ "inner mongolia",
      prov_key == "inner mongolia autonomous region" ~ "inner mongolia",
      prov_key == "xinjiang uyghur" ~ "xinjiang",
      prov_key == "xinjiang uygur" ~ "xinjiang",
      prov_key == "ningxia" ~ "ningxia",
      prov_key == "guangxi" ~ "guangxi",
      TRUE ~ prov_key
    )
  )

gdp_joined <- gdp_raw |>
  dplyr::left_join(prov_lookup, by = "prov_key")

if (any(is.na(gdp_joined$prov_id))) {
  unmatched <- unique(gdp_joined$prov_name_source[is.na(gdp_joined$prov_id)])
  log_message(glue::glue("Unmatched GDP province names: {paste(unmatched, collapse = '; ')}"))

  # Attempt simple nearest-string match for remaining names.
  unmatched_keys <- unique(gdp_joined$prov_key[is.na(gdp_joined$prov_id)])
  for (k in unmatched_keys) {
    if (is.na(k) || k == "") next
    d <- utils::adist(k, prov_lookup$prov_key)
    i <- which.min(d)
    if (length(i) == 1 && d[i] <= 3) {
      gdp_joined$prov_id[gdp_joined$prov_key == k & is.na(gdp_joined$prov_id)] <- prov_lookup$prov_id[i]
      gdp_joined$prov_name[gdp_joined$prov_key == k & is.na(gdp_joined$prov_name)] <- prov_lookup$prov_name[i]
    }
  }
}

gdp_prov_snap <- gdp_joined |>
  dplyr::filter(!is.na(prov_id)) |>
  dplyr::group_by(prov_id, prov_name, prov_name_source, year) |>
  dplyr::summarise(gdp_value = max(gdp_value, na.rm = TRUE), .groups = "drop")

gdp_prov <- gdp_prov_snap |>
  dplyr::group_by(prov_id, prov_name, prov_name_source) |>
  dplyr::group_modify(~{
    d <- .x |>
      dplyr::arrange(year) |>
      dplyr::filter(!is.na(gdp_value))

    if (nrow(d) == 0) {
      return(tibble::tibble(year = YEARS_SNAPSHOT, gdp_value = NA_real_))
    }

    if (nrow(d) == 1) {
      return(tibble::tibble(year = YEARS_SNAPSHOT, gdp_value = d$gdp_value[1]))
    }

    tibble::tibble(
      year = YEARS_SNAPSHOT,
      gdp_value = stats::approx(x = d$year, y = d$gdp_value, xout = YEARS_SNAPSHOT, rule = 2)$y
    )
  }) |>
  dplyr::ungroup() |>
  dplyr::arrange(prov_id, year) |>
  dplyr::group_by(prov_id) |>
  dplyr::mutate(
    gdp_growth_pct = 100 * (gdp_value / dplyr::lag(gdp_value) - 1),
    gdp_unit = "CNY million (nominal)",
    price_basis = "nominal"
  ) |>
  dplyr::ungroup() |>
  dplyr::distinct(prov_id, year, .keep_all = TRUE)

readr::write_csv(gdp_prov, "data/raw/economy/china_provincial_gdp.csv")
log_message(glue::glue("Provincial GDP panel written: {n_distinct(gdp_prov$prov_id)} provinces x {n_distinct(gdp_prov$year)} years."))

log_message("Step 8: Computing prefecture-level zone and rail connectivity metrics")
pref_proj <- sf::st_transform(prefectures, CRS_PROJ) |>
  dplyr::mutate(pref_area_km2 = as.numeric(sf::st_area(geometry)) / 1e6)

# Development zones by prefecture.
if (nrow(sez_sf) > 0) {
  sez_proj <- sf::st_transform(sez_sf, CRS_PROJ)
  sez_pref <- sf::st_join(
    sez_proj |>
      dplyr::select(zone_id, zone_name, zone_type, establishment_year, geometry),
    pref_proj |>
      dplyr::select(pref_id, prov_id, pref_name, prov_name, geometry),
    left = FALSE
  )

  sez_pref_metrics <- sez_pref |>
    sf::st_drop_geometry() |>
    dplyr::group_by(pref_id) |>
    dplyr::summarise(
      n_zones = dplyr::n(),
      n_zone_types = dplyr::n_distinct(zone_type, na.rm = TRUE),
      first_zone_year = safe_min_year(establishment_year),
      sez_area_km2 = NA_real_,
      .groups = "drop"
    )

  sez_active_year <- sez_pref |>
    sf::st_drop_geometry() |>
    dplyr::mutate(establishment_year = as.integer(establishment_year)) |>
    dplyr::filter(!is.na(establishment_year)) |>
    dplyr::group_by(pref_id, establishment_year) |>
    dplyr::summarise(new_zones = dplyr::n(), .groups = "drop") |>
    tidyr::complete(pref_id = pref_proj$pref_id, establishment_year = YEARS_SNAPSHOT, fill = list(new_zones = 0)) |>
    dplyr::group_by(pref_id) |>
    dplyr::arrange(establishment_year, .by_group = TRUE) |>
    dplyr::mutate(active_zones = cumsum(new_zones)) |>
    dplyr::ungroup() |>
    dplyr::rename(year = establishment_year)
} else {
  sez_pref_metrics <- pref_proj |>
    sf::st_drop_geometry() |>
    dplyr::transmute(pref_id, n_zones = 0L, n_zone_types = 0L, first_zone_year = NA_integer_, sez_area_km2 = NA_real_)

  sez_active_year <- tidyr::crossing(pref_id = pref_proj$pref_id, year = YEARS_SNAPSHOT) |>
    dplyr::mutate(active_zones = 0L)
}

rail_snap <- sf::st_transform(rail_snap, CRS_PROJ)
rail_snap_panel <- rail_snap |>
  dplyr::filter(year %in% YEARS_SNAPSHOT) |>
  dplyr::select(segment_id, year, rail_type, geometry)

rail_pref_long <- purrr::map_dfr(YEARS_SNAPSHOT, function(y) {
  log_message(glue::glue("Computing prefecture rail-length intersections for year {y}"))
  rail_y <- rail_snap_panel |>
    dplyr::filter(year == y)

  purrr::map_dfr(c("rail", "hsr"), function(rt) {
    segs <- rail_y |>
      dplyr::filter(rail_type == rt)

    if (nrow(segs) == 0) {
      return(tibble::tibble(pref_id = pref_proj$pref_id, year = y, rail_type = rt, rail_km = 0))
    }

    idx <- sf::st_intersects(pref_proj, segs)
    km_vals <- numeric(nrow(pref_proj))

    for (i in seq_len(nrow(pref_proj))) {
      j <- idx[[i]]
      if (length(j) == 0) next
      inter_geom <- suppressWarnings(sf::st_intersection(sf::st_geometry(segs[j, ]), sf::st_geometry(pref_proj[i, ])))
      if (length(inter_geom) == 0) next
      km_vals[i] <- sum(as.numeric(sf::st_length(inter_geom)), na.rm = TRUE) / 1000
    }

    tibble::tibble(pref_id = pref_proj$pref_id, year = y, rail_type = rt, rail_km = km_vals)
  })
})

rail_pref_metrics <- rail_pref_long |>
  tidyr::pivot_wider(names_from = rail_type, values_from = rail_km, values_fill = 0)

if (!"rail" %in% names(rail_pref_metrics)) rail_pref_metrics$rail <- 0
if (!"hsr" %in% names(rail_pref_metrics)) rail_pref_metrics$hsr <- 0

station_pref_metrics <- sf::st_join(
  station_snap |>
    dplyr::select(station_id, station_type, year, geometry),
  pref_proj |>
    dplyr::select(pref_id, geometry),
  left = FALSE
) |>
  sf::st_drop_geometry() |>
  dplyr::group_by(pref_id, year, station_type) |>
  dplyr::summarise(n_station = dplyr::n(), .groups = "drop") |>
  tidyr::pivot_wider(
    names_from = station_type,
    values_from = n_station,
    values_fill = 0,
    names_prefix = "n_station_"
  )

if (!"n_station_rail" %in% names(station_pref_metrics)) station_pref_metrics$n_station_rail <- 0
if (!"n_station_hsr" %in% names(station_pref_metrics)) station_pref_metrics$n_station_hsr <- 0

pref_centroids <- sf::st_point_on_surface(pref_proj)

station_access_list <- purrr::map_dfr(YEARS_SNAPSHOT, function(y) {
  st_y <- station_snap |>
    dplyr::filter(year == y)

  if (nrow(st_y) == 0) {
    return(tibble::tibble(
      pref_id = pref_centroids$pref_id,
      year = y,
      dist_station_km = NA_real_,
      dist_hsr_station_km = NA_real_
    ))
  }

  idx_all <- sf::st_nearest_feature(pref_centroids, st_y)
  d_all <- sf::st_distance(pref_centroids, st_y[idx_all, ], by_element = TRUE)

  st_hsr <- st_y |>
    dplyr::filter(station_type == "hsr")

  if (nrow(st_hsr) > 0) {
    idx_h <- sf::st_nearest_feature(pref_centroids, st_hsr)
    d_h <- sf::st_distance(pref_centroids, st_hsr[idx_h, ], by_element = TRUE)
    d_h <- as.numeric(units::set_units(d_h, "km"))
  } else {
    d_h <- rep(NA_real_, nrow(pref_centroids))
  }

  tibble::tibble(
    pref_id = pref_centroids$pref_id,
    year = y,
    dist_station_km = as.numeric(units::set_units(d_all, "km")),
    dist_hsr_station_km = d_h
  )
})

log_message("Step 9: Aggregating population to prefecture-year")
pop_pref <- purrr::map_dfr(YEARS_SNAPSHOT, function(y) {
  r <- pop_records[[as.character(y)]]
  if (is.null(r)) {
    return(tibble::tibble(pref_id = pref_proj$pref_id, year = y, pop_total = NA_real_))
  }

  # geodata::population returns density (persons per km2); convert to counts.
  cell_km2 <- terra::cellSize(r, unit = "km")
  pop_count <- r * cell_km2

  pref_r <- sf::st_transform(pref_proj, terra::crs(pop_count))
  vals <- exactextractr::exact_extract(pop_count, pref_r, "sum")

  tibble::tibble(pref_id = pref_r$pref_id, year = y, pop_total = vals)
})

readr::write_csv(pop_pref, "data/interim/pop_pref_snapshots.csv")

log_message("Step 10: Building analysis panel and derived growth metrics")
panel <- tidyr::crossing(pref_id = pref_proj$pref_id, year = YEARS_SNAPSHOT) |>
  dplyr::left_join(pref_proj |> sf::st_drop_geometry() |> dplyr::select(pref_id, prov_id, pref_name, prov_name, pref_area_km2), by = "pref_id") |>
  dplyr::left_join(sez_pref_metrics, by = "pref_id") |>
  dplyr::left_join(sez_active_year, by = c("pref_id", "year")) |>
  dplyr::left_join(rail_pref_metrics, by = c("pref_id", "year")) |>
  dplyr::left_join(station_pref_metrics, by = c("pref_id", "year")) |>
  dplyr::left_join(station_access_list, by = c("pref_id", "year")) |>
  dplyr::left_join(pop_pref, by = c("pref_id", "year")) |>
  dplyr::left_join(gdp_prov |> dplyr::select(prov_id, year, gdp_value, gdp_growth_pct, gdp_unit, price_basis), by = c("prov_id", "year")) |>
  dplyr::mutate(
    rail = dplyr::coalesce(rail, 0),
    hsr = dplyr::coalesce(hsr, 0),
    n_station_rail = dplyr::coalesce(n_station_rail, 0),
    n_station_hsr = dplyr::coalesce(n_station_hsr, 0),
    n_zones = dplyr::coalesce(n_zones, 0L),
    n_zone_types = dplyr::coalesce(n_zone_types, 0L),
    active_zones = dplyr::coalesce(active_zones, n_zones),
    sez_area_km2 = dplyr::coalesce(sez_area_km2, 0)
  ) |>
  dplyr::group_by(prov_id, year) |>
  dplyr::mutate(
    pop_share_in_prov = ifelse(sum(pop_total, na.rm = TRUE) > 0, pop_total / sum(pop_total, na.rm = TRUE), NA_real_),
    pref_gdp_value = gdp_value * pop_share_in_prov
  ) |>
  dplyr::ungroup() |>
  dplyr::group_by(pref_id) |>
  dplyr::arrange(year, .by_group = TRUE) |>
  dplyr::mutate(
    pref_gdp_growth_pct = 100 * (pref_gdp_value / dplyr::lag(pref_gdp_value) - 1),
    ntl_total = NA_real_
  ) |>
  dplyr::ungroup()

readr::write_csv(panel, "data/processed/analysis_panel_prefecture_year.csv")
if (requireNamespace("arrow", quietly = TRUE)) {
  arrow::write_parquet(panel, "data/processed/analysis_panel_prefecture_year.parquet")
} else {
  log_message("Package 'arrow' not installed; parquet export skipped.")
}

panel_growth <- panel |>
  dplyr::group_by(pref_id) |>
  dplyr::summarise(
    pref_name = dplyr::first(pref_name),
    prov_id = dplyr::first(prov_id),
    prov_name = dplyr::first(prov_name),
    pref_area_km2 = dplyr::first(pref_area_km2),
    pop_start = dplyr::first(pop_total[year == START_YEAR], default = NA_real_),
    pop_end = dplyr::first(pop_total[year == END_YEAR], default = NA_real_),
    pref_gdp_start = dplyr::first(pref_gdp_value[year == START_YEAR], default = NA_real_),
    pref_gdp_end = dplyr::first(pref_gdp_value[year == END_YEAR], default = NA_real_),
    rail_start = dplyr::first(rail[year == START_YEAR], default = NA_real_),
    rail_end = dplyr::first(rail[year == END_YEAR], default = NA_real_),
    hsr_start = dplyr::first(hsr[year == START_YEAR], default = NA_real_),
    hsr_end = dplyr::first(hsr[year == END_YEAR], default = NA_real_),
    n_station_hsr_start = dplyr::first(n_station_hsr[year == START_YEAR], default = NA_real_),
    n_station_hsr_end = dplyr::first(n_station_hsr[year == END_YEAR], default = NA_real_),
    dist_station_start = dplyr::first(dist_station_km[year == START_YEAR], default = NA_real_),
    dist_station_end = dplyr::first(dist_station_km[year == END_YEAR], default = NA_real_),
    dist_hsr_station_start = dplyr::first(dist_hsr_station_km[year == START_YEAR], default = NA_real_),
    dist_hsr_station_end = dplyr::first(dist_hsr_station_km[year == END_YEAR], default = NA_real_),
    active_zones = max(active_zones, na.rm = TRUE),
    n_zones = max(n_zones, na.rm = TRUE),
    n_zone_types = max(n_zone_types, na.rm = TRUE),
    sez_area_km2 = max(sez_area_km2, na.rm = TRUE),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    pop_growth_ann = agr(pop_start, pop_end, END_YEAR - START_YEAR),
    pref_gdp_growth_ann = agr(pref_gdp_start, pref_gdp_end, END_YEAR - START_YEAR),
    rail_km_change = dplyr::coalesce(rail_end, 0) - dplyr::coalesce(rail_start, 0),
    hsr_km_change = dplyr::coalesce(hsr_end, 0) - dplyr::coalesce(hsr_start, 0),
    hsr_station_gain = dplyr::coalesce(n_station_hsr_end, 0) - dplyr::coalesce(n_station_hsr_start, 0),
    station_access_improve_km = dplyr::coalesce(dist_station_start, 0) - dplyr::coalesce(dist_station_end, 0),
    hsr_access_improve_km = dplyr::coalesce(dist_hsr_station_start, 0) - dplyr::coalesce(dist_hsr_station_end, 0)
  )

prov_growth <- gdp_prov |>
  dplyr::group_by(prov_id) |>
  dplyr::summarise(
    prov_gdp_growth_mean_pct = mean(gdp_growth_pct, na.rm = TRUE),
    prov_gdp_growth_period_ann = agr(
      dplyr::first(gdp_value[year == START_YEAR], default = NA_real_),
      dplyr::first(gdp_value[year == END_YEAR], default = NA_real_),
      END_YEAR - START_YEAR
    ) * 100,
    .groups = "drop"
  )

analysis_typ <- panel_growth |>
  dplyr::left_join(prov_growth, by = "prov_id") |>
  dplyr::mutate(
    zone_intensity_raw = dplyr::coalesce(active_zones, n_zones, 0) + 0.5 * dplyr::coalesce(n_zone_types, 0) + 0.01 * dplyr::coalesce(sez_area_km2, 0),
    connectivity_gain_raw = dplyr::coalesce(hsr_km_change, 0) + 0.5 * dplyr::coalesce(rail_km_change, 0) +
      2 * dplyr::coalesce(station_access_improve_km, 0) + 1 * dplyr::coalesce(hsr_station_gain, 0),
    pop_growth_raw = pop_growth_ann,
    econ_growth_raw = pref_gdp_growth_ann,
    zone_band = quantile_band(zone_intensity_raw),
    conn_band = quantile_band(connectivity_gain_raw),
    pop_band = quantile_band(pop_growth_raw),
    econ_band = quantile_band(econ_growth_raw)
  ) |>
  dplyr::mutate(
    trajectory_type = assign_trajectory_type(zone_band, conn_band, pop_band, econ_band),
    pop_hi = pop_band == "high",
    econ_hi = econ_band == "high",
    coloc_category = dplyr::case_when(
      pop_hi & econ_hi ~ "High pop + high econ",
      pop_hi & !econ_hi ~ "High pop + lower econ",
      !pop_hi & econ_hi ~ "Lower pop + high econ",
      TRUE ~ "Lower pop + lower econ"
    ),
    zone_conn_class = paste0("zone_", zone_band, "__conn_", conn_band)
  )

readr::write_csv(analysis_typ, "data/processed/typology_prefecture.csv")

# Data quality checks (critical for an airtight descriptive design)
data_quality <- tibble::tribble(
  ~check_name, ~value, ~threshold, ~status, ~notes,
  "panel_duplicate_pref_year", sum(duplicated(panel[c("pref_id", "year")])), 0, ifelse(sum(duplicated(panel[c("pref_id", "year")])) == 0, "PASS", "FAIL"), "Must be 1 row per prefecture-year",
  "panel_pref_count", dplyr::n_distinct(panel$pref_id), dplyr::n_distinct(pref_proj$pref_id), ifelse(dplyr::n_distinct(panel$pref_id) == dplyr::n_distinct(pref_proj$pref_id), "PASS", "FAIL"), "Coverage of analysis frame",
  "panel_year_count", dplyr::n_distinct(panel$year), length(YEARS_SNAPSHOT), ifelse(dplyr::n_distinct(panel$year) == length(YEARS_SNAPSHOT), "PASS", "FAIL"), "Expected time slices present",
  "missing_rate_pop_total", mean(is.na(panel$pop_total)), 0.05, ifelse(mean(is.na(panel$pop_total)) <= 0.05, "PASS", "FAIL"), "Population missingness should be low",
  "missing_rate_gdp_value", mean(is.na(panel$gdp_value)), 0.05, ifelse(mean(is.na(panel$gdp_value)) <= 0.05, "PASS", "WARN"), "GDP missingness expected minimal for mainland frame",
  "missing_rate_pref_gdp_value", mean(is.na(panel$pref_gdp_value)), 0.05, ifelse(mean(is.na(panel$pref_gdp_value)) <= 0.05, "PASS", "WARN"), "Derived economic proxy coverage",
  "negative_rail_values", sum(panel$rail < 0, na.rm = TRUE), 0, ifelse(sum(panel$rail < 0, na.rm = TRUE) == 0, "PASS", "FAIL"), "Rail length cannot be negative",
  "negative_hsr_values", sum(panel$hsr < 0, na.rm = TRUE), 0, ifelse(sum(panel$hsr < 0, na.rm = TRUE) == 0, "PASS", "FAIL"), "HSR length cannot be negative",
  "hsr_presence_rate_2020", mean(panel$hsr[panel$year == END_YEAR] > 0, na.rm = TRUE), 0.05, ifelse(mean(panel$hsr[panel$year == END_YEAR] > 0, na.rm = TRUE) >= 0.05, "PASS", "WARN"), "Share of prefectures with non-zero HSR length in 2020",
  "missing_rate_dist_hsr_2020", mean(is.na(panel$dist_hsr_station_km[panel$year == END_YEAR])), 0.15, ifelse(mean(is.na(panel$dist_hsr_station_km[panel$year == END_YEAR])) <= 0.15, "PASS", "WARN"), "HSR accessibility coverage at end year"
)
readr::write_csv(data_quality, "outputs/tables/table_data_quality_checks.csv")

critical_fail <- data_quality |>
  dplyr::filter(check_name %in% c("panel_duplicate_pref_year", "panel_pref_count", "panel_year_count"), status == "FAIL")
if (nrow(critical_fail) > 0) {
  stop(glue::glue("Critical data-quality checks failed: {paste(critical_fail$check_name, collapse = ', ')}"))
}

# Typology robustness: alternative thresholds and weighting schemes
typology_variants <- dplyr::bind_rows(
  build_typology_variant(panel_growth, "baseline_q33_q67"),
  build_typology_variant(panel_growth, "quantile_q25_q75", ql = 0.25, qh = 0.75),
  build_typology_variant(panel_growth, "quantile_q40_q60", ql = 0.40, qh = 0.60),
  build_typology_variant(panel_growth, "conn_station_heavy", w_conn = c(1.0, 0.3, 3.0, 2.0)),
  build_typology_variant(panel_growth, "conn_length_heavy", w_conn = c(1.5, 1.0, 1.0, 0.5))
) |>
  dplyr::left_join(
    panel_growth |>
      dplyr::select(pref_id, pref_name, prov_name),
    by = "pref_id"
  )

readr::write_csv(typology_variants, "outputs/tables/table_typology_variants.csv")

baseline_type <- typology_variants |>
  dplyr::filter(variant == "baseline_q33_q67") |>
  dplyr::select(pref_id, baseline_trajectory = trajectory_type_v)

typology_stability <- typology_variants |>
  dplyr::left_join(baseline_type, by = "pref_id") |>
  dplyr::group_by(variant) |>
  dplyr::summarise(
    n_prefectures = dplyr::n(),
    share_same_as_baseline = mean(trajectory_type_v == baseline_trajectory, na.rm = TRUE),
    ari_vs_baseline = adjusted_rand_index(baseline_trajectory, trajectory_type_v),
    n_classes = dplyr::n_distinct(trajectory_type_v),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(share_same_as_baseline))

readr::write_csv(typology_stability, "outputs/tables/table_typology_stability.csv")

concentration_tbl <- panel |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    pop_hhi = hh_index(pop_total),
    econ_hhi = hh_index(pref_gdp_value),
    .groups = "drop"
  )

readr::write_csv(concentration_tbl, "outputs/tables/table_concentration_trends.csv")

typology_summary <- analysis_typ |>
  dplyr::group_by(trajectory_type) |>
  dplyr::summarise(
    n_prefectures = dplyr::n(),
    mean_pop_growth_ann_pct = mean(pop_growth_ann, na.rm = TRUE) * 100,
    mean_econ_growth_ann_pct = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
    mean_connectivity_gain = mean(connectivity_gain_raw, na.rm = TRUE),
    mean_zone_intensity = mean(zone_intensity_raw, na.rm = TRUE),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(n_prefectures))

readr::write_csv(typology_summary, "outputs/tables/table_typology_summary.csv")

# Key answer summary table (direct response to research question framing)
conc_start <- concentration_tbl |> dplyr::filter(year == START_YEAR)
conc_end <- concentration_tbl |> dplyr::filter(year == END_YEAR)
pop_hhi_change_pct <- ifelse(nrow(conc_start) == 1 && nrow(conc_end) == 1, 100 * (conc_end$pop_hhi / conc_start$pop_hhi - 1), NA_real_)
econ_hhi_change_pct <- ifelse(nrow(conc_start) == 1 && nrow(conc_end) == 1, 100 * (conc_end$econ_hhi / conc_start$econ_hhi - 1), NA_real_)

hsr_gap_df <- analysis_typ |>
  dplyr::mutate(
    hsr_band = dplyr::case_when(
      dist_hsr_station_end <= 10 ~ "near",
      dist_hsr_station_end > 50 ~ "far",
      TRUE ~ "middle"
    ),
    pop_growth_ann_pct = pop_growth_ann * 100,
    econ_growth_ann_pct = pref_gdp_growth_ann * 100
  )

hsr_pop_inf <- mean_diff_inference(
  hsr_gap_df,
  outcome_col = "pop_growth_ann_pct",
  group_col = "hsr_band",
  hi_value = "near",
  lo_value = "far",
  n_boot = 2000L,
  n_perm = 4000L,
  seed = 20260301L
)

hsr_econ_inf <- mean_diff_inference(
  hsr_gap_df,
  outcome_col = "econ_growth_ann_pct",
  group_col = "hsr_band",
  hi_value = "near",
  lo_value = "far",
  n_boot = 2000L,
  n_perm = 4000L,
  seed = 20260302L
)

hsr_pop_gap <- hsr_pop_inf$estimate[1]
hsr_econ_gap <- hsr_econ_inf$estimate[1]

hsr_gap_inference_tbl <- dplyr::bind_rows(
  hsr_pop_inf |>
    dplyr::mutate(
      outcome = "Population growth gap (pp/year)",
      near_cutoff_km = 10,
      far_cutoff_rule = ">50 km"
    ),
  hsr_econ_inf |>
    dplyr::mutate(
      outcome = "Economic growth gap (pp/year)",
      near_cutoff_km = 10,
      far_cutoff_rule = ">50 km"
    )
) |>
  dplyr::select(outcome, near_cutoff_km, far_cutoff_rule, estimate, ci_low, ci_high, p_perm, n_hi, n_lo)

readr::write_csv(hsr_gap_inference_tbl, "outputs/tables/table_hsr_gap_inference.csv")

hsr_threshold_grid <- c(5, 10, 15, 20, 25, 30, 40)
hsr_threshold_sensitivity <- purrr::map_dfr(hsr_threshold_grid, function(thr) {
  gap_df <- analysis_typ |>
    dplyr::mutate(
      hsr_band = dplyr::case_when(
        dist_hsr_station_end <= thr ~ "near",
        dist_hsr_station_end > 50 ~ "far",
        TRUE ~ "middle"
      ),
      pop_growth_ann_pct = pop_growth_ann * 100,
      econ_growth_ann_pct = pref_gdp_growth_ann * 100
    )

  pop_inf <- mean_diff_inference(
    gap_df,
    outcome_col = "pop_growth_ann_pct",
    group_col = "hsr_band",
    hi_value = "near",
    lo_value = "far",
    n_boot = 1200L,
    n_perm = 2000L,
    seed = 20261000L + thr
  ) |>
    dplyr::mutate(outcome = "Population growth")

  econ_inf <- mean_diff_inference(
    gap_df,
    outcome_col = "econ_growth_ann_pct",
    group_col = "hsr_band",
    hi_value = "near",
    lo_value = "far",
    n_boot = 1200L,
    n_perm = 2000L,
    seed = 20262000L + thr
  ) |>
    dplyr::mutate(outcome = "Economic growth")

  dplyr::bind_rows(pop_inf, econ_inf) |>
    dplyr::mutate(
      near_cutoff_km = thr,
      far_cutoff_rule = ">50 km"
    )
})

readr::write_csv(hsr_threshold_sensitivity, "outputs/tables/table_hsr_threshold_sensitivity.csv")

reg_df <- analysis_typ |>
  dplyr::mutate(
    pop_growth_pct = pop_growth_ann * 100,
    econ_growth_pct = pref_gdp_growth_ann * 100,
    z_hsr_km_change = as.numeric(scale(hsr_km_change)),
    z_rail_km_change = as.numeric(scale(rail_km_change)),
    z_hsr_station_gain = as.numeric(scale(hsr_station_gain)),
    z_station_access_improve_km = as.numeric(scale(station_access_improve_km)),
    z_zone_intensity = as.numeric(scale(zone_intensity_raw)),
    z_log_pop_start = as.numeric(scale(log1p(pop_start))),
    z_log_gdp_start = as.numeric(scale(log1p(pref_gdp_start))),
    z_dist_hsr_start = as.numeric(scale(dist_hsr_station_start)),
    z_prov_gdp_growth = as.numeric(scale(prov_gdp_growth_period_ann))
  ) |>
  dplyr::filter(
    is.finite(pop_growth_pct),
    is.finite(econ_growth_pct),
    is.finite(z_hsr_km_change),
    is.finite(z_rail_km_change),
    is.finite(z_hsr_station_gain),
    is.finite(z_station_access_improve_km),
    is.finite(z_zone_intensity),
    is.finite(z_log_pop_start),
    is.finite(z_log_gdp_start),
    is.finite(z_dist_hsr_start),
    is.finite(z_prov_gdp_growth),
    !is.na(prov_id)
  )

driver_terms_pop <- c(
  "z_hsr_km_change",
  "z_rail_km_change",
  "z_hsr_station_gain",
  "z_station_access_improve_km",
  "z_zone_intensity",
  "z_log_pop_start",
  "z_log_gdp_start",
  "z_dist_hsr_start"
)
driver_terms_econ <- c(driver_terms_pop, "z_prov_gdp_growth")

driver_formula_pop <- stats::as.formula(
  "pop_growth_pct ~ z_hsr_km_change + z_rail_km_change + z_hsr_station_gain + z_station_access_improve_km + z_zone_intensity + z_log_pop_start + z_log_gdp_start + z_dist_hsr_start + factor(prov_id)"
)
driver_formula_econ <- stats::as.formula(
  "econ_growth_pct ~ z_hsr_km_change + z_rail_km_change + z_hsr_station_gain + z_station_access_improve_km + z_zone_intensity + z_log_pop_start + z_log_gdp_start + z_dist_hsr_start + z_prov_gdp_growth"
)

driver_model_pop <- stats::lm(driver_formula_pop, data = reg_df)
driver_model_econ <- stats::lm(driver_formula_econ, data = reg_df)

driver_regression <- dplyr::bind_rows(
  bootstrap_lm_terms(reg_df, driver_formula_pop, driver_terms_pop, n_boot = 1400L, seed = 20260303L) |>
    dplyr::mutate(outcome = "Population growth (%/year)"),
  bootstrap_lm_terms(reg_df, driver_formula_econ, driver_terms_econ, n_boot = 1400L, seed = 20260304L) |>
    dplyr::mutate(outcome = "Economic growth (%/year)")
) |>
  dplyr::mutate(
    term_label = dplyr::recode(
      term,
      z_hsr_km_change = "HSR km change (z)",
      z_rail_km_change = "Rail km change (z)",
      z_hsr_station_gain = "HSR station gain (z)",
      z_station_access_improve_km = "Station access improvement (z)",
      z_zone_intensity = "Zone intensity (z)",
      z_log_pop_start = "Baseline population level (z)",
      z_log_gdp_start = "Baseline GDP proxy level (z)",
      z_dist_hsr_start = "Baseline distance to HSR (z)",
      z_prov_gdp_growth = "Provincial GDP growth baseline (z)"
    )
  )

readr::write_csv(driver_regression, "outputs/tables/table_driver_regression_bootstrap.csv")

driver_model_diagnostics <- tibble::tibble(
  outcome = c("Population growth (%/year)", "Economic growth (%/year)"),
  n_obs = c(stats::nobs(driver_model_pop), stats::nobs(driver_model_econ)),
  r_squared = c(summary(driver_model_pop)$r.squared, summary(driver_model_econ)$r.squared),
  adj_r_squared = c(summary(driver_model_pop)$adj.r.squared, summary(driver_model_econ)$adj.r.squared),
  rmse = c(sqrt(mean(stats::residuals(driver_model_pop)^2, na.rm = TRUE)), sqrt(mean(stats::residuals(driver_model_econ)^2, na.rm = TRUE)))
)

readr::write_csv(driver_model_diagnostics, "outputs/tables/table_driver_model_diagnostics.csv")

typ_share <- typology_summary |>
  dplyr::mutate(share_pct = 100 * n_prefectures / sum(n_prefectures))

synergy_share <- typ_share |> dplyr::filter(trajectory_type == "Zone-Rail Synergy Growth Pole") |> dplyr::pull(share_pct)
corridor_share <- typ_share |> dplyr::filter(trajectory_type == "Rail-Led Corridor Urbanization") |> dplyr::pull(share_pct)
mixed_share <- typ_share |> dplyr::filter(trajectory_type == "Mixed Transitional Pattern") |> dplyr::pull(share_pct)

stability_nonbaseline <- typology_stability |> dplyr::filter(variant != "baseline_q33_q67")
min_stability <- ifelse(nrow(stability_nonbaseline) > 0, min(stability_nonbaseline$share_same_as_baseline, na.rm = TRUE), NA_real_)
min_ari <- ifelse(nrow(stability_nonbaseline) > 0, min(stability_nonbaseline$ari_vs_baseline, na.rm = TRUE), NA_real_)

pop_hsr_coef <- driver_regression |>
  dplyr::filter(outcome == "Population growth (%/year)", term == "z_hsr_km_change")
econ_hsr_coef <- driver_regression |>
  dplyr::filter(outcome == "Economic growth (%/year)", term == "z_hsr_km_change")

key_answer_summary <- tibble::tribble(
  ~finding, ~estimate, ~ci_low, ~ci_high, ~p_value, ~interpretation,
  "Population concentration change (HHI, %)", pop_hhi_change_pct, NA_real_, NA_real_, NA_real_, "Positive implies stronger spatial concentration",
  "Economic concentration change (HHI, %)", econ_hhi_change_pct, NA_real_, NA_real_, NA_real_, "Small change implies broad dispersion in proxy economic activity",
  "HSR-near minus HSR-far population growth gap (pp/year, near<=10km vs far>50km)", hsr_pop_gap, hsr_pop_inf$ci_low[1], hsr_pop_inf$ci_high[1], hsr_pop_inf$p_perm[1], "Bootstrap CI + permutation p-value quantify uncertainty of the corridor gradient",
  "HSR-near minus HSR-far economic growth gap (pp/year, near<=10km vs far>50km)", hsr_econ_gap, hsr_econ_inf$ci_low[1], hsr_econ_inf$ci_high[1], hsr_econ_inf$p_perm[1], "Bootstrap CI + permutation p-value quantify uncertainty of the corridor gradient",
  "Driver model coef: HSR km change (z) -> population growth (%/year)", pop_hsr_coef$estimate[1] %||% NA_real_, pop_hsr_coef$ci_low[1] %||% NA_real_, pop_hsr_coef$ci_high[1] %||% NA_real_, pop_hsr_coef$p_value_ols[1] %||% NA_real_, "Province-FE model with baseline controls; coefficient interpreted as association, not causality",
  "Driver model coef: HSR km change (z) -> economic growth (%/year)", econ_hsr_coef$estimate[1] %||% NA_real_, econ_hsr_coef$ci_low[1] %||% NA_real_, econ_hsr_coef$ci_high[1] %||% NA_real_, econ_hsr_coef$p_value_ols[1] %||% NA_real_, "Adjusted model with baseline controls + provincial GDP growth control; coefficient interpreted as association, not causality",
  "Share: Mixed Transitional Pattern (%)", mixed_share %||% NA_real_, NA_real_, NA_real_, NA_real_, "Large share indicates heterogeneous, non-single-regime transformation",
  "Share: Rail-Led Corridor Urbanization (%)", corridor_share %||% NA_real_, NA_real_, NA_real_, NA_real_, "Captures corridor-led trajectory prevalence",
  "Share: Zone-Rail Synergy Growth Pole (%)", synergy_share %||% NA_real_, NA_real_, NA_real_, NA_real_, "Captures strongest co-location regime prevalence",
  "Minimum typology stability vs baseline (share identical)", min_stability, NA_real_, NA_real_, NA_real_, "Higher values indicate specification-robust typology assignments",
  "Minimum ARI vs baseline (robustness variants)", min_ari, NA_real_, NA_real_, NA_real_, "Higher ARI indicates robust clustering of prefecture trajectories"
)

readr::write_csv(key_answer_summary, "outputs/tables/table_key_answer_summary.csv")

log_message("Step 11: Generating maps and figures")
ggplot2::theme_set(ggplot2::theme_minimal(base_size = 11))

pref_map <- pref_proj |>
  dplyr::left_join(analysis_typ, by = "pref_id")

# Map 1: Rail/HSR expansion by snapshots.
map1_years <- sort(unique(rail_snap$year))
map1_layers <- purrr::map_dfr(seq_along(map1_years), function(i) {
  y <- map1_years[i]
  cur <- rail_snap |>
    dplyr::filter(year == y)

  cur_rail <- cur |>
    dplyr::filter(rail_type == "rail")

  if (nrow(cur_rail) > 0) {
    is_added_after_2000 <- !is.na(cur_rail$start_year) & cur_rail$start_year > START_YEAR & cur_rail$start_year <= y
    cur_rail <- cur_rail |>
      dplyr::mutate(
        map_layer = ifelse(is_added_after_2000, "Rail (added after 2000)", "Rail (baseline <=2000/unknown)")
      )
  }

  cur_hsr <- cur |>
    dplyr::filter(rail_type == "hsr") |>
    dplyr::mutate(map_layer = "HSR")

  dplyr::bind_rows(cur_rail, cur_hsr)
})

map1_layers$seg_km <- as.numeric(sf::st_length(map1_layers)) / 1000
map1_layers <- map1_layers |>
  dplyr::filter(map_layer == "HSR" | is.na(seg_km) | seg_km >= 0.1)

p_map1 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = sf::st_transform(provinces, CRS_PROJ), fill = "grey98", color = "grey75", linewidth = 0.2) +
  ggplot2::geom_sf(
    data = map1_layers |>
      dplyr::filter(map_layer == "Rail (baseline <=2000/unknown)"),
    ggplot2::aes(color = map_layer),
    linewidth = 0.06,
    alpha = 0.30,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map1_layers |>
      dplyr::filter(map_layer == "Rail (added after 2000)"),
    ggplot2::aes(color = map_layer),
    linewidth = 0.14,
    alpha = 0.90,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map1_layers |>
      dplyr::filter(map_layer == "HSR"),
    ggplot2::aes(color = map_layer),
    linewidth = 0.16,
    alpha = 0.9,
    lineend = "round"
  ) +
  ggplot2::facet_wrap(~year, ncol = if (length(map1_years) > 6) 4 else 3) +
  ggplot2::scale_color_manual(
    values = c(
      "Rail (baseline <=2000/unknown)" = "#9ecae1",
      "Rail (added after 2000)" = "#08519c",
      "HSR" = "#e31a1c"
    ),
    drop = FALSE
  ) +
  ggplot2::labs(
    title = "Rail/HSR Expansion Time Slices",
    subtitle = glue::glue(
      "Cumulative snapshots: {paste(map1_years, collapse = ', ')} | ",
      "Blue dark = rail added after 2000 (from StartYear); light blue = baseline rail (<=2000/unknown); red = HSR | tiny rail fragments <0.1 km hidden for readability"
    ),
    color = "Layer"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map1, "map_01_rail_hsr_timeslices.png", width = 13, height = 9)

# Map 2: Development zones.
p_map2 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = sf::st_transform(provinces, CRS_PROJ), fill = "grey98", color = "grey75", linewidth = 0.2) +
  ggplot2::geom_sf(data = rail_proj, color = "grey70", linewidth = 0.08, alpha = 0.5) +
  ggplot2::geom_sf(data = if (exists("sez_proj")) sez_proj else sf::st_transform(sez_sf, CRS_PROJ), ggplot2::aes(color = zone_type), size = 1.0, alpha = 0.85) +
  ggplot2::scale_color_brewer(palette = "Set1", na.translate = FALSE) +
  ggplot2::labs(
    title = "Development Zone Geography",
    subtitle = "Wikidata-derived zone points over current rail network",
    color = "Zone type"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map2, "map_02_development_zones.png", width = 13, height = 9)

# Map 3: Population growth.
p_map3 <- ggplot2::ggplot(pref_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = pop_growth_ann * 100), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "C", na.value = "grey90", labels = scales::label_number(suffix = "%", accuracy = 0.1)) +
  ggplot2::labs(
    title = "Population Growth (Annualized)",
    subtitle = glue::glue("{START_YEAR}-{END_YEAR} | Prefecture-level (GADM L2)"),
    fill = "Pop growth"
  )
write_fig(p_map3, "map_03_population_growth.png", width = 13, height = 9)

# Map 4: Economic activity growth proxy.
p_map4 <- ggplot2::ggplot(pref_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = pref_gdp_growth_ann * 100), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "D", na.value = "grey90", labels = scales::label_number(suffix = "%", accuracy = 0.1)) +
  ggplot2::labs(
    title = "Economic Activity Growth Proxy (Population-Allocated Provincial GDP)",
    subtitle = glue::glue("{START_YEAR}-{END_YEAR} annualized"),
    fill = "Econ growth"
  )
write_fig(p_map4, "map_04_economic_growth_proxy.png", width = 13, height = 9)

# Map 5: Co-location/divergence.
p_map5 <- ggplot2::ggplot(pref_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = coloc_category), color = NA) +
  ggplot2::scale_fill_brewer(palette = "Set2", na.value = "grey90") +
  ggplot2::labs(
    title = "Population-Economic Co-location vs Divergence",
    fill = "Category"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map5, "map_05_population_econ_colocation.png", width = 13, height = 9)

# Map 6: Zone intensity + connectivity gain classes.
p_map6 <- ggplot2::ggplot(pref_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = zone_conn_class), color = NA) +
  ggplot2::scale_fill_brewer(palette = "Spectral", na.value = "grey90") +
  ggplot2::labs(
    title = "Zone Intensity and Connectivity Gain Overlay",
    fill = "Zone-Conn class"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map6, "map_06_zone_connectivity_overlay.png", width = 13, height = 9)

# Map 7: Typology map.
p_map7 <- ggplot2::ggplot(pref_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = trajectory_type), color = NA) +
  ggplot2::scale_fill_brewer(palette = "Paired", na.value = "grey90") +
  ggplot2::labs(
    title = "Trajectory Typology of Spatial Transformation",
    subtitle = glue::glue("{START_YEAR}-{END_YEAR}, descriptive typology"),
    fill = "Trajectory"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map7, "map_07_typology_prefecture.png", width = 13, height = 9)

# Figure 8: Concentration trends.
conc_long <- concentration_tbl |>
  tidyr::pivot_longer(cols = c(pop_hhi, econ_hhi), names_to = "series", values_to = "hhi") |>
  dplyr::mutate(series = dplyr::recode(series, pop_hhi = "Population HHI", econ_hhi = "Economic Activity HHI (proxy)"))

p_fig8 <- ggplot2::ggplot(conc_long, ggplot2::aes(x = year, y = hhi, color = series)) +
  ggplot2::geom_line(linewidth = 1.1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::scale_x_continuous(breaks = YEARS_SNAPSHOT) +
  ggplot2::labs(
    title = "National Concentration Over Time",
    x = "Year",
    y = "HHI",
    color = "Series"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig8, "fig_08_concentration_trends.png", width = 11, height = 6)

# Figure 9: Boxplots by typology.
box_df <- analysis_typ |>
  dplyr::select(trajectory_type, pop_growth_ann, pref_gdp_growth_ann, connectivity_gain_raw, zone_intensity_raw) |>
  tidyr::pivot_longer(
    cols = c(pop_growth_ann, pref_gdp_growth_ann, connectivity_gain_raw, zone_intensity_raw),
    names_to = "metric",
    values_to = "value"
  ) |>
  dplyr::filter(is.finite(value)) |>
  dplyr::mutate(
    metric = dplyr::recode(
      metric,
      pop_growth_ann = "Population growth (ann)",
      pref_gdp_growth_ann = "Economic growth proxy (ann)",
      connectivity_gain_raw = "Connectivity gain",
      zone_intensity_raw = "Zone intensity"
    )
  )

p_fig9 <- ggplot2::ggplot(box_df, ggplot2::aes(x = trajectory_type, y = value, fill = trajectory_type)) +
  ggplot2::geom_boxplot(outlier.alpha = 0.2, linewidth = 0.2) +
  ggplot2::facet_wrap(~metric, scales = "free_y") +
  ggplot2::labs(title = "Comparative Distributions by Typology", x = "Typology", y = "Value") +
  ggplot2::theme(
    axis.text.x = ggplot2::element_text(angle = 40, hjust = 1),
    legend.position = "none"
  )
write_fig(p_fig9, "fig_09_boxplots_by_typology.png", width = 14, height = 9)

# Figure 10: Corridor analysis by HSR access distance.
corridor_df <- analysis_typ |>
  dplyr::mutate(
    dist_hsr_end = dist_hsr_station_end,
    corridor_band = dplyr::case_when(
      is.na(dist_hsr_end) ~ "No HSR station found",
      dist_hsr_end <= 10 ~ "0-10 km",
      dist_hsr_end <= 25 ~ "10-25 km",
      dist_hsr_end <= 50 ~ "25-50 km",
      TRUE ~ ">50 km"
    )
  )

corridor_summary <- corridor_df |>
  dplyr::group_by(corridor_band) |>
  dplyr::summarise(
    n_prefectures = dplyr::n(),
    mean_pop_growth_ann_pct = mean(pop_growth_ann, na.rm = TRUE) * 100,
    mean_econ_growth_ann_pct = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
    .groups = "drop"
  )

readr::write_csv(corridor_summary, "outputs/tables/table_corridor_summary.csv")

# Corridor robustness table: compare HSR-distance, station-distance, and rail-density partitions.
corridor_robustness <- dplyr::bind_rows(
  analysis_typ |>
    dplyr::mutate(
      band = dplyr::case_when(
        dist_hsr_station_end <= 10 ~ "0-10 km",
        dist_hsr_station_end <= 25 ~ "10-25 km",
        dist_hsr_station_end <= 50 ~ "25-50 km",
        TRUE ~ ">50 km"
      ),
      scheme = "hsr_distance"
    ) |>
    dplyr::group_by(scheme, band) |>
    dplyr::summarise(
      n_prefectures = dplyr::n(),
      mean_pop_growth_ann_pct = mean(pop_growth_ann, na.rm = TRUE) * 100,
      mean_econ_growth_ann_pct = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
      .groups = "drop"
    ),
  analysis_typ |>
    dplyr::mutate(
      band = dplyr::case_when(
        dist_station_end <= 10 ~ "0-10 km",
        dist_station_end <= 25 ~ "10-25 km",
        dist_station_end <= 50 ~ "25-50 km",
        TRUE ~ ">50 km"
      ),
      scheme = "rail_station_distance"
    ) |>
    dplyr::group_by(scheme, band) |>
    dplyr::summarise(
      n_prefectures = dplyr::n(),
      mean_pop_growth_ann_pct = mean(pop_growth_ann, na.rm = TRUE) * 100,
      mean_econ_growth_ann_pct = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
      .groups = "drop"
    ),
  analysis_typ |>
    dplyr::mutate(
      rail_density_proxy = (dplyr::coalesce(rail_end, 0) + dplyr::coalesce(hsr_end, 0)) / dplyr::coalesce(pref_area_km2, NA_real_),
      band = paste0("Q", dplyr::ntile(rail_density_proxy, 4)),
      scheme = "rail_density_quartile"
    ) |>
    dplyr::filter(is.finite(rail_density_proxy), !is.na(band)) |>
    dplyr::group_by(scheme, band) |>
    dplyr::summarise(
      n_prefectures = dplyr::n(),
      mean_pop_growth_ann_pct = mean(pop_growth_ann, na.rm = TRUE) * 100,
      mean_econ_growth_ann_pct = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
      .groups = "drop"
    )
)

readr::write_csv(corridor_robustness, "outputs/tables/table_corridor_robustness.csv")

p_fig10 <- ggplot2::ggplot(
  corridor_df |> dplyr::filter(is.finite(pop_growth_ann)),
  ggplot2::aes(x = corridor_band, y = pop_growth_ann * 100, fill = corridor_band)
) +
  ggplot2::geom_boxplot(outlier.alpha = 0.2, linewidth = 0.2) +
  ggplot2::geom_point(
    data = corridor_summary,
    ggplot2::aes(x = corridor_band, y = mean_pop_growth_ann_pct),
    inherit.aes = FALSE,
    color = "black",
    size = 2
  ) +
  ggplot2::labs(
    title = "Corridor Analysis: Population Growth by Distance to HSR",
    x = "Distance band to nearest HSR station (2020)",
    y = "Population annualized growth (%)"
  ) +
  ggplot2::theme(legend.position = "none")
write_fig(p_fig10, "fig_10_corridor_population_growth.png", width = 11, height = 6)

# Optional extra visual: station growth map.
station_change_map <- pref_map |>
  dplyr::mutate(hsr_station_gain = dplyr::coalesce(hsr_station_gain, 0))

p_map_extra <- ggplot2::ggplot(station_change_map) +
  ggplot2::geom_sf(ggplot2::aes(fill = hsr_station_gain), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "A", na.value = "grey90") +
  ggplot2::labs(
    title = "HSR Station Count Gain (2000-2020)",
    fill = "Station gain"
  )
write_fig(p_map_extra, "map_08_hsr_station_gain.png", width = 13, height = 9)

# Build year-end and year-start snapshots for additional orthogonal visuals.
panel_start <- panel |>
  dplyr::filter(year == START_YEAR) |>
  dplyr::group_by(pref_id) |>
  dplyr::summarise(
    pop_start = dplyr::first(pop_total),
    rail_start = dplyr::first(rail),
    hsr_start = dplyr::first(hsr),
    dist_station_start = dplyr::first(dist_station_km),
    dist_hsr_station_start = dplyr::first(dist_hsr_station_km),
    active_zones_start = dplyr::first(active_zones),
    n_station_hsr_start = dplyr::first(n_station_hsr),
    .groups = "drop"
  )

panel_end <- panel |>
  dplyr::filter(year == END_YEAR) |>
  dplyr::group_by(pref_id) |>
  dplyr::summarise(
    pop_end = dplyr::first(pop_total),
    rail_end = dplyr::first(rail),
    hsr_end = dplyr::first(hsr),
    dist_station_end = dplyr::first(dist_station_km),
    dist_hsr_station_end = dplyr::first(dist_hsr_station_km),
    active_zones_end = dplyr::first(active_zones),
    n_station_hsr_end = dplyr::first(n_station_hsr),
    pref_gdp_end = dplyr::first(pref_gdp_value),
    .groups = "drop"
  )

pref_map_extra <- pref_proj |>
  dplyr::left_join(panel_start, by = "pref_id") |>
  dplyr::left_join(panel_end, by = "pref_id") |>
  dplyr::left_join(
    analysis_typ |>
      dplyr::select(
        pref_id, pop_growth_ann, pref_gdp_growth_ann, connectivity_gain_raw,
        zone_intensity_raw, trajectory_type, zone_band, conn_band,
      pop_band, econ_band, hsr_station_gain
      ),
    by = "pref_id"
  ) |>
  dplyr::mutate(
    rail_density_end = rail_end / pref_area_km2,
    hsr_density_end = hsr_end / pref_area_km2,
    pop_density_end = pop_end / pref_area_km2,
    z_pop_growth = as.numeric(scale(pop_growth_ann)),
    z_econ_growth = as.numeric(scale(pref_gdp_growth_ann)),
    z_conn_gain = as.numeric(scale(connectivity_gain_raw)),
    z_zone_intensity = as.numeric(scale(zone_intensity_raw)),
    synergy_hotspot_score = (z_pop_growth + z_econ_growth + z_conn_gain + z_zone_intensity) / 4
  )

# -------------------------------
# Additional maps (09 to 15)
# -------------------------------

# Map 9: Population level in 2020 (log scale).
p_map9 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = log1p(pop_end)), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "B", na.value = "grey90") +
  ggplot2::labs(
    title = "Population Level in 2020",
    subtitle = "log(1 + population) by prefecture",
    fill = "log(1+pop)"
  )
write_fig(p_map9, "map_09_population_level_2020.png", width = 13, height = 9)

# Map 10: Rail intensity in 2020.
p_map10 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = rail_density_end), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "C", na.value = "grey90") +
  ggplot2::labs(
    title = "Rail Density in 2020",
    subtitle = "Rail km per km² of prefecture area",
    fill = "Rail density"
  )
write_fig(p_map10, "map_10_rail_density_2020.png", width = 13, height = 9)

# Map 11: HSR intensity in 2020.
p_map11 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = hsr_density_end), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "D", na.value = "grey90") +
  ggplot2::labs(
    title = "HSR Density in 2020",
    subtitle = "HSR km per km² of prefecture area",
    fill = "HSR density"
  )
write_fig(p_map11, "map_11_hsr_density_2020.png", width = 13, height = 9)

# Map 12: Distance to nearest rail station in 2020.
p_map12 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = dist_station_end), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "E", na.value = "grey90") +
  ggplot2::labs(
    title = "Distance to Nearest Rail Station (2020)",
    fill = "Distance (km)"
  )
write_fig(p_map12, "map_12_dist_to_station_2020.png", width = 13, height = 9)

# Map 13: Distance to nearest HSR station in 2020.
p_map13 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = dist_hsr_station_end), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "F", na.value = "grey90") +
  ggplot2::labs(
    title = "Distance to Nearest HSR Station (2020)",
    fill = "Distance (km)"
  )
write_fig(p_map13, "map_13_dist_to_hsr_station_2020.png", width = 13, height = 9)

# Map 14: Active development zones by 2020.
p_map14 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = active_zones_end), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "A", na.value = "grey90") +
  ggplot2::labs(
    title = "Active Development Zones by 2020",
    fill = "Active zones"
  )
write_fig(p_map14, "map_14_active_zones_2020.png", width = 13, height = 9)

# Map 15: Connectivity gain composite.
p_map15 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = connectivity_gain_raw), color = NA) +
  ggplot2::scale_fill_viridis_c(option = "G", na.value = "grey90") +
  ggplot2::labs(
    title = "Connectivity Gain Composite (2000-2020)",
    fill = "Connectivity gain"
  )
write_fig(p_map15, "map_15_connectivity_gain.png", width = 13, height = 9)

# Map 16: Full infrastructure buildout in 2020.
map16_rail <- rail_snap |>
  dplyr::filter(year == END_YEAR) |>
  dplyr::mutate(
    infra_layer = dplyr::case_when(
      rail_type == "hsr" ~ "HSR lines (2020)",
      !is.na(start_year) & start_year > START_YEAR & start_year <= END_YEAR ~ "Rail added 2001-2020",
      TRUE ~ "Rail baseline <=2000/unknown"
    )
  )
map16_rail$seg_km <- as.numeric(sf::st_length(map16_rail)) / 1000
map16_rail <- map16_rail |>
  dplyr::filter(infra_layer != "Rail baseline <=2000/unknown" | is.na(seg_km) | seg_km >= 0.1)

map16_station <- stations_sf |>
  dplyr::mutate(infra_layer = ifelse(station_type == "hsr", "HSR stations (2020)", "Rail stations (2020)"))

map16_zone <- (if (exists("sez_proj")) sez_proj else sf::st_transform(sez_sf, CRS_PROJ)) |>
  dplyr::mutate(infra_layer = paste0("Zone: ", dplyr::coalesce(zone_type, "Development Zone")))

map16_roads <- roads_mm |>
  dplyr::mutate(infra_layer = "Expressways/National roads")

map16_airports <- airports_mm |>
  dplyr::mutate(infra_layer = "Airports/Airfields")

map16_ports <- ports_hubs_mm |>
  dplyr::mutate(infra_layer = "Ports/Logistics hubs")

map16_counts <- list(
  roads = nrow(map16_roads),
  rail_baseline = sum(map16_rail$infra_layer == "Rail baseline <=2000/unknown", na.rm = TRUE),
  rail_added = sum(map16_rail$infra_layer == "Rail added 2001-2020", na.rm = TRUE),
  hsr_lines = sum(map16_rail$infra_layer == "HSR lines (2020)", na.rm = TRUE),
  rail_stations = sum(map16_station$infra_layer == "Rail stations (2020)", na.rm = TRUE),
  hsr_stations = sum(map16_station$infra_layer == "HSR stations (2020)", na.rm = TRUE),
  airports = nrow(map16_airports),
  ports_hubs = nrow(map16_ports),
  zones = nrow(map16_zone)
)

p_map16 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = sf::st_transform(provinces, CRS_PROJ), fill = "grey98", color = "grey70", linewidth = 0.2) +
  ggplot2::geom_sf(data = sf::st_transform(prefectures, CRS_PROJ), fill = NA, color = "grey90", linewidth = 0.05, alpha = 0.35) +
  ggplot2::geom_sf(
    data = map16_roads,
    ggplot2::aes(color = infra_layer),
    linewidth = 0.08,
    alpha = 0.35,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map16_rail |> dplyr::filter(infra_layer == "Rail baseline <=2000/unknown"),
    ggplot2::aes(color = infra_layer),
    linewidth = 0.05,
    alpha = 0.22,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map16_rail |> dplyr::filter(infra_layer == "Rail added 2001-2020"),
    ggplot2::aes(color = infra_layer),
    linewidth = 0.14,
    alpha = 0.9,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map16_rail |> dplyr::filter(infra_layer == "HSR lines (2020)"),
    ggplot2::aes(color = infra_layer),
    linewidth = 0.16,
    alpha = 0.9,
    lineend = "round"
  ) +
  ggplot2::geom_sf(
    data = map16_station |> dplyr::filter(infra_layer == "Rail stations (2020)"),
    ggplot2::aes(color = infra_layer),
    size = 0.10,
    alpha = 0.30
  ) +
  ggplot2::geom_sf(
    data = map16_station |> dplyr::filter(infra_layer == "HSR stations (2020)"),
    ggplot2::aes(color = infra_layer),
    size = 0.22,
    alpha = 0.70
  ) +
  ggplot2::geom_sf(
    data = map16_airports,
    ggplot2::aes(color = infra_layer),
    shape = 24,
    size = 1.0,
    alpha = 0.85,
    stroke = 0.5
  ) +
  ggplot2::geom_sf(
    data = map16_ports,
    ggplot2::aes(color = infra_layer),
    shape = 23,
    size = 0.9,
    alpha = 0.75,
    stroke = 0.45
  ) +
  ggplot2::geom_sf(
    data = map16_zone,
    ggplot2::aes(color = infra_layer),
    shape = 4,
    size = 1.1,
    alpha = 0.85,
    stroke = 0.65
  ) +
  ggplot2::scale_color_manual(
    values = c(
      "Expressways/National roads" = "#636363",
      "Rail baseline <=2000/unknown" = "#9ecae1",
      "Rail added 2001-2020" = "#08519c",
      "HSR lines (2020)" = "#e31a1c",
      "Rail stations (2020)" = "#fdae6b",
      "HSR stations (2020)" = "#ff7f00",
      "Airports/Airfields" = "#1f78b4",
      "Ports/Logistics hubs" = "#17becf",
      "Zone: SEZ" = "#6a3d9a",
      "Zone: ETDZ" = "#1b9e77",
      "Zone: HIDZ" = "#33a02c",
      "Zone: FTZ/Bonded" = "#a6d854",
      "Zone: Development Zone" = "#b15928"
    ),
    breaks = c(
      "Expressways/National roads",
      "Rail baseline <=2000/unknown",
      "Rail added 2001-2020",
      "HSR lines (2020)",
      "Rail stations (2020)",
      "HSR stations (2020)",
      "Airports/Airfields",
      "Ports/Logistics hubs",
      "Zone: SEZ",
      "Zone: ETDZ",
      "Zone: HIDZ",
      "Zone: FTZ/Bonded",
      "Zone: Development Zone"
    ),
    drop = FALSE
  ) +
  ggplot2::labs(
    title = "Full Infrastructure Buildout (2020 rail base + latest multimodal OSM)",
    subtitle = glue::glue(
      "Road segments: {format(map16_counts$roads, big.mark = ',')} | ",
      "Rail segments: baseline {format(map16_counts$rail_baseline, big.mark = ',')}, added {format(map16_counts$rail_added, big.mark = ',')}, HSR {format(map16_counts$hsr_lines, big.mark = ',')} | ",
      "Stations: rail {format(map16_counts$rail_stations, big.mark = ',')}, HSR {format(map16_counts$hsr_stations, big.mark = ',')} | ",
      "Airports: {format(map16_counts$airports, big.mark = ',')} | ",
      "Ports/logistics hubs: {format(map16_counts$ports_hubs, big.mark = ',')} | ",
      "Development zones: {format(map16_counts$zones, big.mark = ',')} | ",
      "Road/airport/port layers use latest Geofabrik OSM province extracts"
    ),
    color = "Infrastructure layer"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_map16, "map_16_full_infrastructure_buildout.png", width = 13, height = 9)

# Map 17: Composite synergy hotspot score.
p_map17 <- ggplot2::ggplot(pref_map_extra) +
  ggplot2::geom_sf(ggplot2::aes(fill = synergy_hotspot_score), color = NA) +
  ggplot2::scale_fill_gradient2(
    low = "#2b83ba",
    mid = "#f7f7f7",
    high = "#d7191c",
    midpoint = 0,
    na.value = "grey90"
  ) +
  ggplot2::labs(
    title = "Integrated Synergy Hotspots",
    subtitle = "Mean z-score of population growth, economic growth proxy, connectivity gain, and zone intensity",
    fill = "Synergy score"
  )
write_fig(p_map17, "map_17_synergy_hotspots.png", width = 13, height = 9)

# -------------------------------
# Additional figures (11 to 24)
# -------------------------------

# Figure 11: Corridor analysis for economic growth.
p_fig11 <- ggplot2::ggplot(
  corridor_df |> dplyr::filter(is.finite(pref_gdp_growth_ann)),
  ggplot2::aes(x = corridor_band, y = pref_gdp_growth_ann * 100, fill = corridor_band)
) +
  ggplot2::geom_boxplot(outlier.alpha = 0.2, linewidth = 0.2) +
  ggplot2::labs(
    title = "Corridor Analysis: Economic Growth by Distance to HSR",
    x = "Distance band to nearest HSR station (2020)",
    y = "Economic growth proxy (annualized %)"
  ) +
  ggplot2::theme(legend.position = "none")
write_fig(p_fig11, "fig_11_corridor_econ_growth.png", width = 11, height = 6)

# Figure 12: Population vs economic growth scatter.
scatter_df <- analysis_typ |>
  dplyr::filter(is.finite(pop_growth_ann), is.finite(pref_gdp_growth_ann))

p_fig12 <- ggplot2::ggplot(scatter_df, ggplot2::aes(x = pop_growth_ann * 100, y = pref_gdp_growth_ann * 100, color = trajectory_type)) +
  ggplot2::geom_point(alpha = 0.7, size = 1.7) +
  ggplot2::geom_smooth(method = "lm", se = FALSE, color = "black", linewidth = 0.8) +
  ggplot2::labs(
    title = "Population Growth vs Economic Growth",
    x = "Population growth (annualized %)",
    y = "Economic growth proxy (annualized %)",
    color = "Trajectory"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig12, "fig_12_scatter_pop_vs_econ_growth.png", width = 12, height = 7)

# Figure 13: Connectivity gain vs population growth.
p_fig13 <- ggplot2::ggplot(scatter_df, ggplot2::aes(x = connectivity_gain_raw, y = pop_growth_ann * 100, color = conn_band)) +
  ggplot2::geom_point(alpha = 0.7, size = 1.7) +
  ggplot2::geom_smooth(method = "lm", se = FALSE, color = "black", linewidth = 0.8) +
  ggplot2::labs(
    title = "Connectivity Gain vs Population Growth",
    x = "Connectivity gain composite",
    y = "Population growth (annualized %)",
    color = "Conn band"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig13, "fig_13_scatter_connectivity_vs_pop_growth.png", width = 12, height = 7)

# Figure 14: Zone intensity vs economic growth.
p_fig14 <- ggplot2::ggplot(scatter_df, ggplot2::aes(x = zone_intensity_raw, y = pref_gdp_growth_ann * 100, color = zone_band)) +
  ggplot2::geom_point(alpha = 0.7, size = 1.7) +
  ggplot2::geom_smooth(method = "lm", se = FALSE, color = "black", linewidth = 0.8) +
  ggplot2::labs(
    title = "Zone Intensity vs Economic Growth",
    x = "Zone intensity composite",
    y = "Economic growth proxy (annualized %)",
    color = "Zone band"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig14, "fig_14_scatter_zone_vs_econ_growth.png", width = 12, height = 7)

# Figure 15: Typology counts.
typ_count <- analysis_typ |>
  dplyr::count(trajectory_type, sort = TRUE)

p_fig15 <- ggplot2::ggplot(typ_count, ggplot2::aes(x = reorder(trajectory_type, n), y = n, fill = trajectory_type)) +
  ggplot2::geom_col() +
  ggplot2::coord_flip() +
  ggplot2::labs(
    title = "Number of Prefectures by Typology",
    x = "Trajectory type",
    y = "Count"
  ) +
  ggplot2::theme(legend.position = "none")
write_fig(p_fig15, "fig_15_typology_counts.png", width = 11, height = 7)

# Figure 16: Typology metric heatmap.
heat_df <- analysis_typ |>
  dplyr::group_by(trajectory_type) |>
  dplyr::summarise(
    pop_growth = mean(pop_growth_ann, na.rm = TRUE),
    econ_growth = mean(pref_gdp_growth_ann, na.rm = TRUE),
    connectivity_gain = mean(connectivity_gain_raw, na.rm = TRUE),
    zone_intensity = mean(zone_intensity_raw, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(pop_growth, econ_growth, connectivity_gain, zone_intensity),
    names_to = "metric",
    values_to = "value"
  ) |>
  dplyr::group_by(metric) |>
  dplyr::mutate(value_z = as.numeric(scale(value))) |>
  dplyr::ungroup()

p_fig16 <- ggplot2::ggplot(heat_df, ggplot2::aes(x = metric, y = trajectory_type, fill = value_z)) +
  ggplot2::geom_tile(color = "white") +
  ggplot2::scale_fill_gradient2(low = "#2166ac", mid = "white", high = "#b2182b", midpoint = 0) +
  ggplot2::labs(
    title = "Typology Profile Heatmap (z-scored means)",
    x = "Metric",
    y = "Trajectory type",
    fill = "z-score"
  )
write_fig(p_fig16, "fig_16_typology_metric_heatmap.png", width = 12, height = 7)

# Figure 17: Province-level average growth ranking.
prov_rank <- analysis_typ |>
  dplyr::group_by(prov_name) |>
  dplyr::summarise(
    mean_pop_growth = mean(pop_growth_ann, na.rm = TRUE) * 100,
    mean_econ_growth = mean(pref_gdp_growth_ann, na.rm = TRUE) * 100,
    .groups = "drop"
  ) |>
  dplyr::filter(is.finite(mean_pop_growth), is.finite(mean_econ_growth)) |>
  dplyr::arrange(dplyr::desc(mean_pop_growth)) |>
  dplyr::slice_head(n = 20) |>
  tidyr::pivot_longer(
    cols = c(mean_pop_growth, mean_econ_growth),
    names_to = "series",
    values_to = "value"
  )

p_fig17 <- ggplot2::ggplot(prov_rank, ggplot2::aes(x = reorder(prov_name, value), y = value, fill = series)) +
  ggplot2::geom_col(position = "dodge") +
  ggplot2::coord_flip() +
  ggplot2::labs(
    title = "Top 20 Provinces by Mean Prefecture Population Growth",
    x = "Province",
    y = "Annualized growth (%)",
    fill = "Series"
  )
write_fig(p_fig17, "fig_17_province_growth_ranking.png", width = 12, height = 8)

# Figure 18: Station accessibility trend.
dist_trend <- panel |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    mean_dist_station = mean(dist_station_km, na.rm = TRUE),
    mean_dist_hsr_station = mean(dist_hsr_station_km, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(mean_dist_station, mean_dist_hsr_station),
    names_to = "series",
    values_to = "distance_km"
  ) |>
  dplyr::mutate(
    series = dplyr::recode(
      series,
      mean_dist_station = "Mean distance to any rail station",
      mean_dist_hsr_station = "Mean distance to HSR station"
    )
  )

p_fig18 <- ggplot2::ggplot(dist_trend, ggplot2::aes(x = year, y = distance_km, color = series)) +
  ggplot2::geom_line(linewidth = 1.1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::scale_x_continuous(breaks = YEARS_SNAPSHOT) +
  ggplot2::labs(
    title = "Accessibility Trend Over Time",
    x = "Year",
    y = "Mean distance (km)",
    color = "Series"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig18, "fig_18_station_accessibility_trend.png", width = 11, height = 6)

# Figure 19: Network-length trend.
network_trend <- panel |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    total_rail_km = sum(rail, na.rm = TRUE),
    total_hsr_km = sum(hsr, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(cols = c(total_rail_km, total_hsr_km), names_to = "series", values_to = "km") |>
  dplyr::mutate(series = dplyr::recode(series, total_rail_km = "Rail km", total_hsr_km = "HSR km"))

p_fig19 <- ggplot2::ggplot(network_trend, ggplot2::aes(x = year, y = km, color = series)) +
  ggplot2::geom_line(linewidth = 1.1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::scale_x_continuous(breaks = YEARS_SNAPSHOT) +
  ggplot2::labs(
    title = "Aggregate Network Length Trend",
    x = "Year",
    y = "Total km across prefectures",
    color = "Series"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig19, "fig_19_network_length_trend.png", width = 11, height = 6)

# Figure 20: Zone activation trend.
zone_trend <- panel |>
  dplyr::group_by(year) |>
  dplyr::summarise(
    total_active_zones = sum(active_zones, na.rm = TRUE),
    mean_active_zones = mean(active_zones, na.rm = TRUE),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(total_active_zones, mean_active_zones),
    names_to = "series",
    values_to = "value"
  ) |>
  dplyr::mutate(
    series = dplyr::recode(
      series,
      total_active_zones = "Total active zones",
      mean_active_zones = "Mean active zones per prefecture"
    )
  )

p_fig20 <- ggplot2::ggplot(zone_trend, ggplot2::aes(x = year, y = value, color = series)) +
  ggplot2::geom_line(linewidth = 1.1) +
  ggplot2::geom_point(size = 2) +
  ggplot2::scale_x_continuous(breaks = YEARS_SNAPSHOT) +
  ggplot2::labs(
    title = "Development Zone Presence Over Time",
    x = "Year",
    y = "Value",
    color = "Series"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig20, "fig_20_zone_activation_trend.png", width = 11, height = 6)

# Figure 21: Lorenz-style cumulative population distribution.
lorenz_df <- panel |>
  dplyr::filter(year %in% c(START_YEAR, END_YEAR)) |>
  dplyr::group_by(year) |>
  dplyr::arrange(pop_total, .by_group = TRUE) |>
  dplyr::mutate(
    cum_pref_share = dplyr::row_number() / dplyr::n(),
    cum_pop_share = cumsum(dplyr::coalesce(pop_total, 0)) / sum(dplyr::coalesce(pop_total, 0), na.rm = TRUE)
  ) |>
  dplyr::ungroup() |>
  dplyr::mutate(year = as.character(year))

p_fig21 <- ggplot2::ggplot(lorenz_df, ggplot2::aes(x = cum_pref_share, y = cum_pop_share, color = year)) +
  ggplot2::geom_line(linewidth = 1.1) +
  ggplot2::geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "grey50") +
  ggplot2::labs(
    title = "Cumulative Population Distribution Across Prefectures",
    x = "Cumulative share of prefectures",
    y = "Cumulative share of population",
    color = "Year"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig21, "fig_21_lorenz_population_distribution.png", width = 11, height = 6)

# Figure 22: HSR station gain vs economic growth.
p_fig22 <- ggplot2::ggplot(scatter_df, ggplot2::aes(x = hsr_station_gain, y = pref_gdp_growth_ann * 100, color = econ_band)) +
  ggplot2::geom_jitter(width = 0.2, height = 0, alpha = 0.65, size = 1.6) +
  ggplot2::geom_smooth(method = "lm", se = FALSE, color = "black", linewidth = 0.8) +
  ggplot2::labs(
    title = "HSR Station Gain vs Economic Growth",
    x = "HSR station gain (2000-2020)",
    y = "Economic growth proxy (annualized %)",
    color = "Econ band"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig22, "fig_22_hsr_station_gain_vs_econ_growth.png", width = 12, height = 7)

# Figure 23: Sensitivity of near-far growth gaps to near-threshold choice.
fig23_df <- hsr_threshold_sensitivity |>
  dplyr::filter(is.finite(estimate), is.finite(ci_low), is.finite(ci_high))

p_fig23 <- ggplot2::ggplot(fig23_df, ggplot2::aes(x = near_cutoff_km, y = estimate, color = outcome, fill = outcome)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "grey40") +
  ggplot2::geom_ribbon(ggplot2::aes(ymin = ci_low, ymax = ci_high), alpha = 0.18, color = NA) +
  ggplot2::geom_line(linewidth = 1.0) +
  ggplot2::geom_point(size = 1.9) +
  ggplot2::scale_x_continuous(breaks = hsr_threshold_grid) +
  ggplot2::labs(
    title = "HSR Proximity Sensitivity: Near-Far Growth Gaps",
    subtitle = "Near threshold varies; far group fixed at >50 km; ribbons show bootstrap 95% CI",
    x = "Near cutoff to HSR station (km)",
    y = "Near minus far gap (pp/year)",
    color = "Outcome",
    fill = "Outcome"
  ) +
  ggplot2::theme(legend.position = "bottom")
write_fig(p_fig23, "fig_23_hsr_threshold_sensitivity.png", width = 12, height = 7)

# Figure 24: Bootstrapped driver-model coefficients (adjusted specifications).
coef_order <- c(
  "Provincial GDP growth baseline (z)",
  "Baseline distance to HSR (z)",
  "Baseline GDP proxy level (z)",
  "Baseline population level (z)",
  "Zone intensity (z)",
  "Station access improvement (z)",
  "HSR station gain (z)",
  "Rail km change (z)",
  "HSR km change (z)"
)

fig24_df <- driver_regression |>
  dplyr::mutate(term_label = factor(term_label, levels = coef_order))

p_fig24 <- ggplot2::ggplot(fig24_df, ggplot2::aes(x = estimate, y = term_label)) +
  ggplot2::geom_vline(xintercept = 0, linetype = "dashed", color = "grey40") +
  ggplot2::geom_errorbar(ggplot2::aes(xmin = ci_low, xmax = ci_high), width = 0.18, color = "#1f78b4", orientation = "y") +
  ggplot2::geom_point(color = "#08519c", size = 2.1) +
  ggplot2::facet_wrap(~outcome, scales = "free_x") +
  ggplot2::labs(
    title = "Driver Associations with Growth (Adjusted Models)",
    subtitle = "Points are OLS coefficients; intervals are bootstrap 95% CI (population model uses province FE; economic model uses provincial GDP-growth control)",
    x = "Coefficient (percentage-point change in annual growth)",
    y = "Predictor"
  )
write_fig(p_fig24, "fig_24_driver_coefficients_bootstrap.png", width = 13, height = 7)

# Visual catalog: documents orthogonality of the map and figure suite.
visual_catalog <- tibble::tribble(
  ~visual_id, ~visual_type, ~file_name, ~primary_dimension, ~orthogonality_note,
  "map_01", "map", "map_01_rail_hsr_timeslices.png", "transport chronology", "Temporal network expansion baseline",
  "map_02", "map", "map_02_development_zones.png", "policy geography", "Zone placement independent of growth outcomes",
  "map_03", "map", "map_03_population_growth.png", "demographic change", "Population dynamics spatial pattern",
  "map_04", "map", "map_04_economic_growth_proxy.png", "economic change", "Activity shift pattern using GDP proxy",
  "map_05", "map", "map_05_population_econ_colocation.png", "co-location regime", "Joint pop-econ divergence classes",
  "map_06", "map", "map_06_zone_connectivity_overlay.png", "policy-transport interaction", "Overlay of two structural drivers",
  "map_07", "map", "map_07_typology_prefecture.png", "trajectory synthesis", "Integrated final typology",
  "map_08", "map", "map_08_hsr_station_gain.png", "station-based connectivity gain", "Point-access expansion outcome",
  "map_09", "map", "map_09_population_level_2020.png", "demographic stock", "Population stock (level) vs growth map",
  "map_10", "map", "map_10_rail_density_2020.png", "rail infrastructure intensity", "Area-normalized rail presence",
  "map_11", "map", "map_11_hsr_density_2020.png", "HSR infrastructure intensity", "Area-normalized HSR presence",
  "map_12", "map", "map_12_dist_to_station_2020.png", "general accessibility", "Distance-based accessibility for all rail",
  "map_13", "map", "map_13_dist_to_hsr_station_2020.png", "HSR accessibility", "HSR-specific accessibility gradient",
  "map_14", "map", "map_14_active_zones_2020.png", "policy stock", "Active-zone stock independent of transport",
  "map_15", "map", "map_15_connectivity_gain.png", "connectivity composite", "Combined rail/HSR/station accessibility change",
  "map_16", "map", "map_16_full_infrastructure_buildout.png", "full infrastructure stack", "Integrated overlay of roads, rail/HSR, stations, airports, ports/logistics hubs, and policy zones",
  "map_17", "map", "map_17_synergy_hotspots.png", "integrated hotspot intensity", "Composite growth-connectivity-policy synergy surface",
  "fig_08", "figure", "fig_08_concentration_trends.png", "national concentration trend", "Macro concentration dynamics",
  "fig_09", "figure", "fig_09_boxplots_by_typology.png", "within-type distributions", "Distributional heterogeneity by type",
  "fig_10", "figure", "fig_10_corridor_population_growth.png", "corridor-pop relation", "Distance-band demographic response",
  "fig_11", "figure", "fig_11_corridor_econ_growth.png", "corridor-econ relation", "Distance-band economic response",
  "fig_12", "figure", "fig_12_scatter_pop_vs_econ_growth.png", "growth coupling", "Joint demographic-economic gradient",
  "fig_13", "figure", "fig_13_scatter_connectivity_vs_pop_growth.png", "connectivity-demography", "Connectivity link to pop growth",
  "fig_14", "figure", "fig_14_scatter_zone_vs_econ_growth.png", "policy-economy", "Zone intensity relation to econ growth",
  "fig_15", "figure", "fig_15_typology_counts.png", "class composition", "Typology size structure",
  "fig_16", "figure", "fig_16_typology_metric_heatmap.png", "multimetric typology profile", "Metric contrasts by type",
  "fig_17", "figure", "fig_17_province_growth_ranking.png", "province ranking", "Cross-province relative performance",
  "fig_18", "figure", "fig_18_station_accessibility_trend.png", "access trend", "Temporal accessibility shift",
  "fig_19", "figure", "fig_19_network_length_trend.png", "network stock trend", "Aggregate infrastructure growth",
  "fig_20", "figure", "fig_20_zone_activation_trend.png", "zone trend", "Temporal policy-zone intensity trend",
  "fig_21", "figure", "fig_21_lorenz_population_distribution.png", "distribution inequality", "Concentration shape comparison",
  "fig_22", "figure", "fig_22_hsr_station_gain_vs_econ_growth.png", "HSR gain-economy relation", "Station expansion linkage",
  "fig_23", "figure", "fig_23_hsr_threshold_sensitivity.png", "threshold robustness", "Near-far growth contrast stability under alternative proximity cutoffs",
  "fig_24", "figure", "fig_24_driver_coefficients_bootstrap.png", "model-based evidence", "Adjusted coefficient uncertainty for major transport-policy-growth drivers"
)

readr::write_csv(visual_catalog, "outputs/tables/table_visual_catalog.csv")

log_message("Step 12: Writing metadata inventory, variable dictionary, and session info")

data_inventory <- tibble::tribble(
  ~dataset, ~source_url, ~date_accessed, ~license_terms, ~spatial_level, ~temporal_coverage, ~notes,
  "China province boundaries (GADM level 1)", "https://geodata.ucdavis.edu/gadm/gadm4.1/pck/gadm41_CHN_1_pk.rds", Sys.Date(), "GADM terms", "province polygons", "static", "Downloaded via geodata::gadm",
  "China prefecture-level proxy boundaries (GADM level 2)", "https://geodata.ucdavis.edu/gadm/gadm4.1/pck/gadm41_CHN_2_pk.rds", Sys.Date(), "GADM terms", "prefecture-like polygons", "static", "Used as primary analysis unit (closest readily available harmonized subprovincial layer)",
  "China annual rail and HSR network (NBER CSTD)", "https://back.nber.org/appendix/w33515/", Sys.Date(), "NBER appendix supplemental dataset terms", "rail line segments (annual snapshots)", "2000, 2005, 2008, 2010, 2012, 2015, 2018, 2020", "Built from RegularRailway + HighSpeedRailway yearly shapefiles (China-only coverage); cleaned for empty/duplicate segments",
  "China annual railway stations (NBER CSTD)", "https://back.nber.org/appendix/w33515/", Sys.Date(), "NBER appendix supplemental dataset terms", "station points (annual snapshots)", "2000, 2005, 2010, 2015, 2020", "Built from RailwayStations yearly shapefiles; station_type inferred from HghSpdS field",
  "China multimodal infrastructure (Geofabrik OSM)", "https://download.geofabrik.de/asia/china.html", Sys.Date(), "OpenStreetMap ODbL (via Geofabrik extracts)", "roads/airports/ports-logistics hubs", "latest subregion snapshots", "Merged from all mainland province extracts; roads filtered to motorway/trunk classes; airports/ports/logistics from transport/traffic/landuse layers",
  "Development zones", "https://query.wikidata.org/sparql", Sys.Date(), "Wikidata CC0", "zone points", "static + partial inception years", "Filtered by bilingual zone-related labels; station-like features removed",
  "Population density (GPW v4)", "https://geodata.ucdavis.edu/geodata/pop/", Sys.Date(), "CIESIN GPW terms", "global raster aggregated to prefectures", "2000, 2005, 2010, 2015, 2020", "Used as substitute for WorldPop due scripted availability",
  "Provincial GDP", "https://en.wikipedia.org/wiki/List_of_Chinese_administrative_divisions_by_GDP", Sys.Date(), "Wikipedia CC BY-SA", "province-year", "2000, 2010, 2015, 2020 (interpolated to 2005)", "Nominal values; used as contextual economic baseline"
)

readr::write_csv(data_inventory, "data/metadata/data_inventory.csv")

variable_dictionary <- tibble::tribble(
  ~variable, ~description, ~type, ~source,
  "pref_id", "Stable GADM level-2 unit identifier", "character", "GADM",
  "prov_id", "Stable GADM level-1 unit identifier", "character", "GADM",
  "year", "Snapshot year", "integer", "Project parameter",
  "pop_total", "Estimated total population from GPW density x cell area", "numeric", "GPW via geodata",
  "gdp_value", "Provincial GDP nominal value (CNY million)", "numeric", "Wikipedia table",
  "pref_gdp_value", "Population-allocated prefecture GDP proxy", "numeric", "Derived",
  "n_zones", "Count of development-zone points in unit", "integer", "Wikidata-derived",
  "active_zones", "Cumulative active zones by year using establishment year when available", "integer", "Derived",
  "rail", "Regular rail km inside prefecture-year from annual NBER network overlays", "numeric", "NBER CSTD + derived intersections",
  "hsr", "HSR km inside prefecture-year from annual NBER high-speed network overlays", "numeric", "NBER CSTD + derived intersections",
  "dist_station_km", "Distance from prefecture centroid to nearest rail station", "numeric", "Derived",
  "dist_hsr_station_km", "Distance from prefecture centroid to nearest HSR station", "numeric", "Derived",
  "trajectory_type", "Rule-based spatial transformation typology", "character", "Derived"
)

readr::write_csv(variable_dictionary, "data/metadata/variable_dictionary.csv")

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), "NA", formatC(x, format = "f", digits = digits))
}

method_notes <- c(
  "# Method Limitations and Reproducibility Notes",
  "",
  "## Scope and design",
  "- This project is descriptive and association-focused; it does not establish causal treatment effects.",
  "- Core inference triangulates three descriptive-evidence layers: distributional summaries, bootstrap/permutation gap tests, and adjusted regression models.",
  "",
  "## What Was Tightened in This Version",
  "- Rail and HSR geometry uses annual NBER CSTD snapshots rather than back-cast current OSM lines.",
  "- Added uncertainty-aware HSR proximity inference: bootstrap confidence intervals + permutation p-values.",
  "- Added threshold-sensitivity diagnostics for near/far HSR definitions (5-40 km near cutoff; far >50 km).",
  "- Added adjusted driver models (population model with province FE; economic model with provincial-growth controls) and bootstrap coefficient intervals.",
  "- Expanded visual evidence with integrated hotspot map and robustness-focused figures.",
  "",
  "## Key data limitations",
  "- Development zones are compiled from Wikidata points and may be incomplete for sub-provincial inventories.",
  "- Prefecture GDP is still a population-allocated provincial proxy, not official prefecture GDP series.",
  "- Provincial GDP values are nominal snapshots scraped from Wikipedia and interpolated at missing snapshot years.",
  "- Population uses GPW snapshots (2000, 2005, 2010, 2015, 2020), not annual census-equivalent estimates.",
  "",
  "## Reproducibility",
  "- Main pipeline script: `scripts/run_china_spatial_reallocation.R`",
  "- Run command: `Rscript scripts/run_china_spatial_reallocation.R`",
  "- Runtime/session metadata: `logs/pipeline_log.txt`, `logs/session_info.txt`",
  "- Core outputs: `data/processed/analysis_panel_prefecture_year.csv`, `data/processed/typology_prefecture.csv`",
  "- Robustness outputs: `outputs/tables/table_hsr_gap_inference.csv`, `outputs/tables/table_hsr_threshold_sensitivity.csv`, `outputs/tables/table_driver_regression_bootstrap.csv`"
)
writeLines(method_notes, con = "outputs/tables/method_limitations_reproducibility.md")

storyline_brief <- c(
  "# Storyline and Evidence Brief",
  "",
  "## Core narrative",
  "- Spatial transformation is heterogeneous rather than single-regime: Mixed Transitional Pattern remains the modal trajectory class.",
  "- Transport corridor proximity is directionally associated with stronger growth outcomes, but effect magnitudes are modest and uncertainty-aware.",
  "- Stronger connectivity and policy intensity jointly align with concentrated hotspot prefectures rather than a uniform nationwide lift.",
  "",
  "## Quantified anchors",
  glue::glue("- Population concentration change (HHI, 2000-2020): {fmt_num(pop_hhi_change_pct, 2)}%."),
  glue::glue("- Economic concentration change (HHI, 2000-2020): {fmt_num(econ_hhi_change_pct, 2)}%."),
  glue::glue("- HSR near-vs-far population gap (pp/year): {fmt_num(hsr_pop_inf$estimate[1], 3)} [{fmt_num(hsr_pop_inf$ci_low[1], 3)}, {fmt_num(hsr_pop_inf$ci_high[1], 3)}], permutation p={fmt_num(hsr_pop_inf$p_perm[1], 4)}."),
  glue::glue("- HSR near-vs-far economic gap (pp/year): {fmt_num(hsr_econ_inf$estimate[1], 3)} [{fmt_num(hsr_econ_inf$ci_low[1], 3)}, {fmt_num(hsr_econ_inf$ci_high[1], 3)}], permutation p={fmt_num(hsr_econ_inf$p_perm[1], 4)}."),
  glue::glue("- Typology robustness floor: share same-as-baseline={fmt_num(min_stability, 3)}, ARI={fmt_num(min_ari, 3)}."),
  "",
  "## Interpretation discipline",
  "- These results are evidence-weighted descriptive associations with explicit uncertainty, not causal treatment effects.",
  "- Conclusions should be interpreted as pattern-consistent and specification-robust within available data constraints."
)
writeLines(storyline_brief, con = "outputs/tables/storyline_evidence_brief.md")

capture.output(sessionInfo(), file = "logs/session_info.txt")

log_message("Pipeline completed successfully.")
