# libraries --------------------------------------------------------------------------------------------
# general
library(tidyverse)
library(lubridate)
library(glue)
library(data.table)
library(RSQLite)      # database
library(furrr) # parallel processing

# Modeling & stats
library(mgcv)       # gam
library(R0)         # rt
library(Kendall)    # Mann-Kendall
library(zoo) # Time series & forecasting


select          = dplyr::select
conn_prod       = dbConnect(SQLite(), 'db/prod.db')
conn_stage      = dbConnect(SQLite(), 'db/staging.db')
N_CORES         = availableCores()
GENERATION_TIME = generation.time("gamma", c(3.96, 4.75))

# functions --------------------------------------------------------------------------------------------
Format_Output = function(parsed_rt, error_levels, job_id) {
  rt_combined_clean = parsed_rt %>%
    mutate(across(c(Rt, lower, upper), ~ifelse(Level %in% error_levels, NA, .))) %>%
    left_join(cleaned_cases_combined %>%
                select(Level, Level_Type) %>%
                distinct(), by = 'Level') %>%
    select(Date, Level_Type, Level, Rt, lower, upper) %>%
    mutate(JOB_ID = job_id)

  return(rt_combined_clean)
}

RT_Diagnostics_Checks = function(parsed_rt) {
  check_no_dupe = parsed_rt %>%
    group_by(Date, Level) %>%
    filter(n() > 1) %>%
    nrow() == 0

  checks = all(list(check_no_dupe))
}

RT_Diagnostics = function(parsed_rt) {
  runtime           = rt_end_time - rt_start_time
  runtime_per_level = runtime / length(df_levels)

  error_levels = parsed_rt %>%
    mutate(min_date = max(Date) - weeks(3)) %>%
    filter(Date > min_date & Date != max(Date)) %>%
    group_by(Level) %>%
    mutate(CI_error = lower == 0 & upper == 0) %>%
    mutate(Rt_error = is.na(Rt) | Rt == 0 | Rt > 10) %>%
    ungroup() %>%
    filter(is.na(CI_error) | CI_error | Rt_error) %>%
    pull(Level) %>%
    unique()

  error_levels_pct = length(error_levels) / length(df_levels)
  job_result       = RT_Diagnostics_Checks(parsed_rt)
  job_id           = Generate_Job_ID(CURRENT_JOB_ID)

  diagnostic_df = data.frame(JOB_ID = job_id) %>%
    mutate(JOB_TYPE = 'rt_estimation') %>%
    mutate(JOB_RESULT = job_result) %>%
    mutate(error_levels      = length(error_levels),
           error_levels_pct  = error_levels_pct,
           runtime           = runtime,
           runtime_per_level = runtime_per_level,
           time_completed    = Sys.time(),
           case_quantile_70  = case_quant,
    )
  output        = list(
    'df'           = diagnostic_df,
    'error_levels' = error_levels,
    'success'      = job_result,
    'job_id'       = job_id
  )
}

Generate_Job_ID = function(current_job_id) {
  new_job_id = current_job_id %>%
    sum(., 1) %>%
    str_pad(width = 5, pad = '0', side = 'left')

  return(new_job_id)
}

Parse_RT_Results = function(level, rt_results_raw) {
  rt_results_level = rt_results_raw[[level]]
  case_df          = rt_prep_df[[level]]

  if (all(is.na(rt_results_level))) {
    # message(glue('{level}: Rt generation error (despite sufficient cases)'))

    result_df = data.frame(Date      = as.Date(case_df$Date),
                           Level     = level,
                           Rt        = rep(NA, length(case_df$Date)),
                           lower     = rep(NA, length(case_df$Date)),
                           upper     = rep(NA, length(case_df$Date)),
                           case_avg  = NA,
                           threshold = NA)

  } else {
    rt_results = rt_results_level$estimates$TD

    # extract r0 estimate values into dataframe
    result_df = data.frame('Rt' = rt_results[['R']]) %>%
      mutate(Level = level) %>%
      mutate(Date = as.Date(row.names(.))) %>%
      as.data.frame(row.names = 1:nrow(.)) %>%
      mutate(lower = rt_results[['conf.int']][['lower']]) %>%
      mutate(upper = rt_results[['conf.int']][['upper']]) %>%
      rowwise() %>%
      mutate(across(c(Rt, lower, upper), ~ifelse(Rt == 0, NA, .))) %>%
      ungroup() %>%
      select(Date, Level, Rt, lower, upper)
  }
  return(result_df)
}

Calculate_RT = function(case_df) {
  # message(level)
  set.seed(1)
  level     = case_df$Level[1]
  level_pop = population_lookup %>%
    filter(Level == level) %>%
    pull(Population_DSHS)

  cases_ma7 = case_df %>%
    select(Date, MA_7day) %>%
    deframe()

  # TODO: add better error handling
  rt_raw = tryCatch(
  {
    result = suppressWarnings(
      estimate.R(
        epid     = cases_ma7,
        GT       = GENERATION_TIME,
        begin    = 1L,
        end      = length(cases_ma7),
        methods  = 'TD',
        pop.size = level_pop,
        nsim     = 1000
      )
    )
    return(result)
  },
    error = function(e) {
      return(NA)
    }
  )
  return(rt_raw)
}

Prepare_RT = function(case_df) {
  # get case average from past month
  recent_case_avg_df = case_df %>%
    group_by(Level) %>%
    filter(Date > max(Date, na.rm = TRUE) - weeks(3)) %>%
    summarize(recent_case_avg = mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
    ungroup() %>%
    select(Level, recent_case_avg)

  case_df_final = case_df %>%
    left_join(recent_case_avg_df, by = 'Level') %>%
    group_by(Level) %>%
    mutate(MA_7day = rollmean(Cases_Daily_Imputed, k = 7, na.pad = TRUE, align = 'right')) %>%
    mutate(keep_row = Date >= '2020-03-15' & Cases_Daily_Imputed > 0) %>%
    mutate(keep_row = ifelse(keep_row, TRUE, NA)) %>%
    fill(keep_row, .direction = 'down') %>%
    filter(keep_row) %>%
    slice(1:max(which(Cases_Daily_Imputed > 0))) %>%
    ungroup() %>%
    select(Date, Level, MA_7day, Population_DSHS) %>%
    group_split(Level) %>%
    set_names(map_chr(., ~.x$Level[1]))
  return(case_df_final)
}

Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df = df %>%
      select(Date, Cases_Daily_Imputed, Population_DSHS) %>%
      group_by(Date) %>%
      summarize(across(c(Cases_Daily_Imputed, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      mutate(Level_Type = level_type) %>%
      mutate(Level = 'Texas')

  } else {
    clean_df = df %>%
      select(Date, !!as.name(level_type), Cases_Daily_Imputed, Population_DSHS) %>%
      group_by(Date, !!as.name(level_type)) %>%
      summarize(across(c(Cases_Daily_Imputed, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date, !!as.name(level_type)) %>%
      mutate(Level_Type = level_type) %>%
      rename(Level = !!as.name(level_type))
  }
  return(clean_df %>% ungroup())
}

# setup --------------------------------------------------------------------------------------------
# db calls --------------------------------------------------------------------------------------------
county_raw      = dbGetQuery(
  conn_prod,
  "
  select Date, County, Cases_Daily_Imputed
  from main.county
  ")

county_metadata = dbGetQuery(
  conn_stage,
  "
  select *
  from main.county_names
  "
)

CURRENT_JOB_ID = dbGetQuery(conn_prod, 'select max(cast(job_id as int)) from main.stacked_rt')
# transformations --------------------------------------------------------------------------------------------
county = county_raw %>%
  left_join(county_metadata, by = 'County') %>%
  mutate(Date = as.Date(Date)) %>%
  rename(TSA = TSA_Combined, PHR = PHR_Combined, Metro = Metro_Area)

case_levels = c('County', 'TSA', 'PHR', 'Metro', 'State')

cleaned_cases_combined = map(case_levels, ~Clean_Data(county, .)) %>%
  rbindlist(., fill = TRUE) %>%
  relocate(Level_Type, .before = 'Level') %>%
  mutate(Cases_Daily_Imputed = ifelse(is.na(Cases_Daily_Imputed) | Cases_Daily_Imputed < 0, 0,
                                      Cases_Daily_Imputed))

population_lookup = cleaned_cases_combined %>%
  select(Level, Population_DSHS) %>%
  distinct()

case_quant = cleaned_cases_combined %>%
  filter(Level_Type == 'County') %>%
  filter(Date >= (max(Date) - as.difftime(3, unit = 'weeks'))) %>%
  group_by(Level) %>%
  summarize(mean_cases = mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
  ungroup() %>%
  summarize(case_quant = quantile(mean_cases, c(0.4, 0.5, 0.6, 0.7, 0.8), na.rm = TRUE)[4]) %>%
  pull(case_quant)

df_levels  = unique(cleaned_cases_combined$Level)

# perform initial rt preperation to minimize parallelized workload
rt_prep_df = Prepare_RT(cleaned_cases_combined)

# rt loop --------------------------------------------------------------------------------------------
# Generate Rt estimates for each county, using 70% quantile of cases in past 3 weeks as threshold
start_time = Sys.time()
message(glue('Running RT on {length(df_levels)} levels using {N_CORES} cores'))
rt_start_time = Sys.time()
plan(multisession, workers = N_CORES, gc = TRUE)

rt_output = future_map(rt_prep_df,
                       ~Calculate_RT(case_df = .),
                       .options  = furrr_options(seed       = TRUE,
                                                 scheduling = Inf),
                       .progress = TRUE
)
rt_end_time = Sys.time()
plan(sequential)

# formatting --------------------------------------------------------------------------------------------
rt_parsed = map(names(rt_output), ~Parse_RT_Results(., rt_output)) %>%
  rbindlist(fill = TRUE)

diagnostic_results = RT_Diagnostics(parsed_rt = rt_parsed)
stopifnot(diagnostic_results$success)

rt_final = Format_Output(parsed_rt    = rt_parsed,
                         error_levels = diagnostic_results$error_levels,
                         job_id       = diagnostic_results$job_id)

# upload  --------------------------------------------------------------------------------------------
dbWriteTable(conn_prod, DBI::SQL('stats_diagnostics'), diagnostic_results$df, append = TRUE)
dbWriteTable(conn_prod, DBI::SQL('main.stacked_rt'), rt_final, overwrite = TRUE)


# write TPR update
# --------------------------------------------------------------------------------------------
# TPR_df = read.csv('tableau/county_TPR.csv') %>%
TPR_df = dbGetQuery(conn_prod, "select * from main.county_TPR") %>%
  select(-contains('Rt')) %>%
  mutate(Date = as.Date(Date))

# cms_dates = list.files('C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources
# /historical/cms_tpr') %>%
#   gsub('TPR_', '', .) %>%
#   gsub('.csv', '', .) %>%
#   as.Date()
# TODO: update this with actual values
cms_dates = unique(TPR_df$Date)

MIN_TEST_DATE  = as.Date('2020-09-09')
cms_TPR_padded =
  TPR_df %>%
    filter(Date %in% cms_dates) %>%
    arrange(County, Date) %>%
    left_join(rt_combined_clean %>%
                filter(Level_Type == 'County') %>%
                select(Level, Date, Rt),
              by = c('County' = 'Level', 'Date')) %>%
    group_by(County) %>%
    tidyr::fill(TPR, .direction = 'up') %>%
    tidyr::fill(Tests, .direction = 'up') %>%
    tidyr::fill(Rt, .direction = 'up') %>%
    ungroup() %>%
    mutate(Tests = ifelse(Date < MIN_TEST_DATE, NA, Tests))

county_TPR = cms_TPR_padded %>%
  arrange(County, Date)

# upload
# --------------------------------------------------------------------------------------------
dbWriteTable(conn_prod, 'main.county_TPR', county_TPR)

