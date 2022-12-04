#region libraries -----
library(tidyverse)
library(lubridate)
library(glue)
library(reshape2)
library(nlme)        # gapply
library(data.table)

# Modeling & stats
library(mgcv)       # gam
library(R0)         # rt
library(Kendall)    # Mann-Kendall

# parallel processing
library(furrr)

# Time series & forecasting
library(zoo)

# database
library(RSQLite)


select     = dplyr::select
conn_prod  = dbConnect(SQLite(), 'db/prod.db')
conn_stage = dbConnect(SQLite(), 'db/staging.db')
N_CORES    = availableCores() / 2
GENERATION_TIME = generation.time("gamma", c(3.96, 4.75))

# functions -----
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

Parse_RT_Results = function(level, rt_results_raw) {
  rt_results_level = rt_results_raw[[level]]
  case_df = rt_prep_df %>% filter(Level == level)

  if (all(is.na(rt_results_level))) {
    message(glue('{level}: Rt generation error (despite sufficient cases)'))

    result_df = data.frame(Date      = as.Date(case_df$Date),
                           Level = level,
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
  # TODO: perform parsing in separate function
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

# setup --------------------------------------------------------------------------------------------
## params
# --------------------------------------------------------------------------------------------

# Obtain dfs for analysis
county_raw      = dbGetQuery(
  conn_prod,
  "
  select Date, County, Cases_Daily_Imputed from main.county
  ")
county_metadata = dbGetQuery(
  conn_stage,
  "
  select * from county_names
  "
)

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


rt_parsed = map(names(rt_output), ~Parse_RT_Results(., rt_output))

rt_combined = rt_parsed %>%
  rbindlist(., fill = TRUE) %>%
  left_join(rt_prep_df %>% select(Level, recent_case_avg) %>% distinct(),
            by = 'Level'
  ) %>%
  mutate(threshold = ifelse(recent_case_avg > case_quant, 'Above', 'Below'))

# remove errors
error_levels = rt_combined %>%
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

rt_combined_clean = rt_combined %>%
  mutate(across(c(Rt, lower, upper), ~ifelse(Level %in% error_levels, NA, .))) %>%
  left_join(cleaned_cases_combined %>% select(Level, Level_Type) %>% distinct(), by = 'Level')

message(glue('missing rt for n={length(error_levels)} [{round(error_levels_pct * 100, 2)}%]'))

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

# final table organization --------------------------------------------------------------------------------------------
rt_final = rt_combined_clean %>%
  relocate(Level_Type, .before = 'Level')


# diagnostics
# -------------------------------------------------------------------------------------------
# TODO: add
# upload
# --------------------------------------------------------------------------------------------
dbWriteTable(conn_prod, 'main.county_TPR', county_TPR)
dbWriteTable(conn_prod, 'main.stacked_rt', rt_final, overwrite = TRUE)

