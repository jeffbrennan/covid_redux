#region libraries -----
library(tidyverse)
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


select = dplyr::select
conn_prod = dbConnect(SQLite(), 'db/prod.db')
conn_stage = dbConnect(SQLite(), 'db/staging.db')
N_CORES = availableCores() / 2

# functions -----
Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df = df %>%
      dplyr::select(Date, Cases_Daily_Imputed, Population_DSHS) %>%
      group_by(Date) %>%
      summarize(across(c(Cases_Daily_Imputed, Population_DSHS), ~sum(., na.rm = TRUE))) %>%
      ungroup() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      mutate(Level_Type = level_type) %>%
      mutate(Level = 'Texas')

  } else {
    clean_df = df %>%
      dplyr::select(Date, !!as.name(level_type), Cases_Daily_Imputed, Population_DSHS) %>%
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

rt.df.extraction = function(Rt.estimate.output) {

  # extract r0 estimate values into dataframe
  rt.df = setNames(stack(Rt.estimate.output$estimates$TD$R)[2:1], c('Date', 'Rt'))
  rt.df$Date = as.Date(rt.df$Date)

  # get 95% CI
  CI.lower.list = Rt.estimate.output$estimates$TD$conf.int$lower
  CI.upper.list = Rt.estimate.output$estimates$TD$conf.int$upper

  #use unlist function to format as vector
  CI.lower = unlist(CI.lower.list, recursive = TRUE, use.names = TRUE)
  CI.upper = unlist(CI.upper.list, recursive = TRUE, use.names = TRUE)

  rt.df$lower = CI.lower
  rt.df$upper = CI.upper

  rt.df = rt.df %>%
    mutate(lower = replace(lower, Rt == 0, NA)) %>%
    mutate(upper = replace(upper, Rt == 0, NA)) %>%
    mutate(Rt = replace(Rt, Rt == 0, NA))

  return(rt.df)
}

Calculate_RT = function(case_df, level, threshold) {
  set.seed(1)
  gen.time = generation.time("gamma", c(3.96, 4.75))
  pop.DSHS = case_df$Population_DSHS[1]

  #change na values to 0
  case_df = case_df %>%
    filter(Level == level) %>%
    mutate(Cases_Daily_Imputed = ifelse(is.na(Cases_Daily_Imputed) | Cases_Daily_Imputed < 0, 0, Cases_Daily_Imputed))

  # get case average from past month
  recent_case_avg = case_df %>%
    filter(Date > seq(max(Date), length = 2, by = "-3 weeks")[2]) %>%
    summarize(case_avg = mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
    pull(case_avg)

  message(glue("{level} three week case avg {round(recent_case_avg, 2)}"))
  # TODO: lookup value in lookup df instead of repeating value n = nrow times in run df

  cases_ma7 = case_df %>%
    mutate(MA_7day = rollmean(Cases_Daily_Imputed, k = 7, na.pad = TRUE, align = 'right')) %>%
    mutate(keep_row = Date >= '2020-03-15' & Cases_Daily_Imputed > 0) %>%
    mutate(keep_row = ifelse(keep_row, TRUE, NA)) %>%
    fill(keep_row, .direction = 'down') %>%
    filter(keep_row) %>%
    slice(1:max(which(Cases_Daily_Imputed > 0))) %>%
    select(Date, MA_7day) %>%
    deframe()

  # TODO: add better error handling
  tryCatch({
    rt_raw = suppressWarnings(
      estimate.R(
        epid = cases_ma7,
        GT = gen.time,
        begin = 1L,
        end = length(cases_ma7),
        methods = c("TD"),
        pop.size = pop.DSHS,
        nsim = 1000
      )
    )

    rt_df = rt.df.extraction(rt_raw) %>%
      select(Date, Rt, lower, upper) %>%
      mutate(case_avg = recent_case_avg) %>%
      mutate(threshold = ifelse(recent_case_avg > threshold, 'Above', 'Below'))

    return(rt_df)
  },
    error = function(e) {
      writeLines(paste0('Rt generation error (despite sufficient cases)', '\n'))
      rt_failure_df = data.frame(Date = as.Date(case_df$Date),
                                 Rt = rep(NA, length(case_df$Date)),
                                 lower = rep(NA, length(case_df$Date)),
                                 upper = rep(NA, length(case_df$Date)),
                                 case_avg = NA,
                                 threshold = NA)
      return(rt_failure_df)
    })
}


# Obtain dfs for analysis
county_raw = dbGetQuery(
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
  relocate(Level_Type, .before = 'Level')

case_quant = cleaned_cases_combined %>%
  filter(Level_Type == 'County') %>%
  filter(Date >= (max(Date) - as.difftime(3, unit = 'weeks'))) %>%
  group_by(Level) %>%
  summarize(mean_cases = mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
  ungroup() %>%
  summarize(case_quant = quantile(mean_cases, c(0.4, 0.5, 0.6, 0.7, 0.8), na.rm = TRUE)[4]) %>%
  pull(case_quant)

# Generate Rt estimates for each county, using 70% quantile of cases in past 3 weeks as threshold
# TODO: run in parallel
start_time = Sys.time()
df_levels = unique(furrr_test$Level)

message(glue('Running RT on {length(df_levels)} levels using {N_CORES} cores'))
plan(multicore, workers = N_CORES)
start_time = Sys.time()
rt_output = furrr::future_walk(df_levels,
                               ~Calculate_RT(case_df = cleaned_cases_combined,
                                             level = .,
                                             threshold = case_quant),
                               .options = furrr_options(seed = TRUE)
)
run_time = Sys.time() - start_time
# 10 secs with 2 workers
avg_per_level = run_time / length(df_levels)
message(glue('RT calculated for {length(df_levels} [AVG CALCULATION TIME: {round(avg_per_level, 2)}]'))

rt_combined = rt_output %>%
  rbindlist(., fill = TRUE)

# remove errors
min_date = seq(max(RT_County_df_all$Date), length = 2, by = "-3 weeks")[2]
error_counties = RT_County_df_all %>%
  group_by(County) %>%
  mutate(CI_error = factor(ifelse(lower == 0 & upper == 0, 1, 0))) %>%
  mutate(Rt_error = factor(ifelse(is.na(Rt) | Rt == 0 | Rt > 10, 1, 0))) %>%
  filter(Date > min_date & Date != max(Date)) %>%
  filter(is.na(CI_error) | CI_error == 1 | Rt_error == 1) %>%
  dplyr::select(County) %>%
  distinct() %>%
  unlist()


RT_County_df = RT_County_df_all %>%
  mutate(Rt = ifelse(County %in% error_counties, NA, Rt)) %>%
  mutate(lower = ifelse(County %in% error_counties, NA, lower)) %>%
  mutate(upper = ifelse(County %in% error_counties, NA, upper))
good_counties = RT_County_df$County %>% unique() %>% length()
good_counties / 254 # = 0.744


# TPR_df = read.csv('tableau/county_TPR.csv') %>%
TPR_df = dbGetQuery(conn_prod, "select * from main.county_TPR")
dplyr::select(-contains('Rt')) %>%
  mutate(Date = as.Date(Date))

cms_dates = list.files('C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources/historical/cms_tpr') %>%
  gsub('TPR_', '', .) %>%
  gsub('.csv', '', .) %>%
  as.Date()

cms_TPR_padded =
  TPR_df %>%
    filter(Date %in% cms_dates) %>%
    left_join(., RT_County_df[, c('County', 'Date', 'Rt')], by = c('County', 'Date')) %>%
    group_by(County) %>%
    arrange(County, Date) %>%
    tidyr::fill(TPR, .direction = 'up') %>%
    tidyr::fill(Tests, .direction = 'up') %>%
    tidyr::fill(Rt, .direction = 'up') %>%
    arrange(County, Date) %>%
    ungroup() %>%
    mutate(Tests = ifelse(Date < as.Date('2020-09-09'), NA, Tests))


cpr_TPR = TPR_df %>%
  filter(!(Date %in% cms_dates)) %>%
  left_join(., RT_County_df[, c('County', 'Date', 'Rt')], by = c('County', 'Date'))

county_TPR = cms_TPR_padded %>%
  rbind(cpr_TPR) %>%
  arrange(County, Date)
county_TPR_sd = cms_TPR_padded %>%
  rbind(cpr_TPR) %>%
  arrange(County, Date)

dbWriteTable(conn_prod, 'main.county_TPR', cpr_TPR)
dbWriteTable(conn_prod, 'main.stacked_rt', RT_Combined_df, overwrite = TRUE)

