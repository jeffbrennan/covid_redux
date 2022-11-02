# Data manipulation & cleaning
library(tidyverse)
library(reshape2)
library(nlme)        # gapply
library(data.table)

# Modeling & stats
library(mgcv)       # gam
library(R0)         # rt
library(Kendall)    # Mann-Kendall

# Time series & forecasting
# library(forecast)
library(zoo)
# library(astsa)
# library(fpp2)

select = dplyr::select

# setup -----
Clean_Data = function(df, level_type) {
  if (level_type == 'State') {
    clean_df  = df %>%
      dplyr::select(Date, Cases_Daily_Imputed, Population_DSHS) %>%
      group_by(Date) %>%
      mutate_if(is.numeric, sum, na.rm = TRUE) %>%
      distinct() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date) %>%
      distinct()

  } else {
    clean_df = df %>%
      dplyr::select(Date, !!as.name(level_type), Cases_Daily_Imputed, Population_DSHS) %>%
      group_by(Date, !!as.name(level_type)) %>%
      mutate_if(is.numeric, sum, na.rm = TRUE) %>%
      distinct() %>%
      mutate(Date = as.Date(Date)) %>%
      arrange(Date, !!as.name(level_type)) %>%
      distinct()
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

covid.rt = function(mydata, threshold) {
  set.seed(1)

  ### DECLARE VALS ###
  #set generation time
  #Tapiwa, Ganyani "Esimating the gen interval for Covid-19":

  # LOGNORMAL OPS
  # gen.time=generation.time("lognormal", c(4.0, 2.9))
  # gen.time=generation.time("lognormal", c(4.7,2.9)) #Nishiura

  # GAMMA OPS
  # gen.time=generation.time("gamma", c(5.2, 1.72)) #Singapore
  # gen.time=generation.time("gamma", c(3.95, 1.51)) #Tianjin
  gen.time = generation.time("gamma", c(3.96, 4.75))
  print(mydata %>% dplyr::select(2) %>% distinct() %>% unlist() %>% setNames(NULL))

  #change na values to 0
  mydata = mydata %>% mutate(Cases_Daily_Imputed = ifelse(is.na(Cases_Daily_Imputed), 0, Cases_Daily_Imputed))

  # get case average from past month
  recent_case_avg = mydata %>%
    filter(Date > seq(max(Date), length = 2, by = "-3 weeks")[2]) %>%
    summarize(mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
    unlist()

  print(round(recent_case_avg, 2))

  pop.DSHS = mydata$Population_DSHS[1]
  #Get 7 day moving average of daily cases
  mydata$MA_7day=rollmean(mydata$Cases_Daily_Imputed, k=7, na.pad=TRUE, align='right')

  #create a vector of new cases 7 day moving average
  mydata.new=pull(mydata, MA_7day)
  #mydata.new = pull(mydata, Cases_Daily_Imputed)

  # get dates as vectors
  date.vector = pull(mydata, Date)

  #create a named numerical vector using the date.vector as names of new cases
  #Note: this is needed to run R0 package function estimate.R()
  names(mydata.new) = c(date.vector)


  #get row number of March 15 and first nonzero entry
  #NOTE: for 7 day moving average start March 15, for daily start March 9
  #find max row between the two (this will be beginning of rt data used)
  march15.row = which(mydata$Date=="2020-03-15")
  first.nonzero = min(which(mydata$Cases_Daily_Imputed>0))
  last.nonzero = max(which(mydata$Cases_Daily_Imputed>0))

  first.nonzero = ifelse(is.infinite(first.nonzero), NA, first.nonzero)
  last.nonzero = ifelse(is.infinite(last.nonzero), NA, last.nonzero)

  minrow = max(march15.row, first.nonzero, na.rm = TRUE)
  maxrow = as.integer(min(last.nonzero, nrow(mydata), na.rm = TRUE))

  # restrict df to same region as the minrow for addition of TPR & Cases/100
  # TODO: work entirely off mydata and remove individual vars for which row is where
  mydata = mydata %>% slice(minrow:maxrow)

  ### R0 ESTIMATION ###
  #reduce the vector to be rows from min date (March 9 or first nonzero case) to current date
  mydata.newest = mydata.new[minrow:maxrow]

  tryCatch({
    rt.DSHS = estimate.R(mydata.newest,
                         gen.time,
                         begin=as.integer(1),
                         end=length(mydata.newest),
                         methods=c("TD"),
                         pop.size=pop.DSHS,
                         nsim=1000)

    rt.DSHS.df <<- rt.df.extraction(rt.DSHS) %>%
      dplyr::select(Date, Rt, lower, upper)

    rt.DSHS.df$case_avg = recent_case_avg
    rt.DSHS.df$threshold = ifelse(recent_case_avg > threshold, 'Above', 'Below')

  },
  error = function(e) {
    writeLines(paste0('Rt generation error (despite sufficient cases)', '\n'))
    rt.DSHS.df <<- data.frame(Date = as.Date(mydata$Date),
                              Rt = rep(NA, length(mydata$Date)),
                              lower = rep(NA, length(mydata$Date)),
                              upper = rep(NA, length(mydata$Date)),
                              case_avg = NA,
                              threshold = NA)
  })
  return(rt.DSHS.df)
}


# Obtain dfs for analysis
# TODO: setup sqllite here:
county = read.csv("tableau/county.csv") %>%
  dplyr::select(Date, Cases_Daily_Imputed, Tests_Daily,
                County, TSA_Combined, PHR_Combined,
                Metro_Area, Population_DSHS) %>%
  mutate(Date = as.Date(Date)) %>%
  rename(TSA = TSA_Combined, PHR = PHR_Combined, Metro = Metro_Area)

clean_dfs = sapply(c('County', 'TSA', 'PHR', 'Metro', 'State'), function(x) Clean_Data(county, x))

#extract data frames from the list
County_df = clean_dfs$County
TSA_df = clean_dfs$TSA |> distinct()
PHR_df = clean_dfs$PHR
Metro_df = clean_dfs$Metro
State_df = clean_dfs$State

case_quant = County_df %>%
  filter(Date >= (max(Date) - as.difftime(3, unit = 'weeks'))) %>%
  group_by(County) %>%
  mutate(mean_cases = mean(Cases_Daily_Imputed, na.rm = TRUE)) %>%
  dplyr::select(mean_cases) %>%
  ungroup() %>%
  summarize(case_quant = quantile(mean_cases, c(0.4, 0.5, 0.6, 0.7, 0.8), na.rm = TRUE)[4]) %>%
  unlist()


# Generate Rt estimates for each county, using 70% quantile of cases in past 3 weeks as threshold
start_time = Sys.time()
county.rt.output = nlme::gapply(County_df, FUN = covid.rt,
                                 groups = County_df$County, threshold = case_quant)
print(Sys.time() - start_time)

# combine list of dataframes (1 for each county) to single dataframe
RT_County_df_all = rbindlist(county.rt.output, idcol = 'County') %>%
  mutate(Date = as.Date(Date))

RT_County_df_all$County %>% unique() %>% length()

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
good_counties/254 # = 0.744

district = readxl::read_xlsx('tableau/district_school_reopening.xlsx', sheet=1) %>%
             mutate(LEA = as.character(LEA),
                    Date = as.Date(Date))

district_dates =
  data.frame('Date' = rep(unique(district$Date), each = 254),
             'County' = rep(unique(County_df$County), times = length(unique(district$Date))))

TPR_df = read.csv('tableau/county_TPR.csv') %>%
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
    full_join(., district_dates, by = c('County', 'Date')) %>%
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

county_TPR = cms_TPR_padded %>% filter(!(Date %in% district_dates$Date)) %>% rbind(cpr_TPR) %>% arrange(County, Date)
county_TPR_sd = cms_TPR_padded %>%  filter(Date %in% district_dates$Date) %>% rbind(cpr_TPR) %>% arrange(County, Date)

write.csv(county_TPR, 'tableau/county_TPR.csv', row.names = FALSE)
write.csv(county_TPR_sd, 'tableau/county_TPR_sd.csv', row.names = FALSE)

## TSA
RT_TSA_output = nlme::gapply(TSA_df, FUN=covid.rt, groups=TSA_df$TSA, threshold = case_quant)
RT_TSA_df = rbindlist(RT_TSA_output, idcol = 'TSA')

## PHR
RT_PHR_output = nlme::gapply(PHR_df, FUN=covid.rt, groups=PHR_df$PHR, threshold = case_quant)
RT_PHR_df = rbindlist(RT_PHR_output, idcol = 'PHR')


## Metro
RT_Metro_output = nlme::gapply(Metro_df, FUN=covid.rt, groups=Metro_df$Metro, threshold = case_quant)
RT_Metro_df = rbindlist(RT_Metro_output, idcol = 'Metro')

## State
RT_State_df = covid.rt(State_df, threshold = case_quant)


## grouping


# Rename levels for easier parsing in tableau
colnames(RT_County_df)[1] = 'Level'
colnames(RT_Metro_df)[1] = 'Level'
colnames(RT_TSA_df)[1] = 'Level'
colnames(RT_PHR_df)[1] = 'Level'
RT_State_df$Level = 'Texas'

RT_County_df$Level_Type = 'County'
# RT_District_df$Level_Type = 'District'
RT_Metro_df$Level_Type = 'Metro'
RT_TSA_df$Level_Type = 'TSA'
RT_PHR_df$Level_Type = 'PHR'
RT_State_df$Level_Type = 'State'

RT_Combined_df =
  rbind(RT_County_df, RT_TSA_df, RT_PHR_df, RT_Metro_df, RT_State_df) %>%
  filter(Date != max(Date)) %>%
  dplyr::select(-c(threshold, case_avg))

write.csv(RT_Combined_df, 'tableau/stacked_rt.csv', row.names = FALSE)

