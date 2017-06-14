### TO DO:
### ADD BENCHMARK 3 FOR AUS and REVENUES
###
NGU_plot_per_hour
# YOUTUBE AND TV MACRO ATTRIBUTION
# The purpose of this file is to calculate the NGU, active users and revenues uplift of a given youTube or TV campaign
# Author: Angus McKay
# Date: May 2017

# NOTES:
# - inputs below
# - analysis is carried out on an hourly, quarter-daily and daily basis
# - there are two benchmarks, the first is other countries over the same time period
# - the second benchmark is the same country over a selected period before the campaign
# - MAY NEED TO BE CAREFUL ABOUT OTHER CAMPAIGNS OVER SAME TIME (either in analysis country or benchmark countries)

# INPUTS: PLEASE, DEFINE THE FOLLOWING VARIABLES

# name of the game. "DC" or "ML".
game <- "DC"

# platform. "ios" or "ios"
platform <- 'android'

# name of the country. "FR", "US", "AU", ...
country <- 'US'

# dates in format: '2000-01-01 01:00:00'.
# Campaign dates, cost and period of days to analyse after campaign started
campaign_type <- 'Youtube' # 'TV' or 'Youtube'
campaign_start  <- '2017-06-01 00:00:00' # will analyse in 24 hour periods after this time (and hour and 6-hour periods)
analysis_period <- 7 # in days
cost_of_campaign <- 50000

# Benchmark 1: select benchmark countries as a list or leave blank to include all countries except country of analysis
#benchmark_countries <- c('ES', 'FR', 'DE')

# Benchmark 2: select benchmark period to analyse or leave blank to use 28 days before campaign start
benchmark2_start_date <- '2017-05-01 00:00:00'
benchmark2_end_date <- '2017-06-01 00:00:00'

# Benchmark 3: select previous year to analyse (default is previous year)
benchmark3_year <- 2016

# Marketing register_source (e.g. 'youtuber_vanossgaming') - add these inside brackets
marketing_tracker_source <- "('youtuber_vanossgaming')"

# START -----------------------------------------------------------------------------------------------------

# Downloads and loads the required packages
if (!require("RPostgreSQL")) install.packages("RPostgreSQL"); library(RPostgreSQL) 
if (!require("ggplot2")) install.packages("ggplot2"); library(ggplot2)
if (!require("lubridate")) install.packages("lubridate"); library(lubridate)


#CONNECTING TO THE DATABASE
cat("CONNECTING TO THE DATABASE")
cat("\n")

if(game == "DC"){
  host <- "redshift.pro.dc.laicosp.net"
  schema <- "dragoncity"
}else if(game == "ML"){
  host <- "redshift.pro.mc.laicosp.net"
  schema <- "monstercity"
}

# To be filled with your user and pasword
drv <- dbDriver("PostgreSQL")
spdb <- dbConnect(
  drv,
  host = host,
  port = "5439",
  dbname = "datawarehouse",
  user = "amckay",
  password = "o6EbWm4L79jRfegy"
)



# DOWNLOAD CAMPAIGN DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD NGU DATA FROM REDSHIFT")
cat("\n")

analysis_period_pre_campaign <- if(campaign_type == 'Youtube') 2 else 14 # in days
analysis_start_date <- as.POSIXct(campaign_start) - days(analysis_period_pre_campaign)
analysis_end_date <- as.POSIXct(campaign_start) + days(analysis_period)

# adding extra 24 hours either side so that analysis period is covered after adjusting for time-zones
query_start_date <- as.POSIXct(analysis_start_date) - days(1)
query_end_date <- as.POSIXct(analysis_end_date) + days(1)

campaign_query <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', date_register) as date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE date_register >='", query_start_date, "'
                         AND date_register <= '", query_end_date, "'
                         --AND user_category <> 'hacker' AND user_category = 'player' 
                         AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                         AND register_platform = '", platform, "'
                         AND register_ip_country ='", country,"'
                         AND (migrate_date_orphaned IS NULL OR datediff(s,date_register,migrate_date_orphaned) > 86400)
                         AND (is_tester IS NULL OR is_tester != 'true')
                         AND user_category <> 'bot'", sep = "")

NGU_campaign <- dbGetQuery(spdb, campaign_query)

# Add a column with the real date of the country of install and the weekday
NGU_campaign$real_date <- NGU_campaign$date_register

NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+01:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+01:00"] + 3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+02:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+02:00"] + 2*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+03:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+03:00"] + 3*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+04:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+04:00"] + 4*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+05:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+05:00"] + 5*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+06:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+06:00"] + 6*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+07:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+07:00"] + 7*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+08:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+08:00"] + 8*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+09:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+09:00"] + 9*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+09:30"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+10:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+10:00"] + 10*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+10:30"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+11:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+11:00"] + 11*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+12:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "+12:00"] + 12*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-01:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-01:00"] - 3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-02:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-02:00"] - 2*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-03:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-03:00"] - 3*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-03:30"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-04:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-04:00"] - 4*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-05:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-05:00"] - 5*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-06:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-06:00"] - 6*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-07:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-07:00"] - 7*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-08:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-08:00"] - 8*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-09:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-09:00"] - 9*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-10:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-10:00"] - 10*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-11:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-11:00"] - 11*3600
NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-12:00"] <- NGU_campaign$real_date[NGU_campaign$register_ip_timezone == "-12:00"] - 12*3600

NGU_campaign$wday <- as.POSIXlt(NGU_campaign$real_date)$wday

# calculate average timezone shift in order to line up benchmark from other countries later on
NGU_campaign_avg_timezone <- mean(as.numeric(substr(NGU_campaign$register_ip_timezone, 1, 3)))


# DOWNLOAD BENCHMARK 1 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD NGU DATA FOR OTHER COUNTRIES")
cat("\n")

bm_countries <- NULL
if(!exists("benchmark_countries")) {
  bm_countries <- paste0("<> '", country,"'")
} else {
  for(c in benchmark_countries) {
    bm_countries <- paste0(bm_countries, ", ", "'", c, "'")
  }
  bm_countries <- substr(bm_countries, 2, nchar(bm_countries))
  bm_countries <- paste0("IN (", bm_countries, ")")
}

benchmark_query <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', date_register) as date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE date_register >='", query_start_date, "'
                         AND date_register <= '", query_end_date, "'
                         --AND user_category <> 'hacker' AND user_category = 'player' 
                         AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                         AND register_platform = '", platform, "'
                         AND register_ip_country ", bm_countries, "
                         AND (migrate_date_orphaned IS NULL OR datediff(s,date_register,migrate_date_orphaned) > 86400)
                         AND (is_tester IS NULL OR is_tester != 'true')
                         AND user_category <> 'bot'", sep = "")

NGU_benchmark <- dbGetQuery(spdb, benchmark_query)

# Add a column with the real date of the country of install and the weekday
NGU_benchmark$real_date <- NGU_benchmark$date_register

NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+01:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+01:00"] + 3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+02:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+02:00"] + 2*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+03:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+03:00"] + 3*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+04:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+04:00"] + 4*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+05:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+05:00"] + 5*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+06:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+06:00"] + 6*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+07:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+07:00"] + 7*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+08:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+08:00"] + 8*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+09:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+09:00"] + 9*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+09:30"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+10:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+10:00"] + 10*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+10:30"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+11:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+11:00"] + 11*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+12:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "+12:00"] + 12*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-01:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-01:00"] - 3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-02:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-02:00"] - 2*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-03:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-03:00"] - 3*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-03:30"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-04:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-04:00"] - 4*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-05:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-05:00"] - 5*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-06:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-06:00"] - 6*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-07:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-07:00"] - 7*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-08:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-08:00"] - 8*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-09:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-09:00"] - 9*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-10:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-10:00"] - 10*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-11:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-11:00"] - 11*3600
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-12:00"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-12:00"] - 12*3600

NGU_benchmark$wday <- as.POSIXlt(NGU_benchmark$real_date)$wday



# DOWNLOAD BENCHMARK 2 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD NGU DATA FOR PRE CAMPAIGN PERIOD")
cat("\n")

# setting defaults if no benchmark period given
if(exists("benchmark2_start_date")) {
  bm2_query_start_date <- as.POSIXct(benchmark2_start_date)
} else {
  bm2_query_start_date <- as.POSIXct(campaign_start) - days(28)
}

if(exists("benchmark2_end_date")) {
  bm2_query_end_date <- as.POSIXct(benchmark2_end_date)
} else {
  bm2_query_end_date <- as.POSIXct(campaign_start)
}

benchmark2_query <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', date_register) as date_register, register_ip_timezone, user_id
                          FROM ",schema,".t_user
                          WHERE date_register >='", bm2_query_start_date, "'
                          AND date_register <= '", bm2_query_end_date, "'
                          --AND user_category <> 'hacker' AND user_category = 'player' 
                          AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                          AND register_platform = '", platform, "'
                          AND register_ip_country = '", country,"'
                          AND (migrate_date_orphaned IS NULL OR datediff(s,date_register,migrate_date_orphaned) > 86400)
                          AND (is_tester IS NULL OR is_tester != 'true')
                          AND user_category <> 'bot'", sep = "")

NGU_benchmark2 <- dbGetQuery(spdb, benchmark2_query)

# Add a column with the real date of the country of install, the weekday and hour
NGU_benchmark2$real_date <- NGU_benchmark2$date_register

NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+01:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+01:00"] + 3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+02:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+02:00"] + 2*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+03:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+03:00"] + 3*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+04:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+04:00"] + 4*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+05:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+05:00"] + 5*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+06:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+06:00"] + 6*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+07:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+07:00"] + 7*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+08:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+08:00"] + 8*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+09:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+09:00"] + 9*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+09:30"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+10:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+10:00"] + 10*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+10:30"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+11:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+11:00"] + 11*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+12:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "+12:00"] + 12*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-01:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-01:00"] - 3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-02:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-02:00"] - 2*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-03:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-03:00"] - 3*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-03:30"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-04:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-04:00"] - 4*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-05:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-05:00"] - 5*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-06:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-06:00"] - 6*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-07:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-07:00"] - 7*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-08:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-08:00"] - 8*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-09:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-09:00"] - 9*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-10:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-10:00"] - 10*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-11:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-11:00"] - 11*3600
NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-12:00"] <- NGU_benchmark2$real_date[NGU_benchmark2$register_ip_timezone == "-12:00"] - 12*3600

# actually ignoring above and using date_register to calculate wday to be inline with count of actual NGUs
NGU_benchmark2$wday <- as.POSIXlt(NGU_benchmark2$date_register)$wday
NGU_benchmark2$hour <- unlist(strsplit(as.character(NGU_benchmark2$date_register), " "))[seq(2,length(NGU_benchmark2$date_register)*2,by = 2)]

NGU_benchmark2$hour <- as.character(NGU_benchmark2$hour) # Convert factors to characters

# Calculate NGUs per hour
# Get unique hour and weekday combinations to calculate average new users per minute
bm2_unique_hours_periods = data.frame("day" = rep(0:6, each=24), "hour" = rep(0:23, 7))

# Adding the number of times each hour period occurs in benchmark period
bm2_unique_hours_periods$occurences <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(floor_date(bm2_query_start_date, "hour")), to = as.POSIXlt(floor_date(bm2_query_end_date, "hour")), by = "hour"))$hour==bm2_unique_hours_periods$hour[r] & 
                                              as.POSIXlt(seq(from = as.POSIXlt(floor_date(bm2_query_start_date, "hour")), to = as.POSIXlt(floor_date(bm2_query_end_date, "hour")), by = "hour"))$wday==bm2_unique_hours_periods$day[r])
}
# Knock a bit off if query start or end date is not exactly on the hour
bm2_unique_hours_periods$occurences[which(bm2_unique_hours_periods$day == as.POSIXlt(bm2_query_start_date)$wday & bm2_unique_hours_periods$hour == as.POSIXlt(bm2_query_start_date)$hour)] <-
  (bm2_unique_hours_periods$occurences[which(bm2_unique_hours_periods$day == as.POSIXlt(bm2_query_start_date)$wday & bm2_unique_hours_periods$hour == as.POSIXlt(bm2_query_start_date)$hour)]
   - time_length(bm2_query_start_date - floor_date(bm2_query_start_date, "hour"), "hours"))

bm2_unique_hours_periods$occurences[which(bm2_unique_hours_periods$day == as.POSIXlt(bm2_query_end_date)$wday & bm2_unique_hours_periods$hour == as.POSIXlt(bm2_query_end_date)$hour)] <-
  (bm2_unique_hours_periods$occurences[which(bm2_unique_hours_periods$day == as.POSIXlt(bm2_query_end_date)$wday & bm2_unique_hours_periods$hour == as.POSIXlt(bm2_query_end_date)$hour)]
   - time_length(floor_date(bm2_query_end_date+60*60, "hour") - bm2_query_end_date, "hours"))


# Adding the number of NGU registrations for each hour period
bm2_unique_hours_periods$Total_NGUs <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_NGUs[r] <- sum(as.POSIXlt(NGU_benchmark2$date_register)$hour==bm2_unique_hours_periods$hour[r] &
                                              as.POSIXlt(NGU_benchmark2$date_register)$wday==bm2_unique_hours_periods$day[r])
}

# Calculating average NGUs per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_NGU_per_HOUR <- bm2_unique_hours_periods$Total_NGUs/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_NGU_per_MINUTE <- bm2_unique_hours_periods$Avg_NGU_per_HOUR/60



# DOWNLOAD BENCHMARK 3 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD BENCHMARK 3 DATA FROM REDSHIFT")
cat("\n")

# If not specified then setting default year to year before campaign
if(!exists('benchmark3_year')) benchmark3_year <- year(campaign_start)-1

# years lookback (used later on)
bm3_lookback <- year(campaign_start) - benchmark3_year

# setting query start dates
bm3_query_start_date <- query_start_date - years(1)
bm3_query_end_date <- query_end_date - years(1)

benchmark3_query <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', date_register) as date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE date_register >='", bm3_query_start_date, "'
                         AND date_register <= '", bm3_query_end_date, "'
                         --AND user_category <> 'hacker' AND user_category = 'player' 
                         AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                         AND register_platform = '", platform, "'
                         AND register_ip_country ='", country,"'
                         AND (migrate_date_orphaned IS NULL OR datediff(s,date_register,migrate_date_orphaned) > 86400)
                         AND (is_tester IS NULL OR is_tester != 'true')
                         AND user_category <> 'bot'", sep = "")

NGU_benchmark3 <- dbGetQuery(spdb, benchmark3_query)

# Add a column with the real date of the country of install and the weekday
NGU_benchmark3$real_date <- NGU_benchmark3$date_register

NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+01:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+01:00"] + 3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+02:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+02:00"] + 2*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+03:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+03:00"] + 3*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+04:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+04:00"] + 4*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+05:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+05:00"] + 5*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+06:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+06:00"] + 6*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+07:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+07:00"] + 7*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+08:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+08:00"] + 8*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+09:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+09:00"] + 9*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+09:30"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+10:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+10:00"] + 10*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+10:30"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+11:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+11:00"] + 11*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+12:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "+12:00"] + 12*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-01:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-01:00"] - 3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-02:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-02:00"] - 2*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-03:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-03:00"] - 3*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-03:30"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-04:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-04:00"] - 4*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-05:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-05:00"] - 5*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-06:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-06:00"] - 6*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-07:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-07:00"] - 7*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-08:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-08:00"] - 8*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-09:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-09:00"] - 9*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-10:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-10:00"] - 10*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-11:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-11:00"] - 11*3600
NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-12:00"] <- NGU_benchmark3$real_date[NGU_benchmark3$register_ip_timezone == "-12:00"] - 12*3600

NGU_benchmark3$wday <- as.POSIXlt(NGU_benchmark3$real_date)$wday

# calculate average timezone shift in order to line up benchmark from other countries later on
NGU_benchmark3_avg_timezone <- mean(as.numeric(substr(NGU_benchmark3$register_ip_timezone, 1, 3)))



# DOWNLOAD MARKETING INSTALLS DATA --------------------------------------------------------------------------------
cat("DOWNLOAD MARKETING INSTALLS NGU FROM REDSHIFT")
cat("\n")

if(campaign_type != 'Youtube') marketing_tracker_source <- "('set_to_this_string_so_that_marketing_installs_are_zero')"

campaign_query_marketing_NGU <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', date_register) as date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE date_register >='", query_start_date, "'
                         AND date_register <= '", query_end_date, "'
                         --AND user_category <> 'hacker' AND user_category = 'player' 
                         AND lower(register_source) in ", marketing_tracker_source, "
                         AND register_platform = '", platform, "'
                         AND register_ip_country ='", country,"'
                         AND (migrate_date_orphaned IS NULL OR datediff(s,date_register,migrate_date_orphaned) > 86400)
                         AND (is_tester IS NULL OR is_tester != 'true')
                         AND user_category <> 'bot'", sep = "")

NGU_campaign_marketing_tracked_installs <- dbGetQuery(spdb, campaign_query_marketing_NGU)



# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")

## PER HOUR ANALYSIS

# Separate analysis period into hours
NGU_per_hour <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-60*60), by = "hour"))
colnames(NGU_per_hour) <- "hour_start"
NGU_per_hour$hour_end <- NGU_per_hour$hour_start+60*60

# Add benchmark NGU per hour
NGU_per_hour$NGU_benchmark <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  NGU_per_hour$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > (NGU_per_hour$hour_start[r]+NGU_campaign_avg_timezone*60*60)
                                       & NGU_benchmark$real_date <= (NGU_per_hour$hour_end[r]+NGU_campaign_avg_timezone*60*60))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_hour$NGU_benchmark[NGU_per_hour$NGU_benchmark==0] <- 0.1

# Add actual NGU per hour
NGU_per_hour$NGU_actual <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  NGU_per_hour$NGU_actual[r] <- sum(NGU_campaign$date_register > NGU_per_hour$hour_start[r] & NGU_campaign$date_register <= NGU_per_hour$hour_end[r])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
NGU_per_hour$NGU_if_no_campaign <- rep(NGU_per_hour$NGU_actual[1], nrow(NGU_per_hour))
for(r in 2:nrow(NGU_per_hour)) {
  if(NGU_per_hour$hour_end[min(r+1,nrow(NGU_per_hour))] <= campaign_start) {
    NGU_per_hour$NGU_if_no_campaign[r] <- NGU_per_hour$NGU_actual[r]
  } else {
    NGU_per_hour$NGU_if_no_campaign[r] <- NGU_per_hour$NGU_if_no_campaign[r-1]*NGU_per_hour$NGU_benchmark[r]/NGU_per_hour$NGU_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_hour$NGU_benchmark2 <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  if(NGU_per_hour$hour_end[min(r+1,nrow(NGU_per_hour))] <= campaign_start) {
    NGU_per_hour$NGU_benchmark2[r] <- NGU_per_hour$NGU_actual[r]
  } else {
    NGU_per_hour$NGU_benchmark2[r] <- bm2_unique_hours_periods$Avg_NGU_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(NGU_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(NGU_per_hour$hour_start[r])$wday]
  }
}

# Add in benchmark 3 column (based on same country and time period in a previous year)
NGU_per_hour$NGU_benchmark3 <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  if(NGU_per_hour$hour_end[min(r+1,nrow(NGU_per_hour))] <= campaign_start) {
    NGU_per_hour$NGU_benchmark3[r] <- NGU_per_hour$NGU_actual[r]
  } else {
  NGU_per_hour$NGU_benchmark3[r] <- sum(NGU_benchmark3$date_register > NGU_per_hour$hour_start[r]-years(bm3_lookback) & NGU_benchmark3$date_register <= NGU_per_hour$hour_end[r]-years(bm3_lookback))
  }
}

# Add time since campaign column
NGU_per_hour$time_since_campaign <- time_length((NGU_per_hour$hour_start - as.POSIXct(campaign_start)), "hours")


## PER DAY ANALYSIS

# Separate analysis period into days
NGU_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(NGU_per_day) <- "day_start"
NGU_per_day$day_end <- NGU_per_day$day_start+24*60*60

# Add benchmark NGU per day
NGU_per_day$NGU_benchmark <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  NGU_per_day$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > (NGU_per_day$day_start[r]+NGU_campaign_avg_timezone*60*60)
                                      & NGU_benchmark$real_date <= (NGU_per_day$day_end[r]+NGU_campaign_avg_timezone*60*60))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_day$NGU_benchmark[NGU_per_day$NGU_benchmark==0] <- 0.1

# Add actual NGU per day
NGU_per_day$NGU_actual <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  NGU_per_day$NGU_actual[r] <- sum(NGU_campaign$date_register > NGU_per_day$day_start[r] & NGU_campaign$date_register <= NGU_per_day$day_end[r])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
NGU_per_day$NGU_if_no_campaign <- rep(NGU_per_day$NGU_actual[1], nrow(NGU_per_day))
for(r in 2:nrow(NGU_per_day)) {
  if(NGU_per_day$day_end[r] <= campaign_start) {
    NGU_per_day$NGU_if_no_campaign[r] <- NGU_per_day$NGU_actual[r]
  } else {
    NGU_per_day$NGU_if_no_campaign[r] <- NGU_per_day$NGU_if_no_campaign[r-1]*NGU_per_day$NGU_benchmark[r]/NGU_per_day$NGU_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_day$NGU_benchmark2 <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  if(NGU_per_day$day_end[r] <= campaign_start) {
    NGU_per_day$NGU_benchmark2[r] <- NGU_per_day$NGU_actual[r]
  } else {
    NGU_per_day$NGU_benchmark2[r] <- sum(NGU_per_hour$NGU_benchmark2[(r*24-23):(r*24)])
  }
}

# Add in benchmark 3 column (based on same country and time period in a previous year)
NGU_per_day$NGU_benchmark3 <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  if(NGU_per_day$day_end[r] <= campaign_start) {
    NGU_per_day$NGU_benchmark3[r] <- NGU_per_day$NGU_actual[r]
  } else {
    NGU_per_day$NGU_benchmark3[r] <- sum(NGU_benchmark3$date_register > NGU_per_day$day_start[r]-years(bm3_lookback) & NGU_benchmark3$date_register <= NGU_per_day$day_end[r]-years(bm3_lookback))
  }
}

# Add time since campaign column
NGU_per_day$days_since_campaign <- time_length((NGU_per_day$day_start - as.POSIXct(campaign_start)), "days")

# Add marketing tracked installs
NGU_per_day$marketing_tracked_NGUs <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  NGU_per_day$marketing_tracked_NGUs[r] <- sum(NGU_campaign_marketing_tracked_installs$date_register > NGU_per_day$day_start[r] & NGU_campaign_marketing_tracked_installs$date_register <= NGU_per_day$day_end[r])
}


## QUARTER DAY ANALYSIS

# Separate analysis period into quarter days
NGU_per_q_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-6*60*60), by = 6*60*60))
colnames(NGU_per_q_day) <- "q_day_start"
NGU_per_q_day$q_day_end <- NGU_per_q_day$q_day_start+6*60*60

# Add benchmark NGU per q_day
NGU_per_q_day$NGU_benchmark <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  NGU_per_q_day$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > (NGU_per_q_day$q_day_start[r]+NGU_campaign_avg_timezone*60*60)
                                        & NGU_benchmark$real_date <= (NGU_per_q_day$q_day_end[r]+NGU_campaign_avg_timezone*60*60))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_q_day$NGU_benchmark[NGU_per_q_day$NGU_benchmark==0] <- 0.1

# Add actual NGU per q_day
NGU_per_q_day$NGU_actual <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  NGU_per_q_day$NGU_actual[r] <- sum(NGU_campaign$date_register > NGU_per_q_day$q_day_start[r] & NGU_campaign$date_register <= NGU_per_q_day$q_day_end[r])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
NGU_per_q_day$NGU_if_no_campaign <- rep(NGU_per_q_day$NGU_actual[1], nrow(NGU_per_q_day))
for(r in 2:nrow(NGU_per_q_day)) {
  if(NGU_per_q_day$q_day_end[min(r+1,nrow(NGU_per_q_day))] <= campaign_start) {
    NGU_per_q_day$NGU_if_no_campaign[r] <- NGU_per_q_day$NGU_actual[r]
  } else {
    NGU_per_q_day$NGU_if_no_campaign[r] <- sum(NGU_per_hour$NGU_if_no_campaign[(r*6-5):(r*6)])
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_q_day$NGU_benchmark2 <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  if(NGU_per_q_day$q_day_end[min(r+1,nrow(NGU_per_q_day))] <= campaign_start) {
    NGU_per_q_day$NGU_benchmark2[r] <- NGU_per_q_day$NGU_actual[r]
  } else {
    NGU_per_q_day$NGU_benchmark2[r] <- sum(NGU_per_hour$NGU_benchmark2[(r*6-5):(r*6)])
  }
}

# Add time since campaign column
NGU_per_q_day$days_since_campaign <- time_length((NGU_per_q_day$q_day_start - as.POSIXct(campaign_start)), "days")



# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual NGUs
# For hourly analysis (Youtube)
NGU_plot_per_hour <- ggplot(data = NGU_per_hour) +
  geom_line(aes(x = NGU_per_hour$time_since_campaign, y = NGU_per_hour$NGU_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = NGU_per_hour$time_since_campaign, y = NGU_per_hour$NGU_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = NGU_per_hour$time_since_campaign, y = NGU_per_hour$NGU_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("NGUs") +
  ylim(0, max(NGU_per_hour$NGU_actual, NGU_per_hour$NGU_if_no_campaign)) +
  scale_x_continuous(breaks = seq(as.integer(min(NGU_per_hour$time_since_campaign)), as.integer(max(NGU_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
NGU_plot_per_day <- ggplot(data = NGU_per_day) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_benchmark3, colour = "Benchmark (prev year)"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_actual, colour = "Actual"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("NGUs") +
  ylim(0, max(NGU_per_day$NGU_actual, NGU_per_day$NGU_if_no_campaign)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)", "Benchmark (prev year)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon", "Benchmark (prev year)"="yellow3")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For quarter day analysis
NGU_plot_per_q_day <- ggplot(data = NGU_per_q_day) +
  geom_line(aes(x = NGU_per_q_day$days_since_campaign, y = NGU_per_q_day$NGU_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = NGU_per_q_day$days_since_campaign, y = NGU_per_q_day$NGU_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = NGU_per_q_day$days_since_campaign, y = NGU_per_q_day$NGU_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("NGUs") +
  ylim(0, max(NGU_per_q_day$NGU_actual, NGU_per_q_day$NGU_if_no_campaign)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

### Tables with results
# table of comparison between benchmarks and actual NGUs
NGU_table <- cbind(NGU_per_day[,c(1,2)],
                   paste0(wday(NGU_per_day$day_start+1, label = TRUE), "/", wday(NGU_per_day$day_end-1, label = TRUE)),
                   round(NGU_per_day[,4]),round(NGU_per_day[,5]),
                   round(pmax(NGU_per_day[,4]-NGU_per_day[,5],0)),
                   round(NGU_per_day[,6]),
                   round(pmax(NGU_per_day[,4]-NGU_per_day[,6],0)),
                   round(NGU_per_day[,7]),
                   round(pmax(NGU_per_day[,4]-NGU_per_day[,7],0)),
                   round(NGU_per_day$marketing_tracked_NGUs))[NGU_per_day$days_since_campaign>=0,]

colnames(NGU_table) <- c("day_start", "day_end", "weekday", "NGU_actual", "NGU_benchmark(other_countries)", "uplift(cf_other_countries)",
                         "NGU_benchmark(earlier_period)", "uplift(cf_earlier_period)",
                         "NGU_benchmark(prev_year)", "uplift(cf_prev_year)", "marketing tracked NGU")

# table with eCPI from both benchmark methods
NGU_total_uplift_bm1 <- sum(NGU_table$`uplift(cf_other_countries)`)
NGU_total_uplift_bm2 <- sum(NGU_table$`uplift(cf_earlier_period)`)
eCPI_bm1 <- cost_of_campaign/NGU_total_uplift_bm1
eCPI_bm2 <- cost_of_campaign/NGU_total_uplift_bm2

table_names <- c("Cost of campaign", "Uplift cf other countries benchmark", "eCPI cf other countries benchmark",
                 "Uplift cf earlier period benchmark", "eCPI cf earlier period benchmark")
NGU_eCPI <- data.frame(rep(0, length(table_names)), row.names = table_names)
NGU_eCPI[,1] <- c(cost_of_campaign, NGU_total_uplift_bm1, eCPI_bm1,
                  NGU_total_uplift_bm2, eCPI_bm2)



##############################################################
### ---------------- ACTIVE USER ANALYSIS ---------------- ###
##############################################################

# DOWNLOAD CAMPAIGN DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FROM REDSHIFT")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
campaign_query_active_users_per_hour <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                                        log_session_start.ip_timezone,
                                               count(distinct log_session_start.user_id) as active_users,
                                               datepart(y, log_session_start.datetime) as year,
                                               datepart(mm, log_session_start.datetime) as month,
                                               datepart(d, log_session_start.datetime) as day,
                                               datepart(hour, log_session_start.datetime) as hour
                                               FROM ",schema,".log_session_start
                                               LEFT JOIN ",schema,".t_user
                                               ON log_session_start.user_id = t_user.user_id
                                               WHERE log_session_start.datetime >= '", query_start_date, "'
                                               AND log_session_start.datetime <= '", query_end_date, "'
                                               --AND user_category <> 'hacker' AND user_category = 'player'
                                               AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                               AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                               AND platform = '", platform, "'
                                               AND register_ip_country ='", country,"'
                                               AND (is_tester is null or is_tester != 'true')
                                               AND t_user.user_category <> 'bot'
                                               GROUP BY log_session_start.ip_timezone, year, month, day, hour", sep = "")
AUs_plot_per_day
campaign_query_active_users_per_day <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                              log_session_start.ip_timezone,
                                              count(distinct log_session_start.user_id) as active_users,
                                              datepart(y, log_session_start.datetime) as year,
                                              datepart(mm, log_session_start.datetime) as month,
                                              datepart(d, log_session_start.datetime) as day
                                              FROM ",schema,".log_session_start
                                              LEFT JOIN ",schema,".t_user
                                              ON log_session_start.user_id = t_user.user_id
                                              WHERE log_session_start.datetime >= '", query_start_date, "'
                                              AND log_session_start.datetime <= '", query_end_date, "'
                                              --AND user_category <> 'hacker' AND user_category = 'player'
                                              AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                              AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                              AND platform = '", platform, "'
                                              AND register_ip_country ='", country,"'
                                              AND (is_tester is null or is_tester != 'true')
                                              AND t_user.user_category <> 'bot'
                                              GROUP BY log_session_start.ip_timezone, year, month, day", sep = "")

AUs_campaign_per_hour <- dbGetQuery(spdb, campaign_query_active_users_per_hour)
AUs_campaign_per_day <- dbGetQuery(spdb, campaign_query_active_users_per_day)

# Add a column with the real date of the timezone of install
AUs_campaign_per_hour$real_date <- AUs_campaign_per_hour$datetime + round(as.integer(substr(AUs_campaign_per_hour$ip_timezone, 1, 3)))*3600
AUs_campaign_per_hour$real_date[is.na(AUs_campaign_per_hour$real_date)] <- AUs_campaign_per_hour$datetime[is.na(AUs_campaign_per_hour$real_date)]

AUs_campaign_per_day$real_date <- AUs_campaign_per_day$datetime + round(as.integer(substr(AUs_campaign_per_day$ip_timezone, 1, 3)))*3600
AUs_campaign_per_day$real_date[is.na(AUs_campaign_per_day$real_date)] <- AUs_campaign_per_day$datetime[is.na(AUs_campaign_per_day$real_date)]

# Add start and end of each day/hour (based on Madrid time to be consistent with NGU calculation)
AUs_campaign_per_hour$hour_start <- floor_date(AUs_campaign_per_hour$datetime, "hour")
AUs_campaign_per_hour$hour_end <- ceiling_date(AUs_campaign_per_hour$datetime, "hour")

AUs_campaign_per_day$day_start <- floor_date(AUs_campaign_per_day$datetime, "day")
AUs_campaign_per_day$day_end <- ceiling_date(AUs_campaign_per_day$datetime, "day")


### Obtain AUs per day in respect of NGUs only in order to calculate AU uplift in repect of uplift in NGUs
campaign_query_active_NGUs_per_day <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                              log_session_start.ip_timezone,
                                              count(distinct log_session_start.user_id) as active_users,
                                              datepart(y, log_session_start.datetime) as year,
                                              datepart(mm, log_session_start.datetime) as month,
                                              datepart(d, log_session_start.datetime) as day
                                              FROM ",schema,".log_session_start
                                              LEFT JOIN ",schema,".t_user
                                              ON log_session_start.user_id = t_user.user_id
                                              WHERE log_session_start.datetime >= '", query_start_date, "'
                                              AND log_session_start.datetime <= '", query_end_date, "'
                                              AND t_user.date_register >= '", campaign_start, "'
                                              --AND user_category <> 'hacker' AND user_category = 'player'
                                              AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                              AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                              AND platform = '", platform, "'
                                              AND register_ip_country ='", country,"'
                                              AND (is_tester is null or is_tester != 'true')
                                              AND t_user.user_category <> 'bot'
                                              GROUP BY log_session_start.ip_timezone, year, month, day", sep = "")

active_NGUs_campaign_per_day <- dbGetQuery(spdb, campaign_query_active_NGUs_per_day)

# Add a column with the real date of the timezone of install
active_NGUs_campaign_per_day$real_date <- active_NGUs_campaign_per_day$datetime + round(as.integer(substr(active_NGUs_campaign_per_day$ip_timezone, 1, 3)))*3600
active_NGUs_campaign_per_day$real_date[is.na(active_NGUs_campaign_per_day$real_date)] <- active_NGUs_campaign_per_day$datetime[is.na(active_NGUs_campaign_per_day$real_date)]

# Add start and end of each day/hour (based on Madrid time to be consistent with NGU calculation)
active_NGUs_campaign_per_day$day_start <- floor_date(active_NGUs_campaign_per_day$datetime, "day")
active_NGUs_campaign_per_day$day_end <- ceiling_date(active_NGUs_campaign_per_day$datetime, "day")



# DOWNLOAD BENCHMARK 1 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FOR OTHER COUNTRIES")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
benchmark_query_active_users_per_hour <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                                          log_session_start.ip_timezone,
                                                count(distinct log_session_start.user_id) as active_users,
                                                datepart(y, log_session_start.datetime) as year,
                                                datepart(mm, log_session_start.datetime) as month,
                                                datepart(d, log_session_start.datetime) as day,
                                                datepart(hour, log_session_start.datetime) as hour
                                                FROM ",schema,".log_session_start
                                                LEFT JOIN ",schema,".t_user
                                                ON log_session_start.user_id = t_user.user_id
                                                WHERE log_session_start.datetime >= '", query_start_date, "'
                                                AND log_session_start.datetime <= '", query_end_date, "'
                                                --AND user_category <> 'hacker' AND user_category = 'player'
                                                AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                                AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                                AND platform = '", platform, "'
                                                AND register_ip_country ", bm_countries, "
                                                AND (is_tester is null or is_tester != 'true')
                                                AND t_user.user_category <> 'bot'
                                                GROUP BY log_session_start.ip_timezone, year, month, day, hour", sep = "")

benchmark_query_active_users_per_day <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                               log_session_start.ip_timezone,
                                               count(distinct log_session_start.user_id) as active_users,
                                               datepart(y, log_session_start.datetime) as year,
                                               datepart(mm, log_session_start.datetime) as month,
                                               datepart(d, log_session_start.datetime) as day
                                               FROM ",schema,".log_session_start
                                               LEFT JOIN ",schema,".t_user
                                               ON log_session_start.user_id = t_user.user_id
                                               WHERE log_session_start.datetime >= '", query_start_date, "'
                                               AND log_session_start.datetime <= '", query_end_date, "'
                                               --AND user_category <> 'hacker' AND user_category = 'player'
                                               AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                               AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                               AND platform = '", platform, "'
                                               AND register_ip_country ", bm_countries, "
                                               AND (is_tester is null or is_tester != 'true')
                                               AND t_user.user_category <> 'bot'
                                               GROUP BY log_session_start.ip_timezone, year, month, day", sep = "")

AUs_benchmark_per_hour <- dbGetQuery(spdb, benchmark_query_active_users_per_hour)
AUs_benchmark_per_day <- dbGetQuery(spdb, benchmark_query_active_users_per_day)

# Add a column with the real date of the timezone of install
AUs_benchmark_per_hour$real_date <- AUs_benchmark_per_hour$datetime + round(as.integer(substr(AUs_benchmark_per_hour$ip_timezone, 1, 3)))*3600
AUs_benchmark_per_hour$real_date[is.na(AUs_benchmark_per_hour$real_date)] <- AUs_benchmark_per_hour$datetime[is.na(AUs_benchmark_per_hour$real_date)]

AUs_benchmark_per_day$real_date <- AUs_benchmark_per_day$datetime + round(as.integer(substr(AUs_benchmark_per_day$ip_timezone, 1, 3)))*3600
AUs_benchmark_per_day$real_date[is.na(AUs_benchmark_per_day$real_date)] <- AUs_benchmark_per_day$datetime[is.na(AUs_benchmark_per_day$real_date)]

# Add start and end of each hour (based on "real_date" but adjusted to count users at the same time of day as the country being analysed)
AUs_benchmark_per_hour$hour_start <- floor_date(AUs_benchmark_per_hour$real_date - NGU_campaign_avg_timezone*60*60, "hour")
AUs_benchmark_per_hour$hour_end <- ceiling_date(AUs_benchmark_per_hour$real_date - NGU_campaign_avg_timezone*60*60, "hour")

AUs_benchmark_per_day$day_start <- floor_date(AUs_benchmark_per_day$real_date - NGU_campaign_avg_timezone*60*60, "day")
AUs_benchmark_per_day$day_end <- ceiling_date(AUs_benchmark_per_day$real_date - NGU_campaign_avg_timezone*60*60, "day")



# DOWNLOAD BENCHMARK 2 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FOR OTHER COUNTRIES")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
benchmark2_query_active_users_per_hour <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                                log_session_start.ip_timezone,
                                                count(distinct log_session_start.user_id) as active_users,
                                                datepart(y, log_session_start.datetime) as year,
                                                datepart(mm, log_session_start.datetime) as month,
                                                datepart(d, log_session_start.datetime) as day,
                                                datepart(hour, log_session_start.datetime) as hour_spain
                                                FROM ",schema,".log_session_start
                                                LEFT JOIN ",schema,".t_user
                                                ON log_session_start.user_id = t_user.user_id
                                                WHERE log_session_start.datetime >= '", bm2_query_start_date, "'
                                                AND log_session_start.datetime <= '", bm2_query_end_date, "'
                                                --AND user_category <> 'hacker' AND user_category = 'player'
                                                AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                                AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                                AND platform = '", platform, "'
                                                AND register_ip_country ='", country,"'
                                                AND (is_tester is null or is_tester != 'true')
                                                AND t_user.user_category <> 'bot'
                                                GROUP BY log_session_start.ip_timezone, year, month, day, hour_spain", sep = "")

benchmark2_query_active_users_per_day <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                               log_session_start.ip_timezone,
                                               count(distinct log_session_start.user_id) as active_users,
                                               datepart(y, log_session_start.datetime) as year,
                                               datepart(mm, log_session_start.datetime) as month,
                                               datepart(d, log_session_start.datetime) as day
                                               FROM ",schema,".log_session_start
                                               LEFT JOIN ",schema,".t_user
                                               ON log_session_start.user_id = t_user.user_id
                                               WHERE log_session_start.datetime >= '", bm2_query_start_date, "'
                                               AND log_session_start.datetime <= '", bm2_query_end_date, "'
                                               --AND user_category <> 'hacker' AND user_category = 'player'
                                               AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                               AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                               AND platform = '", platform, "'
                                               AND register_ip_country ='", country,"'
                                               AND (is_tester is null or is_tester != 'true')
                                               AND t_user.user_category <> 'bot'
                                               GROUP BY log_session_start.ip_timezone, year, month, day", sep = "")

AUs_benchmark2_per_hour <- dbGetQuery(spdb, benchmark2_query_active_users_per_hour)
AUs_benchmark2_per_day <- dbGetQuery(spdb, benchmark2_query_active_users_per_day)

# Add a column with the real date of the timezone of install
AUs_benchmark2_per_hour$real_date <- AUs_benchmark2_per_hour$datetime + round(as.integer(substr(AUs_benchmark2_per_hour$ip_timezone, 1, 3)))*3600
AUs_benchmark2_per_hour$real_date[is.na(AUs_benchmark2_per_hour$real_date)] <- AUs_benchmark2_per_hour$datetime[is.na(AUs_benchmark2_per_hour$real_date)]

AUs_benchmark2_per_day$real_date <- AUs_benchmark2_per_day$datetime + round(as.integer(substr(AUs_benchmark2_per_day$ip_timezone, 1, 3)))*3600
AUs_benchmark2_per_day$real_date[is.na(AUs_benchmark2_per_day$real_date)] <- AUs_benchmark2_per_day$datetime[is.na(AUs_benchmark2_per_day$real_date)]

# Add start and end of each hour
AUs_benchmark2_per_hour$hour_start <- floor_date(AUs_benchmark2_per_hour$datetime, "hour")
AUs_benchmark2_per_hour$hour_end <- ceiling_date(AUs_benchmark2_per_hour$datetime, "hour")

AUs_benchmark2_per_day$day_start <- floor_date(AUs_benchmark2_per_day$datetime, "day")
AUs_benchmark2_per_day$day_end <- ceiling_date(AUs_benchmark2_per_day$datetime, "day")


# Add the weekday and hour
AUs_benchmark2_per_hour$wday <- as.POSIXlt(AUs_benchmark2_per_hour$datetime)$wday
AUs_benchmark2_per_hour$hour <- unlist(strsplit(as.character(AUs_benchmark2_per_hour$datetime), " "))[seq(2,length(AUs_benchmark2_per_hour$datetime)*2,by = 2)]

NGU_benchmark2$hour <- as.character(NGU_benchmark2$hour) # Convert factors to characters

AUs_benchmark2_per_day$wday <- as.POSIXlt(AUs_benchmark2_per_day$datetime)$wday



### Add active users to bm2 data
# Adding the number of AU registrations for each hour period
bm2_unique_hours_periods$Total_AUs <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_AUs[r] <- sum(AUs_benchmark2_per_hour$active_users[as.POSIXlt(AUs_benchmark2_per_hour$datetime)$hour==bm2_unique_hours_periods$hour[r] &
                                                  as.POSIXlt(AUs_benchmark2_per_hour$datetime)$wday==bm2_unique_hours_periods$day[r]])
}

# Calculating average AUs per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_AUs_per_HOUR <- bm2_unique_hours_periods$Total_AUs/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_AUs_per_MINUTE <- bm2_unique_hours_periods$Avg_AUs_per_HOUR/60



### Creating a similar table for daily analysis
# Get unique hour and weekday combinations to calculate average new users per minute
bm2_unique_day_periods = data.frame("wday" = 0:6)

# Adding the number of times each wday occurs in benchmark period
bm2_unique_day_periods$occurences <- rep(0, nrow(bm2_unique_day_periods))
for(r in 1:nrow(bm2_unique_day_periods)) {
  bm2_unique_day_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(floor_date(bm2_query_start_date, "days")), to = as.POSIXlt(floor_date(bm2_query_end_date, "days")), by = "day"))$wday==bm2_unique_day_periods$wday[r])
}

# Knock a bit off if query start date is not exactly at the start/end of the day
bm2_unique_day_periods$occurences[which(bm2_unique_day_periods$wday == as.POSIXlt(bm2_query_start_date)$wday)] <-
  (bm2_unique_day_periods$occurences[which(bm2_unique_day_periods$wday == as.POSIXlt(bm2_query_start_date)$wday)]
   - time_length(bm2_query_start_date - floor_date(bm2_query_start_date, "day"), "days"))

# Knock a bit off query end date to correct for extra day added in seq fn above
bm2_unique_day_periods$occurences[which(bm2_unique_day_periods$wday == as.POSIXlt(bm2_query_end_date)$wday)] <-
  (bm2_unique_day_periods$occurences[which(bm2_unique_day_periods$wday == as.POSIXlt(bm2_query_end_date)$wday)]
   - time_length(floor_date(bm2_query_end_date+24*60*60, "day") - bm2_query_end_date, "days")) # NB: use floor_date and add 1 day instead of ceiling_date as ceiling and floor round to the same time when it is 00:00:00


# Adding the number of AUs for each day
bm2_unique_day_periods$Total_AUs <- rep(0, nrow(bm2_unique_day_periods))
for(r in 1:nrow(bm2_unique_day_periods)) {
  bm2_unique_day_periods$Total_AUs[r] <- sum(AUs_benchmark2_per_day$active_users[AUs_benchmark2_per_day$wday==bm2_unique_day_periods$wday[r]])
}

# Calculating average AUs per hour and per minute for each hour period
bm2_unique_day_periods$Avg_AUs_per_DAY <- bm2_unique_day_periods$Total_AUs/bm2_unique_day_periods$occurences



# DOWNLOAD CAMPAIGN MARKETING INSTALL DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD MARKETING INSTALL ACTIVE USER DATA FROM REDSHIFT")
cat("\n")

campaign_query_marketing_active_users_per_day <- paste( "SELECT  min(convert_timezone('UTC','Europe/Madrid', log_session_start.datetime)) as datetime,
                                              log_session_start.ip_timezone,
                                              count(distinct log_session_start.user_id) as active_users,
                                              datepart(y, log_session_start.datetime) as year,
                                              datepart(mm, log_session_start.datetime) as month,
                                              datepart(d, log_session_start.datetime) as day
                                              FROM ",schema,".log_session_start
                                              LEFT JOIN ",schema,".t_user
                                              ON log_session_start.user_id = t_user.user_id
                                              WHERE log_session_start.datetime >= '", query_start_date, "'
                                              AND log_session_start.datetime <= '", query_end_date, "'
                                              --AND user_category <> 'hacker' AND user_category = 'player'
                                              AND lower(register_source) in ", marketing_tracker_source, "
                                              AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                              AND platform = '", platform, "'
                                              AND register_ip_country ='", country,"'
                                              AND (is_tester is null or is_tester != 'true')
                                              AND t_user.user_category <> 'bot'
                                              GROUP BY log_session_start.ip_timezone, year, month, day", sep = "")

AUs_campaign_marketing_tracked_installs_per_day <- dbGetQuery(spdb, campaign_query_marketing_active_users_per_day)

# Add a column with the real date of the timezone of install
AUs_campaign_marketing_tracked_installs_per_day$real_date <- AUs_campaign_marketing_tracked_installs_per_day$datetime + round(as.integer(substr(AUs_campaign_marketing_tracked_installs_per_day$ip_timezone, 1, 3)))*3600
AUs_campaign_marketing_tracked_installs_per_day$real_date[is.na(AUs_campaign_marketing_tracked_installs_per_day$real_date)] <- AUs_campaign_marketing_tracked_installs_per_day$datetime[is.na(AUs_campaign_per_day$real_date)]

# Add start and end of each day/hour (based on Madrid time to be consistent with NGU calculation)
AUs_campaign_marketing_tracked_installs_per_day$day_start <- floor_date(AUs_campaign_marketing_tracked_installs_per_day$datetime, "day")
AUs_campaign_marketing_tracked_installs_per_day$day_end <- ceiling_date(AUs_campaign_marketing_tracked_installs_per_day$datetime, "day")



# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")

## PER HOUR ANALYSIS

# Separate analysis period into hours
AUs_per_hour <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-60*60), by = "hour"))
colnames(AUs_per_hour) <- "hour_start"
AUs_per_hour$hour_end <- AUs_per_hour$hour_start+60*60

# Add benchmark AUs per hour
AUs_per_hour$AUs_benchmark <- rep(0, nrow(AUs_per_hour))
for(r in 1:nrow(AUs_per_hour)) {
  AUs_per_hour$AUs_benchmark[r] <-
    sum(AUs_benchmark_per_hour$active_users * (time_length((AUs_benchmark_per_hour$hour_end+1 - AUs_per_hour$hour_start[r]), "hours") * (AUs_benchmark_per_hour$hour_end+1 >= AUs_per_hour$hour_start[r]) * (AUs_benchmark_per_hour$hour_end+1 <= AUs_per_hour$hour_end[r]) +
                                                 time_length((AUs_per_hour$hour_end[r] - AUs_benchmark_per_hour$hour_start+1), "hours") * (AUs_benchmark_per_hour$hour_start+1 > AUs_per_hour$hour_start[r]) * (AUs_benchmark_per_hour$hour_start+1 < AUs_per_hour$hour_end[r])))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
AUs_per_hour$AUs_benchmark[AUs_per_hour$AUs_benchmark==0] <- 0.1

# Add actual AUs per hour
AUs_per_hour$AUs_actual <- rep(0, nrow(AUs_per_hour))
for(r in 1:nrow(AUs_per_hour)) {
  AUs_per_hour$AUs_actual[r] <-
    sum(AUs_campaign_per_hour$active_users * (time_length((AUs_campaign_per_hour$hour_end+1 - AUs_per_hour$hour_start[r]), "hours") * (AUs_campaign_per_hour$hour_end+1 >= AUs_per_hour$hour_start[r]) * (AUs_campaign_per_hour$hour_end+1 <= AUs_per_hour$hour_end[r]) +
                                                time_length((AUs_per_hour$hour_end[r] - AUs_campaign_per_hour$hour_start)+1, "hours") * (AUs_campaign_per_hour$hour_start+1 > AUs_per_hour$hour_start[r]) * (AUs_campaign_per_hour$hour_start+1 < AUs_per_hour$hour_end[r])))
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
AUs_per_hour$AUs_if_no_campaign <- rep(AUs_per_hour$AUs_actual[1], nrow(AUs_per_hour))
for(r in 2:nrow(AUs_per_hour)) {
  if(AUs_per_hour$hour_end[min(r+1,nrow(AUs_per_hour))] <= campaign_start) {
    AUs_per_hour$AUs_if_no_campaign[r] <- AUs_per_hour$AUs_actual[r]
  } else {
    AUs_per_hour$AUs_if_no_campaign[r] <- AUs_per_hour$AUs_if_no_campaign[r-1]*AUs_per_hour$AUs_benchmark[r]/AUs_per_hour$AUs_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
AUs_per_hour$AUs_benchmark2 <- rep(0, nrow(AUs_per_hour))
for(r in 1:nrow(AUs_per_hour)) {
  if(AUs_per_hour$hour_end[min(r+1,nrow(AUs_per_hour))] <= campaign_start) {
    AUs_per_hour$AUs_benchmark2[r] <- AUs_per_hour$AUs_actual[r]
  } else {
    AUs_per_hour$AUs_benchmark2[r] <- bm2_unique_hours_periods$Avg_AUs_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(AUs_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(AUs_per_hour$hour_start[r])$wday]
  }
}

# Add time since campaign column
AUs_per_hour$time_since_campaign <- time_length((AUs_per_hour$hour_start - as.POSIXct(campaign_start)), "hours")


## PER DAY ANALYSIS

# Separate analysis period into days
AUs_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(AUs_per_day) <- "day_start"
AUs_per_day$day_end <- AUs_per_day$day_start+24*60*60

# Add benchmark AUs per day
AUs_per_day$AUs_benchmark <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$AUs_benchmark[r] <-
    sum(AUs_benchmark_per_day$active_users * (time_length((AUs_benchmark_per_day$day_end+1 - AUs_per_day$day_start[r]), "days") * (AUs_benchmark_per_day$day_end+1 >= AUs_per_day$day_start[r]) * (AUs_benchmark_per_day$day_end+1 <= AUs_per_day$day_end[r]) +
                                                time_length((AUs_per_day$day_end[r] - AUs_benchmark_per_day$day_start+1), "days") * (AUs_benchmark_per_day$day_start+1 > AUs_per_day$day_start[r]) * (AUs_benchmark_per_day$day_start+1 < AUs_per_day$day_end[r])))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
AUs_per_day$AUs_benchmark[AUs_per_day$AUs_benchmark==0] <- 0.1

# Add actual AUs per day
AUs_per_day$AUs_actual <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$AUs_actual[r] <-
    sum(AUs_campaign_per_day$active_users * (time_length((AUs_campaign_per_day$day_end+1 - AUs_per_day$day_start[r]), "days") * (AUs_campaign_per_day$day_end+1 >= AUs_per_day$day_start[r]) * (AUs_campaign_per_day$day_end+1 <= AUs_per_day$day_end[r]) +
                                               time_length((AUs_per_day$day_end[r] - AUs_campaign_per_day$day_start+1), "days") * (AUs_campaign_per_day$day_start+1 > AUs_per_day$day_start[r]) * (AUs_campaign_per_day$day_start+1 < AUs_per_day$day_end[r])))
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
AUs_per_day$AUs_if_no_campaign <- rep(AUs_per_day$AUs_actual[1], nrow(AUs_per_day))
for(r in 2:nrow(AUs_per_day)) {
  if(AUs_per_day$day_end[r] <= campaign_start) {
    AUs_per_day$AUs_if_no_campaign[r] <- AUs_per_day$AUs_actual[r]
  } else {
    AUs_per_day$AUs_if_no_campaign[r] <- AUs_per_day$AUs_if_no_campaign[r-1]*AUs_per_day$AUs_benchmark[r]/AUs_per_day$AUs_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
AUs_per_day$wday_start <- as.POSIXlt(AUs_per_day$day_start)$wday
AUs_per_day$wday_start_prop <- time_length((ceiling_date(AUs_per_day$day_start+1, "day") - AUs_per_day$day_start), "days") # has "+1" to deal with rounding when time is exactly "00:00:00" 
AUs_per_day$wday_end <- as.POSIXlt(AUs_per_day$day_end)$wday
AUs_per_day$wday_end_prop <- time_length((AUs_per_day$day_end - floor_date(AUs_per_day$day_end, "day")), "days")

AUs_per_day$AUs_benchmark2 <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  if(AUs_per_day$day_end[r] <= campaign_start) {
    AUs_per_day$AUs_benchmark2[r] <- AUs_per_day$AUs_actual[r]
  } else {
    AUs_per_day$AUs_benchmark2[r] <- bm2_unique_day_periods$Avg_AUs_per_DAY[bm2_unique_day_periods$wday==AUs_per_day$wday_start[r]]*AUs_per_day$wday_start_prop[r] +
      bm2_unique_day_periods$Avg_AUs_per_DAY[bm2_unique_day_periods$wday==AUs_per_day$wday_end[r]]*AUs_per_day$wday_end_prop[r]
  }
}

# Add time since campaign column
AUs_per_day$days_since_campaign <- time_length((AUs_per_day$day_start - as.POSIXct(campaign_start)), "days")

# Add in AUs in respect of NGUs only
AUs_per_day$active_NGUs <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$active_NGUs[r] <-
    sum(active_NGUs_campaign_per_day$active_users * (time_length((active_NGUs_campaign_per_day$day_end+1 - AUs_per_day$day_start[r]), "days") * (active_NGUs_campaign_per_day$day_end+1 >= AUs_per_day$day_start[r]) * (active_NGUs_campaign_per_day$day_end+1 <= AUs_per_day$day_end[r]) +
                                               time_length((AUs_per_day$day_end[r] - active_NGUs_campaign_per_day$day_start+1), "days") * (active_NGUs_campaign_per_day$day_start+1 > AUs_per_day$day_start[r]) * (active_NGUs_campaign_per_day$day_start+1 < AUs_per_day$day_end[r])))
}

# Proportion AUs in respect of NGUs to be only in respect of uplift in NGUs
AUs_per_day$active_NGUs_uplift <- pmax(0, AUs_per_day$active_NGUs*(NGU_per_day$NGU_actual - NGU_per_day$NGU_benchmark2)/NGU_per_day$NGU_actual)

# Calculate AUs in respect of reawakens
AUs_per_day$reawakens <- pmax(0, AUs_per_day$AUs_actual - AUs_per_day$AUs_benchmark2 - AUs_per_day$active_NGUs_uplift)

# Add AU from marketing installs
AUs_per_day$marketing_tracked_AUs <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$marketing_tracked_AUs[r] <-
    sum(AUs_campaign_marketing_tracked_installs_per_day$active_users * (time_length((AUs_campaign_marketing_tracked_installs_per_day$day_end+1 - AUs_per_day$day_start[r]), "days") * (AUs_campaign_marketing_tracked_installs_per_day$day_end+1 >= AUs_per_day$day_start[r]) * (AUs_campaign_marketing_tracked_installs_per_day$day_end+1 <= AUs_per_day$day_end[r]) +
                                               time_length((AUs_per_day$day_end[r] - AUs_campaign_marketing_tracked_installs_per_day$day_start+1), "days") * (AUs_campaign_marketing_tracked_installs_per_day$day_start+1 > AUs_per_day$day_start[r]) * (AUs_campaign_marketing_tracked_installs_per_day$day_start+1 < AUs_per_day$day_end[r])))
}


## QUARTER DAY ANALYSIS (NB - THIS IS UNIQUE AUs PER HOUR SUMMED OVER A 6 HOUR PERIOD, NOT UNIQUE USERS PER 6 HOUR PERIOD)

# Separate analysis period into quarter days
AUs_per_q_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-6*60*60), by = 6*60*60))
colnames(AUs_per_q_day) <- "q_day_start"
AUs_per_q_day$q_day_end <- AUs_per_q_day$q_day_start+6*60*60

# Add benchmark AUs per q_day
AUs_per_q_day$AUs_benchmark <- rep(0, nrow(AUs_per_q_day))
for(r in 1:nrow(AUs_per_q_day)) {
  AUs_per_q_day$AUs_benchmark[r] <- sum(AUs_per_hour$AUs_benchmark[(r*6-5):(r*6)])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
AUs_per_q_day$AUs_benchmark[AUs_per_q_day$AUs_benchmark==0] <- 0.1

# Add actual AUs per q_day
AUs_per_q_day$AUs_actual <- rep(0, nrow(AUs_per_q_day))
for(r in 1:nrow(AUs_per_q_day)) {
  AUs_per_q_day$AUs_actual[r] <- sum(AUs_per_hour$AUs_actual[(r*6-5):(r*6)])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
AUs_per_q_day$AUs_if_no_campaign <- rep(AUs_per_q_day$AUs_actual[1], nrow(AUs_per_q_day))
for(r in 2:nrow(AUs_per_q_day)) {
  if(AUs_per_q_day$q_day_end[r] <= campaign_start) {
    AUs_per_q_day$AUs_if_no_campaign[r] <- AUs_per_q_day$AUs_actual[r]
  } else {
    AUs_per_q_day$AUs_if_no_campaign[r] <- sum(AUs_per_hour$AUs_if_no_campaign[(r*6-5):(r*6)])
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
AUs_per_q_day$AUs_benchmark2 <- rep(0, nrow(AUs_per_q_day))
for(r in 1:nrow(AUs_per_q_day)) {
  if(AUs_per_q_day$q_day_end[r] <= campaign_start) {
    AUs_per_q_day$AUs_benchmark2[r] <- AUs_per_q_day$AUs_actual[r]
  } else {
    AUs_per_q_day$AUs_benchmark2[r] <- sum(AUs_per_hour$AUs_benchmark2[(r*6-5):(r*6)])
  }
}

# Add time since campaign column
AUs_per_q_day$days_since_campaign <- time_length((AUs_per_q_day$q_day_start - as.POSIXct(campaign_start)), "days")



# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual AUs
# For hourly analysis (Youtube)
AUs_plot_per_hour <- ggplot(data = AUs_per_hour) +
  geom_line(aes(x = AUs_per_hour$time_since_campaign, y = AUs_per_hour$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_hour$time_since_campaign, y = AUs_per_hour$AUs_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = AUs_per_hour$time_since_campaign, y = AUs_per_hour$AUs_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("active users") +
  ylim( c(0, max(AUs_per_hour$AUs_actual, AUs_per_hour$AUs_if_no_campaign))) +
  scale_x_continuous(breaks = seq(as.integer(min(AUs_per_hour$time_since_campaign)), as.integer(max(AUs_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
AUs_plot_per_day <- ggplot(data = AUs_per_day) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_actual-AUs_per_day$reawakens, colour = "Actual (excl. reawakens)"), linetype="dashed", size = 0.7) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("active users") +
  ylim(c(0, max(AUs_per_day$AUs_actual, AUs_per_day$AUs_if_no_campaign))) +
  scale_colour_manual("",
                      breaks = c("Actual", "Actual (excl. reawakens)", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Actual (excl. reawakens)"="seagreen2", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For quarter day analysis
AUs_plot_per_q_day <- ggplot(data = AUs_per_q_day) +
  geom_line(aes(x = AUs_per_q_day$days_since_campaign, y = AUs_per_q_day$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_q_day$days_since_campaign, y = AUs_per_q_day$AUs_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = AUs_per_q_day$days_since_campaign, y = AUs_per_q_day$AUs_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("active users") +
  ylim(0, max(AUs_per_q_day$AUs_actual, AUs_per_q_day$AUs_if_no_campaign)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

### Table of uplift in AUs
# table of comparison between benchmarks and actual AUs
AUs_table <- cbind(AUs_per_day[,c(1,2)],
                   paste0(wday(AUs_per_day$day_start+1, label = TRUE), "/", wday(AUs_per_day$day_end-1, label = TRUE)),
                   round(AUs_per_day[,4]), round(AUs_per_day[,5]),
                   round(pmax(AUs_per_day[,4]-AUs_per_day[,5],0)),
                   round(AUs_per_day[,10]),
                   round(pmax(AUs_per_day[,4]-AUs_per_day[,10],0)),
                   round(AUs_per_day[,13]),
                   round(AUs_per_day[,14]),
                   round(AUs_per_day[,15]))[AUs_per_day$days_since_campaign>=0,]

colnames(AUs_table) <- c("day_start", "day_end", "weekday", "AUs_actual", "AUs_benchmark(other_countries)", "uplift(cf_other_countries)",
                         "AUs_benchmark(earlier_period)", "[uplift(cf_earlier_period)", "= NGU uplift", "+ reawakens]", "marketing tracked AUs")



###########################################################
### ---------------- REVENUES ANALYSIS ---------------- ###
###########################################################

# DOWNLOAD CAMPAIGN DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FROM REDSHIFT")
cat("\n")

campaign_query_revenues <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', t_transaction.datetime) as datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                  FROM ",schema,".t_transaction
                                  LEFT JOIN ",schema,".log_session_start
                                  ON t_transaction.session_id = log_session_start.session_id
                                  AND t_transaction.user_id = log_session_start.user_id
                                  LEFT JOIN ",schema,".t_user
                                  ON t_transaction.user_id = t_user.user_id
                                  WHERE t_transaction.datetime >= '", query_start_date, "'
                                  AND t_transaction.datetime <= '", query_end_date, "'
                                  --AND t_user.user_category <> 'hacker' AND t_user.user_category = 'player'
                                  AND (t_user.register_source is NULL or lower(t_user.register_source) like '%organic%' or t_user.register_source = '')
                                  AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                  AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                  AND t_transaction.platform = '", platform, "'
                                  AND t_transaction.ip_country = '", country,"'
                                  AND (t_user.is_tester is null or t_user.is_tester != 'true')
                                  AND t_user.user_category <> 'bot'
                                  AND date_add('d', -1, '", query_start_date, "') <= log_session_start.datetime
                                  AND log_session_start.datetime <= '", query_end_date, "'", sep = "")

revenues_campaign <- dbGetQuery(spdb, campaign_query_revenues)

# Add a column with the real date of the country of install and the weekday
revenues_campaign$real_date <- revenues_campaign$datetime

revenues_campaign$real_date[revenues_campaign$ip_timezone == "+01:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+01:00"] + 3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+02:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+02:00"] + 2*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+03:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+03:00"] + 3*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+04:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+04:00"] + 4*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+05:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+05:00"] + 5*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+06:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+06:00"] + 6*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+07:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+07:00"] + 7*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+08:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+08:00"] + 8*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+09:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+09:00"] + 9*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+09:30"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+10:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+10:00"] + 10*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+10:30"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+11:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+11:00"] + 11*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "+12:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "+12:00"] + 12*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-01:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-01:00"] - 3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-02:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-02:00"] - 2*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-03:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-03:00"] - 3*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-03:30"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-04:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-04:00"] - 4*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-05:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-05:00"] - 5*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-06:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-06:00"] - 6*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-07:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-07:00"] - 7*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-08:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-08:00"] - 8*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-09:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-09:00"] - 9*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-10:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-10:00"] - 10*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-11:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-11:00"] - 11*3600
revenues_campaign$real_date[revenues_campaign$ip_timezone == "-12:00"] <- revenues_campaign$real_date[revenues_campaign$ip_timezone == "-12:00"] - 12*3600

revenues_campaign$wday <- as.POSIXlt(revenues_campaign$real_date)$wday

# Obtain revenues for users who registered before campaign start (used to calculate revenues split between NGU uplift and reawakens)
campaign_query_revenues_nonNGUs <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', t_transaction.datetime) as datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                  FROM ",schema,".t_transaction
                                  LEFT JOIN ",schema,".log_session_start
                                  ON t_transaction.session_id = log_session_start.session_id
                                  AND t_transaction.user_id = log_session_start.user_id
                                  LEFT JOIN ",schema,".t_user
                                  ON t_transaction.user_id = t_user.user_id
                                  WHERE t_transaction.datetime >= '", query_start_date, "'
                                  AND t_transaction.datetime <= '", query_end_date, "'
                                  AND t_user.date_register <= '", campaign_start, "'
                                  --AND t_user.user_category <> 'hacker' AND t_user.user_category = 'player'
                                  AND (t_user.register_source is NULL or lower(t_user.register_source) like '%organic%' or t_user.register_source = '')
                                  AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                  AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                  AND t_transaction.platform = '", platform, "'
                                  AND t_transaction.ip_country = '", country,"'
                                  AND (t_user.is_tester is null or t_user.is_tester != 'true')
                                  AND t_user.user_category <> 'bot'
                                  AND date_add('d', -1, '", query_start_date, "') <= log_session_start.datetime
                                  AND log_session_start.datetime <= '", query_end_date, "'", sep = "")

revenues_campaign_nonNGUs <- dbGetQuery(spdb, campaign_query_revenues_nonNGUs)



# DOWNLOAD BENCHMARK 1 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FOR OTHER COUNTRIES")
cat("\n")

benchmark_query_revenues <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', t_transaction.datetime) as datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                   FROM ",schema,".t_transaction
                                   LEFT JOIN ",schema,".log_session_start
                                   ON t_transaction.session_id = log_session_start.session_id
                                   AND t_transaction.user_id = log_session_start.user_id
                                   LEFT JOIN ",schema,".t_user
                                   ON t_transaction.user_id = t_user.user_id
                                   WHERE t_transaction.datetime >= '", query_start_date, "'
                                   AND t_transaction.datetime <= '", query_end_date, "'
                                   --AND t_user.user_category <> 'hacker' AND t_user.user_category = 'player'
                                   AND (t_user.register_source is NULL or lower(t_user.register_source) like '%organic%' or t_user.register_source = '')
                                   AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                   AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                   AND t_transaction.platform = '", platform, "'
                                   AND t_transaction.ip_country ", bm_countries, "
                                   AND (t_user.is_tester is null or t_user.is_tester != 'true')
                                   AND t_user.user_category <> 'bot'
                                   AND date_add('d', -1, '", query_start_date, "') <= log_session_start.datetime
                                   AND log_session_start.datetime <= '", query_end_date, "'", sep = "")

revenues_benchmark <- dbGetQuery(spdb, benchmark_query_revenues)

# Add a column with the real date of the country of install and the weekday
revenues_benchmark$real_date <- revenues_benchmark$datetime

revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+01:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+01:00"] + 3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+02:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+02:00"] + 2*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+03:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+03:00"] + 3*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+04:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+04:00"] + 4*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+05:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+05:00"] + 5*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+06:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+06:00"] + 6*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+07:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+07:00"] + 7*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+08:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+08:00"] + 8*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+09:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+09:00"] + 9*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+09:30"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+10:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+10:00"] + 10*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+10:30"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+11:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+11:00"] + 11*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+12:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "+12:00"] + 12*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-01:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-01:00"] - 3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-02:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-02:00"] - 2*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-03:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-03:00"] - 3*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-03:30"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-04:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-04:00"] - 4*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-05:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-05:00"] - 5*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-06:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-06:00"] - 6*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-07:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-07:00"] - 7*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-08:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-08:00"] - 8*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-09:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-09:00"] - 9*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-10:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-10:00"] - 10*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-11:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-11:00"] - 11*3600
revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-12:00"] <- revenues_benchmark$real_date[revenues_benchmark$ip_timezone == "-12:00"] - 12*3600

revenues_benchmark$wday <- as.POSIXlt(revenues_benchmark$real_date)$wday



# DOWNLOAD BENCHMARK 2 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FOR OTHER COUNTRIES")
cat("\n")

benchmark2_query_revenues <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', t_transaction.datetime) as datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                   FROM ",schema,".t_transaction
                                   LEFT JOIN ",schema,".log_session_start
                                   ON t_transaction.session_id = log_session_start.session_id
                                   AND t_transaction.user_id = log_session_start.user_id
                                   LEFT JOIN ",schema,".t_user
                                   ON t_transaction.user_id = t_user.user_id
                                   WHERE t_transaction.datetime >= '", bm2_query_start_date, "'
                                   AND t_transaction.datetime <= '", bm2_query_end_date, "'
                                   --AND t_user.user_category <> 'hacker' AND t_user.user_category = 'player'
                                   AND (t_user.register_source is NULL or lower(t_user.register_source) like '%organic%' or t_user.register_source = '')
                                   AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                   AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                   AND t_transaction.platform = '", platform, "'
                                   AND t_transaction.ip_country = '", country,"'
                                   AND (t_user.is_tester is null or t_user.is_tester != 'true')
                                   AND t_user.user_category <> 'bot'
                                   AND date_add('d', -1, '", bm2_query_start_date, "') <= log_session_start.datetime
                                   AND log_session_start.datetime <= '", bm2_query_end_date, "'", sep = "")

revenues_benchmark2 <- dbGetQuery(spdb, benchmark2_query_revenues)

# Add a column with the real date of the country of install, the weekday and hour
revenues_benchmark2$real_date <- revenues_benchmark2$datetime

revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+01:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+01:00"] + 3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+02:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+02:00"] + 2*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+03:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+03:00"] + 3*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+04:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+04:00"] + 4*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+05:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+05:00"] + 5*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+06:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+06:00"] + 6*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+07:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+07:00"] + 7*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+08:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+08:00"] + 8*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+09:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+09:00"] + 9*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+09:30"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+09:30"] + 9*3600 + 3600*0.5
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+10:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+10:00"] + 10*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+10:30"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+10:30"] + 10*3600 + 3600*0.5
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+11:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+11:00"] + 11*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+12:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "+12:00"] + 12*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-01:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-01:00"] - 3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-02:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-02:00"] - 2*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-03:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-03:00"] - 3*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-03:30"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-03:30"] - 3*3600 + 3600*0.5
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-04:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-04:00"] - 4*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-05:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-05:00"] - 5*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-06:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-06:00"] - 6*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-07:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-07:00"] - 7*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-08:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-08:00"] - 8*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-09:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-09:00"] - 9*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-10:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-10:00"] - 10*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-11:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-11:00"] - 11*3600
revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-12:00"] <- revenues_benchmark2$real_date[revenues_benchmark2$ip_timezone == "-12:00"] - 12*3600

# Add the weekday and hour based on Madrid time to be consistent with NGU analysis
revenues_benchmark2$wday <- as.POSIXlt(revenues_benchmark2$datetime)$wday
revenues_benchmark2$hour <- unlist(strsplit(as.character(revenues_benchmark2$datetime), " "))[seq(2,length(revenues_benchmark2$datetime)*2,by = 2)]

revenues_benchmark2$hour <- as.character(revenues_benchmark2$hour) # Convert factors to characters


### Add revenues per hour to bm2 data
# Adding the average revenues for each hour period
bm2_unique_hours_periods$Total_revenues <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_revenues[r] <- sum(revenues_benchmark2$net_value[as.POSIXlt(revenues_benchmark2$datetime)$hour==bm2_unique_hours_periods$hour[r] &
                                                                                      as.POSIXlt(revenues_benchmark2$datetime)$wday==bm2_unique_hours_periods$day[r]])
}

# Calculating average revenues per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_revenues_per_HOUR <- bm2_unique_hours_periods$Total_revenues/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_revenues_per_MINUTE <- bm2_unique_hours_periods$Avg_revenues_per_HOUR/60



# DOWNLOAD CAMPAIGN MARKETING TRACKED DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD MARKETING TRACKED USER REVENUES DATA FROM REDSHIFT")
cat("\n")

campaign_query_marketing_tracked_user_revenues <- paste( "SELECT convert_timezone('UTC','Europe/Madrid', t_transaction.datetime) as datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                  FROM ",schema,".t_transaction
                                  LEFT JOIN ",schema,".log_session_start
                                  ON t_transaction.session_id = log_session_start.session_id
                                  AND t_transaction.user_id = log_session_start.user_id
                                  LEFT JOIN ",schema,".t_user
                                  ON t_transaction.user_id = t_user.user_id
                                  WHERE t_transaction.datetime >= '", query_start_date, "'
                                  AND t_transaction.datetime <= '", query_end_date, "'
                                  AND lower(t_user.register_source) in ", marketing_tracker_source, "
                                  AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                  AND t_transaction.platform = '", platform, "'
                                  AND t_transaction.ip_country = '", country,"'
                                  AND date_add('d', -1, '", query_start_date, "') <= log_session_start.datetime
                                  AND log_session_start.datetime <= '", query_end_date, "'", sep = "")

revenues_campaign_marketing_tracked_installs <- dbGetQuery(spdb, campaign_query_marketing_tracked_user_revenues)

# Add a column with the real date of the country of install and the weekday
revenues_campaign_marketing_tracked_installs$real_date <- revenues_campaign_marketing_tracked_installs$datetime



# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")


## PER HOUR ANALYSIS

# Separate analysis period into hours
revenues_per_hour <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-60*60), by = "hour"))
colnames(revenues_per_hour) <- "hour_start"
revenues_per_hour$hour_end <- revenues_per_hour$hour_start+60*60

# Add benchmark revenues per hour
revenues_per_hour$revenues_benchmark <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  revenues_per_hour$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > (revenues_per_hour$hour_start[r]+NGU_campaign_avg_timezone*60*60)
                                                                              & revenues_benchmark$real_date <= (revenues_per_hour$hour_end[r]+NGU_campaign_avg_timezone*60*60)])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_hour$revenues_benchmark[revenues_per_hour$revenues_benchmark==0] <- 0.1

# Add actual revenues per hour
revenues_per_hour$revenues_actual <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  revenues_per_hour$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$datetime > revenues_per_hour$hour_start[r] & revenues_campaign$datetime <= revenues_per_hour$hour_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_hour$revenues_if_no_campaign <- rep(revenues_per_hour$revenues_actual[1], nrow(revenues_per_hour))
for(r in 2:nrow(revenues_per_hour)) {
  if(revenues_per_hour$hour_end[min(r+1,nrow(revenues_per_hour))] <= campaign_start) {
    revenues_per_hour$revenues_if_no_campaign[r] <- revenues_per_hour$revenues_actual[r]
  } else {
    revenues_per_hour$revenues_if_no_campaign[r] <- min(revenues_per_hour$revenues_actual[r],
                                                        revenues_per_hour$revenues_if_no_campaign[r-1]*revenues_per_hour$revenues_benchmark[r]/revenues_per_hour$revenues_benchmark[r-1])
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
revenues_per_hour$revenues_benchmark2 <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  if(revenues_per_hour$hour_end[min(r+1,nrow(revenues_per_hour))] <= campaign_start) {
    revenues_per_hour$revenues_benchmark2[r] <- revenues_per_hour$revenues_actual[r]
  } else {
    revenues_per_hour$revenues_benchmark2[r] <- bm2_unique_hours_periods$Avg_revenues_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(revenues_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(revenues_per_hour$hour_start[r])$wday]
  }
}

# Add time since campaign column
revenues_per_hour$time_since_campaign <- time_length((revenues_per_hour$hour_start - as.POSIXct(campaign_start)), "hours")



## PER DAY ANALYSIS

# Separate analysis period into days
revenues_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(revenues_per_day) <- "day_start"
revenues_per_day$day_end <- revenues_per_day$day_start+24*60*60

# Add benchmark revenues per day
revenues_per_day$revenues_benchmark <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > (revenues_per_day$day_start[r]+NGU_campaign_avg_timezone*60*60)
                                                                             & revenues_benchmark$real_date <= (revenues_per_day$day_end[r]+NGU_campaign_avg_timezone*60*60)])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_day$revenues_benchmark[revenues_per_day$revenues_benchmark==0] <- 0.1

# Add actual revenues per day
revenues_per_day$revenues_actual <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$datetime > revenues_per_day$day_start[r] & revenues_campaign$datetime <= revenues_per_day$day_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_day$revenues_if_no_campaign <- rep(revenues_per_day$revenues_actual[1], nrow(revenues_per_day))
for(r in 2:nrow(revenues_per_day)) {
  if(revenues_per_day$day_end[r] <= campaign_start) {
    revenues_per_day$revenues_if_no_campaign[r] <- revenues_per_day$revenues_actual[r]
  } else {
    revenues_per_day$revenues_if_no_campaign[r] <- min(revenues_per_day$revenues_if_no_campaign[r-1]*revenues_per_day$revenues_benchmark[r]/revenues_per_day$revenues_benchmark[r-1],
                                                       revenues_per_day$revenues_actual[r])
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
revenues_per_day$revenues_benchmark2 <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  if(revenues_per_day$day_end[r] <= campaign_start) {
    revenues_per_day$revenues_benchmark2[r] <- revenues_per_day$revenues_actual[r]
  } else {
    revenues_per_day$revenues_benchmark2[r] <- sum(revenues_per_hour$revenues_benchmark2[(r*24-23):(r*24)])
  }
}

# Add time since campaign column
revenues_per_day$days_since_campaign <- time_length((revenues_per_day$day_start - as.POSIXct(campaign_start)), "days")

# Add non-NGU total revenues
revenues_per_day$revenues_actual_nonNGU <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_actual_nonNGU[r] <- sum(revenues_campaign_nonNGUs$net_value[revenues_campaign_nonNGUs$datetime > revenues_per_day$day_start[r] & revenues_campaign_nonNGUs$datetime <= revenues_per_day$day_end[r]])
}

# Add revenues for reawakens =  non-NGU_revenues - benchmark revenues (assumes organic NGU revenues are negligible!!)   ### prev doing revenues_actual_nonNGU - (nonNGU active users - reawaken AUs) * RPAU_benchmark
revenues_per_day$revenues_reawakens <- pmax(0, revenues_per_day$revenues_actual_nonNGU - revenues_per_day$revenues_benchmark2)

# Add revenues for NGU uplift users (assumes NGU for uplift users the same as organic NGU - maybe not realistic but organic NGUs only small % of total NGUs)
revenues_per_day$revenues_NGU_uplift <- pmax(0, (revenues_per_day$revenues_actual-revenues_per_day$revenues_actual_nonNGU) * (NGU_per_day$NGU_actual - NGU_per_day$NGU_benchmark2)/NGU_per_day$NGU_actual )

# Add revenues from marketing tracked installs
revenues_per_day$revenues_marketing_tracked_installs <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_marketing_tracked_installs[r] <- sum(revenues_campaign_marketing_tracked_installs$net_value[revenues_campaign_marketing_tracked_installs$datetime > revenues_per_day$day_start[r] & revenues_campaign_marketing_tracked_installs$datetime <= revenues_per_day$day_end[r]])
}



## QUARTER DAY ANALYSIS

# Separate analysis period into quarter days
revenues_per_q_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-6*60*60), by = 6*60*60))
colnames(revenues_per_q_day) <- "q_day_start"
revenues_per_q_day$q_day_end <- revenues_per_q_day$q_day_start+6*60*60

# Add benchmark revenues per q_day
revenues_per_q_day$revenues_benchmark <- rep(0, nrow(revenues_per_q_day))
for(r in 1:nrow(revenues_per_q_day)) {
  revenues_per_q_day$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > (revenues_per_q_day$q_day_start[r]+NGU_campaign_avg_timezone*60*60)
                                                                               & revenues_benchmark$real_date <= (revenues_per_q_day$q_day_end[r]+NGU_campaign_avg_timezone*60*60)])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_q_day$revenues_benchmark[revenues_per_q_day$revenues_benchmark==0] <- 0.1

# Add actual revenues per q_day
revenues_per_q_day$revenues_actual <- rep(0, nrow(revenues_per_q_day))
for(r in 1:nrow(revenues_per_q_day)) {
  revenues_per_q_day$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$datetime > revenues_per_q_day$q_day_start[r] & revenues_campaign$datetime <= revenues_per_q_day$q_day_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_q_day$revenues_if_no_campaign <- rep(revenues_per_q_day$revenues_actual[1], nrow(revenues_per_q_day))
for(r in 2:nrow(revenues_per_q_day)) {
  if(revenues_per_q_day$q_day_end[min(r+1,nrow(revenues_per_q_day))] <= campaign_start) {
    revenues_per_q_day$revenues_if_no_campaign[r] <- revenues_per_q_day$revenues_actual[r]
  } else {
    revenues_per_q_day$revenues_if_no_campaign[r] <- sum(revenues_per_hour$revenues_if_no_campaign[(r*6-5):(r*6)])
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
revenues_per_q_day$revenues_benchmark2 <- rep(0, nrow(revenues_per_q_day))
for(r in 1:nrow(revenues_per_q_day)) {
  if(revenues_per_q_day$q_day_end[min(r+1,nrow(revenues_per_q_day))] <= campaign_start) {
    revenues_per_q_day$revenues_benchmark2[r] <- revenues_per_q_day$revenues_actual[r]
  } else {
    revenues_per_q_day$revenues_benchmark2[r] <- sum(revenues_per_hour$revenues_benchmark2[(r*6-5):(r*6)])
  }
}

# Add time since campaign column
revenues_per_q_day$days_since_campaign <- time_length((revenues_per_q_day$q_day_start - as.POSIXct(campaign_start)), "days")

# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual revenues
# For hourly analysis (Youtube)
revenues_plot_per_hour <- ggplot(data = revenues_per_hour) +
  geom_line(aes(x = revenues_per_hour$time_since_campaign, y = revenues_per_hour$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_hour$time_since_campaign, y = revenues_per_hour$revenues_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = revenues_per_hour$time_since_campaign, y = revenues_per_hour$revenues_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("revenues") +
  ylim(0, max(revenues_per_hour$revenues_actual, revenues_per_hour$revenues_actual)) +
  scale_x_continuous(breaks = seq(as.integer(min(revenues_per_hour$time_since_campaign)), as.integer(max(revenues_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
revenues_plot_per_day <- ggplot(data = revenues_per_day) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_actual-revenues_per_day$revenues_reawakens, colour = "Actual (excl. reawakens)"), linetype="dashed", size = 0.7) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("revenues") +
  ylim(0, max(revenues_per_day$revenues_actual, revenues_per_day$revenues_actual)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Actual (excl. reawakens)", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Actual (excl. reawakens)"="seagreen2", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For quarter day analysis
revenues_plot_per_q_day <- ggplot(data = revenues_per_q_day) +
  geom_line(aes(x = revenues_per_q_day$days_since_campaign, y = revenues_per_q_day$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_q_day$days_since_campaign, y = revenues_per_q_day$revenues_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = revenues_per_q_day$days_since_campaign, y = revenues_per_q_day$revenues_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("revenues") +
  ylim(0, max(revenues_per_q_day$revenues_actual, revenues_per_q_day$revenues_actual)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

### Table of uplift in revenues
# table of comparison between benchmarks and actual revenues
revenues_table <- cbind(revenues_per_day[,c(1,2)],
                   paste0(wday(revenues_per_day$day_start+1, label = TRUE), "/", wday(revenues_per_day$day_end-1, label = TRUE)),
                   round(revenues_per_day[,4]), round(revenues_per_day[,5]),
                   round(pmax(revenues_per_day[,4]-revenues_per_day[,5],0)),
                   round(revenues_per_day[,6]),
                   round(pmax(revenues_per_day[,4]-revenues_per_day[,6],0)),
                   round(revenues_per_day$revenues_NGU_uplift),
                   round(revenues_per_day$revenues_reawakens),
                   round(revenues_per_day$revenues_marketing_tracked_installs, 2))[AUs_per_day$days_since_campaign>=0,]

colnames(revenues_table) <- c("day_start", "day_end", "weekday", "revenues_actual", "revenues_benchmark(other_countries)", "uplift(cf_other_countries)",
                         "revenues_benchmark(earlier_period)", "[uplift(cf_earlier_period)", "= NGU uplift", "+ reawakens]", "marketing tracked revenues")


### ARPU
ARPU_table <- cbind(
  # ARPU_NGU_uplift_only
  round(as.numeric(revenues_table$`= NGU uplift`/AUs_table$`= NGU uplift`),3),
  
  # ARPU marketing tracked NGUs only
  round(as.numeric(revenues_table$`marketing tracked revenues`/AUs_table$`marketing tracked AUs`),3),
  
  # ARPU_reawakens
  round(as.numeric(revenues_table$`+ reawakens]`/AUs_table$`+ reawakens]`),3),
  
  # ARPU_all_"organic"_users
  round(as.numeric(revenues_table$revenues_actual/AUs_table$AUs_actual),3),
  
  # ARPU_all_users
  rep("see SPBO", nrow(revenues_table))
)


colnames(ARPU_table) <- c("ARPU_NGU_uplift", "ARPU_marketing_tracked_installs", "ARPU_reawakens", "ARPU_'organic'_users", "ARPU_all_users")

### RPI table
campaign_RPI_table <- cbind(
  # day start, day end and weekday
  revenues_table$day_start, revenues_table$day_end, as.character(revenues_table$weekday),
  
  # cumulative NGUs
  sapply(1:analysis_period, function(d) sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign>=0][1:d])),
  
  # cumulative revenues
  round(
    sapply(1:analysis_period, function(d) sum((revenues_per_day$revenues_actual
                                             - revenues_per_day$revenues_actual_nonNGU
                                             )[revenues_per_day$days_since_campaign>=0][1:d]))
  ),
  
  # RPI
  round(
    sapply(1:analysis_period, function(d) sum((revenues_per_day$revenues_actual
                                             - revenues_per_day$revenues_actual_nonNGU
                                             )[revenues_per_day$days_since_campaign>=0][1:d]))
    /sapply(1:analysis_period, function(d) sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign>=0][1:d]))
  , 3)
)

colnames(campaign_RPI_table) <- c("day_start", "day_end", "weekday", "cumulative NGUs", "cumulative revenues", "RPI")




