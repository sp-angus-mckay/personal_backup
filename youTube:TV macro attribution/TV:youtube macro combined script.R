
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
game <- "ML"

# platform. "ios" or "android"
platform <- "ios"

# name of the country. "FR", "US", "AU", ...
country <- 'DE'

# dates in format: '2000-01-01 01:00:00'.
# Campaign dates, cost and period of days to analyse after campaign started
campaign_type <- 'Youtube' # 'TV' or 'Youtube'
campaign_start  <- '2016-09-02 10:29:00'
analysis_period <- 7 # in days
cost_of_campaign <- 300

# Benchmark 1: select benchmark countries as a list or leave blank to include all countries except country of analysis
#benchmark_countries <- c('ES', 'FR')

# Benchmark 2: select benchmark period to analyse or leave blank to use 28 days before campaign start
benchmark2_start_date <- '2016-08-02 10:29:00'
benchmark2_end_date <- '2016-09-02 10:29:00'



# START -----------------------------------------------------------------------------------------------------

# Downloads and loads the required packages
if (!require("RPostgreSQL")) install.packages("RPostgreSQL"); library(RPostgreSQL) 
if (!require("ggplot2")) install.packages("ggplot2"); library(ggplot2)


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

analysis_period_pre_campaign <- if(campaign_type == 'Youtube') 2 else 7 # in days
analysis_start_date <- as.POSIXct(campaign_start) - analysis_period_pre_campaign*24*60*60
analysis_end_date <- as.POSIXct(campaign_start) + analysis_period*24*60*60

# adding extra 24 hours either side so that analysis period is covered after adjusting for time-zones
query_start_date <- as.POSIXct(analysis_start_date - 24*60*60)
query_end_date <- as.POSIXct(analysis_end_date + 24*60*60)

campaign_query <- paste( "SELECT date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE date_register >='", query_start_date, "'
                         AND date_register <= '", query_end_date, "'
                         AND user_category <> 'hacker' AND user_category = 'player' 
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

benchmark_query <- paste( "SELECT date_register, register_ip_timezone, user_id
                         FROM ",schema,".t_user
                         WHERE (date_register+register_ip_timezone) >='", query_start_date, "'
                         AND (date_register+register_ip_timezone) <= '", query_end_date, "'
                         AND user_category <> 'hacker' AND user_category = 'player' 
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

# adding extra 24 hours either side so that analysis period is covered after adjusting for time-zones
if(exists("benchmark2_start_date")) {
  bm2_query_start_date <- as.POSIXct(benchmark2_start_date) - 24*60*60
} else {
  bm2_query_start_date <- as.POSIXct(campaign_start) - 28*24*60*60
}

if(exists("benchmark2_end_date")) {
  bm2_query_end_date <- as.POSIXct(benchmark2_end_date) - 24*60*60
} else {
  bm2_query_end_date <- as.POSIXct(campaign_start)
}

benchmark2_query <- paste( "SELECT date_register, register_ip_timezone, user_id
                          FROM ",schema,".t_user
                          WHERE (date_register+register_ip_timezone) >='", bm2_query_start_date, "'
                          AND (date_register+register_ip_timezone) <= '", bm2_query_end_date, "'
                          AND user_category <> 'hacker' AND user_category = 'player' 
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

NGU_benchmark2$wday <- as.POSIXlt(NGU_benchmark2$real_date)$wday
NGU_benchmark2$hour <- unlist(strsplit(as.character(NGU_benchmark2$real_date), " "))[seq(2,length(NGU_benchmark2$real_date)*2,by = 2)]

NGU_benchmark2$hour <- as.character(NGU_benchmark2$hour) # Convert factors to characters

# Calculate NGUs per hour
# Get unique hour and weekday combinations to calculate average new users per minute
bm2_unique_hours_periods = data.frame("day" = rep(0:6, each=24), "hour" = rep(0:23, 7))

# Adding the number of times each hour period occurs in benchmark period
bm2_unique_hours_periods$occurences <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(min(NGU_benchmark2$real_date)), to = as.POSIXlt(max(NGU_benchmark2$real_date)), by = "hour"))$hour==bm2_unique_hours_periods$hour[r] & 
                                              as.POSIXlt(seq(from = as.POSIXlt(min(NGU_benchmark2$real_date)), to = as.POSIXlt(max(NGU_benchmark2$real_date)), by = "hour"))$wday==bm2_unique_hours_periods$day[r])
}

# Adding the number of NGU registrations for each hour period
bm2_unique_hours_periods$Total_NGUs <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_NGUs[r] <- sum(as.POSIXlt(NGU_benchmark2$real_date)$hour==bm2_unique_hours_periods$hour[r] &
                                              as.POSIXlt(NGU_benchmark2$real_date)$wday==bm2_unique_hours_periods$day[r])
}

# Calculating average NGUs per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_NGU_per_HOUR <- bm2_unique_hours_periods$Total_NGUs/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_NGU_per_MINUTE <- bm2_unique_hours_periods$Avg_NGU_per_HOUR/60



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
  NGU_per_hour$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > NGU_per_hour$hour_start[r] & NGU_benchmark$real_date <= NGU_per_hour$hour_end[r])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_hour$NGU_benchmark[NGU_per_hour$NGU_benchmark==0] <- 0.1

# Add actual NGU per hour
NGU_per_hour$NGU_actual <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  NGU_per_hour$NGU_actual[r] <- sum(NGU_campaign$real_date > NGU_per_hour$hour_start[r] & NGU_campaign$real_date <= NGU_per_hour$hour_end[r])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
NGU_per_hour$NGU_if_no_campaign <- rep(NGU_per_hour$NGU_actual[1], nrow(NGU_per_hour))
for(r in 2:nrow(NGU_per_hour)) {
  if(NGU_per_hour$hour_end[r] <= campaign_start) {
    NGU_per_hour$NGU_if_no_campaign[r] <- NGU_per_hour$NGU_actual[r]
  } else {
    NGU_per_hour$NGU_if_no_campaign[r] <- NGU_per_hour$NGU_if_no_campaign[r-1]*NGU_per_hour$NGU_benchmark[r]/NGU_per_hour$NGU_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_hour$NGU_benchmark2 <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  if(NGU_per_hour$hour_end[r] <= campaign_start) {
    NGU_per_hour$NGU_benchmark2[r] <- NGU_per_hour$NGU_actual[r]
  } else {
    NGU_per_hour$NGU_benchmark2[r] <- bm2_unique_hours_periods$Avg_NGU_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(NGU_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(NGU_per_hour$hour_start[r])$wday]
  }
}

# Add time since campaign column
NGU_per_hour$time_since_campaign <- (NGU_per_hour$hour_start - as.POSIXct(campaign_start))/3600


## PER DAY ANALYSIS (for TV)

# Separate analysis period into days
NGU_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(NGU_per_day) <- "day_start"
NGU_per_day$day_end <- NGU_per_day$day_start+24*60*60

# Add benchmark NGU per day
NGU_per_day$NGU_benchmark <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  NGU_per_day$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > NGU_per_day$day_start[r] & NGU_benchmark$real_date <= NGU_per_day$day_end[r])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_day$NGU_benchmark[NGU_per_day$NGU_benchmark==0] <- 0.1

# Add actual NGU per day
NGU_per_day$NGU_actual <- rep(0, nrow(NGU_per_day))
for(r in 1:nrow(NGU_per_day)) {
  NGU_per_day$NGU_actual[r] <- sum(NGU_campaign$real_date > NGU_per_day$day_start[r] & NGU_campaign$real_date <= NGU_per_day$day_end[r])
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

# Add time since campaign column
NGU_per_day$days_since_campaign <- (NGU_per_day$day_start - as.POSIXct(campaign_start))/(24*60*60)


## QUARTER DAY ANALYSIS

# Separate analysis period into quarter days
NGU_per_q_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-6*60*60), by = 6*60*60))
colnames(NGU_per_q_day) <- "q_day_start"
NGU_per_q_day$q_day_end <- NGU_per_q_day$q_day_start+6*60*60

# Add benchmark NGU per q_day
NGU_per_q_day$NGU_benchmark <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  NGU_per_q_day$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > NGU_per_q_day$q_day_start[r] & NGU_benchmark$real_date <= NGU_per_q_day$q_day_end[r])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
NGU_per_q_day$NGU_benchmark[NGU_per_q_day$NGU_benchmark==0] <- 0.1

# Add actual NGU per q_day
NGU_per_q_day$NGU_actual <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  NGU_per_q_day$NGU_actual[r] <- sum(NGU_campaign$real_date > NGU_per_q_day$q_day_start[r] & NGU_campaign$real_date <= NGU_per_q_day$q_day_end[r])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
NGU_per_q_day$NGU_if_no_campaign <- rep(NGU_per_q_day$NGU_actual[1], nrow(NGU_per_q_day))
for(r in 2:nrow(NGU_per_q_day)) {
  if(NGU_per_q_day$q_day_end[r] <= campaign_start) {
    NGU_per_q_day$NGU_if_no_campaign[r] <- NGU_per_q_day$NGU_actual[r]
  } else {
    NGU_per_q_day$NGU_if_no_campaign[r] <- NGU_per_q_day$NGU_if_no_campaign[r-1]*NGU_per_q_day$NGU_benchmark[r]/NGU_per_q_day$NGU_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_q_day$NGU_benchmark2 <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  if(NGU_per_q_day$q_day_end[r] <= campaign_start) {
    NGU_per_q_day$NGU_benchmark2[r] <- NGU_per_q_day$NGU_actual[r]
  } else {
    NGU_per_q_day$NGU_benchmark2[r] <- sum(NGU_per_hour$NGU_benchmark2[(r*6-5):(r*6)])
  }
}

# Add time since campaign column
NGU_per_q_day$days_since_campaign <- (NGU_per_q_day$q_day_start - as.POSIXct(campaign_start))/(24*60*60)



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
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("NGUs") +
  ylim(0, max(NGU_per_day$NGU_actual, NGU_per_day$NGU_if_no_campaign)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
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

### Table of uplift in NGUs
# Create table row names
table_names <- "Total uplift in NGUs"
for(d in 1:analysis_period) {
  table_names <- c(table_names, paste0("Uplift day ",d))
}
table_names <- c(table_names, "Cost of campaign", "eCPI")

NGU_output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(NGU_output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift NGUs
total_uplift_NGUs <- sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign >= 0])-sum(NGU_per_day$NGU_if_no_campaign[NGU_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_NGUs <- sapply(1:analysis_period, function(d) max(0, sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign >= (d-1) & NGU_per_day$days_since_campaign < d])-sum(NGU_per_day$NGU_if_no_campaign[NGU_per_day$days_since_campaign >= (d-1) & NGU_per_day$days_since_campaign < d])))

# Inserting values into table
NGU_output_table[,1] <- c(format(round(total_uplift_NGUs)),
                         round(uplift_per_day_NGUs),
                         round(cost_of_campaign),
                         cost_of_campaign/total_uplift_NGUs)



##############################################################
### ---------------- ACTIVE USER ANALYSIS ---------------- ###
##############################################################

# DOWNLOAD CAMPAIGN DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FROM REDSHIFT")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
campaign_query_active_users_per_hour <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                               AND user_category <> 'hacker' AND user_category = 'player'
                                               AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                               AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                               AND platform = '", platform, "'
                                               AND register_ip_country ='", country,"'
                                               AND (is_tester is null or is_tester != 'true')
                                               AND t_user.user_category <> 'bot'
                                               GROUP BY log_session_start.ip_timezone, year, month, day, hour", sep = "")

campaign_query_active_users_per_day <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                              AND user_category <> 'hacker' AND user_category = 'player'
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
AUs_campaign_per_hour$hour_start <- floor_date(AUs_campaign_per_hour$real_date, "hour")
AUs_campaign_per_hour$hour_end <- ceiling_date(AUs_campaign_per_hour$real_date, "hour")

AUs_campaign_per_day$real_date <- AUs_campaign_per_day$datetime + round(as.integer(substr(AUs_campaign_per_day$ip_timezone, 1, 3)))*3600
AUs_campaign_per_day$real_date[is.na(AUs_campaign_per_day$real_date)] <- AUs_campaign_per_day$datetime[is.na(AUs_campaign_per_day$real_date)]
AUs_campaign_per_day$day_start <- floor_date(AUs_campaign_per_day$real_date, "day")
AUs_campaign_per_day$day_end <- ceiling_date(AUs_campaign_per_day$real_date, "day")



# DOWNLOAD BENCHMARK 1 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FOR OTHER COUNTRIES")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
benchmark_query_active_users_per_hour <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                                AND user_category <> 'hacker' AND user_category = 'player'
                                                AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                                AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                                AND platform = '", platform, "'
                                                AND register_ip_country ", bm_countries, "
                                                AND (is_tester is null or is_tester != 'true')
                                                AND t_user.user_category <> 'bot'
                                                GROUP BY log_session_start.ip_timezone, year, month, day, hour", sep = "")

benchmark_query_active_users_per_day <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                               AND user_category <> 'hacker' AND user_category = 'player'
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
AUs_benchmark_per_hour$hour_start <- floor_date(AUs_benchmark_per_hour$real_date, "hour")
AUs_benchmark_per_hour$hour_end <- ceiling_date(AUs_benchmark_per_hour$real_date, "hour")

AUs_benchmark_per_day$real_date <- AUs_benchmark_per_day$datetime + round(as.integer(substr(AUs_benchmark_per_day$ip_timezone, 1, 3)))*3600
AUs_benchmark_per_day$real_date[is.na(AUs_benchmark_per_day$real_date)] <- AUs_benchmark_per_day$datetime[is.na(AUs_benchmark_per_day$real_date)]
AUs_benchmark_per_day$day_start <- floor_date(AUs_benchmark_per_day$real_date, "day")
AUs_benchmark_per_day$day_end <- ceiling_date(AUs_benchmark_per_day$real_date, "day")



# DOWNLOAD BENCHMARK 2 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FOR OTHER COUNTRIES")
cat("\n")

# Need two queries for active users as aggregating active users per hour over 24 hours will double count some users
benchmark2_query_active_users_per_hour <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                                AND user_category <> 'hacker' AND user_category = 'player'
                                                AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                                                AND (t_user.migrate_date_orphaned is null or datediff(s, t_user.date_register, t_user.migrate_date_orphaned) > 86400)
                                                AND platform = '", platform, "'
                                                AND register_ip_country ='", country,"'
                                                AND (is_tester is null or is_tester != 'true')
                                                AND t_user.user_category <> 'bot'
                                                GROUP BY log_session_start.ip_timezone, year, month, day, hour_spain", sep = "")

benchmark2_query_active_users_per_day <- paste( "SELECT  min(log_session_start.datetime) as datetime,
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
                                               AND user_category <> 'hacker' AND user_category = 'player'
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
AUs_benchmark2_per_hour$hour_start <- floor_date(AUs_benchmark2_per_hour$real_date, "hour")
AUs_benchmark2_per_hour$hour_end <- ceiling_date(AUs_benchmark2_per_hour$real_date, "hour")

AUs_benchmark2_per_day$real_date <- AUs_benchmark2_per_day$datetime + round(as.integer(substr(AUs_benchmark2_per_day$ip_timezone, 1, 3)))*3600
AUs_benchmark2_per_day$real_date[is.na(AUs_benchmark2_per_day$real_date)] <- AUs_benchmark2_per_day$datetime[is.na(AUs_benchmark2_per_day$real_date)]
AUs_benchmark2_per_day$day_start <- floor_date(AUs_benchmark2_per_day$real_date, "day")
AUs_benchmark2_per_day$day_end <- ceiling_date(AUs_benchmark2_per_day$real_date, "day")

# Add the weekday and hour
AUs_benchmark2_per_hour$wday <- as.POSIXlt(AUs_benchmark2_per_hour$real_date)$wday
AUs_benchmark2_per_hour$hour <- unlist(strsplit(as.character(AUs_benchmark2_per_hour$real_date), " "))[seq(2,length(AUs_benchmark2_per_hour$real_date)*2,by = 2)]

NGU_benchmark2$hour <- as.character(NGU_benchmark2$hour) # Convert factors to characters

AUs_benchmark2_per_day$wday <- as.POSIXlt(AUs_benchmark2_per_day$real_date)$wday



### Add active users to bm2 data
# Adding the number of NGU registrations for each hour period
bm2_unique_hours_periods$Total_AUs <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_AUs[r] <- sum(AUs_benchmark2_per_hour$active_users[as.POSIXlt(AUs_benchmark2_per_hour$real_date)$hour==bm2_unique_hours_periods$hour[r] &
                                                  as.POSIXlt(AUs_benchmark2_per_hour$real_date)$wday==bm2_unique_hours_periods$day[r]])
}

# Calculating average NGUs per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_AUs_per_HOUR <- bm2_unique_hours_periods$Total_AUs/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_AUs_per_MINUTE <- bm2_unique_hours_periods$Avg_AUs_per_HOUR/60



### Creating a similar table for daily analysis
# Get unique hour and weekday combinations to calculate average new users per minute
bm2_unique_day_periods = data.frame("wday" = 0:6)

# Adding the number of times each wday occurs in benchmark period
bm2_unique_day_periods$occurences <- rep(0, nrow(bm2_unique_day_periods))
for(r in 1:nrow(bm2_unique_day_periods)) {
  bm2_unique_day_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(min(AUs_benchmark2_per_day$real_date)), to = as.POSIXlt(max(AUs_benchmark2_per_day$real_date)), by = "day"))$wday==bm2_unique_day_periods$wday[r])
}

# Adding the number of AUs for each day
bm2_unique_day_periods$Total_AUs <- rep(0, nrow(bm2_unique_day_periods))
for(r in 1:nrow(bm2_unique_day_periods)) {
  bm2_unique_day_periods$Total_AUs[r] <- sum(AUs_benchmark2_per_day$active_users[AUs_benchmark2_per_day$wday==bm2_unique_day_periods$wday[r]])
}

# Calculating average AUs per hour and per minute for each hour period
bm2_unique_day_periods$Avg_AUs_per_DAY <- bm2_unique_day_periods$Total_AUs/bm2_unique_day_periods$occurences



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
    sum(AUs_benchmark_per_hour$active_users * ((AUs_benchmark_per_hour$hour_end - AUs_per_hour$hour_start[r]) * (AUs_benchmark_per_hour$hour_end >= AUs_per_hour$hour_start[r]) * (AUs_benchmark_per_hour$hour_end <= AUs_per_hour$hour_end[r])/60 +
                                                 (AUs_per_hour$hour_end[r] - AUs_benchmark_per_hour$hour_start) * (AUs_benchmark_per_hour$hour_start > AUs_per_hour$hour_start[r]) * (AUs_benchmark_per_hour$hour_start < AUs_per_hour$hour_end[r])/60))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
AUs_per_hour$AUs_benchmark[AUs_per_hour$AUs_benchmark==0] <- 0.1

# Add actual AUs per hour
AUs_per_hour$AUs_actual <- rep(0, nrow(AUs_per_hour))
for(r in 1:nrow(AUs_per_hour)) {
  AUs_per_hour$AUs_actual[r] <-
    sum(AUs_campaign_per_hour$active_users * ((AUs_campaign_per_hour$hour_end - AUs_per_hour$hour_start[r]) * (AUs_campaign_per_hour$hour_end >= AUs_per_hour$hour_start[r]) * (AUs_campaign_per_hour$hour_end <= AUs_per_hour$hour_end[r])/60 +
                                                (AUs_per_hour$hour_end[r] - AUs_campaign_per_hour$hour_start) * (AUs_campaign_per_hour$hour_start > AUs_per_hour$hour_start[r]) * (AUs_campaign_per_hour$hour_start < AUs_per_hour$hour_end[r])/60))
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
AUs_per_hour$AUs_if_no_campaign <- rep(AUs_per_hour$AUs_actual[1], nrow(AUs_per_hour))
for(r in 2:nrow(AUs_per_hour)) {
  if(AUs_per_hour$hour_end[r] <= campaign_start) {
    AUs_per_hour$AUs_if_no_campaign[r] <- AUs_per_hour$AUs_actual[r]
  } else {
    AUs_per_hour$AUs_if_no_campaign[r] <- AUs_per_hour$AUs_if_no_campaign[r-1]*AUs_per_hour$AUs_benchmark[r]/AUs_per_hour$AUs_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
AUs_per_hour$AUs_benchmark2 <- rep(0, nrow(AUs_per_hour))
for(r in 1:nrow(AUs_per_hour)) {
  if(AUs_per_hour$hour_end[r] <= campaign_start) {
    AUs_per_hour$AUs_benchmark2[r] <- AUs_per_hour$AUs_actual[r]
  } else {
    AUs_per_hour$AUs_benchmark2[r] <- bm2_unique_hours_periods$Avg_AUs_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(AUs_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(AUs_per_hour$hour_start[r])$wday]
  }
}

# Add time since campaign column
AUs_per_hour$time_since_campaign <- (AUs_per_hour$hour_start - as.POSIXct(campaign_start))/3600


## PER DAY ANALYSIS

# Separate analysis period into days
AUs_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(AUs_per_day) <- "day_start"
AUs_per_day$day_end <- AUs_per_day$day_start+24*60*60

# Add benchmark AUs per day
AUs_per_day$AUs_benchmark <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$AUs_benchmark[r] <-
    sum(AUs_benchmark_per_day$active_users * ((AUs_benchmark_per_day$day_end - AUs_per_day$day_start[r]) * (AUs_benchmark_per_day$day_end >= AUs_per_day$day_start[r]) * (AUs_benchmark_per_day$day_end <= AUs_per_day$day_end[r])/24 +
                                                (AUs_per_day$day_end[r] - AUs_benchmark_per_day$day_start) * (AUs_benchmark_per_day$day_start > AUs_per_day$day_start[r]) * (AUs_benchmark_per_day$day_start < AUs_per_day$day_end[r])/24))
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
AUs_per_day$AUs_benchmark[AUs_per_day$AUs_benchmark==0] <- 0.1

# Add actual AUs per day
AUs_per_day$AUs_actual <- rep(0, nrow(AUs_per_day))
for(r in 1:nrow(AUs_per_day)) {
  AUs_per_day$AUs_actual[r] <-
    sum(AUs_campaign_per_day$active_users * ((AUs_campaign_per_day$day_end - AUs_per_day$day_start[r]) * (AUs_campaign_per_day$day_end >= AUs_per_day$day_start[r]) * (AUs_campaign_per_day$day_end <= AUs_per_day$day_end[r])/24 +
                                               (AUs_per_day$day_end[r] - AUs_campaign_per_day$day_start) * (AUs_campaign_per_day$day_start > AUs_per_day$day_start[r]) * (AUs_campaign_per_day$day_start < AUs_per_day$day_end[r])/24))
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
AUs_per_day$wday_start_prop <- (ceiling_date(AUs_per_day$day_start, "day") - AUs_per_day$day_start)/24
AUs_per_day$wday_end <- as.POSIXlt(AUs_per_day$day_end)$wday
AUs_per_day$wday_end_prop <- (AUs_per_day$day_end - floor_date(AUs_per_day$day_end, "day"))/24

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
AUs_per_day$days_since_campaign <- (AUs_per_day$day_start - as.POSIXct(campaign_start))/(24*60*60)


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
    AUs_per_q_day$AUs_if_no_campaign[r] <- AUs_per_q_day$AUs_if_no_campaign[r-1]*AUs_per_q_day$AUs_benchmark[r]/AUs_per_q_day$AUs_benchmark[r-1]
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
AUs_per_q_day$days_since_campaign <- (AUs_per_q_day$q_day_start - as.POSIXct(campaign_start))/(24*60*60)



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
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_if_no_campaign, colour = "Benchmark (other countries)"), size = 0.7) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_benchmark2, colour = "Benchmark (earlier period)"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("active users") +
  ylim(c(0, max(AUs_per_day$AUs_actual, AUs_per_day$AUs_if_no_campaign))) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark (other countries)", "Benchmark (earlier period)"),
                      values = c("Actual"="seagreen3", "Benchmark (other countries)"="orange", "Benchmark (earlier period)"="salmon")) +
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
# Create table row names
table_names <- "Total uplift in AUs"
for(d in 1:analysis_period) {
  table_names <- c(table_names, paste0("Uplift day ",d))
}
table_names <- c(table_names, "Cost of campaign")

AUs_output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(AUs_output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift AUs
total_uplift_AUs <- sum(AUs_per_day$AUs_actual[AUs_per_day$days_since_campaign >= 0])-sum(AUs_per_day$AUs_if_no_campaign[AUs_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_AUs <- sapply(1:analysis_period, function(d) max(0, sum(AUs_per_day$AUs_actual[AUs_per_day$days_since_campaign >= (d-1) & AUs_per_day$days_since_campaign < d])-sum(AUs_per_day$AUs_if_no_campaign[AUs_per_day$days_since_campaign >= (d-1) & AUs_per_day$days_since_campaign < d])))

# Inserting values into table
AUs_output_table[,1] <- c(format(round(total_uplift_AUs)),
                      round(uplift_per_day_AUs),
                      round(cost_of_campaign))



###########################################################
### ---------------- REVENUES ANALYSIS ---------------- ###
###########################################################

# DOWNLOAD CAMPAIGN DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FROM REDSHIFT")
cat("\n")

campaign_query_revenues <- paste( "SELECT t_transaction.datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                  FROM ",schema,".t_transaction
                                  LEFT JOIN ",schema,".log_session_start
                                  ON t_transaction.session_id = log_session_start.session_id
                                  AND t_transaction.user_id = log_session_start.user_id
                                  WHERE t_transaction.datetime >= '", query_start_date, "'
                                  AND t_transaction.datetime <= '", query_end_date, "'
                                  AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                  AND t_transaction.platform = '", platform, "'
                                  AND t_transaction.ip_country = '", country,"'
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



# DOWNLOAD BENCHMARK 1 DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FOR OTHER COUNTRIES")
cat("\n")

benchmark_query_revenues <- paste( "SELECT t_transaction.datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                   FROM ",schema,".t_transaction
                                   LEFT JOIN ",schema,".log_session_start
                                   ON t_transaction.session_id = log_session_start.session_id
                                   AND t_transaction.user_id = log_session_start.user_id
                                   WHERE t_transaction.datetime >= '", query_start_date, "'
                                   AND t_transaction.datetime <= '", query_end_date, "'
                                   AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                   AND t_transaction.platform = '", platform, "'
                                   AND t_transaction.ip_country ", bm_countries, "
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

benchmark2_query_revenues <- paste( "SELECT t_transaction.datetime, t_transaction.ip_timezone, COALESCE(t_transaction.amount_gross*0.7, 0) AS net_value
                                   FROM ",schema,".t_transaction
                                   LEFT JOIN ",schema,".log_session_start
                                   ON t_transaction.session_id = log_session_start.session_id
                                   AND t_transaction.user_id = log_session_start.user_id
                                   WHERE t_transaction.datetime >= '", bm2_query_start_date, "'
                                   AND t_transaction.datetime <= '", bm2_query_end_date, "'
                                   AND (t_transaction.offer != 'earncash' OR t_transaction.offer IS NULL)
                                   AND t_transaction.platform = '", platform, "'
                                   AND t_transaction.ip_country = '", country,"'
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

revenues_benchmark2$wday <- as.POSIXlt(revenues_benchmark2$real_date)$wday
revenues_benchmark2$hour <- unlist(strsplit(as.character(revenues_benchmark2$real_date), " "))[seq(2,length(revenues_benchmark2$real_date)*2,by = 2)]

revenues_benchmark2$hour <- as.character(revenues_benchmark2$hour) # Convert factors to characters


### Add revenues per hour to bm2 data
# Adding the average revenues for each hour period
bm2_unique_hours_periods$Total_revenues <- rep(0, nrow(bm2_unique_hours_periods))
for(r in 1:nrow(bm2_unique_hours_periods)) {
  bm2_unique_hours_periods$Total_revenues[r] <- sum(revenues_benchmark2$net_value[as.POSIXlt(revenues_benchmark2$real_date)$hour==bm2_unique_hours_periods$hour[r] &
                                                                                      as.POSIXlt(revenues_benchmark2$real_date)$wday==bm2_unique_hours_periods$day[r]])
}

# Calculating average revenues per hour and per minute for each hour period
bm2_unique_hours_periods$Avg_revenues_per_HOUR <- bm2_unique_hours_periods$Total_revenues/bm2_unique_hours_periods$occurences
bm2_unique_hours_periods$Avg_revenues_per_MINUTE <- bm2_unique_hours_periods$Avg_revenues_per_HOUR/60



# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")


## PER HOUR ANALYSIS (for youtube)

# Separate analysis period into hours
revenues_per_hour <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-60*60), by = "hour"))
colnames(revenues_per_hour) <- "hour_start"
revenues_per_hour$hour_end <- revenues_per_hour$hour_start+60*60

# Add benchmark revenues per hour
revenues_per_hour$revenues_benchmark <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  revenues_per_hour$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > revenues_per_hour$hour_start[r] & revenues_benchmark$real_date <= revenues_per_hour$hour_end[r]])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_hour$revenues_benchmark[revenues_per_hour$revenues_benchmark==0] <- 0.1

# Add actual revenues per hour
revenues_per_hour$revenues_actual <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  revenues_per_hour$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$real_date > revenues_per_hour$hour_start[r] & revenues_campaign$real_date <= revenues_per_hour$hour_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_hour$revenues_if_no_campaign <- rep(revenues_per_hour$revenues_actual[1], nrow(revenues_per_hour))
for(r in 2:nrow(revenues_per_hour)) {
  if(revenues_per_hour$hour_end[r] <= campaign_start) {
    revenues_per_hour$revenues_if_no_campaign[r] <- revenues_per_hour$revenues_actual[r]
  } else {
    revenues_per_hour$revenues_if_no_campaign[r] <- revenues_per_hour$revenues_if_no_campaign[r-1]*revenues_per_hour$revenues_benchmark[r]/revenues_per_hour$revenues_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
revenues_per_hour$revenues_benchmark2 <- rep(0, nrow(revenues_per_hour))
for(r in 1:nrow(revenues_per_hour)) {
  if(revenues_per_hour$hour_end[r] <= campaign_start) {
    revenues_per_hour$revenues_benchmark2[r] <- revenues_per_hour$revenues_actual[r]
  } else {
    revenues_per_hour$revenues_benchmark2[r] <- bm2_unique_hours_periods$Avg_revenues_per_HOUR[bm2_unique_hours_periods$hour==as.POSIXlt(revenues_per_hour$hour_start[r])$hour & bm2_unique_hours_periods$day==as.POSIXlt(revenues_per_hour$hour_start[r])$wday]
  }
}

# Add time since campaign column
revenues_per_hour$time_since_campaign <- (revenues_per_hour$hour_start - as.POSIXct(campaign_start))/3600


## PER DAY ANALYSIS (for TV)

# Separate analysis period into days
revenues_per_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-24*60*60), by = "day"))
colnames(revenues_per_day) <- "day_start"
revenues_per_day$day_end <- revenues_per_day$day_start+24*60*60

# Add benchmark revenues per day
revenues_per_day$revenues_benchmark <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > revenues_per_day$day_start[r] & revenues_benchmark$real_date <= revenues_per_day$day_end[r]])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_day$revenues_benchmark[revenues_per_day$revenues_benchmark==0] <- 0.1

# Add actual revenues per day
revenues_per_day$revenues_actual <- rep(0, nrow(revenues_per_day))
for(r in 1:nrow(revenues_per_day)) {
  revenues_per_day$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$real_date > revenues_per_day$day_start[r] & revenues_campaign$real_date <= revenues_per_day$day_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_day$revenues_if_no_campaign <- rep(revenues_per_day$revenues_actual[1], nrow(revenues_per_day))
for(r in 2:nrow(revenues_per_day)) {
  if(revenues_per_day$day_end[r] <= campaign_start) {
    revenues_per_day$revenues_if_no_campaign[r] <- revenues_per_day$revenues_actual[r]
  } else {
    revenues_per_day$revenues_if_no_campaign[r] <- revenues_per_day$revenues_if_no_campaign[r-1]*revenues_per_day$revenues_benchmark[r]/revenues_per_day$revenues_benchmark[r-1]
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
revenues_per_day$days_since_campaign <- (revenues_per_day$day_start - as.POSIXct(campaign_start))/(24*60*60)


## QUARTER DAY ANALYSIS

# Separate analysis period into quarter days
revenues_per_q_day <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-6*60*60), by = 6*60*60))
colnames(revenues_per_q_day) <- "q_day_start"
revenues_per_q_day$q_day_end <- revenues_per_q_day$q_day_start+6*60*60

# Add benchmark revenues per q_day
revenues_per_q_day$revenues_benchmark <- rep(0, nrow(revenues_per_q_day))
for(r in 1:nrow(revenues_per_q_day)) {
  revenues_per_q_day$revenues_benchmark[r] <- sum(revenues_benchmark$net_value[revenues_benchmark$real_date > revenues_per_q_day$q_day_start[r] & revenues_benchmark$real_date <= revenues_per_q_day$q_day_end[r]])
}
# Replace any 0s with 0.1s in benchmark data so that benchmark calculation doesn't return 0s or errors (and it will end up at same result)
revenues_per_q_day$revenues_benchmark[revenues_per_q_day$revenues_benchmark==0] <- 0.1

# Add actual revenues per q_day
revenues_per_q_day$revenues_actual <- rep(0, nrow(revenues_per_q_day))
for(r in 1:nrow(revenues_per_q_day)) {
  revenues_per_q_day$revenues_actual[r] <- sum(revenues_campaign$net_value[revenues_campaign$real_date > revenues_per_q_day$q_day_start[r] & revenues_campaign$real_date <= revenues_per_q_day$q_day_end[r]])
}

# Add what would actually have happened if there had been no marketing campaign (according to benchmark)
revenues_per_q_day$revenues_if_no_campaign <- rep(revenues_per_q_day$revenues_actual[1], nrow(revenues_per_q_day))
for(r in 2:nrow(revenues_per_q_day)) {
  if(revenues_per_q_day$q_day_end[r] <= campaign_start) {
    revenues_per_q_day$revenues_if_no_campaign[r] <- revenues_per_q_day$revenues_actual[r]
  } else {
    revenues_per_q_day$revenues_if_no_campaign[r] <- revenues_per_q_day$revenues_if_no_campaign[r-1]*revenues_per_q_day$revenues_benchmark[r]/revenues_per_q_day$revenues_benchmark[r-1]
  }
}

# Add in benchmark 2 column (based on same country over a pre campaign period)
NGU_per_q_day$NGU_benchmark2 <- rep(0, nrow(NGU_per_q_day))
for(r in 1:nrow(NGU_per_q_day)) {
  if(NGU_per_q_day$q_day_end[r] <= campaign_start) {
    NGU_per_q_day$NGU_benchmark2[r] <- NGU_per_q_day$NGU_actual[r]
  } else {
    NGU_per_q_day$NGU_benchmark2[r] <- sum(NGU_per_hour$NGU_benchmark2[(r*6-5):(r*6)])
  }
}

# Add time since campaign column
revenues_per_q_day$days_since_campaign <- (revenues_per_q_day$q_day_start - as.POSIXct(campaign_start))/(24*60*60)


# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual revenues
# For hourly analysis (Youtube)
revenues_plot_per_hour <- ggplot(data = revenues_per_hour) +
  geom_line(aes(x = revenues_per_hour$time_since_campaign, y = revenues_per_hour$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_hour$time_since_campaign, y = revenues_per_hour$revenues_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("revenues") +
  scale_x_continuous(breaks = seq(as.integer(min(revenues_per_hour$time_since_campaign)), as.integer(max(revenues_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
revenues_plot_per_day <- ggplot(data = revenues_per_day) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_day$days_since_campaign, y = revenues_per_day$revenues_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("revenues") +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For quarter day analysis
revenues_plot_per_q_day <- ggplot(data = revenues_per_q_day) +
  geom_line(aes(x = revenues_per_q_day$days_since_campaign, y = revenues_per_q_day$revenues_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = revenues_per_q_day$days_since_campaign, y = revenues_per_q_day$revenues_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("revenues") +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))


### Table of uplift in revenues
# Create table row names
table_names <- "Total uplift in revenues"
for(d in 1:analysis_period) {
  table_names <- c(table_names, paste0("Uplift day ",d))
}
table_names <- c(table_names, "Cost of campaign", "revenues - cost")

revenues_output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(revenues_output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift revenues
total_uplift_revenues <- sum(revenues_per_day$revenues_actual[revenues_per_day$days_since_campaign >= 0])-sum(revenues_per_day$revenues_if_no_campaign[revenues_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_revenues <- sapply(1:analysis_period, function(d) max(0, sum(revenues_per_day$revenues_actual[revenues_per_day$days_since_campaign >= (d-1) & revenues_per_day$days_since_campaign < d])-sum(revenues_per_day$revenues_if_no_campaign[revenues_per_day$days_since_campaign >= (d-1) & revenues_per_day$days_since_campaign < d])))

# Inserting values into table
revenues_output_table[,1] <- c(format(round(total_uplift_revenues)),
                      round(uplift_per_day_revenues),
                      round(cost_of_campaign),
                      total_uplift_revenues - cost_of_campaign)

NGU_plot_per_hour
NGU_plot_per_q_day
NGU_plot_per_day
NGU_output_table

AUs_plot_per_hour
AUs_plot_per_q_day
AUs_plot_per_day
AUs_output_table

revenues_plot_per_hour
revenues_plot_per_q_day
revenues_plot_per_day
revenues_output_table








