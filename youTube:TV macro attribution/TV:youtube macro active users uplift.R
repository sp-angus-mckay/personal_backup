
# YOUTUBE AND TV MACRO ATTRIBUTION
# The purpose of this file is to calculate the active users uplift of a given youTube or TV campaign 
# Author: Angus McKay
# Date: May 2017

# NOTES:
# - inputs below
# - for TV the analysis is daily and the chart starts from 7 days before campaign starts
# - for youtube the analysis is hourly and the chart starts 48 hours before the campaign starts
# - by default the benchmark is all other countries over the same timeframe, but specific countries can be specified for the benchmark
# - MAY NEED TO BE CAREFUL ABOUT OTHER CAMPAIGNS OVER SAME TIME (either in analysis country or benchmark countries)
# - obtaining the benchmark data can be slow, especially if using all other countries as benchmark

# INPUTS: PLEASE, DEFINE THE FOLLOWING VARIABLES

# name of the game. "DC" or "ML".
game <- "ML"

# platform. "ios" or "android"
platform <- "ios"

# name of the country. "FR", "US", "AU", ...
country <- 'DE'

# select bernchmark countries as a list or ignore to include all countries except country of analysis
benchmark_countries <- c('ES', 'FR')

# dates in format: '2000-01-01 01:00:00'.
# Campaign dates, cost and period of days to analyse after campaign started
campaign_type <- 'Youtube' # 'TV' or 'Youtube'
campaign_start  <- '2016-09-02 10:29:00'
analysis_period <- 7 # in days
cost_of_campaign <- 300



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
cat("DOWNLOAD ACTIVE USER DATA FROM REDSHIFT")
cat("\n")

analysis_period_pre_campaign <- if(campaign_type == 'Youtube') 2 else 7 # in days
analysis_start_date <- as.POSIXct(campaign_start) - analysis_period_pre_campaign*24*60*60
analysis_end_date <- as.POSIXct(campaign_start) + analysis_period*24*60*60

# adding extra 24 hours either side so that analysis period is covered after adjusting for time-zones
query_start_date <- as.POSIXct(analysis_start_date - 24*60*60)
query_end_date <- as.POSIXct(analysis_end_date + 24*60*60)

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


# DOWNLOAD BENCHMARK DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD ACTIVE USER DATA FOR OTHER COUNTRIES")
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

# Add time since campaign column
AUs_per_day$days_since_campaign <- (AUs_per_day$day_start - as.POSIXct(campaign_start))/(24*60*60)


## QUARTER DAY ANALYSIS (NB - THIS IS UNIQUE AUs PER HOUR SUMMED OVER A 6 HOUR PERIOD)

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

# Add time since campaign column
AUs_per_q_day$days_since_campaign <- (AUs_per_q_day$q_day_start - as.POSIXct(campaign_start))/(24*60*60)



# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual AUs
# For hourly analysis (Youtube)
plot_per_hour <- ggplot(data = AUs_per_hour) +
  geom_line(aes(x = AUs_per_hour$time_since_campaign, y = AUs_per_hour$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_hour$time_since_campaign, y = AUs_per_hour$AUs_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("active users") +
  ylim( c(0, max(AUs_per_hour$AUs_actual, AUs_per_hour$AUs_if_no_campaign))) +
  scale_x_continuous(breaks = seq(as.integer(min(AUs_per_hour$time_since_campaign)), as.integer(max(AUs_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
plot_per_day <- ggplot(data = AUs_per_day) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_day$days_since_campaign, y = AUs_per_day$AUs_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("active users") +
  ylim(c(0, max(AUs_per_day$AUs_actual, AUs_per_day$AUs_if_no_campaign))) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For quarter day analysis
plot_per_q_day <- ggplot(data = AUs_per_q_day) +
  geom_line(aes(x = AUs_per_q_day$days_since_campaign, y = AUs_per_q_day$AUs_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = AUs_per_q_day$days_since_campaign, y = AUs_per_q_day$AUs_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("active users") +
  ylim(0, max(AUs_per_q_day$AUs_actual, AUs_per_q_day$AUs_if_no_campaign)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
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
table_names <- c(table_names, "Cost of campaign", "eCPI")

output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift AUs
total_uplift_AUs <- sum(AUs_per_day$AUs_actual[AUs_per_day$days_since_campaign >= 0])-sum(AUs_per_day$AUs_if_no_campaign[AUs_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_AUs <- sapply(1:analysis_period, function(d) max(0, sum(AUs_per_day$AUs_actual[AUs_per_day$days_since_campaign >= (d-1) & AUs_per_day$days_since_campaign < d])-sum(AUs_per_day$AUs_if_no_campaign[AUs_per_day$days_since_campaign >= (d-1) & AUs_per_day$days_since_campaign < d])))

# Inserting values into table
output_table[,1] <- c(format(round(total_uplift_AUs)),
                         round(uplift_per_day_AUs),
                         round(cost_of_campaign),
                         cost_of_campaign/total_uplift_AUs)

