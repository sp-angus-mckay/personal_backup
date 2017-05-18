
# YOUTUBE AND TV MACRO ATTRIBUTION
# The purpose of this file is to calculate the revenue uplift of a given youTube or TV campaign 
# Author: Angus McKay
# Date: May 2017

# NOTES:
# - inputs below
# - for TV the analysis is daily and the chart starts from 7 days before campaign starts
# - for youtube the analysis is hourly and the chart starts 48 hours before the campaign starts
# - by default the benchmark is all other countries over the same timeframe, but specific countries can be specified for the benchmark
# - MAY NEED TO BE CAREFUL ABOUT OTHER CAMPAIGNS OVER SAME TIME (either in analysis country or benchmark countries)

# INPUTS: PLEASE, DEFINE THE FOLLOWING VARIABLES

# name of the game. "DC" or "ML".
game <- "ML"

# platform. "ios" or "android"
platform <- "ios"

# name of the country. "FR", "US", "AU", ...
country <- 'DE'

# select bernchmark countries as a list or ignore to include all countries except country of analysis
benchmark_countries <- c('GB', 'FR')

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
cat("DOWNLOAD REVENUES DATA FROM REDSHIFT")
cat("\n")

analysis_period_pre_campaign <- if(campaign_type == 'Youtube') 2 else 7 # in days
analysis_start_date <- as.POSIXct(campaign_start) - analysis_period_pre_campaign*24*60*60
analysis_end_date <- as.POSIXct(campaign_start) + analysis_period*24*60*60

# adding extra 24 hours either side so that analysis period is covered after adjusting for time-zones
query_start_date <- as.POSIXct(analysis_start_date - 24*60*60)
query_end_date <- as.POSIXct(analysis_end_date + 24*60*60)

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



# DOWNLOAD BENCHMARK DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD REVENUES DATA FOR OTHER COUNTRIES")
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

# Add time since campaign column
revenues_per_q_day$days_since_campaign <- (revenues_per_q_day$q_day_start - as.POSIXct(campaign_start))/(24*60*60)



# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual revenues
# For hourly analysis (Youtube)
plot_per_hour <- ggplot(data = revenues_per_hour) +
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
plot_per_day <- ggplot(data = revenues_per_day) +
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
plot_per_q_day <- ggplot(data = revenues_per_q_day) +
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
plot_per_q_day

### Table of uplift in revenues
# Create table row names
table_names <- "Total uplift in revenues"
for(d in 1:analysis_period) {
  table_names <- c(table_names, paste0("Uplift day ",d))
}
table_names <- c(table_names, "Cost of campaign", "eCPI")

output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift revenues
total_uplift_revenues <- sum(revenues_per_day$revenues_actual[revenues_per_day$days_since_campaign >= 0])-sum(revenues_per_day$revenues_if_no_campaign[revenues_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_revenues <- sapply(1:analysis_period, function(d) max(0, sum(revenues_per_day$revenues_actual[revenues_per_day$days_since_campaign >= (d-1) & revenues_per_day$days_since_campaign < d])-sum(revenues_per_day$revenues_if_no_campaign[revenues_per_day$days_since_campaign >= (d-1) & revenues_per_day$days_since_campaign < d])))

# Inserting values into table
output_table[,1] <- c(format(round(total_uplift_revenues)),
                         round(uplift_per_day_revenues),
                         round(cost_of_campaign),
                         cost_of_campaign/total_uplift_revenues)

