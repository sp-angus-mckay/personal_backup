
# YOUTUBE AND TV MACRO ATTRIBUTION
# The purpose of this file is to calculate the NGU uplift of a given youTube or TV campaign 
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



# DOWNLOAD BENCHMARK DATA ----------------------------------------------------------------------------------------
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



# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")

## PER HOUR ANALYSIS (for youtube)

# Separate analysis period into hours
NGU_per_hour <- as.data.frame(seq(from = as.POSIXct(analysis_start_date), to = as.POSIXct(analysis_end_date-60*60), by = "hour"))
colnames(NGU_per_hour) <- "hour_start"
NGU_per_hour$hour_end <- NGU_per_hour$hour_start+60*60

# Add benchmark NGU per hour
NGU_per_hour$NGU_benchmark <- rep(0, nrow(NGU_per_hour))
for(r in 1:nrow(NGU_per_hour)) {
  NGU_per_hour$NGU_benchmark[r] <- sum(NGU_benchmark$real_date > NGU_per_hour$hour_start[r] & NGU_benchmark$real_date <= NGU_per_hour$hour_end[r])
}

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

# Add time since campaign column
NGU_per_day$days_since_campaign <- (NGU_per_day$day_start - as.POSIXct(campaign_start))/(24*60*60)



# OUTPUT ------------------------------------------------------------------------------------------------------------------------
### Plot benchmark vs actual NGUs
# For hourly analysis (Youtube)
plot_per_hour <- ggplot(data = NGU_per_hour) +
  geom_line(aes(x = NGU_per_hour$time_since_campaign, y = NGU_per_hour$NGU_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = NGU_per_hour$time_since_campaign, y = NGU_per_hour$NGU_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (hours)") +
  ylab("NGUs") +
  scale_x_continuous(breaks = seq(as.integer(min(NGU_per_hour$time_since_campaign)), as.integer(max(NGU_per_hour$time_since_campaign))+1, 24)) +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

# For daily analysis (TV)
plot_per_day <- ggplot(data = NGU_per_day) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_actual, colour = "Actual"), size = 0.7) +
  geom_line(aes(x = NGU_per_day$days_since_campaign, y = NGU_per_day$NGU_if_no_campaign, colour = "Benchmark"), size = 0.7) +
  xlab("Time since campaign (days)") +
  ylab("NGUs") +
  scale_colour_manual("",
                      breaks = c("Actual", "Benchmark"),
                      values = c("Actual"="seagreen3", "Benchmark"="orange")) +
  theme(legend.position = "top", legend.background = element_rect(fill = "grey90"), legend.key = element_rect(fill = "grey90"),
        plot.background = element_rect(fill = "grey90"),
        panel.background = element_rect(fill = "grey90"),
        panel.grid.major = element_line(colour = "grey75"),
        panel.grid.minor = element_line(colour = "grey75"))

if(campaign_type=='TV') plot_per_day else plot_per_hour

### Table of uplift in NGUs
# Create table row names
table_names <- "Total uplift in NGUs"
for(d in 1:analysis_period) {
  table_names <- c(table_names, paste0("Uplift day ",d))
}
table_names <- c(table_names, "Cost of campaign", "eCPI")

output_table <- data.frame(rep(0, length(table_names)), row.names = table_names)
colnames(output_table) <- paste(country, platform, sep = "_")

# calculate figures
# Total uplift NGUs
total_uplift_NGUs <- sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign >= 0])-sum(NGU_per_day$NGU_if_no_campaign[NGU_per_day$days_since_campaign >= 0])
# Uplift per day
uplift_per_day_NGUs <- sapply(1:analysis_period, function(d) max(0, sum(NGU_per_day$NGU_actual[NGU_per_day$days_since_campaign >= (d-1) & NGU_per_day$days_since_campaign < d])-sum(NGU_per_day$NGU_if_no_campaign[NGU_per_day$days_since_campaign >= (d-1) & NGU_per_day$days_since_campaign < d])))

# Inserting values into table
output_table[,1] <- c(format(round(total_uplift_NGUs)),
                         round(uplift_per_day_NGUs),
                         round(cost_of_campaign),
                         cost_of_campaign/total_uplift_NGUs)

