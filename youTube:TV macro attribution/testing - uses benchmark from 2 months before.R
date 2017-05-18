
# YOUTUBE ATTRIBUTION
# The purpose of this file is to calculate the youTube attribution of a given youTube campaign 
# Author: Angus McKay
# Date: June 2017

# INCLUDE SOME NOTES ON THE SCRIPT HERE - E.G. INSTRUCTIONS, RUNTIME

# INPUTS: PLEASE, DEFINE THE FOLLOWING VARIABLES

# name of the game. "DC" or "ML".
game <- "DC"

# platform. "ios" or "android"
platform <- "ios"

# name of the country. "FR", "US", "AU", ...
country <- 'US'
benchmark_countries <- c('CA', 'GB', 'AU', 'FR', 'DE')

# dates in format: '2000-01-01 01:00:00'.
# Campaign dates and period of days to analyse after campaign started
campaign_start  <- '2016-09-23 11:04:00' # this is Youtuber EvantubeHD - US,CA,AU,GB
analysis_period <- 7 # in days

# If possible, use a benchmark of two months. 
benchmark_start <- '2016-07-23 00:00:00'
benchmark_end   <- '2016-09-23 00:00:00'

# starting time of the dayparts of the given country. Format: "03:00:00"
day <- "07:00:00"
prime <- "19:00:00"
night <- "22:00:00"

# folder where you want to store the graphs. Has to be created before running the script, otherwise it'll fail.
folder <- "graphs"


# START -----------------------------------------------------------------------------------------------------

# Downloads and loads the required packages
if (!require("dplyr")) install.packages("dplyr"); library(dplyr)
if (!require("RPostgreSQL")) install.packages("RPostgreSQL"); library(RPostgreSQL) 
if (!require("ggplot2")) install.packages("ggplot2"); library(ggplot2)
if (!require("MASS")) install.packages("MASS"); library(MASS)


#CONNECTING TO THE DATABASE
cat("CONNECTING TO THE DATABASE")
cat("\n")

if(game == "DC"){
  host <- "redshift.pro.dc.laicosp.net"
  table <- "dragoncity.t_user"
}else if(game == "ML"){
  host <- "redshift.pro.mc.laicosp.net"
  table <- "monstercity.t_user"
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



# DOWNLOAD SPOTS DATA ----------------------------------------------------------------------------------------
cat("DOWNLOAD NGU DATA FROM REDSHIFT")
cat("\n")

analysis_period_pre_campaign <- 2 # in days
analysis_start_date <- as.POSIXct(campaign_start) - analysis_period_pre_campaign*24*60*60
analysis_end_date <- as.POSIXct(campaign_start) + analysis_period*24*60*60

campaign_query <- paste( "SELECT date_register, register_ip_timezone, user_id
                         FROM ",table,
                         " WHERE (date_register+register_ip_timezone) >='", analysis_start_date, "'
                         AND (date_register+register_ip_timezone) <= '", analysis_end_date, "'
                         AND user_category <> 'hacker' AND user_category = 'player' 
                         AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                         AND register_platform = '", platform, "'
                         AND register_ip_country ='", country,"'", sep = "")


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
cat("DOWNLOAD SPOTS DATA")
cat("\n")

benchmark_query <- paste( "SELECT date_register, register_ip_timezone, user_id
                          FROM ", table, 
                          " WHERE (date_register+register_ip_timezone) >='", benchmark_start, "'
                          AND (date_register+register_ip_timezone) <= '", benchmark_end, "'
                          AND user_category <> 'hacker'  AND user_category = 'player' 
                          AND (register_source is NULL or lower(register_source) like '%organic%' or register_source = '')
                          AND register_platform = '", platform, "'
                          AND register_ip_country = '", country,"'", sep = "")

NGU_benchmark <- dbGetQuery(spdb, benchmark_query)

# Add a column with the real date of install, the weekday and hour
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
NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-02:30"] <- NGU_benchmark$real_date[NGU_benchmark$register_ip_timezone == "-02:30"] - 2*3600 + 3600*0.5
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
NGU_benchmark$hour <- unlist(strsplit(as.character(NGU_benchmark$real_date), " "))[seq(2,length(NGU_benchmark$real_date)*2,by = 2)]

NGU_benchmark$hour <- as.character(NGU_benchmark$hour) # Convert factors to characters

# BENCHMARK DATA ----------------------------------------------------------------------------------------

# Calculate NGUs per hour
# Get unique hour and weekday combinations to calculate average new users per minute
unique_hours_periods = data.frame("day" = rep(0:6, each=24), "hour" = rep(0:23, 7))

# Adding the number of times each hour period occurs in benchmark period
unique_hours_periods$occurences <- rep(0, nrow(unique_hours_periods))
for(r in 1:nrow(unique_hours_periods)) {
  unique_hours_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(min(NGU_benchmark$real_date)), to = as.POSIXlt(max(NGU_benchmark$real_date)), by = "hour"))$hour==unique_hours_periods$hour[r] & 
                                              as.POSIXlt(seq(from = as.POSIXlt(min(NGU_benchmark$real_date)), to = as.POSIXlt(max(NGU_benchmark$real_date)), by = "hour"))$wday==unique_hours_periods$day[r])
}

unique_hours_periods$Total_NGUs <- rep(0, nrow(unique_hours_periods))
for(r in 1:nrow(unique_hours_periods)) {
  unique_hours_periods$Total_NGUs[r] <- sum(as.POSIXlt(NGU_benchmark$real_date)$hour==unique_hours_periods$hour[r] &
                                              as.POSIXlt(NGU_benchmark$real_date)$wday==unique_hours_periods$day[r])
}

unique_hours_periods$Avg_NGU_per_HOUR <- unique_hours_periods$Total_NGUs/unique_hours_periods$occurences
unique_hours_periods$Avg_NGU_per_MINUTE <- unique_hours_periods$Avg_NGU_per_HOUR/60

# plotting benchmark NGU per minute pattern
plot(unique_hours_periods$Avg_NGU_per_MINUTE, type = 'l')

# ATTRIBUTION ANALYSIS --------------------------------------------------------------------------------------------------
cat("ATTRIBUTION ANALYSIS")
cat("\n")

# Separate analysis period into hours
NGU_per_hour <- as.data.frame(seq(from = as.POSIXct(min(NGU_campaign$real_date)), to = as.POSIXct(max(NGU_campaign$real_date))-60*60, by = "hour"))
colnames(NGU_per_hour) <- "hour_start"
NGU_per_hour$hour_end <- min()
NGU_per_hour$day <- as.POSIXlt(NGU_per_hour$hour_start)$wday
NGU_per_hour$hour <- as.POSIXlt(NGU_per_hour$hour_start)$hour
min(NGU_per_hour$hour_start, 10)
# Add actual NGU per hour
NGU_campaign$real_date > NGU_per_hour$hour_start[1]

# matrix where we will store the counts
counts <- matrix(rep(NA, lag*dim(spots)[1]), ncol = lag)

# iterate over all spots and compute NGU counts
for (i in 1:dim(spots)[1]){
  cat(paste0("Spot ",i,"/", dim(spots)[1]))
  cat("\n")
  
  current_time  <- spots$times[i]
  
  # store the overlapping spots (previous and posterior)
  overlaps <- spots[spots$times > current_time & spots$times < (current_time + lag*60), ]
  previous_overlaps <- spots[spots$times < current_time & spots$times > (current_time - lag*60), ]
  
  # if there is an overlap:   
  if( dim(overlaps)[1] != 0 | dim(previous_overlaps)[1] != 0){
    for(t in 1:lag){
      NGU_s <- NGU_campaign$user_id[NGU_campaign$real_date >= (current_time + 60*(t-1)) & NGU_campaign$real_date < (current_time + 60*t) ]
      
      # store the channels of the overlapping spots
      channels_post <- overlaps$Channel[overlaps$times < (current_time + 60*t)]
      channels_pre  <- previous_overlaps$Channel[ (previous_overlaps$times + (lag*60)) >= (current_time + 60*t)]
      all_channels <- unique(c(spots$Channel[i], channels_post, channels_pre))
      
      # attributed percentage
      perc  <- spots$ContactsMen[i] / sum( c(spots$ContactsMen[i], previous_overlaps$ContactsMen[previous_overlaps$Channel != spots$Channel[i]], overlaps$ContactsMen[overlaps$Channel != spots$Channel[i]] ), na.rm = TRUE ) 
      if(is.nan(perc)) perc <- 0.5
      if(spots$ContactsMen[i] == 0) perc <- 0.5
      
      actual_ngu <- max((length(unique(NGU_s)) - benchmark(current_time + 60*(t-1)) ), 0)
      counts[i, t] <- actual_ngu * perc
      
      # if there is overlaping within the same channel, attribute the NGU to the other and set the counts to 0  
      if(spots$Channel[i] %in% channels_post) counts[i, t] <- 0
    }
    
  } else{ # when there are no overlapping spots, attribute all the NGU to that channel
    # Compute NGU per minute
    for(t in 1:lag){
      NGU_s <- NGU_campaign$user_id[NGU_campaign$real_date >= (current_time + 60*(t-1)) & NGU_campaign$real_date < (current_time + 60*t) ]
      counts[i, t] <- max((length(unique(NGU_s)) - benchmark(current_time + 60*(t-1)) ), 0)
    }
  }
}

#####
### TESTING DIFFERENT SPOTS METHOD - PER MINUTE (WHICH COULD LEAD TO REGRESSION??)
#####
# Get unique hour and weekday combinations to calculate average new users per minute
unique_hours_periods = data.frame("day" = rep(0:6, each=24), "hour" = rep(0:23, 7))

# Adding the number of times each hour period occurs in benchmark period
unique_hours_periods$occurences <- rep(0, nrow(unique_hours_periods))
for(r in 1:nrow(unique_hours_periods)) {
  unique_hours_periods$occurences[r] <- sum(as.POSIXlt(seq(from = as.POSIXlt(benchmark_start), to = as.POSIXlt(benchmark_end), by = "hour"))$hour==unique_hours_periods$hour[r] & 
                                              as.POSIXlt(seq(from = as.POSIXlt(benchmark_start), to = as.POSIXlt(benchmark_end), by = "hour"))$wday==unique_hours_periods$day[r])
}

unique_hours_periods$Total_NGUs <- rep(0, nrow(unique_hours_periods))
for(r in 1:nrow(unique_hours_periods)) {
  unique_hours_periods$Total_NGUs[r] <- sum(as.POSIXlt(NGU_benchmark$real_date)$hour==unique_hours_periods$hour[r] &
                                              as.POSIXlt(NGU_benchmark$real_date)$wday==unique_hours_periods$day[r])
}

unique_hours_periods$Avg_NGU_per_HOUR <- unique_hours_periods$Total_NGUs/unique_hours_periods$occurences
unique_hours_periods$Avg_NGU_per_MINUTE <- unique_hours_periods$Avg_NGU_per_HOUR/60



######### END OF TESTING - NOT FINISHED

# compute cumulative counts 

counts <- round(counts)
cumulative_counts <- counts




for(col in 2:lag){
  cumulative_counts[, col] <- cumulative_counts[, col] + cumulative_counts[, (col-1)]
}
cum_counts <- cumulative_counts[cumulative_counts[,15]> 0,]
plot(apply(cum_counts, 2, mean))
plot(apply(cumulative_counts, 2, mean))


# GRANULAR ANALYSIS ---------------------------------------------------------------------------------------------------------------------------

# Plot the Response Curves
channels <- unique(spots$Channel)
dayparts <- unique(spots$daypart)


cat("Night Time ")
cat("\n")
cc_df_n <- data.frame(NGU = NA, time = NA, impacts = NA, station = NA )
for(c in 1:length(channels)){
  d <- "NightTime"
  cc <- cumulative_counts[spots$Channel == channels[c] &  spots$daypart == d, ]
  impacts <- spots$ContactsMen[spots$Channel == channels[c] &  spots$daypart == d]
  
  if(dim(cc)[1] == 0) next 
  
  for (i in 1:dim(cc)[1]){
    for(j in 1:dim(cc)[2]){
      cc_df_n <- rbind(cc_df_n, c(cc[i, j], j, impacts[i], paste(channels[c])) )
    }
  }
  
  cc_df_15 <- cc_df_n[cc_df_n$time == 15,]
  
  print(channels[c])
  reg1 <- rlm(formula = as.numeric(NGU) ~ as.numeric(impacts), data = cc_df_15)
  print(reg1$coefficients*10000)
}

cc_df_n <- cc_df_n[-1,]

cat("Prime Time ")
cat("\n")
cc_df_p <- data.frame(NGU = NA, time = NA, impacts = NA, station = NA )
for(c in 1:length(channels)){
  d <- "PrimeTime"
  cc <- cumulative_counts[spots$Channel == channels[c] &  spots$daypart == d, ]
  impacts <- spots$ContactsMen[spots$Channel == channels[c] &  spots$daypart == d]
  
  if(dim(cc)[1] == 0) next 
  
  for (i in 1:dim(cc)[1]){
    for(j in 1:dim(cc)[2]){
      cc_df_p <- rbind(cc_df_p, c(cc[i, j], j, impacts[i], paste(channels[c])) )
    }
  }
  
  cc_df_15 <- cc_df_p[cc_df_p$time == 15,]
  
  print(channels[c])
  reg1 <- rlm(formula = as.numeric(NGU) ~ as.numeric(impacts), data = cc_df_15)
  print(reg1$coefficients*10000)
}
cc_df_p <- cc_df_p[-1,]

cat("Day Time ")
cat("\n")
cc_df_d <- data.frame(NGU = NA, time = NA, impacts = NA, station = NA )
for(c in 1:length(channels)){
  
  d <- "DayTime"
  cc <- cumulative_counts[spots$Channel == channels[c] &  spots$daypart == d, ]
  impacts <- spots$ContactsMen[spots$Channel == channels[c] &  spots$daypart == d]
  
  if(dim(cc)[1] == 0) next 
  
  for (i in 1:dim(cc)[1]){
    for(j in 1:dim(cc)[2]){
      cc_df_d <- rbind(cc_df_d, c(cc[i, j], j, impacts[i], paste(channels[c])) )
    }
  }
  
  cc_df_15 <- cc_df_d[cc_df_d$time == 15,]
  
  print(channels[c])
  reg1 <- rlm(formula = as.numeric(NGU) ~ as.numeric(impacts), data = cc_df_15)
  print(reg1$coefficients*10000)
}
cc_df_d <- cc_df_d[-1,]


m <- as.numeric(max(c(cc_df_d$NGU, cc_df_p$NGU, cc_df_n$NGU)))

png(paste0(folder, "/2_RCurves_night.png"), width = 873, height = 482)

ggplot(cc_df_n, aes(x = as.numeric(time), y = as.numeric(NGU), colour = station)) + 
  ylim(c(0, m)) +
  xlab("Time") + 
  ylab("Cumulative NGU") +
  ggtitle( "Response Curves: NightTime") + 
  stat_smooth(se=FALSE) + 
  theme_bw()

dev.off()

png(paste0(folder, "/2_RCurves_prime.png"), width = 873, height = 482)

ggplot(cc_df_p, aes(x = as.numeric(time), y = as.numeric(NGU), colour = station)) + 
  ylim(c(0, m)) +
  xlab("Time") + 
  ylab("Cumulative NGU") +
  ggtitle( "Response Curves: PrimeTime") + 
  stat_smooth(se=FALSE) + 
  theme_bw()

dev.off()

png(paste0(folder, "/2_RCurves_day.png"), width = 873, height = 482)

ggplot(cc_df_d, aes(x = as.numeric(time), y = as.numeric(NGU), colour = station)) + 
  xlab("Time") + 
  ylim(c(0, m)) + 
  ylab("Cumulative NGU") +
  ggtitle( "Response Curves: DayTime") + 
  stat_smooth(se=FALSE) + 
  theme_bw()

dev.off()

# RESULTS: eCPI and VTR by daypart and day of the week------------------------------------------------------------------------------------------------------------
# eCPI and VTR by daypart
channels <- unique(spots$Channel)
ecpi_daypart <- data.frame(eCPI = NA, channel = NA, daypart = NA)
vtr_daypart <- data.frame(VTR = NA, channel = NA, daypart = NA)

for(c in 1:length(channels)){
  spots2<- spots[spots$Channel == channels[c], ]
  cc <- cumulative_counts[spots$Channel == channels[c], 15]
  price_general <- tapply(spots2$NetCost, spots2$daypart, sum, na.rm = TRUE)
  impacts_general <- tapply(spots2$ContactsMen, spots2$daypart, sum, na.rm = TRUE)
  
  ngus <- round(tapply(cc, spots2$daypart, sum, na.rm = TRUE))
  
  
  if(is.na(price_general['NightTime'])) {
    price_general['NightTime'] <- 0
    impacts_general['NightTime'] <- 0
    ngus['NightTime'] <- 0
  }
  
  if(is.na(price_general['DayTime'])) {
    price_general['DayTime'] <- 0
    impacts_general['DayTime'] <- 0
    ngus['DayTime'] <- 0
  }
  
  if(is.na(price_general['PrimeTime'])) {
    price_general['PrimeTime'] <- 0
    impacts_general['PrimeTime'] <- 0
    ngus['PrimeTime'] <- 0
  }
  
  
  
  eCPI <- price_general/ngus
  VTR <- ngus/impacts_general*100
  VTR[VTR == 'Inf'] <- 0 
  eCPI[eCPI == 'Inf'] <-0
  VTR[VTR == '-Inf'] <- 0 
  eCPI[eCPI == '-Inf'] <-0
  
  VTR[is.nan(VTR)] <- 0 
  eCPI[is.nan(eCPI)] <-0
  
  df <- data.frame(eCPI = eCPI, channel=  rep(channels[c], length(ngus)))
  df$daypart <- rownames(df)
  df2 <- data.frame(VTR = VTR, channel=  rep(channels[c], length(ngus)))
  df2$daypart <- rownames(df2)
  ecpi_daypart <- rbind(ecpi_daypart, df)
  vtr_daypart <- rbind(vtr_daypart, df2)
}

ecpi_daypart <- ecpi_daypart[-1, ]
vtr_daypart <- vtr_daypart[-1, ]

ecpi_daypart$eCPI[is.nan(ecpi_daypart$eCPI)] <-0
vtr_daypart$VTR[is.nan(vtr_daypart$VTR)] <- 0

#png(paste0(folder, "/3_eCPIS_daypart.png"), width = 873, height = 482)
#ggplot(ecpi_daypart, aes(x = daypart, y =eCPI , group = channel, color = channel)) + 
#  geom_line() +
#  ggtitle("eCPI by station and daypart")+ 
#  scale_x_discrete(limits=c("DayTime","PrimeTime","NightTime")) + 
#  theme_bw()
# dev.off()


#png(paste0(folder, "/3_VTR_daypart.png"), width = 873, height = 482)
#ggplot(vtr_daypart, aes(x = daypart, y =VTR , group = channel, color = channel)) + 
#  geom_line() +
#  ylab("Install Rate")+
#  ggtitle("Install Rate by station and daypart")+ 
#  scale_x_discrete(limits=c("DayTime","PrimeTime","NightTime")) + 
#  theme_bw()
#dev.off()

png(paste0(folder, "/3_eCPIS_daypart.png"), width = 873, height = 482)
qplot(factor(daypart), data =  ecpi_daypart, fill = daypart, geom="bar", weight = eCPI, 
      main = "eCPIs by Station",  ylab="eCPI") + 
  scale_x_discrete(limits=c("DayTime","PrimeTime","NightTime")) +
  scale_y_continuous(limits = c(0, max(ecpi_daypart$eCPI))) + 
  facet_wrap( ~ channel, scales="free")
dev.off()

png(paste0(folder, "/3_VTR_daypart.png"), width = 873, height = 482)
qplot(factor(daypart), data =  vtr_daypart, fill = daypart, geom="bar", weight = VTR, 
      main = "Install Rate by Station",  ylab="Install Rate") + 
  scale_x_discrete(limits=c("DayTime","PrimeTime","NightTime")) +
  scale_y_continuous(limits = c(0, max(vtr_daypart$VTR))) + 
  facet_wrap( ~ channel, scales="free")
dev.off()




# eCPI and VTR by day of the week 
ecpi_daypart <- data.frame(eCPI = NA, channel = NA, weekdays = NA)
vtr_daypart <- data.frame(VTR = NA, channel = NA, weekdays = NA)

for(c in 1:length(channels)){
  spots2 <- spots[spots$Channel == channels[c], ]
  cc <- cumulative_counts[spots$Channel == channels[c], 15]
  
  price_general <- tapply(spots2$NetCost, spots2$wday, sum)
  impacts_general <- tapply(spots2$ContactsMen, spots2$wday, sum)
  ngus <- tapply(cc, spots2$wday, sum )
  
  # Compute eCPI
  eCPI <- price_general/ngus
  VTR <- ngus/impacts_general
  
  VTR[VTR == 'Inf'] <- 0 
  eCPI[eCPI == 'Inf'] <-0
  VTR[VTR == '-Inf'] <- 0 
  eCPI[eCPI == '-Inf'] <-0
  
  
  df <- data.frame(eCPI = eCPI, channel=  rep(channels[c], length(ngus)))
  df$weekdays <- rownames(df)
  ecpi_daypart <- rbind(ecpi_daypart, df)
  
  df2 <- data.frame(VTR = VTR, channel=  rep(channels[c], length(ngus)))
  df2$weekdays <- rownames(df2)
  vtr_daypart <- rbind(vtr_daypart, df2)
}

ecpi_daypart <- ecpi_daypart[-1, ]
vtr_daypart  <- vtr_daypart[-1, ]




#ecpi_daypart <- ecpi_daypart[ecpi_daypart$channel %in% c("Servus TVD", "DELUXE", "VIVA", "TNT FILMGE", "TNT COMGE", "Eurosport"), ]
png(paste0(folder, "/4_eCPIS_dayweek.png"), width = 873, height = 482)
ggplot(ecpi_daypart, aes(x = weekdays, y =eCPI , group = channel, color = channel)) + 
  geom_line() +
  ggtitle("eCPI by station and Day of the week ") +
  scale_x_discrete( limits=c("1","2","3","4","5","6","0"), labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")) + 
  theme_bw()
dev.off()

#vtr_daypart <- vtr_daypart[vtr_daypart$channel %in%   c("Sony Set", "Star Network", "MCN", "SEVEN", "PRIME7"), ]
png(paste0(folder, "/4_VTR_dayweek.png"), width = 873, height = 482)
ggplot(vtr_daypart, aes(x = weekdays, y =VTR , group = channel, color = channel)) + 
  ylab("Install Rate") + 
  geom_line() +
  ggtitle("Install Rate by station and Day of the week ") +
  scale_x_discrete( limits=c("1","2","3","4","5","6","0"), labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")) + 
  theme_bw()
dev.off()

# RESULTS: Station Analysis --------------------------------------------------------------------------------------------------
# Colors
impacts.col <- '#558ED5'
eCPI.col <- '#77933C'
VTR.col <- '#ff751a'

# impacts by station
impacts_general <- tapply(spots$ContactsMen, spots$Channel, sum, na.rm = TRUE)
price_general <- tapply(spots$NetCost, spots$Channel, sum, na.rm = TRUE)
ngus <- tapply(cumulative_counts[,15], spots$Channel, sum, na.rm = TRUE )

# Compute eCPI and VTR
eCPIs <- price_general/ngus
VTR   <- ngus/impacts_general*100
ord   <- order(eCPIs)

sink(paste0(folder, "data.txt"))
# Print the details of the campaign in the screen
cat(" GRANULAR OVERVIEW: ")
cat("\n")
cat("Total Spend: ")
cat("\n")
print(price_general)
cat("\n")
cat("Number of Spots: ")
cat("\n")
print(tapply(spots$ContactsMen, spots$Channel, length))
cat("\n")
cat("TV Attributed NGUs: ")
cat("\n")
print(ngus)
cat("\n")
cat("Impacts: ")
cat("\n")
print(impacts_general)
cat("\n")
cat("eCPIs: ")
cat("\n")
print(eCPIs)
cat("\n")
cat("Install Rate: ")
cat("\n")
print(VTR)
cat("\n")
sink()
# Order data from lower eCPI to higher eCPI
ngus <- ngus[ord]
eCPIs <- eCPIs[ord]
VTR <- VTR[ord]

# Ratio = max Impacts/ max eCPI
ng_m <- (max(ngus) + 100)
ec_m <- (max(eCPIs) + 0.5)
ratio <- ng_m/(round(ec_m/6,2)*6)

# Plot 1
png(paste0(folder, "/5_StationAnalysis_eCPI.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs eCPI', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(eCPIs), tick=FALSE, las=2, line=-0.5, cex.axis=0.8)
lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("eCPI", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,2), bty='n')

dev.off()

# Plot 2: VTR
# Ratio = max Impacts/ maxVTR
vt_m <- (max(VTR) + 0.5)
ratio <- ng_m/(round(vt_m/6,2)*6)

png(paste0(folder, "/5_StationAnalysis_VTR.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs Install Rate', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(VTR), tick=FALSE, las=2, line=-0.5, cex.axis=0.80)
lines(b, VTR * ratio, col = VTR.col, lwd=2) 
points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("Install Rate (%)", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','IR (%)'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,2), bty='n')
dev.off()

# RESULTS: NGUs vs eCPIs/VTR --------------------------------------------------------------------------------------------------

# Colors
impacts.col <- '#558ED5'
eCPI.col    <- '#77933C'
VTR.col    <- '#ff751a'

spots2 <- spots[spots$daypart == "DayTime", ]
cc <- cumulative_counts[spots$daypart == "DayTime", 15]

# impacts by station
impacts_general <- tapply(spots2$ContactsMen, spots2$Channel, sum, na.rm = TRUE)
price_general <- tapply(spots2$NetCost, spots2$Channel, sum)
ngus <- tapply(cc, spots2$Channel, sum )

# Compute eCPI
eCPIs <- price_general/ngus
VTR <- ngus/impacts_general*100

VTR[VTR == 'Inf'] <- 0 
eCPI[eCPI == 'Inf'] <-0
VTR[VTR == '-Inf'] <- 0 
eCPI[eCPI == '-Inf'] <-0

# Order data from lower eCPI to higher eCPI
ord <- order(eCPIs)
ngus <- ngus[ord]
eCPIs <- eCPIs[ord]
impacts_general <- impacts_general[ord]
VTR <- VTR[ord]

ord <- row.names(eCPIs)

# Ratio = max Impacts/ maxeCPI
ng_m <- (max(ngus) + 100)
ec_m <- (max(eCPIs) + 0.5)

ratio <- ng_m/(round(ec_m/6,2)*6)


# Plot 1

png(paste0(folder, "/6_ngusecpi_Day.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs eCPI: DayTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(eCPIs), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("eCPI", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,2), bty='n')

dev.off()



# Compute eCPI
vt_m <- (max(VTR) + 0.5)
ratio <- ng_m/(round(vt_m/6,2)*6)

png(paste0(folder, "/6_ngusvtr_Day.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs Install Rate: DayTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(VTR), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, VTR * ratio, col = VTR.col, lwd=2) 
points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("Install Rate (%)", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','IR (%)'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,2), bty='n')

dev.off()

# PrimeTime
spots2 <- spots[spots$daypart == "PrimeTime", ]
cc <- cumulative_counts[spots$daypart == "PrimeTime", 15]

# impacts by station
impacts_general <- tapply(spots2$ContactsMen, spots2$Channel, sum, na.rm = TRUE)
price_general <- tapply(spots2$NetCost, spots2$Channel, sum)
ngus <- tapply(cc, spots2$Channel, sum )

# Compute eCPI
eCPIs <- price_general/ngus
VTR <- ngus/impacts_general*100

VTR[VTR == 'Inf'] <- 0 
eCPIs[eCPIs == 'Inf'] <-0
VTR[VTR == '-Inf'] <- 0 
eCPIs[eCPIs == '-Inf'] <-0

# Order data from lower eCPI to higher eCPI
ngus <- ngus[ord]
eCPIs <- eCPIs[ord]
impacts_general <- impacts_general[ord]
VTR <- VTR[ord]

# Ratio = max Impacts/ maxeCPI
ng_m <- (max(ngus, na.rm = TRUE) + 100)
ec_m <- (max(eCPIs, na.rm = TRUE) + 0.5)

ratio <- ng_m/(round(ec_m/6,2)*6)


# Plot 1

png(paste0(folder, "/6_ngusecpi_Prime.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs eCPI: PrimeTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(eCPIs), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("eCPI", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,2), bty='n')

dev.off()



# Compute eCPI
vt_m <- (max(VTR, na.rm = TRUE) + 0.5)
ratio <- ng_m/(round(vt_m/6,2)*6)

png(paste0(folder, "/6_ngusvtr_Prime.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs Install Rate: PrimeTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(VTR), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, VTR * ratio, col = VTR.col, lwd=2) 
points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("Install Rate (%)", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','IR (%)'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,2), bty='n')

dev.off()


# NightTime
spots2 <- spots[spots$daypart == "NightTime", ]
cc <- cumulative_counts[spots$daypart == "NightTime", 15]

# impacts by station
impacts_general <- tapply(spots2$ContactsMen, spots2$Channel, sum, na.rm = TRUE)
price_general <- tapply(spots2$NetCost, spots2$Channel, sum)
ngus <- tapply(cc, spots2$Channel, sum )

# Compute eCPI
eCPIs <- price_general/ngus
VTR <- ngus/impacts_general*100

VTR[VTR == 'Inf'] <- 0 
eCPIs[eCPIs == 'Inf'] <-0
VTR[VTR == '-Inf'] <- 0 
eCPIs[eCPIs == '-Inf'] <-0

# Order data from lower eCPI to higher eCPI
ngus <- ngus[ord]
eCPIs <- eCPIs[ord]
impacts_general <- impacts_general[ord]
VTR <- VTR[ord]

# Ratio = max Impacts/ maxeCPI
ng_m <- (max(ngus, na.rm = TRUE) + 100)
ec_m <- (max(eCPIs, na.rm = TRUE) + 0.5)

ratio <- ng_m/(round(ec_m/6,2)*6)


# Plot 1

png(paste0(folder, "/6_ngusecpi_Night.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs eCPI: NightTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(eCPIs), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("eCPI", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,2), bty='n')

dev.off()


vt_m <- (max(VTR, na.rm = TRUE) + 0.5)
ratio <- ng_m/(round(vt_m/6,2)*6)

png(paste0(folder, "/6_ngusvtr_Night.png"), width = 873, height = 482)
par(mar=c(6,6,4,4)+0.1)
b <- barplot(ngus, col = impacts.col, border=FALSE, main='NGUs vs Install Rate: NightTime', ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
axis(1, at=b, labels=row.names(VTR), tick=FALSE, las=2, line=-0.5, cex.axis=0.75)
lines(b, VTR * ratio, col = VTR.col, lwd=2) 
points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 

left.axis.pos<-seq(0, ng_m, by = ng_m/6)
axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)

mtext("Install Rate (%)", side=4, line = 3)
mtext("NGUs", side=2, line = 3)

legend('topleft', legend=c('NGUs','IR (%)'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,2), bty='n')

dev.off()

# ANALYSIS: by Station --------------------------------------------------------------------------------------------------

# Colors
impacts.col <- '#558ED5'
eCPI.col    <- '#77933C'
VTR.col     <- '#ff751a'

channels <- unique(spots$Channel)

# eCPI and VTR by station and daypart
for(c in 1:length(channels)){
  spots2 <- spots[spots$Channel == channels[c], ]
  cc <- cumulative_counts[spots$Channel == channels[c], 15]
  
  ngus <- tapply(cc, spots2$daypart, sum )
  impacts_general <- tapply(spots2$ContactsMen, spots2$daypart, sum)
  price_general <- tapply(spots2$NetCost, spots2$daypart, sum)
  
  # Compute eCPI and VTR
  eCPIs <- price_general/ngus
  VTR   <- ngus/impacts_general*100
  
  VTR[VTR == 'Inf'] <- 0 
  eCPIs[eCPIs == 'Inf'] <-0
  VTR[VTR == '-Inf'] <- 0 
  eCPIs[eCPIs == '-Inf'] <-0
  
  
  # Order data from lower eCPI to higher eCPI
  ord   <- c("DayTime", "PrimeTime", "NightTime")
  eCPIs <- eCPIs[ord]
  VTR   <- VTR[ord]
  impacts_general <- impacts_general[ord]
  ngus <- ngus[ord]
  
  # Ratio = max Impacts/ maxe CPI
  ng_m <- (max(ngus, na.rm = TRUE) + 150)
  ec_m <- (max(eCPIs, na.rm = TRUE) + 0.2)
  vt_m <- (max(VTR, na.rm =TRUE) + 0.05)
  ratio <- ng_m/(round(ec_m/6,2)*6)
  
  png(paste0(folder, "/7_",channels[c],"_eCPI.png"), width = 873, height = 482)
  
  par(mar=c(5,5,4,4)+0.1)
  b <- barplot(ngus, col = impacts.col, border=FALSE, main= paste0(channels[c],': NGU vs eCPI'), ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
  axis(1, at=b, labels=row.names(ngus), tick=FALSE, las=1, line=-0.5, cex.axis=0.9)
  lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
  points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 
  
  left.axis.pos<-seq(0, ng_m, by = ng_m/6)
  axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
  right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
  axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)
  
  mtext("eCPI", side=4, line = 3)
  mtext("NGU", side=2, line = 3)
  
  legend('topleft', legend=c('NGU','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,3), bty='n')
  
  dev.off()
  
  
  ratio <- ng_m/(round(vt_m/6,2)*6)
  
  png(paste0(folder, "/7_",channels[c],"_VTR.png"), width = 873, height = 482)
  
  par(mar=c(5,5,4,4)+0.1)
  b <- barplot(ngus, col = impacts.col, border=FALSE, main= paste0(channels[c],': NGU vs Install Rate'), ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
  axis(1, at=b, labels=row.names(ngus), tick=FALSE, las=1, line=-0.5, cex.axis=0.9)
  lines(b, VTR * ratio, col = VTR.col, lwd=2) 
  points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 
  
  left.axis.pos<-seq(0, ng_m, by = ng_m/6)
  axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
  right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
  axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)
  
  mtext("Install Rate (%)", side=4, line = 3)
  mtext("NGU", side=2, line = 3)
  
  legend('topleft', legend=c('NGU','IR'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,3), bty='n')
  
  dev.off()
  
  
}

# eCPI and VTR by station and weekday
for(c in 1:length(channels)){
  spots2 <- spots[spots$Channel == channels[c], ]
  cc <- cumulative_counts[spots$Channel == channels[c], 15]
  
  ngus <- tapply(cc, spots2$wday, sum )
  impacts_general <- tapply(spots2$ContactsMen, spots2$wday, sum)
  price_general <- tapply(spots2$NetCost, spots2$wday, sum)
  
  ngus[ngus == 0] <- 1
  
  # Compute eCPI and VTR
  eCPIs <- price_general/ngus
  VTR   <- ngus/impacts_general*100
  ord <- c(2,3,4,5,6,7,1)
  
  VTR[VTR == 'Inf'] <- 0 
  eCPI[eCPI == 'Inf'] <-0
  VTR[VTR == '-Inf'] <- 0 
  eCPI[eCPI == '-Inf'] <-0
  
  # Order data from lower eCPI to higher eCPI
  eCPIs <- eCPIs[ord]
  VTR   <- VTR[ord]
  impacts_general <- impacts_general[ord]
  ngus <- ngus[ord]
  
  # Ratio = max Impacts/ maxe CPI
  ng_m <- (max(ngus, na.rm = TRUE) + 150)
  ec_m <- (max(eCPIs, na.rm = TRUE) + 0.2)
  vt_m <- (max(VTR, na.rm = TRUE) + 0.05)
  ratio <- ng_m/(round(ec_m/6,2)*6)
  
  png(paste0(folder, "/7_",channels[c],"_eCPI_wday.png"), width = 873, height = 482)
  
  par(mar=c(5,5,4,4)+0.1)
  b <- barplot(ngus, col = impacts.col, border=FALSE, main= paste0(channels[c],': NGU vs eCPI'), ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
  axis(1, at=b, labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"), tick=FALSE, las=1, line=-0.5, cex.axis=0.9)
  lines(b, eCPIs * ratio, col = eCPI.col, lwd=2) 
  points(b, eCPIs * ratio, col = eCPI.col, bg='white', pch=21) 
  
  left.axis.pos<-seq(0, ng_m, by = ng_m/6)
  axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
  right.axis.ticks<-seq(0, round(ec_m/6,2)*6, by = round(ec_m/6,2))
  axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)
  
  mtext("eCPI", side=4, line = 3)
  mtext("NGU", side=2, line = 3)
  
  legend('topleft', legend=c('NGU','eCPI'), col=c(impacts.col,eCPI.col), lty=1, lwd=c(4,3), bty='n')
  
  dev.off()
  
  
  
  ratio <- ng_m/(round(vt_m/6,2)*6)
  
  png(paste0(folder, "/7_",channels[c],"_VTR_wday.png"), width = 873, height = 482)
  
  par(mar=c(5,5,4,4)+0.1)
  b <- barplot(ngus, col = impacts.col, border=FALSE, main= paste0(channels[c],': NGU vs Install Rate'), ylim=c(0,ng_m), axes=FALSE, las=2, xaxt = 'n')
  axis(1, at=b, labels=c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"), tick=FALSE, las=1, line=-0.5, cex.axis=0.9)
  lines(b, VTR * ratio, col = VTR.col, lwd=2) 
  points(b, VTR * ratio, col = VTR.col, bg='white', pch=21) 
  
  left.axis.pos<-seq(0, ng_m, by = ng_m/6)
  axis(2, at=left.axis.pos, labels=formatC(left.axis.pos, big.mark = ",", format = "d"),las=2, cex.axis=0.8)
  right.axis.ticks<-seq(0, round(vt_m/6,2)*6, by = round(vt_m/6,2))
  axis(4, at=(right.axis.ticks)*ratio, labels=right.axis.ticks, las=2,  cex.axis=0.8)
  
  mtext("Install Rate (%)", side=4, line = 3)
  mtext("NGU", side=2, line = 3)
  
  legend('topleft', legend=c('NGU','IR'), col=c(impacts.col,VTR.col), lty=1, lwd=c(4,3), bty='n')
  
  dev.off()
  
  
}

# OVERVIEW ------------------------------------------------------------------------------------------------------
impacts_general <- tapply(spots$ContactsMen, spots$Channel, sum, na.rm = TRUE)
price_general   <- tapply(spots$NetCost, spots$Channel, sum)
ngus            <- tapply(cumulative_counts[,15], spots$Channel, sum, na.rm = TRUE)

# Compute the benchmark of each day
days <- unique(as.Date(seq(from = as.POSIXct(campaign_start), to = as.POSIXct(campaign_end), by = "day")))

cat("COMPUTING THE BENCHMARK")
cat("\n")
NGU_b <- data.frame(days , ngu = rep(0, length(days))) 
NGU_b$ngu <- sapply(as.POSIXlt(NGU_b$days)$wday, benchmark_day)

# Create the dataframes for the plot
NGU_campaign2      <- NGU_campaign
NGU_benchmark$date <- as.Date(NGU_benchmark$date_register)
NGU_campaign2      <- NGU_campaign2[NGU_campaign2$real_date <= campaign_end & NGU_campaign2$real_date >= campaign_start, ]

cc     <- cumulative_counts[,15]
NGU_TV <- tapply(cc, as.Date(spots$times), sum, na.rm = TRUE)

NGU_total    <- tapply( NGU_campaign2$user_id, as.Date(NGU_campaign2$real_date), FUN = function(x) length(unique(x)) )

NGU_TV<-NGU_TV[row.names(NGU_total)]
row.names(NGU_TV) <- row.names(NGU_total)
NGU_TV[is.na(NGU_TV )] <- 0 

NGU_Branding <- NGU_total  - NGU_b$ngu - NGU_TV[row.names(NGU_total)]

NGU_Branding[NGU_Branding < 0 ] <- 0


df_NGU <- data.frame(days = NGU_b$days, NGU = NGU_b$ngu, Attributed = rep("Benchmark", length(NGU_b$ngu)))
df_NGU <- rbind(df_NGU, data.frame(days = row.names(NGU_Branding), NGU = NGU_Branding, Attributed = rep("Branding", length(NGU_Branding))))
df_NGU <- rbind(df_NGU, data.frame(days = row.names(NGU_TV), NGU = NGU_TV, Attributed = rep("TV", length(NGU_TV))))


# save the plot 
png(paste0(folder, "/1_OverviewIII_stackeareadgraph.png"), width = 873, height = 482)

# Stacked Area graph 
ggplot(df_NGU, aes( days, NGU)) + 
  geom_area(aes(colour = Attributed, fill= Attributed), position = 'stack', alpha = 0.8) + 
  theme_bw() + 
  theme(axis.text.x  = element_text(angle=90, vjust=0.5))

dev.off()

sink(paste0(folder, "data1.txt"))
# Print the details of the campaign in the screen
cat("OVERVIEW II: ")
cat("\n")
cat(paste0("Total Spend: ",sum(price_general)))
cat("\n")
cat(paste0("Number of Spots: ",dim(spots)[1]))
cat("\n")
cat(paste0("Total Impacts: ",sum(impacts_general)))
cat("\n")
cat(paste0("Total NGU: ",  sum(NGU_total) ))
cat("\n")
cat(paste0("TV attributed NGU: ", sum(NGU_TV) , " (",  sum(NGU_TV)/sum(NGU_total) *100, " %)"  ))
cat("\n")
cat(paste0("Branding attributed NGU: ", sum(NGU_Branding, na.rm = TRUE)  , " (",  sum(NGU_Branding, na.rm = TRUE)/sum(NGU_total, na.rm = TRUE) *100, " %)"     ))
cat("\n")
cat(paste0("CP NGU: ",  sum(price_general)/sum(ngus)   ))
cat("\n")
cat(paste0("View-Through Ratio: ",  sum(ngus)/sum(impacts_general)*100))
cat("\n")
sink()


