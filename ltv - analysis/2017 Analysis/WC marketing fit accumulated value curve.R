### World Chef RPI prediction based RPI curve from past data
rm(list=ls())

#install.packages("RPostgreSQL")
library(RPostgreSQL)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(gridExtra)
library(scales)

##############################
### Connecting to the database
##############################
drv <- dbDriver("PostgreSQL")
spdb <- dbConnect(
  drv,
  host = "redshift.pro.rc.laicosp.net", # (rc, mc, dc)
  port = "5439",
  dbname = "datawarehouse",
  user = "amckay",
  password = "o6EbWm4L79jRfegy"
)

game_schema = 'restaurantcity' # (restaurantcity, monstercity, dragoncity)

### SQL query functions - see bottom of script for neat version of queries!
# to obtain total spend per day for users who registered between 01/03/16 and 31/08/16 (only taking revenues up to end of September)
SQL_total_spend_per_day <- function(register_platform, register_country, register_source) {
  paste("SELECT days_since_register, SUM(amount_gross) AS total_income FROM (SELECT user_id, date(date_register) AS day_register, DATEDIFF(d, date(date_register), '2016-08-31') cohort, register_platform, register_ip_country, register_source FROM ", game_schema,".t_user WHERE date_register > '2016-03-01' AND date_register < '2016-09-01' AND register_platform = '",register_platform,"' AND register_ip_country = '",register_country,"' AND lower(register_source) = '",register_source,"' AND user_category = 'player') tu LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime FROM ", game_schema,".t_transaction WHERE datetime < '2016-09-01') tt ON tu.user_id = tt.user_id GROUP BY days_since_register ORDER BY days_since_register LIMIT 500;", sep="")
}

# to obtain number of users active for each period
SQL_users_per_active_period <- function(register_platform, register_country, register_source) {
  paste("SELECT COUNT(DISTINCT tu.user_id), active_days FROM (SELECT user_id, date_register, DATEDIFF(d, date(date_register), '2016-08-31') active_days, register_platform, register_ip_country, register_source FROM ", game_schema,".t_user WHERE date_register > '2016-03-01' AND date_register < '2016-09-01' AND register_platform = '",register_platform,"' AND register_ip_country = '",register_country,"' AND lower(register_source) = '",register_source,"' AND user_category = 'player') tu LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime FROM ", game_schema,".t_transaction WHERE datetime < '2016-09-01') tt ON tu.user_id = tt.user_id GROUP BY active_days ORDER BY active_days;", sep="")
}

# to obtain average spend per day for users who registered between 01/09/16 and 30/09/16
SQL_get_avg_spend_per_day_test_dates <- function(register_platform, register_country, register_source) {
  paste("SELECT days_since_register, sum(amount_gross)/(SELECT COUNT(DISTINCT user_id) FROM ", game_schema,".t_user WHERE date_register > '2016-09-01' AND date_register < '2016-10-01' AND register_platform = '",register_platform,"' AND register_ip_country = '",register_country,"' AND lower(register_source) = '",register_source,"' AND user_category = 'player') AS avg_spend, (SELECT COUNT(DISTINCT user_id) FROM ", game_schema,".t_user WHERE date_register > '2016-09-01' AND date_register < '2016-10-01' AND register_platform = '",register_platform,"' AND register_ip_country = '",register_country,"' AND lower(register_source) = '",register_source,"' AND user_category = 'player') AS total_users FROM (SELECT user_id, date_register, register_platform, register_ip_country, register_source FROM ", game_schema,".t_user WHERE date_register > '2016-09-01' AND date_register < '2016-10-01' AND register_platform = '",register_platform,"' AND register_ip_country = '",register_country,"' AND lower(register_source) = '",register_source,"' AND user_category = 'player') tu LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime FROM ", game_schema,".t_transaction) tt ON tu.user_id = tt.user_id GROUP BY days_since_register ORDER BY days_since_register LIMIT 500;", sep="")
}

################################################
### function to return prediction, actual and prediction as % of actual
################################################
# step by step version of function at end of script

cumulative_spend_fn <- function(daily_data, t) {
  sum(daily_data[1:t], na.rm = TRUE)
}

sum_users_fn <- function(active_period_data, t) {
  sum(active_period_data[t:length(active_period_data)])
}

extrapolation_model <- function(register_source, register_platform, register_country, days) {
  # setup data
  income <- dbGetQuery(spdb, SQL_total_spend_per_day(register_platform, register_country, register_source))
  users_active_period <- dbGetQuery(spdb, SQL_users_per_active_period(register_platform, register_country, register_source))
  users_per_tenure <- sapply(days, function(t) sum_users_fn(users_active_period$count, t))
  daily_spend <- sapply(days, function(t) sum(income$total_income[income$days_since_register==t-1], na.rm = TRUE))
  avg_daily_spend <- daily_spend/users_per_tenure
  cumulative_spend <- sapply(days, function(t) cumulative_spend_fn(avg_daily_spend, t))
  
  # fitting non-linear curve to training data
  nls_fit <- nls(cumulative_spend~a+days^b, start = list(a = 1, b = 1))
  predicted <- predict(nls_fit,newdata=1:365, interval="prediction")
  expCoef <- round(coefficients(nls_fit),digits=2)
  rsquaredExp <- round(summary(lm(cumulative_spend ~ predicted[days]))$r.squared, digits=2)
  
  # looking at how the fit compares to test data
  income_test_dates <- dbGetQuery(spdb, SQL_get_avg_spend_per_day_test_dates(register_platform, register_country, register_source))
  income_test_dates <- income_test_dates[income_test_dates$days_since_register<=180,]
  cumulative_spend_test_dates <- sapply(days, function(t) cumulative_spend_fn(income_test_dates$avg_spend, t))
  number_users_test_dates <- income_test_dates[1,3]
  c(predict(nls_fit,newdata=days)[length(days)],
    cumulative_spend_test_dates[length(days)],
    predict(nls_fit,newdata=days)[length(days)]/cumulative_spend_test_dates[length(days)],
    number_users_test_dates)
}

##########
### output
##########
# running for each mix of:
# register source = mobilda, adcolony, google adwords, facebook ads, vungle
# platform = android, ios
# country = US, DE, BR, FR, GB

### US
mobilda_android_US <- extrapolation_model('mobilda', 'android', 'US', 1:180)
mobilda_ios_US <- extrapolation_model('mobilda', 'ios', 'US', 1:180)
adcolony_android_US <- extrapolation_model('adcolony', 'android', 'US', 1:180)
adcolony_ios_US <- extrapolation_model('adcolony', 'ios', 'US', 1:180)
google_adwords_android_US <- extrapolation_model('google adwords', 'android', 'US', 1:180)
facebook_ads_android_US <- extrapolation_model('facebook ads', 'android', 'US', 1:180)
facebook_ads_ios_US <- extrapolation_model('facebook ads', 'ios', 'US', 1:180)
vungle_android_US <- extrapolation_model('vungle', 'android', 'US', 1:180)
vungle_ios_US <- extrapolation_model('vungle', 'ios', 'US', 1:180)

### DE
mobilda_android_DE <- extrapolation_model('mobilda', 'android', 'DE', 1:180)
mobilda_ios_DE <- extrapolation_model('mobilda', 'ios', 'DE', 1:180)
adcolony_android_DE <- extrapolation_model('adcolony', 'android', 'DE', 1:180)
adcolony_ios_DE <- extrapolation_model('adcolony', 'ios', 'DE', 1:180)
google_adwords_android_DE <- extrapolation_model('google adwords', 'android', 'DE', 1:180)
google_adwords_ios_DE <- extrapolation_model('google adwords', 'ios', 'DE', 1:180)
facebook_ads_android_DE <- extrapolation_model('facebook ads', 'android', 'DE', 1:180)
facebook_ads_ios_DE <- extrapolation_model('facebook ads', 'ios', 'DE', 1:180)
vungle_android_DE <- extrapolation_model('vungle', 'android', 'DE', 1:180)
vungle_ios_DE <- extrapolation_model('vungle', 'ios', 'DE', 1:180)

### BR
mobilda_android_BR <- extrapolation_model('mobilda', 'android', 'BR', 1:180)
mobilda_ios_BR <- extrapolation_model('mobilda', 'ios', 'BR', 1:180)
adcolony_android_BR <- extrapolation_model('adcolony', 'android', 'BR', 1:180)
adcolony_ios_BR <- extrapolation_model('adcolony', 'ios', 'BR', 1:180)
google_adwords_android_BR <- extrapolation_model('google adwords', 'android', 'BR', 1:180)
google_adwords_ios_BR <- extrapolation_model('google adwords', 'ios', 'BR', 1:180)
facebook_ads_android_BR <- extrapolation_model('facebook ads', 'android', 'BR', 1:180)
facebook_ads_ios_BR <- extrapolation_model('facebook ads', 'ios', 'BR', 1:180)
vungle_android_BR <- extrapolation_model('vungle', 'android', 'BR', 1:180)
vungle_ios_BR <- extrapolation_model('vungle', 'ios', 'BR', 1:180)

### FR
mobilda_android_FR <- extrapolation_model('mobilda', 'android', 'FR', 1:180)
mobilda_ios_FR <- extrapolation_model('mobilda', 'ios', 'FR', 1:180)
adcolony_android_FR <- extrapolation_model('adcolony', 'android', 'FR', 1:180)
adcolony_ios_FR <- extrapolation_model('adcolony', 'ios', 'FR', 1:180)
google_adwords_android_FR <- extrapolation_model('google adwords', 'android', 'FR', 1:180)
google_adwords_ios_FR <- extrapolation_model('google adwords', 'ios', 'FR', 1:180)
facebook_ads_android_FR <- extrapolation_model('facebook ads', 'android', 'FR', 1:180)
facebook_ads_ios_FR <- extrapolation_model('facebook ads', 'ios', 'FR', 1:180)
vungle_android_FR <- extrapolation_model('vungle', 'android', 'FR', 1:180)
vungle_ios_FR <- extrapolation_model('vungle', 'ios', 'FR', 1:180)

### GB
mobilda_android_GB <- extrapolation_model('mobilda', 'android', 'GB', 1:180)
mobilda_ios_GB <- extrapolation_model('mobilda', 'ios', 'GB', 1:180)
adcolony_android_GB <- extrapolation_model('adcolony', 'android', 'GB', 1:180)
adcolony_ios_GB <- extrapolation_model('adcolony', 'ios', 'GB', 1:180)
google_adwords_android_GB <- extrapolation_model('google adwords', 'android', 'GB', 1:180)
google_adwords_ios_GB <- extrapolation_model('google adwords', 'ios', 'GB', 1:180)
facebook_ads_android_GB <- extrapolation_model('facebook ads', 'android', 'GB', 1:180)
facebook_ads_ios_GB <- extrapolation_model('facebook ads', 'ios', 'GB', 1:180)
vungle_android_GB <- extrapolation_model('vungle', 'android', 'GB', 1:180)
vungle_ios_GB <- extrapolation_model('vungle', 'ios', 'GB', 1:180)

output_US <- as.data.frame(rbind("mobilda_android_US" = mobilda_android_US,
                "mobilda-ios" = mobilda_ios_US,
                "adcolony-android" = adcolony_android_US,
                "adcolony-ios" = adcolony_ios_US,
                "google aw-android" = google_adwords_android_US,
                #"google aw-ios" = google_adwords_ios_US,
                "fb ads-android" = facebook_ads_android_US,
                "fb ads-ios" = facebook_ads_ios_US,
                "vungle-android" = vungle_android_US,
                "vungle-ios" = vungle_ios_US))

colnames(output_US) = c('predicted', 'actual', 'pred/actual', 'actual users')

output_DE <- as.data.frame(rbind("mobilda-android" = mobilda_android_DE,
                                 "mobilda-ios" = mobilda_ios_DE,
                                 "adcolony-android" = adcolony_android_DE,
                                 "adcolony-ios" = adcolony_ios_DE,
                                 "google aw-android" = google_adwords_android_DE,
                                 #"google aw-ios" = google_adwords_ios_DE,
                                 "fb ads-android" = facebook_ads_android_DE,
                                 "fb ads-ios" = facebook_ads_ios_DE,
                                 "vungle-android" = vungle_android_DE,
                                 "vungle-ios" = vungle_ios_DE))

colnames(output_DE) = c('predicted', 'actual', 'pred/actual', 'actual users')

output_BR <- as.data.frame(rbind("mobilda-android" = mobilda_android_BR,
                                 "mobilda-ios" = mobilda_ios_BR,
                                 "adcolony-android" = adcolony_android_BR,
                                 "adcolony-ios" = adcolony_ios_BR,
                                 "google aw-android" = google_adwords_android_BR,
                                 "google aw-ios" = google_adwords_ios_BR,
                                 "fb ads-android" = facebook_ads_android_BR,
                                 "fb ads-ios" = facebook_ads_ios_BR,
                                 "vungle-android" = vungle_android_BR,
                                 "vungle-ios" = vungle_ios_BR))

colnames(output_BR) = c('predicted', 'actual', 'pred/actual', 'actual users')

output_FR <- as.data.frame(rbind("mobilda-android" = mobilda_android_FR,
                                 "mobilda-ios" = mobilda_ios_FR,
                                 "adcolony-android" = adcolony_android_FR,
                                 "adcolony-ios" = adcolony_ios_FR,
                                 "google aw-android" = google_adwords_android_FR,
                                 "google aw-ios" = google_adwords_ios_FR,
                                 "fb ads-android" = facebook_ads_android_FR,
                                 "fb ads-ios" = facebook_ads_ios_FR,
                                 "vungle-android" = vungle_android_FR,
                                 "vungle-ios" = vungle_ios_FR))

colnames(output_FR) = c('predicted', 'actual', 'pred/actual', 'actual users')

output_GB <- as.data.frame(rbind("mobilda-android" = mobilda_android_GB,
                                 "mobilda-ios" = mobilda_ios_GB,
                                 "adcolony-android" = adcolony_android_GB,
                                 "adcolony-ios" = adcolony_ios_GB,
                                 "google aw-android" = google_adwords_android_GB,
                                 #"google aw-ios" = google_adwords_ios_GB,
                                 "fb ads-android" = facebook_ads_android_GB,
                                 "fb ads-ios" = facebook_ads_ios_GB,
                                 "vungle-android" = vungle_android_GB,
                                 "vungle-ios" = vungle_ios_GB))

colnames(output_GB) = c('predicted', 'actual', 'pred/actual', 'actual users')


#################################
### step by step through function
#################################
days <- 1:180
register_source <- 'google adwords'

### US, android
register_platform <- 'android'
register_country <- 'BR'
income_android_US <- dbGetQuery(spdb, SQL_total_spend_per_day(register_platform, register_country, register_source))
users_active_period_android_US <- dbGetQuery(spdb, SQL_users_per_active_period(register_platform, register_country, register_source))

users_per_tenure <- 
  sapply(days, function(t) sum_users_fn(users_active_period_android_US$count, t))

daily_spend <- sapply(days, function(t)
  sum(income_android_US$total_income[income_android_US$days_since_register==t-1], na.rm = TRUE))

avg_daily_spend <- daily_spend/users_per_tenure

cumulative_spend_android_US <-
  sapply(days, function(t) cumulative_spend_fn(avg_daily_spend, t))

# fitting non-linear curve to training data - maybe overfitting??
nls_fit_android_US <- nls(cumulative_spend_android_US~a+days^b, start = list(a = 1, b = 1))
predicted<-predict(nls_fit_android_US,newdata=1:365, interval="prediction")
expCoef<-round(coefficients(nls_fit_android_US),digits=2)
rsquaredExp<-round(summary(lm(cumulative_spend_android_US ~ predicted[days]))$r.squared, digits=2)

plot(days, cumulative_spend_android_US,
     xlab = "days since register", ylab = "average cumulative spend",
     main = "Average cumulative spend
     users who registered in Mar-16 to Aug-16",
     ylim = c(0,4))
lines(1:180, predicted[1:180], col = "red", lwd=2)
legend("bottomright",legend=paste0("y = ", expCoef[1], " + ","x^",expCoef[2],"\nR-squared: ", rsquaredExp,"\n"),lty=1,col="red",lwd=2, cex = 0.6, bg ="white" )


# looking at how the fit compares to test data
income_android_US_test_dates <- dbGetQuery(spdb, SQL_get_avg_spend_per_day_test_dates())
income_android_US_test_dates <- income_android_US_test_dates[income_android_US_test_dates$days_since_register<=180,]

cumulative_spend_android_US_test_dates <-
  sapply(days, function(t) cumulative_spend_fn(income_android_US_test_dates$avg_spend, t))

predict(nls_fit_android_US,newdata=days)[180]/cumulative_spend_android_US_test_dates[180]

plot(days, cumulative_spend_android_US_test_dates,
     xlab = "days since register", ylab = "average cumulative spend",
     main = "Average cumulative spend
     users who registered in Sep-16",
     ylim = c(0,4))
lines(1:180, predicted[1:180], col = "red", lwd=2)
legend("bottomright",legend=paste0("y = ", expCoef[1], " + ","x^",expCoef[2],"\nR-squared: ", rsquaredExp,"\n"),lty=1,col="red",lwd=2, cex = 0.6, bg ="white" )



################################
### neat versions of sql queries
################################

# total spend per day for users registered in Mar-16 to Aug-16 (but only looking at revenues up to Aug-16)
"SELECT days_since_register, SUM(amount_gross)
FROM      (SELECT user_id, date(date_register) AS day_register, DATEDIFF(d, date(date_register), '2016-08-31') cohort, register_platform, register_ip_country, register_source
           FROM t_user
           WHERE date_register > '2016-03-01'
           AND date_register < '2016-09-01'
           AND register_platform = 'android'
           AND register_ip_country = 'US'
           AND lower(register_source) = 'adcolony') tu
LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime
           FROM t_transaction
           WHERE datetime < '2016-09-01') tt
ON tu.user_id = tt.user_id
GROUP BY days_since_register
ORDER BY days_since_register
LIMIT 500;"

# count users who registered at each date
"SELECT COUNT(DISTINCT tu.user_id), active_days
FROM      (SELECT user_id, date_register, DATEDIFF(d, date(date_register), '2016-08-31') active_days, register_platform, register_ip_country, register_source
           FROM t_user
           WHERE date_register > '2016-03-01'
           AND date_register < '2016-09-01'
           AND register_platform = 'android'
           AND register_ip_country = 'US'
           AND lower(register_source) = 'adcolony') tu
LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime
           FROM t_transaction
           WHERE datetime < '2016-09-01') tt
ON tu.user_id = tt.user_id
GROUP BY active_days
ORDER BY active_days;"

# average spend per day for users registered in Sep-16
"SELECT days_since_register, sum(amount_gross)/(SELECT COUNT(DISTINCT user_id)
                                               FROM t_user
                                               WHERE date_register > '2016-09-01'
                                               AND date_register < '2016-10-01'
                                               AND register_platform = 'android'
                                               AND register_ip_country = 'US'
                                               AND lower(register_source) = 'adcolony') AS avg_spend
FROM      (SELECT user_id, date_register, register_platform, register_ip_country, register_source
           FROM t_user
           WHERE date_register > '2016-09-01'
           AND date_register < '2016-10-01'
           AND register_platform = 'android'
           AND register_ip_country = 'US'
           AND lower(register_source) = 'adcolony') tu
LEFT JOIN (SELECT user_id, DATEDIFF(d, date_register, datetime) AS days_since_register, days_from_register, amount_gross, datetime
           FROM t_transaction) tt
ON tu.user_id = tt.user_id
GROUP BY days_since_register
ORDER BY days_since_register
LIMIT 500;"












































