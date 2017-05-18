### Dragon City actual vs day 7 predicted ltv
rm(list=ls())

#install.packages("RPostgreSQL")
library(RPostgreSQL)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(ggplot2)
library(gridExtra)
library(scales)

##############################
### Connecting to the database
##############################
drv <- dbDriver("PostgreSQL")
spdb <- dbConnect(
  drv,
  host = "redshift.pro.dc.laicosp.net", # (rc, mc, dc)
  port = "5439",
  dbname = "datawarehouse",
  user = "amckay",
  password = "o6EbWm4L79jRfegy"
)

game_schema = 'dragoncity' # (restaurantcity, monstercity, dragoncity)
pred_date_start = '2016-05-24' # (2016-07-08 ,2016-05-03, 2016-05-24)

### SQL query to obtain table of predictions for a particular date - see bottom of script for neat version of query!
SQL_get_ltv_predictions <- function(register_source, register_platform, register_country, date) {
  paste("SELECT users.user_id, date_register, ltv_actual, predicted_category, ltv_median FROM (SELECT user_id, date_register FROM ", game_schema,".t_user WHERE date_register > convert(datetime, '", date,"') AND date_register < DATEADD(day, 1, convert(datetime, '", date,"')) AND register_platform = '", register_platform,"' AND register_ip_country = '", register_country,"' AND lower(register_source) = '", register_source,"' AND user_category = 'player') users LEFT JOIN (SELECT user_id, NVL(sum(amount_gross)) as ltv_actual FROM ", game_schema,".t_transaction WHERE date_register > convert(datetime, '", date,"') AND date_register < DATEADD(day, 1, convert(datetime, '", date,"')) AND datetime <= DATEADD(day, 180, convert(datetime, '", date,"')) GROUP BY user_id) actual_ltv ON users.user_id = actual_ltv.user_id LEFT JOIN (SELECT * FROM (SELECT DISTINCT user_id, FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels FROM ", game_schema,".mlp_ltv_output WHERE datetime >= convert(datetime, '", date,"') AND datetime <= DATEADD(day, ", forecast_day,", convert(datetime, '", date,"'))) mlp LEFT JOIN (SELECT predicted_category, ltv_median FROM ", game_schema,".aux_category_ltv_median WHERE date_start = convert(datetime, '", pred_date_start,"')) aux ON mlp.scored_labels = aux.predicted_category) predicted_ltv ON users.user_id = predicted_ltv.user_id", sep = "")
}

#############################################
### setup functions and some dates to analyse
#############################################
# create list of registration dates to analyse
dates <- list(NULL)
count = 1
for(day in 1:30) {
  dates[count] <- paste(2016,09,day, sep = "-")
  count <- count+1
}

# function to extract predicted, actual, pred/actual and number of users
ltv_prediction_model <- function(register_source, register_platform, register_country, dates) {
  # loop over dates to generate data
  temp_data <- NULL
  for(d in dates) {
    temp_data <- rbind(temp_data, dbGetQuery(spdb, SQL_get_ltv_predictions(register_source, register_platform, register_country, d)))
  }
  
  # output
  c(sum(temp_data$ltv_median, na.rm = TRUE)/nrow(temp_data),
    sum(temp_data$ltv_actual, na.rm = TRUE)/nrow(temp_data),
    sum(temp_data$ltv_median, na.rm = TRUE)/sum(temp_data$ltv_actual, na.rm = TRUE),
    nrow(temp_data))
}

#####################
### day 7 predictions
#####################
forecast_day = 7

### US
ML_applifier_android_US <- ltv_prediction_model('applifier', 'android', 'US', dates)
ML_applifier_ios_US <- ltv_prediction_model('applifier', 'ios', 'US', dates)
ML_adcolony_android_US <- ltv_prediction_model('adcolony', 'android', 'US', dates)
ML_adcolony_ios_US <- ltv_prediction_model('adcolony', 'ios', 'US', dates)
ML_google_adwords_android_US <- ltv_prediction_model('google adwords', 'android', 'US', dates)
ML_google_adwords_ios_US <- ltv_prediction_model('google adwords', 'ios', 'US', dates)
ML_resultsmedia_android_US <- ltv_prediction_model('resultsmedia', 'android', 'US', dates)
ML_resultsmedia_ios_US <- ltv_prediction_model('resultsmedia', 'ios', 'US', dates)
ML_vungle_android_US <- ltv_prediction_model('vungle', 'android', 'US', dates)
ML_vungle_ios_US <- ltv_prediction_model('vungle', 'ios', 'US', dates)

output_US_ML <- as.data.frame(rbind("applifier_android_US" = ML_applifier_android_US,
                                 "applifier-ios" = ML_applifier_ios_US,
                                 "adcolony-android" = ML_adcolony_android_US,
                                 "adcolony-ios" = ML_adcolony_ios_US,
                                 "google aw-android" = ML_google_adwords_android_US,
                                 "google aw-ios" = ML_google_adwords_ios_US,
                                 "res med-android" = ML_resultsmedia_android_US,
                                 "res med-ios" = ML_resultsmedia_ios_US,
                                 "vungle-android" = ML_vungle_android_US,
                                 "vungle-ios" = ML_vungle_ios_US))

colnames(output_US_ML) = c('predicted', 'actual', 'pred/actual', 'actual users')

#############################
### neat version of sql query
#############################

"SELECT users.user_id, date_register, ltv_actual, predicted_category, ltv_median
FROM      (SELECT user_id, date_register
           FROM t_user
           WHERE date_register > convert(datetime, '2016-09-01')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-09-01'))
           AND register_platform = 'android'
           AND register_ip_country = 'US'
           AND lower(register_source) = 'applifier'
           AND user_category = 'player') users
LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual
           FROM t_transaction
           WHERE date_register > convert(datetime, '2016-09-01')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-09-01')) 
           AND datetime <= DATEADD(day, 180, convert(datetime, '2016-09-01'))
           GROUP BY user_id) actual_ltv
ON users.user_id = actual_ltv.user_id
LEFT JOIN (SELECT *
             FROM      (SELECT DISTINCT user_id,
                        FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels
                        FROM mlp_ltv_output
                        WHERE datetime >= convert(datetime, '2016-09-01')
                        AND datetime <= DATEADD(day, 7, convert(datetime, '2016-09-01'))) mlp
           LEFT JOIN (SELECT predicted_category, ltv_median
                      FROM aux_category_ltv_median
                      WHERE date_start = convert(datetime, '2016-05-24')) aux
           ON mlp.scored_labels = aux.predicted_category) predicted_ltv
ON users.user_id = predicted_ltv.user_id"













































