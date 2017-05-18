### World Chef actual vs predicted ltv
rm(list=ls())

#install.packages("RPostgreSQL")
library(RPostgreSQL)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)

##############################
### Connecting to the database
##############################
drv <- dbDriver("PostgreSQL")
spdb <- dbConnect(
  drv,
  host = "redshift.pro.rc.laicosp.net",
  port = "5439",
  dbname = "datawarehouse",
  user = "amckay",
  password = "o6EbWm4L79jRfegy"
)

### Obtaining data
SQL_get_ltvs <- function(datetime) {
  paste("SELECT users.user_id, date_register, register_platform, register_ip_country, ltv_actual, predicted_category, ltv_median FROM (SELECT user_id, date_register, register_platform, register_ip_country FROM restaurantcity.t_user WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "'))) users LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual FROM restaurantcity.t_transaction WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "')) AND datetime <= DATEADD(day, 180, convert(datetime, '", datetime, "')) GROUP BY user_id) actual_ltv ON users.user_id = actual_ltv.user_id LEFT JOIN (SELECT * FROM (SELECT DISTINCT user_id, FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels FROM restaurantcity.mlp_ltv_output WHERE datetime >= convert(datetime, '", datetime, "') AND datetime <= DATEADD(day, 7, convert(datetime, '", datetime, "'))) mlp LEFT JOIN (SELECT predicted_category, ltv_median FROM restaurantcity.aux_category_ltv_median WHERE date_start = convert(datetime, '2016-07-08')) aux ON mlp.scored_labels = aux.predicted_category) predicted_ltv ON users.user_id = predicted_ltv.user_id;", sep = "")
}

ltvs <- dbGetQuery(spdb, SQL_get_ltvs("2016-07-01"))

# check user coming through to table correctly (see SQL script, should be actual value 0.99, predicted value 4.79 - looks ok)
ltvs[which(ltvs$user_id == 3151213926601288688),]

##################################
### accuracy for different periods
##################################
# create list of registration dates to loop over to generate data for various period

### July 2016 week 1
regdatesJuly1w <- c("2016-07-01", "2016-07-02", "2016-07-03", "2016-07-04", "2016-07-05", "2016-07-06", "2016-07-07")

ltvsJuly1w = NULL
for(regdate in regdatesJuly1w) {
  ltvsJuly1w <- rbind(ltvsJuly1w, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}

# Comparing actual vs expected ltvs for this week
sum(ltvsJuly1w$ltv_median)/sum(ltvsJuly1w$ltv_actual, na.rm = TRUE)
# result 29%

### August 2016 week 1
regdatesAug1w <- c("2016-08-01", "2016-08-02", "2016-08-03", "2016-08-04", "2016-08-05", "2016-08-06", "2016-08-07")
ltvsAug1w = NULL
for(regdate in regdatesAug1w) {
  ltvsAug1w <- rbind(ltvsAug1w, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}
sum(ltvsAug1w$ltv_median, na.rm = TRUE)/sum(ltvsAug1w$ltv_actual, na.rm = TRUE)
# result 211%

### September 2016 week 1
regdatesSep1w <- c("2016-09-01", "2016-09-02", "2016-09-03", "2016-09-04", "2016-09-05", "2016-09-06", "2016-09-07")
ltvsSep1w = NULL
for(regdate in regdatesSep1w) {
  ltvsSep1w <- rbind(ltvsSep1w, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}
sum(ltvsSep1w$ltv_median)/sum(ltvsSep1w$ltv_actual, na.rm = TRUE)
# result 264%

### October 2016 week 1
regdatesOct1w <- c("2016-10-01", "2016-10-02", "2016-10-03", "2016-10-04", "2016-10-05", "2016-10-06", "2016-10-07")
ltvsOct1w = NULL
for(regdate in regdatesOct1w) {
  ltvsOct1w <- rbind(ltvsOct1w, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}
sum(ltvsOct1w$ltv_median)/sum(ltvsOct1w$ltv_actual, na.rm = TRUE)
# result 232%

### Total over all periods
sum(ltvsJuly1w$ltv_median, ltvsAug1w$ltv_median, ltvsSep1w$ltv_median, ltvsOct1w$ltv_median, na.rm = TRUE)/sum(ltvsJuly1w$ltv_actual, ltvsAug1w$ltv_actual, ltvsSep1w$ltv_actual, ltvsOct1w$ltv_actual, na.rm = TRUE)
# result 176%

### Random selection of dates
regdatesRandom <- c("2016-07-08", "2016-07-13", "2016-07-19", "2016-07-29", "2016-08-03", "2016-08-09", "2016-08-14", "2016-08-20", "2016-08-25", "2016-08-30", "2016-09-5", "2016-09-11", "2016-09-17", "2016-09-28", "2016-10-05")
ltvsRandDates = NULL
for(regdate in regdatesRandom) {
  ltvsRandDates <- rbind(ltvsRandDates, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}
totalPredicted <- sum(ltvsRandDates$ltv_median)
totalRevenues <- sum(ltvsRandDates$ltv_actual, na.rm = TRUE)
totalPredicted/totalRevenues
# result 157% (i.e. roughly 64% accuracy)

### testing per category
# cat 0
cat0Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==0),]$ltv_median) # 0 obviously
cat0Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==0),]$ltv_actual, na.rm = TRUE) # 67718
cat0Predicted/cat0Actual

# cat 1
cat1Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==1),]$ltv_median) # 11510
cat1Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==1),]$ltv_actual, na.rm = TRUE) # 16736
cat1Predicted/cat1Actual
# 69%

# cat 2
cat2Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==2),]$ltv_median) # 54632
cat2Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==2),]$ltv_actual, na.rm = TRUE) # 44393
cat2Predicted/cat2Actual
# 123%

# cat 3
cat3Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==3),]$ltv_median) # 89644
cat3Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==3),]$ltv_actual, na.rm = TRUE) # 43776
cat3Predicted/cat3Actual
# 205%

# cat 4
cat4Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==4),]$ltv_median) # 141518
cat4Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==4),]$ltv_actual, na.rm = TRUE) # 55259
cat4Predicted/cat4Actual
# 256%

# cat 5
cat5Predicted <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==5),]$ltv_median) # 98678
cat5Actual <- sum(ltvsRandDates[which(ltvsRandDates$predicted_category==5),]$ltv_actual, na.rm = TRUE) # 23904
cat5Predicted/cat5Actual
# 413%

# Predictions are too high for higher categories (or users are in those categories who shouldn't be), offset slightly by category 0

### Old stuff and testing
subset_data <- ltvs_7day_pred_1Jul[which(ltvs_7day_pred_1Jul$scored_labels==2),]
accuracy_per_scored_label <- function(ltv_data, scored_label) {
  subset_data <- ltv_data[which(ltv_data$scored_labels==scored_label),]
  total_predicted_ltv <- sum(subset_data$ltv_median)
  total_actual_ltv <- sum(subset_data$ltv_actual)
  total_predicted_ltv/total_actual_ltv
}


accuracy_per_scored_label(ltvs_7day_pred_1Jul, 0)
































































