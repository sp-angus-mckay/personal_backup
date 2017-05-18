### Monster Legends actual vs day 30 predicted ltv
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
  host = "redshift.pro.mc.laicosp.net", # (rc, mc, dc)
  port = "5439",
  dbname = "datawarehouse",
  user = "amckay",
  password = "o6EbWm4L79jRfegy"
)

game_schema = 'monstercity' # (restaurantcity, monstercity, dragoncity)
pred_date_start = '2016-05-03' # (2016-07-08 ,2016-05-03, )

### Obtaining data - see bottom of script for neat version of query!
SQL_get_ltvs <- function(datetime) {
  paste("SELECT users.user_id, date_register, register_platform, register_ip_country, ltv_actual, predicted_category, ltv_median FROM (SELECT user_id, date_register, register_platform, register_ip_country FROM ", game_schema, ".t_user WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "')) AND register_platform = '", register_platform, "' AND register_source_type = '", register_source_type, "') users LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual FROM ", game_schema, ".t_transaction WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "')) AND datetime <= DATEADD(day, 180, convert(datetime, '", datetime, "')) GROUP BY user_id) actual_ltv ON users.user_id = actual_ltv.user_id LEFT JOIN (SELECT * FROM (SELECT DISTINCT user_id, FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels FROM ", game_schema, ".mlp_ltv_output WHERE datetime >= convert(datetime, '", datetime, "') AND datetime <= DATEADD(day, ", forecast_day, ", convert(datetime, '", datetime, "'))) mlp LEFT JOIN (SELECT predicted_category, ltv_median FROM ", game_schema, ".aux_category_ltv_median WHERE date_start = convert(datetime, '", pred_date_start, "')) aux ON mlp.scored_labels = aux.predicted_category) predicted_ltv ON users.user_id = predicted_ltv.user_id;", sep = "")
}

#############################################
### setup functions and some dates to analyse
#############################################
# create list of registration dates to analyse over
dates <- list(NULL)
count = 1
for(month in 7:9) {
  for(day in 1:30) {
    dates[count] <- paste(2016,month,day, sep = "-")
    count <- count+1
  }
}

num_format <- function(n) {format(round(n), big.mark=",", scientific=FALSE)}

######################
### day 30 predictions
######################
forecast_day = 30 # (7, 30, 90)

### organic, android
register_source_type = 'organic' # (organic, marketing)
register_platform = 'android' # (android,ios)

# reading in data
ltvs_organic_android_30day = NULL
for(date in dates) {
  ltvs_organic_android_30day <- rbind(ltvs_organic_android_30day,
                                     dbGetQuery(spdb, SQL_get_ltvs(date)))
}
sum(is.na(ltvs_organic_android_30day$predicted_category)) # players who install but don't play for 30 days or longer
sum(!is.na(ltvs_organic_android_30day$ltv_actual)) # actual payers

# adding actual category for predicted payers and assessing error
ltvs_organic_android_30day_noCat0 <-
  ltvs_organic_android_30day[ltvs_organic_android_30day$predicted_category%in%1:5,]

ltvs_organic_android_30day_noCat0$ltv_actual[is.na(ltvs_organic_android_30day_noCat0$ltv_actual)] <- 0

ltvs_organic_android_30day_noCat0$actual_category <-
  as.integer(cut(ltvs_organic_android_30day_noCat0$ltv_actual, c(-1,0,10,50,200,500,99999), labels = 0:5))-1

ltvs_organic_android_30day_noCat0$category_diff <-
  abs(ltvs_organic_android_30day_noCat0$actual_category-ltvs_organic_android_30day_noCat0$predicted_category)

pc_correct_organic_android_30day <- sum(ltvs_organic_android_30day_noCat0$category_diff==0)/nrow(ltvs_organic_android_30day_noCat0)
pc_1away_organic_android_30day <- sum(ltvs_organic_android_30day_noCat0$category_diff==1)/nrow(ltvs_organic_android_30day_noCat0)
pc_error_organic_android_30day <- sum(ltvs_organic_android_30day_noCat0$category_diff>1)/nrow(ltvs_organic_android_30day_noCat0)


### organic, ios
register_source_type = 'organic' # (organic, marketing)
register_platform = 'ios' # (android,ios)

# reading in data
ltvs_organic_ios_30day = NULL
for(date in dates) {
  ltvs_organic_ios_30day <- rbind(ltvs_organic_ios_30day,
                                     dbGetQuery(spdb, SQL_get_ltvs(date)))
}
sum(is.na(ltvs_organic_ios_30day$predicted_category)) # players who install but don't play for 30 days or longer
sum(!is.na(ltvs_organic_ios_30day$ltv_actual)) # actual payers

# adding actual category for predicted payers and assessing error
ltvs_organic_ios_30day_noCat0 <-
  ltvs_organic_ios_30day[ltvs_organic_ios_30day$predicted_category%in%1:5,]

ltvs_organic_ios_30day_noCat0$ltv_actual[is.na(ltvs_organic_ios_30day_noCat0$ltv_actual)] <- 0

ltvs_organic_ios_30day_noCat0$actual_category <-
  as.integer(cut(ltvs_organic_ios_30day_noCat0$ltv_actual, c(-1,0,10,50,200,500,99999), labels = 0:5))-1

ltvs_organic_ios_30day_noCat0$category_diff <-
  abs(ltvs_organic_ios_30day_noCat0$actual_category-ltvs_organic_ios_30day_noCat0$predicted_category)

pc_correct_organic_ios_30day <- sum(ltvs_organic_ios_30day_noCat0$category_diff==0)/nrow(ltvs_organic_ios_30day_noCat0)
pc_1away_organic_ios_30day <- sum(ltvs_organic_ios_30day_noCat0$category_diff==1)/nrow(ltvs_organic_ios_30day_noCat0)
pc_error_organic_ios_30day <- sum(ltvs_organic_ios_30day_noCat0$category_diff>1)/nrow(ltvs_organic_ios_30day_noCat0)


### marketing, android
register_source_type = 'marketing' # (organic, marketing)
register_platform = 'android' # (android,ios)

# reading in data
ltvs_marketing_android_30day = NULL
for(date in dates) {
  ltvs_marketing_android_30day <- rbind(ltvs_marketing_android_30day,
                                     dbGetQuery(spdb, SQL_get_ltvs(date)))
}
sum(is.na(ltvs_marketing_android_30day$predicted_category)) # players who install but don't play for 30 days or longer
sum(!is.na(ltvs_marketing_android_30day$ltv_actual)) # actual payers

# adding actual category for predicted payers and assessing error
ltvs_marketing_android_30day_noCat0 <-
  ltvs_marketing_android_30day[ltvs_marketing_android_30day$predicted_category%in%1:5,]

ltvs_marketing_android_30day_noCat0$ltv_actual[is.na(ltvs_marketing_android_30day_noCat0$ltv_actual)] <- 0

ltvs_marketing_android_30day_noCat0$actual_category <-
  as.integer(cut(ltvs_marketing_android_30day_noCat0$ltv_actual, c(-1,0,10,50,200,500,99999), labels = 0:5))-1

ltvs_marketing_android_30day_noCat0$category_diff <-
  abs(ltvs_marketing_android_30day_noCat0$actual_category-ltvs_marketing_android_30day_noCat0$predicted_category)

pc_correct_marketing_android_30day <- sum(ltvs_marketing_android_30day_noCat0$category_diff==0)/nrow(ltvs_marketing_android_30day_noCat0)
pc_1away_marketing_android_30day <- sum(ltvs_marketing_android_30day_noCat0$category_diff==1)/nrow(ltvs_marketing_android_30day_noCat0)
pc_error_marketing_android_30day <- sum(ltvs_marketing_android_30day_noCat0$category_diff>1)/nrow(ltvs_marketing_android_30day_noCat0)


### marketing, ios
register_source_type = 'marketing' # (organic, marketing)
register_platform = 'ios' # (android,ios)

# reading in data
ltvs_marketing_ios_30day = NULL
for(date in dates) {
  ltvs_marketing_ios_30day <- rbind(ltvs_marketing_ios_30day,
                                       dbGetQuery(spdb, SQL_get_ltvs(date)))
}
sum(is.na(ltvs_marketing_ios_30day$predicted_category)) # players who install but don't play for 30 days or longer
sum(!is.na(ltvs_marketing_ios_30day$ltv_actual)) # actual payers

# adding actual category for predicted payers and assessing error
ltvs_marketing_ios_30day_noCat0 <-
  ltvs_marketing_ios_30day[ltvs_marketing_ios_30day$predicted_category%in%1:5,]

ltvs_marketing_ios_30day_noCat0$ltv_actual[is.na(ltvs_marketing_ios_30day_noCat0$ltv_actual)] <- 0

ltvs_marketing_ios_30day_noCat0$actual_category <-
  as.integer(cut(ltvs_marketing_ios_30day_noCat0$ltv_actual, c(-1,0,10,50,200,500,99999), labels = 0:5))-1

ltvs_marketing_ios_30day_noCat0$category_diff <-
  abs(ltvs_marketing_ios_30day_noCat0$actual_category-ltvs_marketing_ios_30day_noCat0$predicted_category)

pc_correct_marketing_ios_30day <- sum(ltvs_marketing_ios_30day_noCat0$category_diff==0)/nrow(ltvs_marketing_ios_30day_noCat0)
pc_1away_marketing_ios_30day <- sum(ltvs_marketing_ios_30day_noCat0$category_diff==1)/nrow(ltvs_marketing_ios_30day_noCat0)
pc_error_marketing_ios_30day <- sum(ltvs_marketing_ios_30day_noCat0$category_diff>1)/nrow(ltvs_marketing_ios_30day_noCat0)


### table of results
errors_30day <- matrix(c(pc_correct_organic_android_30day,pc_1away_organic_android_30day,pc_error_organic_android_30day,
                        pc_correct_organic_ios_30day,pc_1away_organic_ios_30day,pc_error_organic_ios_30day,
                        pc_correct_marketing_android_30day,pc_1away_marketing_android_30day,pc_error_marketing_android_30day,
                        pc_correct_marketing_ios_30day,pc_1away_marketing_ios_30day,pc_error_marketing_ios_30day),
                      4, byrow = TRUE)

errors_30day <- as.data.frame(errors_30day)
colnames(errors_30day) <- c('correct','1-away','more than 1 away')
rownames(errors_30day) <- c('organic-android', 'organic-ios', 'marketing-android', 'marketing-ios')






#############################
### neat version of sql query
#############################

"SELECT users.user_id, date_register, register_platform, register_ip_country, ltv_actual, predicted_category, ltv_median
FROM      (SELECT user_id, date_register, register_platform, register_ip_country
           FROM t_user
           WHERE date_register > convert(datetime, '2016-07-16')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-07-16'))
           AND register_platform = 'android'
           AND register_source_type = 'organic') users
LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual
           FROM t_transaction
           WHERE date_register > convert(datetime, '2016-07-16')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-07-16')) 
           AND datetime <= DATEADD(day, 180, convert(datetime, '2016-07-16'))
           GROUP BY user_id) actual_ltv
ON users.user_id = actual_ltv.user_id
LEFT JOIN (SELECT *
             FROM      (SELECT DISTINCT user_id,
                        FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels
                        FROM mlp_ltv_output
                        WHERE datetime >= convert(datetime, '2016-07-16')
                        AND datetime <= DATEADD(day, 30, convert(datetime, '2016-07-16'))) mlp
           LEFT JOIN (SELECT predicted_category, ltv_median
                      FROM aux_category_ltv_median
                      WHERE date_start = convert(datetime, '2016-05-03')) aux
           ON mlp.scored_labels = aux.predicted_category) predicted_ltv
ON users.user_id = predicted_ltv.user_id
ORDER BY ltv_actual
LIMIT 100;"













































