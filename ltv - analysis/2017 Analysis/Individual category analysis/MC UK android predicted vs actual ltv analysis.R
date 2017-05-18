### World Chef actual vs predicted ltv
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

country = 'GB' # (US,GB,CA,AU,DE,FR)
register_platform = 'android' # (android,ios)
forecast_day = 7 # (7, 30, 90)
game_schema = 'monstercity' # (restaurantcity, monstercity, dragoncity)
pred_date_start = '2016-05-03' # (2016-07-08 ,2016-05-03, )

### Obtaining data - see bottom of script for neat version of query!
SQL_get_ltvs <- function(datetime) {
  paste("SELECT users.user_id, date_register, register_platform, register_ip_country, ltv_actual, predicted_category, ltv_median FROM (SELECT user_id, date_register, register_platform, register_ip_country FROM ", game_schema, ".t_user WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "')) AND register_platform = '", register_platform, "' AND register_ip_country = '", country, "') users LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual FROM ", game_schema, ".t_transaction WHERE date_register > convert(datetime, '", datetime, "') AND date_register < DATEADD(day, 1, convert(datetime, '", datetime, "')) AND datetime <= DATEADD(day, 180, convert(datetime, '", datetime, "')) GROUP BY user_id) actual_ltv ON users.user_id = actual_ltv.user_id LEFT JOIN (SELECT * FROM (SELECT DISTINCT user_id, FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels FROM ", game_schema, ".mlp_ltv_output WHERE datetime >= convert(datetime, '", datetime, "') AND datetime <= DATEADD(day, ", forecast_day, ", convert(datetime, '", datetime, "'))) mlp LEFT JOIN (SELECT predicted_category, ltv_median FROM ", game_schema, ".aux_category_ltv_median WHERE date_start = convert(datetime, '", pred_date_start, "')) aux ON mlp.scored_labels = aux.predicted_category) predicted_ltv ON users.user_id = predicted_ltv.user_id;", sep = "")
}

#############################################
### setup functions and some dates to analyse
#############################################
# create list of random registration dates and looping over to generate data for various period
regdatesRandom <- c("2016-07-01", "2016-07-03", "2016-07-05", "2016-07-07", "2016-07-09", "2016-07-11", "2016-07-13", "2016-07-15", "2016-07-17", "2016-07-19", "2016-07-21", "2016-07-23", "2016-07-25", "2016-07-27", "2016-07-29", "2016-07-31", "2016-08-01", "2016-08-03", "2016-08-05", "2016-08-07", "2016-08-09", "2016-08-11", "2016-08-13", "2016-08-15", "2016-08-17", "2016-08-19", "2016-08-21", "2016-08-23", "2016-08-25", "2016-08-27", "2016-08-29", "2016-08-31", "2016-09-01", "2016-09-03", "2016-09-05", "2016-09-07", "2016-09-09", "2016-09-11", "2016-09-13", "2016-09-15", "2016-09-17", "2016-09-19", "2016-09-21", "2016-09-23", "2016-09-25", "2016-09-27", "2016-09-29", "2016-09-30")

actual_category_counter <- function(ltvData, k) {
  c(sum(ltvData$actual_category[ltvData$predicted_category==k]==0),
    sum(ltvData$actual_category[ltvData$predicted_category==k]==1),
    sum(ltvData$actual_category[ltvData$predicted_category==k]==2),
    sum(ltvData$actual_category[ltvData$predicted_category==k]==3),
    sum(ltvData$actual_category[ltvData$predicted_category==k]==4),
    sum(ltvData$actual_category[ltvData$predicted_category==k]==5))
}

predictionAccuracyPerCategory <- function(ltvdata, category) {
  sum(ltvdata[which(ltvdata$predicted_category==category),]$ltv_median)/sum(ltvdata[which(ltvdata$predicted_category==category),]$ltv_actual)*100
}

predicted_income <- function(ltvData, k) {
  sum(ltvData[which(ltvData$predicted_category==k),]$ltv_median)
}

actual_income <- function(ltvData, k) {
  sum(ltvData[which(ltvData$predicted_category==k),]$ltv_actual)
}

num_format <- function(n) {format(round(n), big.mark=",", scientific=FALSE)}

##################################
### accuracy for different periods
##################################
### 7-days
forecast_day = 7

ltvsRandDates = NULL
for(regdate in regdatesRandom) {
  ltvsRandDates <- rbind(ltvsRandDates, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}

ltvsRandDates <- ltvsRandDates[!is.na(ltvsRandDates$ltv_median),] # nb loses 3000 rows

ltvsRandDates$ltv_actual[is.na(ltvsRandDates$ltv_actual)] <- 0
ltvsRandDates$actual_category <- cut(ltvsRandDates$ltv_actual,
                                     c(-1,0,10,50,200,500,99999), labels = 0:5)

predictions_per_category <- sapply(0:5, function(k) actual_category_counter(ltvsRandDates, k))
colnames(predictions_per_category) = 0:5
rownames(predictions_per_category) = c('Actual Category 0','Actual Category 1',
                                       'Actual Category 2','Actual Category 3',
                                       'Actual Category 4','Actual Category 5')

pc_pred_per_category <- t(t(predictions_per_category)/colSums(predictions_per_category))

library(RColorBrewer)

barplot(pc_pred_per_category,
        main = paste(game_schema,": ",register_platform," ",country,
                     ", day ",forecast_day," prediction", sep = ""),
        xlab = "predicted category", ylab = "actual category (%)",
        legend.text = 0:5,
        col=brewer.pal(6, "Paired"),
        args.legend=list(
          x=ncol(pc_pred_per_category) + 2,
          y=max(colSums(pc_pred_per_category)),
          bty = "n")
        )

totalPredicted <- sum(ltvsRandDates$ltv_median)
totalRevenues <- sum(ltvsRandDates$ltv_actual)
totalPredicted/totalRevenues
# result 39%

### table of actual values per category
categories <- 0:5

income_table <- rbind("predicted category" = 0:5,
      "users" = num_format(sapply(0:5, function(k) sum(ltvsRandDates$predicted_category==k))),
      "predicted income" = num_format(sapply(0:5, function(k) predicted_income(ltvsRandDates,k))),
      "actual income" = num_format(sapply(0:5, function(k) actual_income(ltvsRandDates,k))),
      "predicted / actual" = percent(sapply(0:5, function(k) predicted_income(ltvsRandDates,k))/
                                       sapply(0:5, function(k) actual_income(ltvsRandDates,k))))

grid.table(income_table)

###################
### 30 day forecast
###################
forecast_day = 30 # (7, 30, 90)

ltvsRandDates30day = NULL
for(regdate in regdatesRandom) {
  ltvsRandDates30day <- rbind(ltvsRandDates30day, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}

ltvsRandDates30day$ltv_actual[is.na(ltvsRandDates30day$ltv_actual)] <- 0
ltvsRandDates30day$actual_category <- cut(ltvsRandDates30day$ltv_actual,
                                     c(-1,0,10,50,200,500,99999), labels = 0:5)

predictions_per_category_30day <- sapply(0:5,function(k) actual_category_counter(ltvsRandDates30day, k))
colnames(predictions_per_category_30day) = 0:5
rownames(predictions_per_category_30day) = c('Actual Category 0','Actual Category 1',
                                       'Actual Category 2','Actual Category 3',
                                       'Actual Category 4','Actual Category 5')

pc_pred_per_category_30day <- t(t(predictions_per_category_30day)/
                                  colSums(predictions_per_category_30day))

barplot(pc_pred_per_category_30day,
        main = paste(game_schema,": ",register_platform," ",country,
                     ", day ",forecast_day," prediction", sep = ""),
        xlab = "predicted category", ylab = "actual category (%)",
        legend.text = 0:5,
        col=brewer.pal(6, "Paired"),
        args.legend=list(
          x=ncol(pc_pred_per_category_30day) + 2,
          y=max(colSums(pc_pred_per_category_30day)),
          bty = "n")
)

totalPredicted30day <- sum(ltvsRandDates30day$ltv_median)
totalRevenues30day <- sum(ltvsRandDates30day$ltv_actual)
totalPredicted30day/totalRevenues30day
# result 44%

### table of actual values per category
income_table30day <- rbind("predicted category" = 0:5,
      "users" = num_format(sapply(0:5, function(k) sum(ltvsRandDates30day$predicted_category==k))),
      "predicted income" = num_format(sapply(0:5, function(k) predicted_income(ltvsRandDates30day,k))),
      "actual income" = num_format(sapply(0:5, function(k) actual_income(ltvsRandDates30day,k))),
      "predicted / actual" = percent(sapply(0:5, function(k) predicted_income(ltvsRandDates30day,k))/sapply(0:5, function(k) actual_income(ltvsRandDates30day,k))))

grid.table(income_table30day)

###################
### 90 day forecast
###################
forecast_day = 90 # (7, 30, 90)

ltvsRandDates90day = NULL
for(regdate in regdatesRandom) {
  ltvsRandDates90day <- rbind(ltvsRandDates90day, dbGetQuery(spdb, SQL_get_ltvs(regdate)))
}

ltvsRandDates90day$ltv_actual[is.na(ltvsRandDates90day$ltv_actual)] <- 0
ltvsRandDates90day$actual_category <- cut(ltvsRandDates90day$ltv_actual,
                                          c(-1,0,10,50,200,500,99999), labels = 0:5)

predictions_per_category_90day <- sapply(0:5,function(k) actual_category_counter(ltvsRandDates90day, k))
colnames(predictions_per_category_90day) = 0:5
rownames(predictions_per_category_90day) = c('Actual Category 0','Actual Category 1',
                                             'Actual Category 2','Actual Category 3',
                                             'Actual Category 4','Actual Category 5')

pc_pred_per_category_90day <- t(t(predictions_per_category_90day)/
                                  colSums(predictions_per_category_90day))

barplot(pc_pred_per_category_90day,
        main = paste(game_schema,": ",register_platform," ",country,
                     ", day ",forecast_day," prediction", sep = ""),
        xlab = "predicted category", ylab = "actual category (%)",
        legend.text = 0:5,
        col=brewer.pal(6, "Paired"),
        args.legend=list(
          x=ncol(pc_pred_per_category_90day) + 2,
          y=max(colSums(pc_pred_per_category_90day)),
          bty = "n")
)

totalPredicted90day <- sum(ltvsRandDates90day$ltv_median)
totalRevenues90day <- sum(ltvsRandDates90day$ltv_actual)
totalPredicted90day/totalRevenues90day
# result 61%

### table of actual values per category
income_table90day <- rbind("predicted category" = 0:5,
      "users" = num_format(sapply(0:5, function(k) sum(ltvsRandDates90day$predicted_category==k))),
      "predicted income" = num_format(sapply(0:5, function(k) predicted_income(ltvsRandDates90day,k))),
      "actual income" = num_format(sapply(0:5, function(k) actual_income(ltvsRandDates90day,k))),
      "predicted / actual" = percent(sapply(0:5, function(k) predicted_income(ltvsRandDates90day,k))/sapply(0:5, function(k) actual_income(ltvsRandDates90day,k))))

grid.table(income_table90day)


### neat version of sql query
"SELECT users.user_id, date_register, register_platform, register_ip_country, ltv_actual, predicted_category, ltv_median
FROM      (SELECT user_id, date_register, register_platform, register_ip_country
           FROM t_user
           WHERE date_register > convert(datetime, '2016-07-01')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-07-01'))) users
LEFT JOIN (SELECT user_id, sum(amount_gross) as ltv_actual
           FROM t_transaction
           WHERE date_register > convert(datetime, '2016-07-01')
           AND date_register < DATEADD(day, 1, convert(datetime, '2016-07-01')) 
           AND datetime <= DATEADD(day, 180, convert(datetime, '2016-07-01'))
           GROUP BY user_id) actual_ltv
ON users.user_id = actual_ltv.user_id
LEFT JOIN (SELECT *
             FROM      (SELECT DISTINCT user_id,
                        FIRST_VALUE(scored_labels) OVER (PARTITION BY user_id ORDER BY datetime DESC ROWS UNBOUNDED PRECEDING) AS scored_labels
                        FROM mlp_ltv_output
                        WHERE datetime >= convert(datetime, '2016-07-01')
                        AND datetime <= DATEADD(day, 7, convert(datetime, '2016-07-01'))) mlp
           LEFT JOIN (SELECT predicted_category, ltv_median
                      FROM aux_category_ltv_median
                      WHERE date_start = convert(datetime, '2016-05-03')) aux
           ON mlp.scored_labels = aux.predicted_category) predicted_ltv
ON users.user_id = predicted_ltv.user_id
ORDER BY ltv_actual
LIMIT 100;"















































