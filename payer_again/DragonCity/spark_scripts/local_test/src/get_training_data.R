source("./payer_again_dc/src/database_connection.R")
#source("./payer_again_dc/src/get_redshift_data_functions.R")


training_dates <- list('2016-10-01', '2016-10-09', '2016-10-17', '2016-10-25', '2016-11-02', '2016-11-10', '2016-11-18')
training_dates_minus_2 <- lapply(training_dates, function(d) date(as.POSIXct(d) - days(2)))
training_dates_minus_7 <- lapply(training_dates, function(d) date(as.POSIXct(d) - days(7)))

query1 <- function(date, date_minus_2, date_minus_7) {
          paste0("SELECT  CASE WHEN future_payers.user_id IS NULL THEN 0 ELSE 1 END AS future_payer,
                          current_transaction.user_id,
                          CASE WHEN t_user.fb_sex = 'f' THEN 0 ELSE 1 END AS sex,
                          DATEDIFF(d, current_transaction.datetime, '", date," 22:00:00') AS days_since_transaction,
                          CASE WHEN current_transaction.platform = 'android' THEN 1 ELSE 0 END AS android,
                          CASE WHEN current_transaction.platform = 'ios' THEN 1 ELSE 0 END AS ios,
                          current_transaction.amount_gross,
                          current_transaction.level, DATEDIFF(d, current_transaction.date_register, '", date," 22:00:00') AS days_since_register,
                          CASE WHEN current_transaction.first_buy = 'true' THEN 1 ELSE 0 END AS first_buy,
                          total_transaction.total_spend, total_transaction.transaction_count,
                          (total_transaction.total_spend)*60*24*7/DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00') AS spend_per_week,
                          CONVERT(decimal(32, 8), total_transaction.transaction_count)*60*24*7/DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00') AS transactions_per_week,
                          COALESCE(CASE WHEN current_transaction.date_register < '", date_minus_7," 22:00:00'
                                        THEN total_transaction_last7days.total_spend_last7days * 1
                                        ELSE total_transaction_last7days.total_spend_last7days * 7*24*60 / DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00')
                                        END, 0) AS total_spend_last7days,
                          COALESCE(CASE WHEN current_transaction.date_register < '", date_minus_7," 22:00:00'
                                        THEN CONVERT(decimal(32, 8), total_transaction_last7days.transaction_count_last7days) * 1
                                        ELSE CONVERT(decimal(32, 8), total_transaction_last7days.transaction_count_last7days) * 7*24*60 / DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00')
                                        END, 0) AS transaction_count_last7days,
                          COALESCE(DATEDIFF(m, previous_transaction.datetime, current_transaction.datetime), DATEDIFF(m, current_transaction.date_register, current_transaction.datetime)) AS minutes_since_prev,
                          session.game_start_gold, session.game_start_xp, session.game_start_food,
                          session.game_start_cash, session.game_start_level, session.game_start_num_expansions, session.game_start_num_dragons,
                          session.game_start_num_dragons_legend, session.game_start_num_habitats, session.session_length,
                          activity.total_sessions, activity.avg_session_length,
                          CONVERT(decimal(32, 8), activity.total_sessions)*60*24*7/DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00') AS sessions_per_week,
                          COALESCE(CASE WHEN current_transaction.date_register < '", date_minus_2," 22:00:00'
                                        THEN CONVERT(decimal(32, 8), recent_activity.total_sessions_last2days) * 1
                                        ELSE CONVERT(decimal(32, 8), recent_activity.total_sessions_last2days) * 2*24*60 / DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00')
                                        END, 0) AS total_sessions_last2days,
                          COALESCE(videoads.total_videoads, 0) AS total_videoads,
                          COALESCE(CONVERT(decimal(32, 8), videoads.total_videoads)*60*24*7/DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00'), 0) AS videoads_per_week,
                          COALESCE(CASE WHEN current_transaction.date_register < '", date_minus_7," 22:00:00'
                                        THEN CONVERT(decimal(32, 8), recent_videoads.total_videoads_last7days) * 1
                                        ELSE CONVERT(decimal(32, 8), recent_videoads.total_videoads_last7days) * 7*24*60 / DATEDIFF(m, current_transaction.date_register, '", date," 22:00:00')
                                        END, 0) AS total_videoads_last7days,
                          CASE WHEN current_transaction.ip_country = 'AU' THEN 1 ELSE 0 END AS AU,
                          CASE WHEN current_transaction.ip_country = 'CA' THEN 1 ELSE 0 END AS CA,
                          CASE WHEN current_transaction.ip_country = 'DE' THEN 1 ELSE 0 END AS DE,
                          CASE WHEN current_transaction.ip_country = 'ES' THEN 1 ELSE 0 END AS ES,
                          CASE WHEN current_transaction.ip_country = 'FR' THEN 1 ELSE 0 END AS FR,
                          CASE WHEN current_transaction.ip_country = 'GB' THEN 1 ELSE 0 END AS GB,
                          CASE WHEN current_transaction.ip_country = 'US' THEN 1 ELSE 0 END AS US,
                          CASE WHEN current_transaction.ip_country NOT IN ('AU', 'CA', 'DE', 'ES', 'FR', 'GB', 'US') THEN 1 ELSE 0 END AS other_countries
                 -- select most recent transaction before the set date
                 FROM        (SELECT *
                 FROM      (SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY datetime DESC) AS row_num
                 FROM dragoncity.t_transaction
                 WHERE datetime <= '", date," 22:00:00')
                 WHERE row_num = 1) current_transaction
                 -- select the transaction before the most recent one
                 LEFT JOIN   (SELECT user_id, datetime
                 FROM      (SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY datetime DESC) AS row_num
                 FROM dragoncity.t_transaction
                 WHERE datetime <= '", date," 22:00:00')
                 WHERE row_num = 2) previous_transaction
                 ON current_transaction.user_id = previous_transaction.user_id
                 -- calculate total spend and number of transactions
                 LEFT JOIN   (SELECT user_id, SUM(amount_gross) AS total_spend, COUNT(datetime) AS transaction_count
                 FROM dragoncity.t_transaction
                 WHERE datetime <= '", date," 22:00:00'
                 GROUP BY user_id) total_transaction
                 ON current_transaction.user_id = total_transaction.user_id
                 -- calcualte total spend and transactions in last week
                 LEFT JOIN   (SELECT user_id, SUM(amount_gross) AS total_spend_last7days, COUNT(datetime) AS transaction_count_last7days
                 FROM dragoncity.t_transaction
                 WHERE datetime <= '", date," 22:00:00'
                 AND datetime > '", date_minus_7," 22:00:00'
                 GROUP BY user_id) total_transaction_last7days
                 ON current_transaction.user_id = total_transaction_last7days.user_id
                 -- observe whether the user spends again after the set date
                 LEFT JOIN   (SELECT DISTINCT user_id
                 FROM dragoncity.t_transaction
                 WHERE datetime > '", date," 22:00:00') future_payers
                 ON current_transaction.user_id = future_payers.user_id
                 -- get game status from t_session (food, gold, dragons etc)
                 LEFT JOIN   (SELECT *
                 FROM dragoncity.t_session) session
                 ON current_transaction.user_id = session.user_id
                 AND current_transaction.session_id = session.session_id
                 -- get activity from t_sessions (number sessions etc)
                 LEFT JOIN   (SELECT user_id, COUNT(DISTINCT date_session_start) AS total_sessions, AVG(session_length) AS avg_session_length
                 FROM dragoncity.t_session
                 WHERE date_session_start < '", date," 22:00:00'
                 GROUP BY user_id) activity
                 ON current_transaction.user_id = activity.user_id
                 -- get number of sessions in last 2 days
                 LEFT JOIN   (SELECT user_id, COUNT(DISTINCT date_session_start) AS total_sessions_last2days
                 FROM dragoncity.t_session
                 WHERE date_session_start < '", date," 22:00:00'
                 AND date_session_start > '", date_minus_2," 22:00:00'
                 GROUP BY user_id) recent_activity
                 ON current_transaction.user_id = recent_activity.user_id
                 -- get video ad data
                 LEFT JOIN   (SELECT user_id, COUNT(datetime) AS total_videoads
                 FROM dragoncity.log_videoads_impress
                 WHERE datetime < '", date," 22:00:00'
                 GROUP BY user_id) videoads
                 ON current_transaction.user_id = videoads.user_id
                 -- get video ads in last week
                 LEFT JOIN   (SELECT user_id, COUNT(datetime) AS total_videoads_last7days
                 FROM dragoncity.log_videoads_impress
                 WHERE datetime < '", date," 22:00:00'
                 AND datetime > '", date_minus_7," 22:00:00'
                 GROUP BY user_id) recent_videoads
                 ON current_transaction.user_id = recent_videoads.user_id
                 -- condition on payments within 15 days before set date
                 LEFT JOIN   dragoncity.t_user
                 ON current_transaction.user_id = t_user.user_id
                 WHERE DATEDIFF(d, current_transaction.datetime, '", date," 22:00:00') < 15
                 AND current_transaction.platform IN ('android', 'ios')
                 AND t_user.user_category = 'player' 
                 AND (t_user.migrate_date_orphaned IS NULL OR datediff(s,t_user.date_register,t_user.migrate_date_orphaned) > 86400)
                 AND (t_user.is_tester IS NULL OR t_user.is_tester != 'true')
                 AND t_user.user_category <> 'bot';")
}



if (!require("RPostgreSQL")) install.packages("RPostgreSQL"); library(RPostgreSQL) 
drv <- dbDriver("PostgreSQL")
spdb <- dbConnect(drv,host = "redshift.pro.dc.laicosp.net",port = "5439",dbname = "datawarehouse",user = "amckay",password = "o6EbWm4L79jRfegy")

payer_again_data <- NULL
for(i in 1:length(dates)) {
  payer_again_data <- rbind(payer_again_data, dbGetQuery(spdb, query1(dates[[i]], dates_minus_2[[i]], dates_minus_7[[i]])))
  #payer_again_data_train <- rbind(payer_again_data_train, redshift.query(myconn, query1(dates[[i]], dates_minus_2[[i]], dates_minus_7[[i]])))
}

payer_again_data <- na.omit(payer_again_data) # example data reduced from 427620 to 427445 after omitting NAs

#dbDisconnect(myconn)

# Read a file 
#offer_dragons<-fread("./recommender_dc/input_data/dragons_offers_xm.csv")


#saveRDS(payer_again_data_train, file=paste0("./payer_again_dc/input_data/training_data",dates[[1]],".rds"))
saveRDS(payer_again_data, file=paste0("~/Desktop/projects/payer_again/spark_scripts/local_test/input_data/training_data",dates[[1]],".rds"))


# library(httr)
#library(rjson)
#aws_credentials_get<-GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/EMR_EC2_DefaultRole")
#aws_credentials<-fromJSON(content(aws_credentials_get))
#Sys.setenv("AWS_ACCESS_KEY_ID" = aws_credentials$AccessKeyId,
#            "AWS_SECRET_ACCESS_KEY" = aws_credentials$SecretAccessKey,
 #           "AWS_DEFAULT_REGION" = "us-east-1",
 #           "AWS_SESSION_TOKEN" = aws_credentials$Token)
 #library(aws.s3)
 #buck<-get_bucket("sp-dataproduct")
 #files<-list.files(path="./payer_again_dc/input_data", pattern="*.rds")
 #for(file in files){
 #  print(paste0("./payer_again_dc/input_data/",file))
 #  print(put_object(paste0("./payer_again_dc/input_data/",file), paste0("payer_again_dc/preproduction/input_data/",file), bucket = buck))
 #}

 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 