setwd("~/Desktop/sp-analytics-machine-learning-develop-master/payer_again/spark_scripts/local_test/further_testing")
print(paste(base::date(), " Starting Spark Payer Again DC Training"))
options(java.parameters = "-Xmx30g")
refreshDataTables_inredshift<-TRUE
source("../src/loadPackages.R")
#source("./get_training_data.R")
source("../src/set_spark_session.R")
source("../src/test_accuracies.R")
#system("rm -rf /Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/local_test/current_model/rf_model")

print(paste(base::date(), " Starting Data Preparation"))

# read data and add some features
payer_again_data <- readRDS("./data/training_data2016-10-01.rds")
payer_again_data["spending_momentum"] = payer_again_data$total_spend_last7days/payer_again_data$spend_per_week
payer_again_data["transactions_momentum"] = payer_again_data$transaction_count_last7days/payer_again_data$transactions_per_week
payer_again_data["sessions_momentum"] = payer_again_data$total_sessions_last2days*3.5/payer_again_data$sessions_per_week
payer_again_data["videoads_momentum"] = ifelse(payer_again_data$videoads_per_week==0, 0, payer_again_data$total_videoads_last7days/payer_again_data$videoads_per_week)

payer_again_test_date1 <- readRDS("./data/test_data2016-08-01.rds")
payer_again_test_date1["spending_momentum"] = payer_again_test_date1$total_spend_last7days/payer_again_test_date1$spend_per_week
payer_again_test_date1["transactions_momentum"] = payer_again_test_date1$transaction_count_last7days/payer_again_test_date1$transactions_per_week
payer_again_test_date1["sessions_momentum"] = payer_again_test_date1$total_sessions_last2days*3.5/payer_again_test_date1$sessions_per_week
payer_again_test_date1["videoads_momentum"] = ifelse(payer_again_test_date1$videoads_per_week==0, 0, payer_again_test_date1$total_videoads_last7days/payer_again_test_date1$videoads_per_week)

payer_again_test_date2 <- readRDS("./data/test_data2016-09-01.rds")
payer_again_test_date2["spending_momentum"] = payer_again_test_date2$total_spend_last7days/payer_again_test_date2$spend_per_week
payer_again_test_date2["transactions_momentum"] = payer_again_test_date2$transaction_count_last7days/payer_again_test_date2$transactions_per_week
payer_again_test_date2["sessions_momentum"] = payer_again_test_date2$total_sessions_last2days*3.5/payer_again_test_date2$sessions_per_week
payer_again_test_date2["videoads_momentum"] = ifelse(payer_again_test_date2$videoads_per_week==0, 0, payer_again_test_date2$total_videoads_last7days/payer_again_test_date2$videoads_per_week)

payer_again_test_date3 <- readRDS("./data/test_data2016-12-15.rds")
payer_again_test_date3["spending_momentum"] = payer_again_test_date3$total_spend_last7days/payer_again_test_date3$spend_per_week
payer_again_test_date3["transactions_momentum"] = payer_again_test_date3$transaction_count_last7days/payer_again_test_date3$transactions_per_week
payer_again_test_date3["sessions_momentum"] = payer_again_test_date3$total_sessions_last2days*3.5/payer_again_test_date3$sessions_per_week
payer_again_test_date3["videoads_momentum"] = ifelse(payer_again_test_date3$videoads_per_week==0, 0, payer_again_test_date3$total_videoads_last7days/payer_again_test_date3$videoads_per_week)




# Convert the datasets into spark dataframes
#payer_again_data_dt <- createDataFrame(payer_again_data[payer_again_data$days_since_register>14,])
payer_again_data_train <- createDataFrame(payer_again_data[payer_again_data$days_since_register>7,])
payer_again_data_test1 <- createDataFrame(payer_again_test_date1[payer_again_test_date1$days_since_register>7,])
payer_again_data_test2 <- createDataFrame(payer_again_test_date2[payer_again_test_date2$days_since_register>7,])
payer_again_data_test3 <- createDataFrame(payer_again_test_date3[payer_again_test_date3$days_since_register>7,])




## Train the different models: Random Forest and Gradient Boosted Trees

print(paste(base::date(), " Creating Random Forest Models"))

#rf_model <- spark.randomForest(payer_again_data_train,  future_payer ~  sex + days_since_transaction + android + ios + amount_gross + level + days_since_register + first_buy + total_spend +  transaction_count + minutes_since_prev + game_start_gold + game_start_xp + game_start_food + game_start_cash + game_start_level + game_start_num_expansions + game_start_num_dragons + game_start_num_dragons_legend + game_start_num_habitats + session_length + total_sessions + avg_session_length + sessions_per_week + total_sessions_last2days + total_videoads + videoads_per_week + total_videoads_last7days + au + ca + de + es + fr + gb + us + other_countries + spending_momentum + transactions_momentum + sessions_momentum + videoads_momentum, "regression")
rf_model <- read.ml("~/Desktop/sp-analytics-machine-learning-develop-master/payer_again/spark_scripts/local_test/further_testing/model/rf_model")
rf_summary <- summary(rf_model)
rf_feature_importance <- rf_summary$featureImportances



print(paste(base::date(), " Creating GBT Models"))


#gbt_model <- spark.gbt(payer_again_data_train,  future_payer ~  sex + days_since_transaction + android + ios + amount_gross + level + days_since_register + first_buy + total_spend +  transaction_count + minutes_since_prev + game_start_gold + game_start_xp + game_start_food + game_start_cash + game_start_level + game_start_num_expansions + game_start_num_dragons + game_start_num_dragons_legend + game_start_num_habitats + session_length + total_sessions + avg_session_length + sessions_per_week + total_sessions_last2days + total_videoads + videoads_per_week + total_videoads_last7days + au + ca + de + es + fr + gb + us + other_countries + spending_momentum + transactions_momentum + sessions_momentum + videoads_momentum, "regression")
#gbt_model <- read.ml("~/Desktop/sp-analytics-machine-learning-develop-master/payer_again/spark_scripts/local_test/further_testing/model/gbt_model")
gbt_summary <- summary(gbt_model)


#rf_model <- read.ml(object = rf_android, path = paste0("s3://sp-dataproduct/payer_again_dc/preproduction/current_model/rf_model"))



#print(paste(base::date(), " Predictions with Random Forest"))

#pred_rf <- predict(rf_model, payer_again_data_test)


#print(paste(base::date(), " Predictions with GBT"))

pred1_rf <- predict(rf_model, payer_again_data_test1)
pred2_rf <- predict(rf_model, payer_again_data_test2)
pred3_rf <- predict(rf_model, payer_again_data_test3)

# Collect the data into base R tables
print(paste(base::date(), " Test the accuracies"))
payer_again_predictions1 <- collect(pred1_rf)
payer_again_predictions2 <- collect(pred2_rf)
payer_again_predictions3 <- collect(pred3_rf)
payer_again_predictions_combined <- rbind(payer_again_predictions1, payer_again_predictions2, payer_again_predictions3)

## rf
#print(head(pred_rf))
#prob_pay_again <- collect(pred_rf)$prediction
#pred_table <- cbind("future_payer" = payer_again_data_test_R$future_payer, prob_pay_again)
#acc_rf <- acc_function(predicted = prob_pay_again>0.5, real = payer_again_data_test_R$future_payer)


## gbt
#acc1_gbt <- acc_function(predicted = payer_again_predictions1$prediction<0.5, real = 1-payer_again_predictions1$label)
#acc2_gbt <- acc_function(predicted = payer_again_predictions2$prediction<0.5, real = 1-payer_again_predictions2$label)
#acc3_gbt <- acc_function(predicted = payer_again_predictions3$prediction<0.5, real = 1-payer_again_predictions3$label)
#acc_combined_gbt <- acc_function(predicted = payer_again_predictions_combined$prediction<0.5, real = 1-payer_again_predictions_combined$label)

## rf
acc1_rf <- acc_function(predicted = payer_again_predictions1$prediction<0.5, real = 1-payer_again_predictions1$label)
acc2_rf <- acc_function(predicted = payer_again_predictions2$prediction<0.5, real = 1-payer_again_predictions2$label)
acc3_rf <- acc_function(predicted = payer_again_predictions3$prediction<0.5, real = 1-payer_again_predictions3$label)
acc_combined_rf <- acc_function(predicted = payer_again_predictions_combined$prediction<0.5, real = 1-payer_again_predictions_combined$label)



### VALIDATION

# testing for different 'days since transaction' cut-offs
payer_again_predictions_combined_1dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=1,]
payer_again_predictions_combined_2dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=2,]
payer_again_predictions_combined_3dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=3,]
payer_again_predictions_combined_4dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=4,]
payer_again_predictions_combined_5dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=5,]
payer_again_predictions_combined_6dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=6,]
payer_again_predictions_combined_7dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=7,]
payer_again_predictions_combined_8dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=8,]
payer_again_predictions_combined_9dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=9,]
payer_again_predictions_combined_10dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=10,]
payer_again_predictions_combined_11dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=11,]
payer_again_predictions_combined_12dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=12,]
payer_again_predictions_combined_13dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=13,]
payer_again_predictions_combined_14dst <- payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=14,]

cut_off <- 0.5
acc_combined_rf_1dst <- acc_function(predicted = payer_again_predictions_combined_1dst$prediction<cut_off, real = 1-payer_again_predictions_combined_1dst$label)
acc_combined_rf_2dst <- acc_function(predicted = payer_again_predictions_combined_2dst$prediction<cut_off, real = 1-payer_again_predictions_combined_2dst$label)
acc_combined_rf_3dst <- acc_function(predicted = payer_again_predictions_combined_3dst$prediction<cut_off, real = 1-payer_again_predictions_combined_3dst$label)
acc_combined_rf_4dst <- acc_function(predicted = payer_again_predictions_combined_4dst$prediction<cut_off, real = 1-payer_again_predictions_combined_4dst$label)
acc_combined_rf_5dst <- acc_function(predicted = payer_again_predictions_combined_5dst$prediction<cut_off, real = 1-payer_again_predictions_combined_5dst$label)
acc_combined_rf_6dst <- acc_function(predicted = payer_again_predictions_combined_6dst$prediction<cut_off, real = 1-payer_again_predictions_combined_6dst$label)
acc_combined_rf_7dst <- acc_function(predicted = payer_again_predictions_combined_7dst$prediction<cut_off, real = 1-payer_again_predictions_combined_7dst$label)
acc_combined_rf_8dst <- acc_function(predicted = payer_again_predictions_combined_8dst$prediction<cut_off, real = 1-payer_again_predictions_combined_8dst$label)
acc_combined_rf_9dst <- acc_function(predicted = payer_again_predictions_combined_9dst$prediction<cut_off, real = 1-payer_again_predictions_combined_9dst$label)
acc_combined_rf_10dst <- acc_function(predicted = payer_again_predictions_combined_10dst$prediction<cut_off, real = 1-payer_again_predictions_combined_10dst$label)
acc_combined_rf_11dst <- acc_function(predicted = payer_again_predictions_combined_11dst$prediction<cut_off, real = 1-payer_again_predictions_combined_11dst$label)
acc_combined_rf_12dst <- acc_function(predicted = payer_again_predictions_combined_12dst$prediction<cut_off, real = 1-payer_again_predictions_combined_12dst$label)
acc_combined_rf_13dst <- acc_function(predicted = payer_again_predictions_combined_13dst$prediction<cut_off, real = 1-payer_again_predictions_combined_13dst$label)
acc_combined_rf_14dst <- acc_function(predicted = payer_again_predictions_combined_14dst$prediction<cut_off, real = 1-payer_again_predictions_combined_14dst$label)

model.


days_since_transaction_stats <- rbind(acc_combined_rf, acc_combined_rf_1dst, acc_combined_rf_2dst, acc_combined_rf_3dst,
                                      acc_combined_rf_4dst, acc_combined_rf_5dst, acc_combined_rf_6dst, acc_combined_rf_7dst,
                                      acc_combined_rf_8dst, acc_combined_rf_9dst, acc_combined_rf_10dst, acc_combined_rf_11dst,
                                      acc_combined_rf_12dst, acc_combined_rf_13dst, acc_combined_rf_14dst)

days_since_transaction_stats['specificity'] <- days_since_transaction_stats$tn/(days_since_transaction_stats$tn + days_since_transaction_stats$fp)

# calculating number of each type of user 
no_users_days_since_transaction <- sapply(0:14, function(t) {
  c('users' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t,]),
    'fishes' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$total_spend<3,]),
    'seals' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$total_spend>=3 & payer_again_predictions_combined$total_spend<10,]),
    'dolphins' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$total_spend>=10 & payer_again_predictions_combined$total_spend<200,]),
    'whales' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$total_spend>=200 & payer_again_predictions_combined$total_spend<2000,]),
    'killer whales' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$total_spend>=2000,]),
    'future-payer' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$label==1,]),
    'non-payer' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$label==0,]),
    'classified-future-payer' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$prediction>cut_off,]),
    'classified-non-payer' = NROW(payer_again_predictions_combined[payer_again_predictions_combined$days_since_transaction>=t & payer_again_predictions_combined$prediction<cut_off,]))
})

days_since_transaction_stats <- cbind(days_since_transaction_stats, t(no_users_days_since_transaction))


# Save the data

#print(paste(base::date(), "Sending to S3 all the data"))
#aws_credentials_get<-GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/EMR_EC2_DefaultRole")
#aws_credentials<-fromJSON(content(aws_credentials_get))

#print(aws_credentials$AccessKeyId)
#print(aws_credentials$SecretAccessKey)

#Sys.setenv("AWS_ACCESS_KEY_ID" = aws_credentials$AccessKeyId,
#           "AWS_SECRET_ACCESS_KEY" = aws_credentials$SecretAccessKey,
#           "AWS_DEFAULT_REGION" = "us-east-1",
#           "AWS_SESSION_TOKEN" = aws_credentials$Token)

#buck<-get_bucket("sp-dataproduct")
#put_object("./payer_again_dc/output/accuracies_rf_a.csv", paste0("payer_again_dc/preproduction/output/accuracies_rf_a.csv"), bucket = buck)
#put_object("./payer_again_dc/output/accuracies_rf_i.csv", paste0("payer_again_dc/preproduction/output/accuracies_rf_i.csv"), bucket = buck)
#put_object("./payer_again_dc/output/accuracies_gbt_a.csv", paste0("payer_again_dc/preproduction/output/accuracies_gbt_a.csv"), bucket = buck)
#put_object("./payer_again_dc/output/accuracies_gbt_i.csv", paste0("payer_again_dc/preproduction/output/accuracies_gbt_i.csv"), bucket = buck)

print(paste(base::date(), "Sending to S3 all the data"))


#writer <- sparkR.callJMethod(rf_android@jobj, "write")
#writer <- sparkR.callJMethod(writer, "overwrite")
#sparkR.callJMethod(writer, "saveImpl", path = paste0("./payer_again_dc/current_model/rf_android25"))

#f <- list.files("./payer_again_dc/current_model")
#print(f)

#sparkR.callJMethod(writer, "saveImpl", path = paste0("s3://sp-dataproduct/payer_again_dc/preproduction/current_model/rf_android25"))

write.ml(object = rf_model, path = paste0("model/rf_model"), overwrite=TRUE)
write.ml(object = gbt_model, path = paste0("model/gbt_model"), overwrite=TRUE)

#write.ml(object = gbt_model, path = paste0("s3://sp-dataproduct/payer_again_dc/preproduction/current_model/gbt_model"))


