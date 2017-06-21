setwd("/Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/local_test/src")
print(paste(base::date(), " Starting Spark Payer Again DC Training"))
options(java.parameters = "-Xmx30g")
refreshDataTables_inredshift<-TRUE
source("./loadPackages.R")
#source("./get_training_data.R")
source("./set_spark_session.R")
source("./test_accuracies.R")
#system("rm -rf /Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/local_test/current_model/rf_model")

print(paste(base::date(), " Starting Data Preparation"))

# read data and add some features
payer_again_data <- readRDS("../input_data/training_data2016-09-01.rds")
payer_again_data["spending_momentum"] = payer_again_data$total_spend_last7days/payer_again_data$spend_per_week
payer_again_data["transactions_momentum"] = payer_again_data$transaction_count_last7days/payer_again_data$transactions_per_week
payer_again_data["sessions_momentum"] = payer_again_data$total_sessions_last2days*3.5/payer_again_data$sessions_per_week
payer_again_data["videoads_momentum"] = ifelse(payer_again_data$videoads_per_week==0, 0, payer_again_data$total_videoads_last7days/payer_again_data$videoads_per_week)

# Convert the dataset into a spark dataframe
#payer_again_data_dt <- createDataFrame(payer_again_data[payer_again_data$days_since_register>14,])
payer_again_data_dt <- createDataFrame(payer_again_data[payer_again_data$days_since_register>0,])

# Create a training and a test set 

payer_again_data_train <- sample(payer_again_data_dt, withReplacement = FALSE, fraction = 0.7, seed = 123)
payer_again_data_test  <- except(payer_again_data_dt, payer_again_data_train)


## Train the different models: Random Forest and Gradient Boosted Trees

#print(paste(base::date(), " Creating Random Forest Models"))

#rf_model <- spark.randomForest(payer_again_data_train,  future_payer ~  sex + days_since_transaction + android + ios + amount_gross + level + days_since_register + first_buy + total_spend +  transaction_count + minutes_since_prev + game_start_gold + game_start_xp + game_start_food + game_start_cash + game_start_level + game_start_num_expansions + game_start_num_dragons + game_start_num_dragons_legend + game_start_num_habitats + session_length + total_sessions + avg_session_length + sessions_per_week + total_sessions_last2days + total_videoads + videoads_per_week + total_videoads_last7days + au + ca + de + es + fr + gb + us + other_countries + spending_momentum + transactions_momentum + sessions_momentum + videoads_momentum, "regression")


print(paste(base::date(), " Creating GBT Models"))


gbt_model <- spark.gbt(payer_again_data_train,  future_payer ~  sex + days_since_transaction + android + ios + amount_gross + level + days_since_register + first_buy + total_spend +  transaction_count + minutes_since_prev + game_start_gold + game_start_xp + game_start_food + game_start_cash + game_start_level + game_start_num_expansions + game_start_num_dragons + game_start_num_dragons_legend + game_start_num_habitats + session_length + total_sessions + avg_session_length + sessions_per_week + total_sessions_last2days + total_videoads + videoads_per_week + total_videoads_last7days + au + ca + de + es + fr + gb + us + other_countries + spending_momentum + transactions_momentum + sessions_momentum + videoads_momentum, "regression")




#rf_model <- read.ml(object = rf_android, path = paste0("s3://sp-dataproduct/payer_again_dc/preproduction/current_model/rf_model"))



#print(paste(base::date(), " Predictions with Random Forest"))

#pred_rf <- predict(rf_model, payer_again_data_test)


#print(paste(base::date(), " Predictions with GBT"))

pred_gbt <- predict(gbt_model, payer_again_data_test)


# Test the accuracies 
print(paste(base::date(), " Test the accuracies"))
payer_again_data_test_R <- collect(payer_again_data_test)


## rf
#print(head(pred_rf))
#prob_pay_again <- collect(pred_rf)$prediction
#pred_table <- cbind("future_payer" = payer_again_data_test_R$future_payer, prob_pay_again)
#acc_rf <- acc_function(predicted = prob_pay_again>0.5, real = payer_again_data_test_R$future_payer)


## gbt
prob_pay_again_gbt <- collect(pred_gbt)$prediction
pred_table_gbt <- cbind("future_payer" = payer_again_data_test_R$future_payer, prob_pay_again_gbt)
acc_gbt <- acc_function(predicted = prob_pay_again_gbt>0.5, real = payer_again_data_test_R$future_payer)

#print(paste(base::date(), " Accuracy RF"))
#print(acc_rf)
print(paste(base::date(), " Accuracy GBT"))
print(acc_gbt)

#fwrite(acc_rf, file = "./payer_again_dc/output/accuracies_rf.csv", row.names = FALSE, col.names=TRUE, sep=",")
#fwrite(acc_gbt, file = "./payer_again_dc/output/accuracies_gbt.csv", row.names = FALSE, col.names=TRUE, sep=",")



### VALIDATION
#sum(pred_table[,1]==0 & pred_table[,2]<0.11)/sum(pred_table[,2]<0.11)
#sum(pred_table[,1]==0 & pred_table[,2]<0.2 & pred_table[,2]>0.1)/sum(pred_table[,2]<0.2 & pred_table[,2]>0.1)
#sum(pred_table[,1]==0 & pred_table[,2]<0.3 & pred_table[,2]>0.2)/sum(pred_table[,2]<0.3 & pred_table[,2]>0.2)
#sum(pred_table[,1]==0 & pred_table[,2]<0.4 & pred_table[,2]>0.3)/sum(pred_table[,2]<0.4 & pred_table[,2]>0.3)
#sum(pred_table[,1]==0 & pred_table[,2]<0.5 & pred_table[,2]>0.4)/sum(pred_table[,2]<0.5 & pred_table[,2]>0.4)

# number of users in each category
#sum(pred_table[,2]<0.1)
#sum(pred_table[,2]<0.2 & pred_table[,2]>0.1)
#sum(pred_table[,2]<0.3 & pred_table[,2]>0.2)
#sum(pred_table[,2]<0.4 & pred_table[,2]>0.3)
#sum(pred_table[,2]<0.5 & pred_table[,2]>0.4)
#sum(pred_table[,2]>0.5)

### Filtering by payer category
#Fish -> (0, 3)
#pred_table_fish <- pred_table[payer_again_data_test_R$total_spend<3,]
#sum(pred_table_fish[,1]==0 & pred_table_fish[,2]<0.15)/sum(pred_table_fish[,2]<0.15)
#sum(pred_table_fish[,1]==0 & pred_table_fish[,2]<0.25)/sum(pred_table_fish[,2]<0.25)
#sum(pred_table_fish[,1]==0 & pred_table_fish[,2]<0.35)/sum(pred_table_fish[,2]<0.35)
#sum(pred_table_fish[,1]==0 & pred_table_fish[,2]<0.45)/sum(pred_table_fish[,2]<0.45)
#sum(pred_table_fish[,1]==0 & pred_table_fish[,2]<0.5)/sum(pred_table_fish[,2]<0.5)

#Seals ->[3, 10)
#pred_table_seals <- pred_table[payer_again_data_test_R$total_spend>=3 & payer_again_data_test_R$total_spend<10,]
#sum(pred_table_seals[,1]==0 & pred_table_seals[,2]<0.15)/sum(pred_table_seals[,2]<0.15)
#sum(pred_table_seals[,1]==0 & pred_table_seals[,2]<0.25)/sum(pred_table_seals[,2]<0.25)
#sum(pred_table_seals[,1]==0 & pred_table_seals[,2]<0.35)/sum(pred_table_seals[,2]<0.35)
#sum(pred_table_seals[,1]==0 & pred_table_seals[,2]<0.45)/sum(pred_table_seals[,2]<0.45)
#sum(pred_table_seals[,1]==0 & pred_table_seals[,2]<0.5)/sum(pred_table_seals[,2]<0.5)

#Dolphins -> [10,200)
#pred_table_dolphins <- pred_table[payer_again_data_test_R$total_spend>=10 & payer_again_data_test_R$total_spend<200,]
#sum(pred_table_dolphins[,1]==0 & pred_table_dolphins[,2]<0.15)/sum(pred_table_dolphins[,2]<0.15)
#sum(pred_table_dolphins[,1]==0 & pred_table_dolphins[,2]<0.25)/sum(pred_table_dolphins[,2]<0.25)
#sum(pred_table_dolphins[,1]==0 & pred_table_dolphins[,2]<0.35)/sum(pred_table_dolphins[,2]<0.35)
#sum(pred_table_dolphins[,1]==0 & pred_table_dolphins[,2]<0.45)/sum(pred_table_dolphins[,2]<0.45)
#sum(pred_table_dolphins[,1]==0 & pred_table_dolphins[,2]<0.5)/sum(pred_table_dolphins[,2]<0.5)

#Whales ->[200,2000)
#pred_table_whales <- pred_table[payer_again_data_test_R$total_spend>=200 & payer_again_data_test_R$total_spend<500,]
#sum(pred_table_whales[,1]==0 & pred_table_whales[,2]<0.15)/sum(pred_table_whales[,2]<0.15)
#sum(pred_table_whales[,1]==0 & pred_table_whales[,2]<0.25)/sum(pred_table_whales[,2]<0.25)
#sum(pred_table_whales[,1]==0 & pred_table_whales[,2]<0.35)/sum(pred_table_whales[,2]<0.35)
#sum(pred_table_whales[,1]==0 & pred_table_whales[,2]<0.45)/sum(pred_table_whales[,2]<0.45)
#sum(pred_table_whales[,1]==0 & pred_table_whales[,2]<0.5)/sum(pred_table_whales[,2]<0.5)

#Killer Whales -> >= 2000
#pred_table_k_whales <- pred_table[payer_again_data_test_R$total_spend>=500,]
#sum(pred_table_k_whales[,1]==0 & pred_table_k_whales[,2]<0.15)/sum(pred_table_k_whales[,2]<0.15)
#sum(pred_table_k_whales[,1]==0 & pred_table_k_whales[,2]<0.25)/sum(pred_table_k_whales[,2]<0.25)
#sum(pred_table_k_whales[,1]==0 & pred_table_k_whales[,2]<0.35)/sum(pred_table_k_whales[,2]<0.35)
#sum(pred_table_k_whales[,1]==0 & pred_table_k_whales[,2]<0.45)/sum(pred_table_k_whales[,2]<0.45)
#sum(pred_table_k_whales[,1]==0 & pred_table_k_whales[,2]<0.50)/sum(pred_table_k_whales[,2]<0.50)







### testing precision of different cut offs for gbt
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<0.11)/sum(pred_table_gbt[,2]<0.11)
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<0.2 & pred_table_gbt[,2]>0.1)/sum(pred_table_gbt[,2]<0.2 & pred_table_gbt[,2]>0.1)
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<0.3 & pred_table_gbt[,2]>0.2)/sum(pred_table_gbt[,2]<0.3 & pred_table_gbt[,2]>0.2)
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<0.4 & pred_table_gbt[,2]>0.3)/sum(pred_table_gbt[,2]<0.4 & pred_table_gbt[,2]>0.3)
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<0.5 & pred_table_gbt[,2]>0.4)/sum(pred_table_gbt[,2]<0.5 & pred_table_gbt[,2]>0.4)

# number of users in each category
sum(pred_table_gbt[,2]<0.1)
sum(pred_table_gbt[,2]<0.2 & pred_table_gbt[,2]>0.1)
sum(pred_table_gbt[,2]<0.3 & pred_table_gbt[,2]>0.2)
sum(pred_table_gbt[,2]<0.4 & pred_table_gbt[,2]>0.3)
sum(pred_table_gbt[,2]<0.5 & pred_table_gbt[,2]>0.4)
sum(pred_table_gbt[,2]>0.5)

# numbers of users
NROW(pred_table_gbt) # #users
sum(pred_table_gbt[,1]==1) # pay again
sum(pred_table_gbt[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt[,2]<cut_off)/NROW(pred_table_gbt) # % classified as non-payer-again
(sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off) + sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off))/NROW(pred_table_gbt)# accuracy
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,2]<cut_off) # precision
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,1]==0) # recall
sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off)/sum(pred_table_gbt[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt[,2]<cut_off)/NROW(pred_table_gbt) # % classified as non-payer-again
(sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off) + sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off))/NROW(pred_table_gbt)# accuracy
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,2]<cut_off) # precision
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,1]==0) # recall
sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off)/sum(pred_table_gbt[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt[,2]<cut_off)/NROW(pred_table_gbt) # % classified as non-payer-again
(sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off) + sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off))/NROW(pred_table_gbt)# accuracy
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,2]<cut_off) # precision
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,1]==0) # recall
sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off)/sum(pred_table_gbt[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt[,2]<cut_off)/NROW(pred_table_gbt) # % classified as non-payer-again
(sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off) + sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off))/NROW(pred_table_gbt)# accuracy
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,2]<cut_off) # precision
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,1]==0) # recall
sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off)/sum(pred_table_gbt[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt[,2]<cut_off)/NROW(pred_table_gbt) # % classified as non-payer-again
(sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off) + sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off))/NROW(pred_table_gbt)# accuracy
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,2]<cut_off) # precision
sum(pred_table_gbt[,1]==0 & pred_table_gbt[,2]<cut_off)/sum(pred_table_gbt[,1]==0) # recall
sum(pred_table_gbt[,1]==1 & pred_table_gbt[,2]>cut_off)/sum(pred_table_gbt[,1]==1) # specificity





### Filtering by payer category
#Fish -> (0, 3)
pred_table_gbt_fish <- pred_table_gbt[payer_again_data_test_R$total_spend<3,]

# numbers of users
NROW(pred_table_gbt_fish) # #users
sum(pred_table_gbt_fish[,1]==1) # pay again
sum(pred_table_gbt_fish[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_fish[,2]<cut_off)/NROW(pred_table_gbt_fish) # % classified as non-payer-again
(sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off) + sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off))/NROW(pred_table_gbt_fish)# accuracy
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,2]<cut_off) # precision
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,1]==0) # recall
sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off)/sum(pred_table_gbt_fish[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_fish[,2]<cut_off)/NROW(pred_table_gbt_fish) # % classified as non-payer-again
(sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off) + sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off))/NROW(pred_table_gbt_fish)# accuracy
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,2]<cut_off) # precision
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,1]==0) # recall
sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off)/sum(pred_table_gbt_fish[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_fish[,2]<cut_off)/NROW(pred_table_gbt_fish) # % classified as non-payer-again
(sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off) + sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off))/NROW(pred_table_gbt_fish)# accuracy
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,2]<cut_off) # precision
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,1]==0) # recall
sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off)/sum(pred_table_gbt_fish[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_fish[,2]<cut_off)/NROW(pred_table_gbt_fish) # % classified as non-payer-again
(sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off) + sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off))/NROW(pred_table_gbt_fish)# accuracy
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,2]<cut_off) # precision
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,1]==0) # recall
sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off)/sum(pred_table_gbt_fish[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_fish[,2]<cut_off)/NROW(pred_table_gbt_fish) # % classified as non-payer-again
(sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off) + sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off))/NROW(pred_table_gbt_fish)# accuracy
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,2]<cut_off) # precision
sum(pred_table_gbt_fish[,1]==0 & pred_table_gbt_fish[,2]<cut_off)/sum(pred_table_gbt_fish[,1]==0) # recall
sum(pred_table_gbt_fish[,1]==1 & pred_table_gbt_fish[,2]>cut_off)/sum(pred_table_gbt_fish[,1]==1) # specificity




#Seals ->[3, 10)
pred_table_gbt_seals <- pred_table_gbt[payer_again_data_test_R$total_spend>=3 & payer_again_data_test_R$total_spend<10,]
# numbers of users
NROW(pred_table_gbt_seals) # #users
sum(pred_table_gbt_seals[,1]==1) # pay again
sum(pred_table_gbt_seals[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_seals[,2]<cut_off)/NROW(pred_table_gbt_seals) # % classified as non-payer-again
(sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off) + sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off))/NROW(pred_table_gbt_seals)# accuracy
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,2]<cut_off) # precision
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,1]==0) # recall
sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off)/sum(pred_table_gbt_seals[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_seals[,2]<cut_off)/NROW(pred_table_gbt_seals) # % classified as non-payer-again
(sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off) + sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off))/NROW(pred_table_gbt_seals)# accuracy
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,2]<cut_off) # precision
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,1]==0) # recall
sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off)/sum(pred_table_gbt_seals[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_seals[,2]<cut_off)/NROW(pred_table_gbt_seals) # % classified as non-payer-again
(sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off) + sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off))/NROW(pred_table_gbt_seals)# accuracy
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,2]<cut_off) # precision
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,1]==0) # recall
sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off)/sum(pred_table_gbt_seals[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_seals[,2]<cut_off)/NROW(pred_table_gbt_seals) # % classified as non-payer-again
(sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off) + sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off))/NROW(pred_table_gbt_seals)# accuracy
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,2]<cut_off) # precision
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,1]==0) # recall
sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off)/sum(pred_table_gbt_seals[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_seals[,2]<cut_off)/NROW(pred_table_gbt_seals) # % classified as non-payer-again
(sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off) + sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off))/NROW(pred_table_gbt_seals)# accuracy
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,2]<cut_off) # precision
sum(pred_table_gbt_seals[,1]==0 & pred_table_gbt_seals[,2]<cut_off)/sum(pred_table_gbt_seals[,1]==0) # recall
sum(pred_table_gbt_seals[,1]==1 & pred_table_gbt_seals[,2]>cut_off)/sum(pred_table_gbt_seals[,1]==1) # specificity




#Dolphins -> [10,200)
pred_table_gbt_dolphins <- pred_table_gbt[payer_again_data_test_R$total_spend>=10 & payer_again_data_test_R$total_spend<200,]
# numbers of users
NROW(pred_table_gbt_dolphins) # #users
sum(pred_table_gbt_dolphins[,1]==1) # pay again
sum(pred_table_gbt_dolphins[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_dolphins[,2]<cut_off)/NROW(pred_table_gbt_dolphins) # % classified as non-payer-again
(sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off) + sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off))/NROW(pred_table_gbt_dolphins)# accuracy
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,2]<cut_off) # precision
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,1]==0) # recall
sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off)/sum(pred_table_gbt_dolphins[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_dolphins[,2]<cut_off)/NROW(pred_table_gbt_dolphins) # % classified as non-payer-again
(sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off) + sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off))/NROW(pred_table_gbt_dolphins)# accuracy
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,2]<cut_off) # precision
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,1]==0) # recall
sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off)/sum(pred_table_gbt_dolphins[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_dolphins[,2]<cut_off)/NROW(pred_table_gbt_dolphins) # % classified as non-payer-again
(sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off) + sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off))/NROW(pred_table_gbt_dolphins)# accuracy
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,2]<cut_off) # precision
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,1]==0) # recall
sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off)/sum(pred_table_gbt_dolphins[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_dolphins[,2]<cut_off)/NROW(pred_table_gbt_dolphins) # % classified as non-payer-again
(sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off) + sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off))/NROW(pred_table_gbt_dolphins)# accuracy
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,2]<cut_off) # precision
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,1]==0) # recall
sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off)/sum(pred_table_gbt_dolphins[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_dolphins[,2]<cut_off)/NROW(pred_table_gbt_dolphins) # % classified as non-payer-again
(sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off) + sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off))/NROW(pred_table_gbt_dolphins)# accuracy
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,2]<cut_off) # precision
sum(pred_table_gbt_dolphins[,1]==0 & pred_table_gbt_dolphins[,2]<cut_off)/sum(pred_table_gbt_dolphins[,1]==0) # recall
sum(pred_table_gbt_dolphins[,1]==1 & pred_table_gbt_dolphins[,2]>cut_off)/sum(pred_table_gbt_dolphins[,1]==1) # specificity




#Whales ->[200,2000)
pred_table_gbt_whales <- pred_table_gbt[payer_again_data_test_R$total_spend>=200 & payer_again_data_test_R$total_spend<2000,]
# numbers of users
NROW(pred_table_gbt_whales) # #users
sum(pred_table_gbt_whales[,1]==1) # pay again
sum(pred_table_gbt_whales[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_whales[,2]<cut_off)/NROW(pred_table_gbt_whales) # % classified as non-payer-again
(sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off) + sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off))/NROW(pred_table_gbt_whales)# accuracy
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,2]<cut_off) # precision
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,1]==0) # recall
sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off)/sum(pred_table_gbt_whales[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_whales[,2]<cut_off)/NROW(pred_table_gbt_whales) # % classified as non-payer-again
(sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off) + sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off))/NROW(pred_table_gbt_whales)# accuracy
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,2]<cut_off) # precision
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,1]==0) # recall
sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off)/sum(pred_table_gbt_whales[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_whales[,2]<cut_off)/NROW(pred_table_gbt_whales) # % classified as non-payer-again
(sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off) + sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off))/NROW(pred_table_gbt_whales)# accuracy
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,2]<cut_off) # precision
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,1]==0) # recall
sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off)/sum(pred_table_gbt_whales[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_whales[,2]<cut_off)/NROW(pred_table_gbt_whales) # % classified as non-payer-again
(sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off) + sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off))/NROW(pred_table_gbt_whales)# accuracy
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,2]<cut_off) # precision
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,1]==0) # recall
sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off)/sum(pred_table_gbt_whales[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_whales[,2]<cut_off)/NROW(pred_table_gbt_whales) # % classified as non-payer-again
(sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off) + sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off))/NROW(pred_table_gbt_whales)# accuracy
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,2]<cut_off) # precision
sum(pred_table_gbt_whales[,1]==0 & pred_table_gbt_whales[,2]<cut_off)/sum(pred_table_gbt_whales[,1]==0) # recall
sum(pred_table_gbt_whales[,1]==1 & pred_table_gbt_whales[,2]>cut_off)/sum(pred_table_gbt_whales[,1]==1) # specificity




#Killer Whales -> >= 2000
pred_table_gbt_k_whales <- pred_table_gbt[payer_again_data_test_R$total_spend>=2000,]

# numbers of users
NROW(pred_table_gbt_k_whales) # #users
sum(pred_table_gbt_k_whales[,1]==1) # pay again
sum(pred_table_gbt_k_whales[,1]==0) # not pay again

# statistics
cut_off <- 0.5 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_k_whales[,2]<cut_off)/NROW(pred_table_gbt_k_whales) # % classified as non-payer-again
(sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off) + sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off))/NROW(pred_table_gbt_k_whales)# accuracy
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,2]<cut_off) # precision
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,1]==0) # recall
sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off)/sum(pred_table_gbt_k_whales[,1]==1) # specificity

cut_off <- 0.4 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_k_whales[,2]<cut_off)/NROW(pred_table_gbt_k_whales) # % classified as non-payer-again
(sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off) + sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off))/NROW(pred_table_gbt_k_whales)# accuracy
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,2]<cut_off) # precision
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,1]==0) # recall
sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off)/sum(pred_table_gbt_k_whales[,1]==1) # specificity

cut_off <- 0.3 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_k_whales[,2]<cut_off)/NROW(pred_table_gbt_k_whales) # % classified as non-payer-again
(sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off) + sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off))/NROW(pred_table_gbt_k_whales)# accuracy
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,2]<cut_off) # precision
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,1]==0) # recall
sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off)/sum(pred_table_gbt_k_whales[,1]==1) # specificity

cut_off <- 0.2 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_k_whales[,2]<cut_off)/NROW(pred_table_gbt_k_whales) # % classified as non-payer-again
(sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off) + sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off))/NROW(pred_table_gbt_k_whales)# accuracy
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,2]<cut_off) # precision
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,1]==0) # recall
sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off)/sum(pred_table_gbt_k_whales[,1]==1) # specificity

cut_off <- 0.1 # nb: setting as 0.2 for example means prob NOT-pay-again is 0.8
sum(pred_table_gbt_k_whales[,2]<cut_off)/NROW(pred_table_gbt_k_whales) # % classified as non-payer-again
(sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off) + sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off))/NROW(pred_table_gbt_k_whales)# accuracy
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,2]<cut_off) # precision
sum(pred_table_gbt_k_whales[,1]==0 & pred_table_gbt_k_whales[,2]<cut_off)/sum(pred_table_gbt_k_whales[,1]==0) # recall
sum(pred_table_gbt_k_whales[,1]==1 & pred_table_gbt_k_whales[,2]>cut_off)/sum(pred_table_gbt_k_whales[,1]==1) # specificity



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

#write.ml(object = rf_model, path = paste0("/Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/current_model"), overwrite=TRUE)
write.ml(object = gbt_model, path = paste0("/Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/current_model"), overwrite=TRUE)

write.ml(object = gbt_model, path = paste0("s3://sp-dataproduct/payer_again_dc/preproduction/current_model/gbt_model"))


