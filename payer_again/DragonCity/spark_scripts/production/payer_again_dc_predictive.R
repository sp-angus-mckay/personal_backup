setwd("/home/hadoop")
print(paste(base::date(), " Starting Spark Payer Again DC Prediction"))
options(java.parameters = "-Xmx30g")
refreshDataTables_inredshift<-TRUE
source("./payer_again_dc/src/loadPackages.R")
source("./payer_again_dc/src/get_input_data.R")
source("./payer_again_dc/src/set_spark_session.R")
system("rm -rf ./payer_again_dc/output/*")


print(paste(base::date(), " Starting Data Preperation"))

# Convert the dataset into a spark dataframe
#payer_again_prediction_data_dt <- createDataFrame(payer_again_prediction_data)
statement<-input_data_query
cat("\n",statement,"\n")
payer_again_prediction_data_dt<- read.df(NULL,"com.databricks.spark.redshift",
                            tempdir = "s3://sp-dataproduct/tmp/",
                            query = statement,
                            url = url, forward_spark_s3_credentials = "TRUE")
print(paste(base::date(), " Data Frame Created"))
payer_again_prediction_data_dt$spending_momentum <- ifelse(payer_again_prediction_data_dt$spend_per_week==0, 0, payer_again_prediction_data_dt$total_spend_last7days/payer_again_prediction_data_dt$spend_per_week)
payer_again_prediction_data_dt$transactions_momentum <- ifelse(payer_again_prediction_data_dt$transactions_per_week==0, 0, payer_again_prediction_data_dt$transaction_count_last7days/payer_again_prediction_data_dt$transactions_per_week)
payer_again_prediction_data_dt$sessions_momentum <- ifelse(payer_again_prediction_data_dt$sessions_per_week==0, 0, payer_again_prediction_data_dt$total_sessions_last2days*3.5/payer_again_prediction_data_dt$sessions_per_week)
payer_again_prediction_data_dt$videoads_momentum <- ifelse(payer_again_prediction_data_dt$videoads_per_week==0, 0, payer_again_prediction_data_dt$total_videoads_last7days/payer_again_prediction_data_dt$videoads_per_week)
payer_again_prediction_data_dt<-dropna(payer_again_prediction_data_dt)

print(paste(base::date(), " Load ML models"))

gbt_model <- read.ml("s3://sp-dataproduct/payer_again_dc/production/models/gbt_model")

print(paste(base::date(), " Predict"))

predictions_gbt <- predict(gbt_model, payer_again_prediction_data_dt)

print(paste(base::date(), " Collect"))
pred_gbt <- collect(predictions_gbt)

print(paste(base::date(), " Column Selection"))
#all_predictions <- pred_a_df[, c('user_id', 'model', 'prediction')]
prediction_output <- pred_gbt[, c('user_id', 'prediction', 'total_spend')]
today<-as.Date(Sys.time())
prediction_output$datetime<-paste0("'",today,"'")
  
print(paste(base::date(), " Saving the data in local"))
write.csv(prediction_output, file = "./payer_again_dc/prediction_output.csv", row.names = FALSE, quote = FALSE)

# Save the data
print(paste(base::date(), " Sending to S3 all the data"))
aws_credentials_get<-GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/EMR_EC2_DefaultRole")
aws_credentials<-fromJSON(content(aws_credentials_get))
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_credentials$AccessKeyId,
           "AWS_SECRET_ACCESS_KEY" = aws_credentials$SecretAccessKey,
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = aws_credentials$Token)


buck<-get_bucket("sp-dataproduct")
put_object("./payer_again_dc/prediction_output.csv", paste0("payer_again_dc/production/output/payer_again_dc_",gsub("-","",today),".csv"), bucket = buck)


