setwd("/home/amckay")
print(paste(base::date(), " Starting Spark Payer Again DC Prediction"))
options(java.parameters = "-Xmx30g")
refreshDataTables_inredshift<-TRUE
source("./payer_again_dc/production/src/loadPackages.R")
source("./payer_again_dc/production/src/get_input_data.R")
source("./payer_again_dc/production/src/set_spark_session.R")
system("rm -rf ./payer_again_dc/production/output/*")




print(paste(base::date(), " Starting Data Preperation"))

# Convert the dataset into a spark dataframe
payer_again_prediction_data_dt <- createDataFrame(payer_again_prediction_data)


print(paste(base::date(), " Load ML models"))

gbt_model <- read.ml("s3://sp-dataproduct/payer_again_dc/production/models/gbt_model")


print(paste(base::date(), " Predict"))

predictions_gbt <- predict(gbt_model, payer_again_prediction_data_dt)


pred_gbt <- collect(predictions_gbt)

#all_predictions <- pred_a_df[, c('user_id', 'model', 'prediction')]
prediction_output <- pred_gbt[, c('user_id', 'prediction', 'total_spend')]
#date_to <- Sys.Date()
date_to <- '2017-04-01'

print(paste(base::date(), " Saving the data in local"))
write.csv(prediction_output, file = paste0("./payer_again_dc/production/output/prediction_output.csv"), row.names = FALSE, col.names=TRUE,sep=",")



# Save the data

print(paste(base::date(), " Sending to S3 all the data"))

library(httr)
library(rjson)
aws_credentials_get<-GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/EMR_EC2_DefaultRole")
aws_credentials<-fromJSON(content(aws_credentials_get))
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_credentials$AccessKeyId,
           "AWS_SECRET_ACCESS_KEY" = aws_credentials$SecretAccessKey,
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = aws_credentials$Token)


buck<-get_bucket("sp-dataproduct")
put_object("./payer_again_dc/production/output/predication_output.csv", paste0("payer_again_dc/production/output/predictions_",date_to,".csv"), bucket = buck)


