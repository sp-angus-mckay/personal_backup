environment_todeploy<-"production"

repoPath<-"~/Projects/sp-analytics-machine-learning-develop/"
recommenders_path<-paste0(repoPath,"payer_again/")
current_game<-"DragonCity"
current_recommender_path<-paste0(recommenders_path,current_game,"/")
current_recommender_scripts_path<-paste0(current_recommender_path,"spark_scripts/")
folder<-paste0(current_recommender_scripts_path,environment_todeploy,"/")
current_recommender_data_folder<-paste0(current_recommender_path,"Data/")

bucket_name<-"sp-dataproduct"
bucket_folder<-paste0("payer_again_dc/",environment_todeploy,"/")
bucket_src_folder<-paste0(bucket_folder,"src/")
bucket_input_data_folder<-paste0(bucket_folder,"input_data/")
library(httr)
library(rjson)
library(aws.s3)
aws_credentials_get<-system("/Applications/gowsumed.app/Contents/MacOS/gowsume use analytics:sp-admin-rw", intern=TRUE)
aws_credentials<-paste0("{",paste0(mapply(function(cred){
  x<-unlist(strsplit(cred,"="))
  if(length(x)==1) x[2] == "" 
  paste0("\"",x[1],"\" : \"", x[2],"\"")
}, aws_credentials_get),collapse=","),"}")
cat(aws_credentials, file = "/tmp/credentials.json")
aws_credentials<-fromJSON(file="/tmp/credentials.json")
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_credentials$AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = aws_credentials$AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = aws_credentials$AWS_SESSION_TOKEN)

buck<-get_bucket(bucket_name)
for(file in list.files(folder, pattern="*.R")) print(paste(paste0(folder,file), paste0(bucket_src_folder,file), put_object(file = paste0(folder,file), paste0(bucket_src_folder,file), bucket = buck)))
for(file in list.files(folder, pattern="*.sh")) print(paste(paste0(folder,file), paste0(bucket_folder,file),put_object(paste0(folder,file), paste0(bucket_folder,file), bucket = buck)))

put_object(paste0(repoPath,"database_connection/get_redshift_data_functions.R"), paste0(bucket_src_folder,"get_redshift_data_functions.R"), bucket = buck)
put_object(paste0(current_recommender_scripts_path,"database_connection.R"), paste0(bucket_src_folder,"database_connection.R"), bucket = buck)
put_object(paste0(current_recommender_scripts_path,"set_spark_session.R"), paste0(bucket_src_folder,"set_spark_session.R"), bucket = buck)
put_object(paste0(current_recommender_scripts_path,"install-cran-packages.sh"), paste0(bucket_folder,"install-cran-packages.sh"), bucket = buck)
put_object(paste0(current_recommender_scripts_path,"install_packages.R"), paste0(bucket_folder,"install_packages.R"), bucket = buck)


