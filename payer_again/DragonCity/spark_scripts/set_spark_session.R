#### set up environment variables
spark_home<-"/usr/lib/spark/"
Sys.setenv(SPARK_HOME = spark_home)
spark_bin <- paste0(spark_home, '/bin')
Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), spark_bin, sep=':')) 
## HADOOP_HOME
hadoop_home <- "/usr/lib/hadoop/"
Sys.setenv(HADOOP_HOME = hadoop_home) # hadoop-common missing on Windows

#postgresql_drv <- "/home/xmaresma/postgresql-9.4.1212.jar"
postgresql_drv <- "/home/hadoop/payer_again_dc/packages/postgresql-9.4.1212.jar"
if(!file.exists(postgresql_drv)){
  system("wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jar /home/xmaresma/recommender_ml/packages") 
}
#### specify master host name or localhost
spark_link <- "local[*]"
#spark_link<-"yarn"
#spark_link <- 'spark://192.168.1.10:7077'

#library(SparkR)
library(SparkR, lib.loc = paste0(spark_home,"/R/lib/"))
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-redshift_2.10:3.0.0-preview1" "sparkr-shell"')
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-redshift_2.11:3.0.0-preview1" "sparkr-shell"')
Sys.setenv('SPARKR_SUBMIT_ARGS'='"sparkr-shell"')

sc <- sparkR.session(master = spark_link, appName = "SparkR_local",
                     sparkEnvir = list(spark.driver.extraClassPath = postgresql_drv),
                     sparkJars = postgresql_drv, 
                     sparkPackages = c("com.databricks:spark-redshift_2.11:3.0.0-preview1"), 
                     sparkConfig = list(spark.driver.memory = "100g")) 

#sparkR.stop()