#### set up environment variables
spark_home<-"/Users/angus.mckay/spark/spark-2.1.1-bin-hadoop2.7"
Sys.setenv(SPARK_HOME = spark_home)
spark_bin <- paste0(spark_home, '/bin')
Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), spark_bin, sep=':')) 
## HADOOP_HOME
hadoop_home <- "/usr/lib/hadoop/"
Sys.setenv(HADOOP_HOME = hadoop_home) # hadoop-common missing on Windows

postgresql_drv <- "/Users/angus.mckay/Desktop/projects/payer_again/spark_scripts/local_test/postgresql-9.4.1212.jar"
if(!file.exists(postgresql_drv)){
  system('curl "https://jdbc.postgresql.org/download/postgresql-9.4.1212.jar" -o "~/Desktop/projects/payer_again/spark_scripts/local_test/"') 
}
## JAVA_HOME
java_home = "/Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
Sys.setenv(JAVA_HOME = java_home)


#### specify master host name or localhost
spark_link <- "local[*]"
#spark_link<-"yarn"
#spark_link <- 'spark://192.168.1.10:7077'
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR, lib.loc = "/Users/angus.mckay/spark/spark-2.1.1-bin-hadoop2.7/R/lib/")

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-redshift_2.10:3.0.0-preview1" "sparkr-shell"')
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-redshift_2.11:3.0.0-preview1" "sparkr-shell"')
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"sparkr-shell"')

sc <- sparkR.session(master = spark_link, appName = "SparkR_local",
                     sparkEnvir = list(spark.driver.extraClassPath = postgresql_drv),
                     sparkJars = postgresql_drv, 
                     sparkPackages = c("com.databricks:spark-csv_2.10:1.3.0"), 
                     sparkConfig = list(spark.driver.memory = "100g")) 

#sparkR.stop()