if(!require(devtools)){
  install.packages("devtools", repos="http://cran.rstudio.com")
}
library(devtools)
if(!require(redshift)){
  devtools::install_github("pingles/redshift-r")
}
library(redshift)
if(!require(aws.s3)){
  if (!require("ghit")) {
    install.packages("ghit", repos="http://cran.rstudio.com")
  }
  require(devtools)
  require(ghit)
  devtools::install_github("hadley/xml2", lib = "/usr/lib64/R/library")
  devtools::install_github("cloudyr/aws.s3", lib = "/usr/lib64/R/library")
}
library(httr)
library(rjson)
library(aws.s3)
library(lubridate)
library(Matrix)
library(data.table)
library(bit64)
library(parallel)
