library(tools)
ins_pkg<-installed.packages()
packages_list<-list.files(path="payer_again_dc/packages/")
if(!"devtools" %in% ins_pkg[,"Package"]){
  pkg_dep<-package_dependencies("devtools", db = available.packages(contrib.url("https://cran.rstudio.com/", type = "both")), which = c("Depends", "Imports", "LinkingTo"), recursive = TRUE, reverse=FALSE, verbose = TRUE)
  rr<-mapply(function(pkg_dep_name){
    print(pkg_dep_name)
    pkg_dep<-package_dependencies(pkg_dep_name, db = available.packages(contrib.url("https://cran.rstudio.com/", type = "both")), which = c("Depends", "Imports", "LinkingTo"), recursive = TRUE, reverse=FALSE, verbose = TRUE)
    rr<-mapply(function(pkg_dep_name){
      pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
      print(pkg_dep_name)
      print(pkg_dep_name %in% ins_pkg[,"Package"])
      print(pkg_dep_name_file)
      if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
    }, pkg_dep[[pkg_dep_name]])
    pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    print(pkg_dep_name)
    print(pkg_dep_name %in% ins_pkg[,"Package"])
    print(pkg_dep_name_file)
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
  }, pkg_dep$devtools)
  pkg_name<-list.files(path="payer_again_dc/packages/", pattern="devtools_",full.names = TRUE)
  install.packages(pkg_name, repos=NULL)
}
library(devtools)
ins_pkg<-installed.packages()
if(!"redshift" %in% ins_pkg[,"Package"]){
  if(!"RJDBC" %in% ins_pkg[,"Package"]){
    pkg_dep_name<-"DBI";pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
    pkg_dep_name<-"rJava";pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
    pkg_dep_name<-"RJDBC";pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
  }
  pkg_name<-list.files(path="/home/hadoop/payer_again_dc/packages", pattern="redshift-r-",full.names = TRUE)
  system(paste("unzip",pkg_name,"-d /home/hadoop/payer_again_dc/packages"))
  install.packages(gsub("\\.zip","",pkg_name), dependencies=F, repos=NULL, type="source")
}
library(redshift)
ins_pkg<-installed.packages()
if(!"aws.s3" %in% ins_pkg[,"Package"]){
  if (!"ghit" %in% ins_pkg[,"Package"]) {
    pkg_name<-list.files(path="/home/hadoop/payer_again_dc/packages", pattern="ghit_",full.names = TRUE)
    install.packages(pkg_name, repos=NULL)
  }
  require(ghit)
  ins_pkg<-installed.packages()
  pkg_dep<-package_dependencies("xml2", db = available.packages(contrib.url("https://cran.rstudio.com/", type = "both")), which = c("Depends", "Imports", "LinkingTo"), recursive = TRUE, reverse=FALSE, verbose = TRUE)
  rr<-mapply(function(pkg_dep_name){
    pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    print(pkg_dep_name)
    print(pkg_dep_name %in% ins_pkg[,"Package"])
    print(pkg_dep_name_file)
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
  }, pkg_dep$xml2)
  pkg_name<-list.files(path="/home/hadoop/payer_again_dc/packages", pattern="xml2_",full.names = TRUE)
  install.packages(pkg_name, repos=NULL)
  library(xml2)
  ins_pkg<-installed.packages()
  
  pkg_dep<-package_dependencies("aws.s3", db = available.packages(contrib.url("https://cran.rstudio.com/", type = "both")), which = c("Depends", "Imports", "LinkingTo"), recursive = TRUE, reverse=FALSE, verbose = TRUE)
  rr<-mapply(function(pkg_dep_name){
    pkg_dep_name_file<-list.files(path="payer_again_dc/packages/", pattern=paste0("^",pkg_dep_name,"_"),full.names = TRUE)  
    print(pkg_dep_name)
    print(pkg_dep_name %in% ins_pkg[,"Package"])
    print(pkg_dep_name_file)
    if(!pkg_dep_name %in% ins_pkg[,"Package"]) install.packages(pkg_dep_name_file, repos=NULL)  
  }, pkg_dep$aws.s3)
  pkg_name<-list.files(path="/home/hadoop/payer_again_dc/packages", pattern="aws.s3_",full.names = TRUE)
  install.packages(pkg_name, repos=NULL)
  ins_pkg<-installed.packages()
}
if(!"rjson" %in% ins_pkg[,"Package"]){
  pkg_name<-list.files(path="/home/hadoop/payer_again_dc/packages", pattern="rjson_",full.names = TRUE)
  install.packages(pkg_name, repos=NULL)
  ins_pkg<-installed.packages()
}
require(ghit)
library(httr)
library(rjson)
library(xml2)
library(aws.s3)
# library(lubridate)
# library(Matrix)
# library(data.table)
# library(bit64)
# library(FNN)