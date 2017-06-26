cat("\n\n\n\n\n")
print("######    INSIDE  R   SCRIPT  ")
cat("\n\n\n\n\n")
library(tools)
args<-commandArgs(trailingOnly = TRUE)
packages_path<-args[1]
packages_list<-unlist(strsplit(args[2],";"))

cat("\n\n\n\n\n")
print("packages_path")
print(packages_path)
print("packages_list")
print(packages_list)
cat("\n\n\n\n\n")

packages_to_install<-mapply(function(package){
  print(package)
  deps<-package_dependencies(package, db = available.packages(contrib.url("https://cran.rstudio.com/", type = "both")), which = c("Depends", "Imports", "LinkingTo"), recursive = TRUE, reverse=FALSE, verbose = TRUE)
}, packages_list)
packages_to_install<-union(unlist(packages_to_install), packages_list)

packages_to_install_files<-unlist(mapply(function(pkg_name){
  grep(paste0("^",pkg_name,"_"),list.files(packages_path),value=T)
}, packages_to_install))
cat("\n\n\n\n\n")
print("packages_to_install_files")
print(packages_to_install_files)
cat("\n\n\n\n\n")

for(package in packages_to_install_files){
  cat("\n\n\n\n\n")
  install.packages(paste0(packages_path,"/",package),repos=NULL)
  print(paste0(packages_to_install_files," installed"))
  cat("\n\n\n\n\n")
}
