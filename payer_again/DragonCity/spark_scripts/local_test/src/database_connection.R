getBDPath<-function(your_game){
  switch(your_game,DragonCity="jdbc:postgresql://pro-sp-dc.cm7gsqupavgd.us-east-1.redshift.amazonaws.com:5439/datawarehouse"
         ,MonsterLegends="jdbc:postgresql://pro-sp-mc.cm7gsqupavgd.us-east-1.redshift.amazonaws.com:5439/datawarehouse"
         ,DragonStadium="jdbc:postgresql://redshift.pro.ds.laicosp.net:5439/datawarehouse"
         ,DragonLand="jdbc:postgresql://dl-redshift.socialpointgames.com:5439/datawarehouse"
         ,WorldChef="jdbc:postgresql://pro-sp-rc.cm7gsqupavgd.us-east-1.redshift.amazonaws.com:5439/datawarehouse")
}
user<-'amckay'
password<-'o6EbWm4L79jRfegy'
getDatabaseSchema<-function(your_game) {
  switch(your_game
         ,DragonCity="dragoncity"
         ,MonsterLegends="monstercity"
         ,DragonStadium="dragonstadium"
         ,DragonLand="dragonland"
         ,WorldChef="restaurantcity" )
}

your_game<-"DragonCity"
databaseSchema<-getDatabaseSchema(your_game)
BDPath<-getBDPath(your_game)
set_path<-paste(paste("SET search_path TO ",databaseSchema,sep=""),";" ,sep="")
if(exists("myconn") && !("PSQLException" %in% class(myconn)) && !("error" %in% class(myconn))){
  print("Disconnecting from redshift")
  redshift.disconnect(myconn)
} 
myconn<-tryCatch(redshift.connect(BDPath, user, password),error=function(e){
  print(e)
})
print(myconn)
redshift.submitquery(myconn,set_path)
url<-paste0(BDPath,"?user=",user,"&password=",password)
#####