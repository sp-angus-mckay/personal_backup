.verify.JDBC.result <- function (result, ...) {
  if (is.jnull(result)) {
    x <- .jgetEx(TRUE)
    if (is.jnull(x))
      stop(...)
    else
      stop(...," (",.jcall(x, "S", "getMessage"),")")
  }
}

retrieveDataFromRedshift<-function(statement, table, nfetch = 1000000, outputPath = "/tmp/"){
  myconn@jc$setAutoCommit(TRUE)
  if(!is.null(statement)) print(nrows<-dbGetQuery(myconn, paste0("select count(1) from (",statement,")")))
  nchunks<-ceiling(nrows$count/nfetch)
  if(myconn@jc$autoCommit) myconn@jc$setAutoCommit(FALSE)
  s <- .jcall(myconn@jc, "Ljava/sql/Statement;", "createStatement")
  x<-.jnew("java/lang/Integer", as.character(format(nfetch, scientific = F)))
  .jcall(s, returnSig = "V", method = "setFetchSize", x$intValue())
  .verify.JDBC.result(s, "Unable to create simple JDBC statement ",statement)
  
  r <- .jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(statement)[1], check=FALSE)
  .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement)
  md <- .jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
  .verify.JDBC.result(md, "Unable to retrieve JDBC result set meta data for ",statement, " in dbSendQuery")
  rs<-new("JDBCResult", jr=r, md=md, stat=s, pull=.jnull())
  
  nn<-0;i<-0
  outputPath<-paste0(outputPath,table)
  if(!file.exists(outputPath)){
    system(paste0("mkdir ",outputPath))  
  }else{
    system(paste0("rm -f ",outputPath,"/*"))  
  }
  appendVar<-FALSE
  while (nn<nrows$count) {
    if(nn %% (nfetch*10) == 0){
      print(paste(nn, base::date()))
      appendVar = FALSE
      i<-i+1
    }else{
      appendVar<-TRUE
    } 
    chunk <- fetch(rs, nfetch)
    nn<-nn+nrow(chunk)
    fwrite(chunk, file=paste0(outputPath,"/chunk_",i,".csv"), row.names=F, append = appendVar, sep=",", quote=F, col.names=!appendVar)
    appendVar<-TRUE
  }
  dbClearResult(rs)
  s$close()
}
