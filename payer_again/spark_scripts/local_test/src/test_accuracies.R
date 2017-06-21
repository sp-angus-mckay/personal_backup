

acc_function <- function(predicted, real){
  
  tp <- length(predicted[predicted == 1 & real == 1])
  fp <- length(predicted[predicted == 1 & real == 0])
  tn <- length(predicted[predicted == 0 & real == 0])
  fn <- length(predicted[predicted == 0 & real == 1])
    
    
  accuracy <- (tp+tn)/(tp+fp+tn+fn)
  precision <- tp/(tp+fp)
  recall <- tp/(tp+fn)
  
  
  data.frame(tp,fp,tn,fn, accuracy, precision, recall)

  
    
}