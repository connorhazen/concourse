predictor_hour <- function(df, current_date) {
  source("~/concourse/tbats_hour.R")
  library(imputeTS)
  library(forecast) 
  library(xts)
  
  library(checkmate)
  library(data.table)
  library(lubridate)
  library(system1)
  numCores <- detectCores()-1 # get the number of cores available
  
  system(paste("echo '\n",current_date,"'"))
  
  
 
  df_amt <- df%>%
    filter(date<=current_date)%>%
    filter(date>current_date-3)%>%
    group_by(date, landingContentGroup2, country, deviceCategory, operatingSystem)%>%
    summarise(ses = sum(ses, na.rm = TRUE))%>%
    filter(ses >= 100)%>%
    group_by(landingContentGroup2, country, deviceCategory, operatingSystem)%>%
    summarise(count = n(), ses = sum(ses, na.rm = TRUE))%>%
    arrange(desc(count))
  
  
  
  df_check <- df%>%
    filter(date == current_date+1, !is.na(hpred))%>%
    select(date, timestamp, landingContentGroup2, country, deviceCategory, operatingSystem, hpred)
  

  

  
  
  
  inner_func_hour <- function(row){
    
    
    name1<-df_amt[row,]
    
    df_hour_tbats<-df_check%>%
      filter(landingContentGroup2 == name1$landingContentGroup2 , country == name1$country ,
             deviceCategory == name1$deviceCategory, operatingSystem == name1$operatingSystem)%>%
      select(date, timestamp, landingContentGroup2, country, deviceCategory, operatingSystem, hpred)
    
    
    
    
    if(nrow(df_hour_tbats)!=24){
      
      df_1<-df%>%
        filter(landingContentGroup2 == name1$landingContentGroup2 , country == name1$country ,
               deviceCategory == name1$deviceCategory, operatingSystem == name1$operatingSystem)%>%
        filter(date > current_date-30)%>%
        filter(date <= current_date)%>%
        arrange(timestamp)%>%
        select(timestamp, rps, ses)%>%
        rename(metric = 2, weight = 3)
      
      
      #HERE_________---------___________---------------@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      
      
      df_hour_tbats <-fit_tbats_concourse(df_1, current_date = current_date, forecast = 37)%>%
        mutate(date = date(timestamp))%>%
        filter(date == current_date+1)%>%
        rename(hpred = 2)%>%
        select(timestamp, hpred, date)%>%
        mutate(landingContentGroup2 = name1$landingContentGroup2 , country = name1$country ,
               deviceCategory = name1$deviceCategory, operatingSystem = name1$operatingSystem)%>%
        select(date, timestamp, landingContentGroup2, country, deviceCategory, operatingSystem, hpred)
      
      #end pred
      
      
    }
    return(df_hour_tbats)
    
  }
  
  
  mcresults <- pbmclapply(1:nrow(df_amt),
                          FUN=function(i) inner_func_hour(i),
                          mc.cores = 1,
                          mc.cleanup = TRUE,
                          mc.preschedule = FALSE,
                          ignore.interactive = TRUE)
  

  mcresults <-bind_rows(mcresults)
  
  
 
  return(mcresults)
  
}
