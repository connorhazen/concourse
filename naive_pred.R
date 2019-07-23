# 3day moving avg ------------------------------------------------------------------------------------------------




#' Function to run 3day moving average predictions
#' @param df data frame must contain date, landingcontentgroup2, country, devicecategory, operatingsystem, ses, rev, rps, avg
#' @param current_date date representing "current date" can be used to backtest by setting date in the past
#' @return data frame containg new avg predictions
#' 
f10_predictor <- function(df, pred_date){
  
  df_test <- df%>%
    filter(date<=current_date)%>%
    filter(date>current_date-3)
  
  df_check <- df%>%
    filter(date == current_date+1, !is.na(avg))%>%
    data.frame()%>%
    select(landingcontentgroup2, country, devicecategory, operatingsystem, avg)
  
  
  
  df_amt<-df_test%>%
    filter(ses >= 100)%>%
    group_by(landingcontentgroup2, country, devicecategory, operatingsystem)%>%
    summarise(count = n(), ses = sum(ses, na.rm = TRUE))%>%
    mutate(avg = as.numeric(NA))%>%
    data.frame()%>%
    mutate(date = current_date+1)
  
  for(x in 1:length(df_amt$count)){
    
    
    
    name1<-df_amt[x,]
    
    df_filt<-df_check%>%
      filter(landingcontentgroup2 == name1$landingcontentgroup2 , country == name1$country ,
             devicecategory == name1$devicecategory, operatingsystem == name1$operatingsystem)
    
    if(nrow(df_filt)>0){
      df_amt<-df_amt%>%
        mutate(avg = case_when(landingcontentgroup2 == name1$landingcontentgroup2 & country == name1$country &
                                 devicecategory == name1$devicecategory & operatingsystem == name1$operatingsystem~df_filt$avg[1],
                               TRUE ~ avg))
      
      next
    }
    
    df_mean <- df_test%>%
      filter(landingcontentgroup2 == name1$landingcontentgroup2 , country == name1$country ,
             devicecategory == name1$devicecategory, operatingsystem == name1$operatingsystem)
    
    d3_ma <- mean(df_mean$rps, na.rm = TRUE)
    
    if(is.nan(d3_ma)){
      print("WTF")
      d3_ma <-0
    }
    
    df_amt<-df_amt%>%
      mutate(avg = case_when(landingcontentgroup2 == name1$landingcontentgroup2 & country == name1$country &
                               devicecategory == name1$devicecategory & operatingsystem == name1$operatingsystem~d3_ma,
                             TRUE ~ avg))
    
  }
  df_amt<-df_amt%>%
    select(date, landingcontentgroup2, devicecategory, operatingsystem, country, avg)
  
  return(df_amt)
}


