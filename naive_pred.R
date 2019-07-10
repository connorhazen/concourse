f10_predictor <- function(df, current_date){
  
  df_test <- df%>%
    filter(date<=current_date)%>%
    filter(date>current_date-3)
  
  df_check <- df%>%
    filter(date == current_date+1, !is.na(avg))%>%
    data.frame()%>%
    select(landingContentGroup2, country, deviceCategory, operatingSystem, avg)
  
  
  
  df_amt<-df_test%>%
    filter(ses >= 100)%>%
    group_by(landingContentGroup2, country, deviceCategory, operatingSystem)%>%
    summarise(count = n(), ses = sum(ses, na.rm = TRUE))%>%
    data.frame()
  
  df_amt <- merge(df_check, df_amt, by = c("landingContentGroup2", "country", "deviceCategory", "operatingSystem"), all = TRUE)%>%
    arrange(desc(ses))%>%
    mutate(date = current_date+1)
  
  
  
  for(x in 1:length(df_amt$count)){
    
    
    
    name1<-df_amt[x,]
    
    df_filt<-df_test%>%
      filter(landingContentGroup2 == name1$landingContentGroup2 , country == name1$country ,
             deviceCategory == name1$deviceCategory, operatingSystem == name1$operatingSystem)%>%
      arrange(date)
    
    if(!is.na(df_filt$avg)){
      next
    }
    
    d3_ma <- mean(df_filt$rps, na.rm = TRUE)
    
    if(is.nan(d3_ma)){
      print("WTF")
      d3_ma <-0
    }
    
    df_amt<-df_amt%>%
      mutate(avg = case_when(landingContentGroup2 == name1$landingContentGroup2 & country == name1$country &
                             deviceCategory == name1$deviceCategory & operatingSystem == name1$operatingSystem~d3_ma,
                             TRUE ~ avg))
    
  }
  df_amt<-df_amt%>%
    select(date, landingContentGroup2, deviceCategory, operatingSystem, country, avg)
  
  return(df_amt)
}
