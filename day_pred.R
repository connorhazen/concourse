predictor_day <- function(df, current_date) {

  
  df_test <- df%>%
    filter(date<=current_date)%>%
    filter(date>current_date-3)
  
  df_check <- df%>%
    filter(date == current_date+1, !is.na(dpred))%>%
    select(landingContentGroup2, country, deviceCategory, operatingSystem, dpred)
  
  
  
  df_amt<-df_test%>%
    filter(ses >= 100)%>%
    group_by(landingContentGroup2, country, deviceCategory, operatingSystem)%>%
    summarise(count = n(), ses = sum(ses, na.rm = TRUE))
  
  df_amt <- merge(df_amt, df_check,
                  by = c("landingContentGroup2", "country", "deviceCategory", "operatingSystem"), all = TRUE)%>%
    arrange(desc(ses))%>%
    mutate(date = current_date+1)
  
  
  for(x in 1:nrow(df_amt)){
    
    
    
    
    name1<-df_amt[x,]
    
    if(!is.na(name1$dpred)){
      next
    }
    
    
    df_1<-df%>%
      filter(landingContentGroup2 == name1$landingContentGroup2 , country == name1$country ,
             deviceCategory == name1$deviceCategory, operatingSystem == name1$operatingSystem)%>%
      filter(date > current_date-12, date <= current_date)%>%
      pad(start_val = current_date-11, end_val = current_date, interval = "day" )%>%
      arrange(desc(date))
    

    #begin pred
    
    weights <- rep(.6,12)^seq(1:12)
    
    
    df_exp <- data.frame(df_1, weights)%>%
      mutate(rps_adj = rps*weights)%>%
      filter(!is.na(rps_adj))
    
    
    
    dval <- sum(df_exp$rps_adj)/sum(df_exp$weights)
    


    
    # dval <- mean(df_1$rps, na.rm = TRUE)
    # 
    # df_3d <- df_1%>%
    #   filter(date > current_date-3)%>%
    #   filter(date <= current_date)
    # 
    # d_avg <- mean(df_3d$rps, na.rm = TRUE)
    # 
    # dval <- (d_avg-dval) * .3 + dval
    
    print(dval)
    if(dval < 0){
      dval <- 0
    }
    
    

    
    df_amt<-df_amt%>%
      mutate(dpred = case_when(landingContentGroup2 == name1$landingContentGroup2 & country == name1$country &
                                 deviceCategory == name1$deviceCategory & operatingSystem == name1$operatingSystem~dval,
                    TRUE ~ dpred))
 
    #end pred
    
    
   
  }
  
  df_amt<-df_amt%>%
    select(date, landingContentGroup2, deviceCategory, operatingSystem, country, dpred)
  
  return(df_amt)
  
}
