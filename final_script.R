

fin_script <- function(){
  source("~/rstudio-workspace/f10/pull_script.R")
  source("~/rstudio-workspace/f10/pull_script_day.R")
  
  library(dplyr)
  library(padr)
  library(dplyr.snowflakedb)
  options(dplyr.jdbc.classpath = "~/snowflake_jdbc.jar")
  
  
  
  
  
  #Pulling and merging data sets ---------------------------------------------------------------------
  
  # Connection to Snowflake database
  
  my_db <- src_snowflakedb(user = "CONNOR_HAZEN",password = "FFg-nvU-83U-nYh", account = "system1",
                           opts = list(warehouse = "S1_DS", db = "DATA_SCIENCE",schema = "CONCOURSE"))
  
  
  
  
  
  
  
  # Insures the table is in snowflake, if not it creates a new one
  if("CONCOURSE_SELL_SIDE" %in% db_list_tables(my_db$con)){
    
    
    # Pull old table
    df_sf<- tbl(my_db, "CONCOURSE_SELL_SIDE")%>%
      data.frame()%>%
      arrange(desc(date))
    
    new_data_start <- df_sf$date[1]
    
    # Find number of days to refresh, the +5 is in case modifiers updated
    days <- as.numeric(Sys.Date()-new_data_start)+5
    
    
    
    
    
    # Pull New data from concourse
    df_new<-pull_script(days)%>%
      filter(rev >= .1)
    
    
    # Combine with snowflake table 
    df_total <- merge(df_sf, df_new, 
                      by = c("timestamp","landingContentGroup2", 
                             "country", "deviceCategory", "operatingSystem"),
                      all = TRUE)%>%
      mutate(rev = case_when(!is.na(rev.y)~rev.y, 
                             TRUE~rev.x))%>%
      mutate(ses = case_when(!is.na(ses.y)~ses.y, 
                             TRUE~ses.x))%>%
      mutate(rps = case_when(rev>0&ses>0~rev/ses,
                             TRUE~as.numeric(NA)))%>%
      select(date,timestamp, landingContentGroup2, country, deviceCategory, 
             operatingSystem, ses, rev, rps, hpred, dpred, avg)
    
  }else{
    
    # If no snowflake present, pull maximum amount of data.
    df_total<-pull_script(40)%>%
      filter(rev >= .1)%>%
      mutate(rps = case_when(rev>0&ses>0~rev/ses,
                             TRUE~as.numeric(NA)))%>%
      mutate(hpred = as.numeric(NA), dpred = as.numeric(NA), avg = as.numeric(NA))%>%
      arrange(date)
    
    new_data_start <- df_total$date[1]+1
    new_data_hour <- df_total$date[1]+18
    
  }
  
  
  
  
  
  # This is the table agregated into days not hours
  df_total_day<- df_total%>%
    group_by(date, landingContentGroup2, operatingSystem , deviceCategory, country)%>%
    summarise(ses = sum(ses, na.rm = TRUE), rev = sum(rev, na.rm = TRUE), 
              avg = mean(avg, na.rm = TRUE), dpred = mean(dpred, na.rm = TRUE))%>%
    mutate(rps = case_when(rev>0&ses>0~rev/ses,
                           TRUE~as.numeric(NA)))
  
  
  
  
  
  
  
  
  
  
  # Recreating concourse 3day moving average, runs over all new data. --------------------------------
  
  source("~/rstudio-workspace/f10/naive_pred.R")
  pred_date <- new_data_start
  
  
  if(exists("df_fin")){
    rm(df_fin)
  }
  
  
  while(pred_date<=Sys.Date()){
    print(paste("date:", pred_date))
    df_ret <- f10_predictor(df_total_day, pred_date)
    
    if(exists("df_fin")){
      df_fin <- rbind(df_fin, df_ret)
    }else{
      df_fin<- df_ret
    }
    pred_date<- pred_date +1
  }
  
  
  
  df_total_day <- merge(df_total_day, df_fin, 
                        by = c("date","landingContentGroup2", "country", 
                               "deviceCategory", "operatingSystem"), 
                        all = TRUE)%>%
    mutate(avg = case_when(!is.na(avg.y)~avg.y,
                           TRUE~avg.x))%>%
    select(-avg.x, -avg.y)
  
  rm(df_fin)
  rm(df_ret)
  
  
  
  
  
  
  
  
  
  
  
  # 12 day expontialy weighted moving average model over new data ------------------------------------
  
  
  source("~/rstudio-workspace/f10/day_pred.R")
  pred_date <- new_data_start
  
  
  if(exists("df_pred_loop")){
    rm(df_pred_loop)
  }
  
  while(pred_date<=Sys.Date()){
    
    print(paste("date:", pred_date))
    print("")
    print("")
    
    
    df_ret <- predictor_day(df_total_day, pred_date)
    
    pred_date<- pred_date +1
    
    
    if(exists("df_pred_loop")){
      df_pred_loop <- rbind(df_pred_loop, df_ret)
    }else{
      df_pred_loop<- df_ret
    }
  }
  
  
  df_total_day <- merge(df_total_day, df_pred_loop, 
                        by = c("date","landingContentGroup2", 
                               "country", "deviceCategory", "operatingSystem"), 
                        all = TRUE)%>%
    mutate(dpred = case_when(!is.na(dpred.y)~dpred.y,
                             TRUE~dpred.x))%>%
    select(-dpred.x, -dpred.y)
  rm(df_pred_loop)
  rm(df_ret)
  
  
  
  
  
  
  
  
  
  
  
  # Tbats on hourly data -----------------------------------------------------------------------------
  
  
  
  
  source("~/rstudio-workspace/f10/hour_pred.R")
  
  
  pred_date <- new_data_hour
  end_date <- Sys.Date()
  total_timer <- Sys.time()
  
  
  
  
  
  
  
  
  if(exists("df_pred_loop")){
    rm(df_pred_loop)
  }
  while(pred_date<=end_date){
    
    print(paste("date:", pred_date))
    print("")
    print("")
    
    loop_timer <- Sys.time()
    df_ret <- predictor_hour(df_total, pred_date)
    print(Sys.time() - loop_timer)
    
    pred_date<- pred_date +1
    
    if(exists("df_pred_loop")){
      df_pred_loop <- rbind(df_pred_loop, df_ret)
    }else{
      df_pred_loop<- df_ret
    }
  }
  
  
  
  print(Sys.time()-total_timer)
  
  
  
  
  
  
  
  
  df_total <- merge(df_total, df_pred_loop, 
                    by = c("date","timestamp","landingContentGroup2", "country", 
                           "deviceCategory", "operatingSystem"), 
                    all = TRUE)%>%
    mutate(hpred = case_when(!is.na(hpred.y)~hpred.y,
                             TRUE~hpred.x))%>%
    select(-hpred.x, -hpred.y)
  
  
  rm(df_pred_loop)
  rm(df_ret)
  
  
  
  
  
  
  
  
  # Combining day and hourly estimates and pushing to snow flake -------------------------------------
  
  
  df_total <- merge(select(df_total_day, -ses,-rev, -rps), 
                    df_final, 
                    by = c("date","landingContentGroup2", "country", 
                           "deviceCategory", "operatingSystem"), 
                    all = TRUE)%>%
    mutate(dpred = case_when(!is.na(dpred.y)~dpred.y,
                             TRUE~dpred.x))%>%
    mutate(avg = case_when(!is.na(avg.y)~avg.y,
                           TRUE~avg.x))%>%
    mutate(hpred = case_when(hpred<=0~dpred,
                             TRUE ~ hpred))%>%
    select(date, timestamp, landingContentGroup2, country, deviceCategory, 
           operatingSystem, ses, rev, rps, hpred, dpred, avg)
  
  
  
  my_db <- src_snowflakedb(user = "CONNOR_HAZEN",password = "FFg-nvU-83U-nYh" ,account = "system1", 
                           opts = list(warehouse = "S1_DS", db = "DATA_SCIENCE",schema = "CONCOURSE"))
  
  
  
  copy_to(my_db, df_total, "CONCOURSE_SELL_SIDE", overwrite = TRUE)
  
  
  
  
  print("CONCOURSE_SELL_SIDE")
  
  return(df_total)
  
  
}

