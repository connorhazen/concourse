


fin_script <- function(){
  source("~/concourse/pull_script.R")
  
  library(dplyr)
  library(tidyr)
  library(padr)
  library(dplyr.snowflakedb)
  library(parallel)
  library(pbmcapply)

  options(dplyr.jdbc.classpath = "~/snowflake_jdbc.jar")
  numCores <- detectCores() # get the number of cores available
  
  
  
  
  
  #Pulling and merging data sets ---------------------------------------------------------------------
  
  # Connection to Snowflake database
  
  my_db <- src_snowflakedb(user = "CONNOR_HAZEN",password = "FFg-nvU-83U-nYh", account = "system1",
                           opts = list(warehouse = "S1_DS", db = "DATA_SCIENCE",schema = "CONCOURSE"))
  
  
  
  
  
  
  
  # Insures the table is in snowflake, if not it creates a new one
  if("CONCOURSE_SELL_SIDE_HAZEN" %in% db_list_tables(my_db$con) && FALSE){
    
    
    # Pull old table
    df_sf<- tbl(my_db, "CONCOURSE_SELL_SIDE_HAZEN")%>%
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
  
  
  
  
  
  
  source("~/concourse/naive_pred.R")
  pred_date <- new_data_start
  
 
  
 
  
  mcresults <- pbmclapply(seq(from = pred_date, to = Sys.Date(), by = "day"),
                        FUN=function(i) f10_predictor(df_total_day, i),
                        mc.cores = numCores)
  mcresults <-bind_rows(mcresults)
  
  
  df_total_day <- merge(df_total_day, mcresults, 
                        by = c("date","landingContentGroup2", "country", 
                               "deviceCategory", "operatingSystem"), 
                        all = TRUE)%>%
    mutate(avg = case_when(!is.na(avg.y)~avg.y,
                           TRUE~avg.x))%>%
    select(-avg.x, -avg.y)
  
  
  
  
  
  
  
  
  
  
  
  
  # 12 day expontialy weighted moving average model over new data ------------------------------------
  
  
  source("~/concourse/day_pred.R")
  pred_date <- new_data_start
  
  mcresults <- pbmclapply(seq(from = pred_date, to = Sys.Date(), by = "day"),
                        FUN=function(i) predictor_day(df_total_day, i),
                        mc.cores = numCores)
  mcresults <-bind_rows(mcresults)
  
  
 
  
  
  df_total_day <- merge(df_total_day, mcresults, 
                        by = c("date","landingContentGroup2", 
                               "country", "deviceCategory", "operatingSystem"), 
                        all = TRUE)%>%
    mutate(dpred = case_when(!is.na(dpred.y)~dpred.y,
                             TRUE~dpred.x))%>%
    select(-dpred.x, -dpred.y)

  
  
  
  
  
  
  
  
  
  
  # Tbats on hourly data -----------------------------------------------------------------------------

  
  source("~/concourse/hour_pred.R")
  
  
  pred_date <- new_data_hour
  total_timer <- Sys.time()
  
  
  
  lapp_res <- pbmclapply(seq(from = pred_date, to = Sys.Date(), by = "day"),
                       FUN=function(i) predictor_hour(df_total, i),
                       mc.cores = numCores,
                       ignore.interactive = TRUE)
  
  lapp_res <-bind_rows(lapp_res)
  
  
  
  print(Sys.time()-total_timer)
  
  
  
  
  
  
  
  
  df_total <- merge(df_total, lapp_res, 
                    by = c("date","timestamp","landingContentGroup2", "country", 
                           "deviceCategory", "operatingSystem"), 
                    all = TRUE)%>%
    mutate(hpred = case_when(!is.na(hpred.y)~hpred.y,
                             TRUE~hpred.x))%>%
    select(-hpred.x, -hpred.y)
  
  
  rm(mcresults)
  
  
  
  
  
  
  
  
  
  # Combining day and hourly estimates and pushing to snow flake -------------------------------------
  
  
  df_total <- merge(select(df_total_day, -ses,-rev, -rps), 
                    df_total, 
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
  
  
  
  copy_to(my_db, df_total, "CONCOURSE_SELL_SIDE_HAZEN", overwrite = TRUE)
  
  
  
  
  print("CONCOURSE_SELL_SIDE_HAZEN")
  
  return(df_total)
  
  
}




fin_script()

