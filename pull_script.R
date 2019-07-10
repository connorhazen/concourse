pull_script <- function(days){
  library(gsheet)
  library(dplyr)
  source("~/concourse/dataPull.R")
  
  
  end.date <- as.character(Sys.Date()-1)
  start.date <- as.character(Sys.Date()-days)
  
  dimensions = "ga:date,ga:hour,ga:operatingSystem,ga:deviceCategory,ga:country,ga:landingContentGroup2"
  metrics = "ga:sessions,ga:adsenseRevenue,ga:dfpRevenue,ga:backfillRevenue"
  filters = "ga:sourceMedium=~facebook / cpc;ga:sessions>0;ga:country=~Canada|United States|Australia|New Zealand|United Kingdom;ga:deviceCategory=~mobile|tablet;ga:sourcePropertyDisplayName=~Fame10;ga:landingContentGroup2!~(not set)"
  
  ga.df <- pull(start.date, end.date, dimensions, metrics, filters)
  
  print(nrow(ga.df))

  AdSense_df_1 <- gsheet2tbl("https://docs.google.com/spreadsheets/d/1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M/edit#gid=0")
  
  prebid_df_1 <- gsheet2tbl("https://docs.google.com/spreadsheets/d/1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M/edit#gid=99403060")
  
  AdX_df_1 <- gsheet2tbl("https://docs.google.com/spreadsheets/d/1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M/edit#gid=1262680589")
  
  EBDA_df_1 <- gsheet2tbl("https://docs.google.com/spreadsheets/d/1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M/edit#gid=1738720175")
  
  
  usd<-gsheet2tbl("https://docs.google.com/spreadsheets/d/1N87gk5rPEbRK578OM264JsyoEFbbX7VXIVfrXm30tCE/edit#gid=695689619")
  
  usdcad <- as.numeric(colnames(usd)[1])
  
  df_1<-ga.df%>%
    rename(date_og = date)%>%
    mutate(date = ymd(date_og))
  
  AdSense_df <- AdSense_df_1%>%
    mutate(date = mdy(Date))%>%
    rename(adSense_mod = 4)%>%
    mutate(adSense_mod = as.numeric(sub("%", "", adSense_mod))/100)%>%
    select(date, adSense_mod)%>%
    fill(adSense_mod)%>%
    fill(adSense_mod, .direction = "up")
  
  
  prebid_df <- prebid_df_1%>%
    mutate(date = mdy(Date))%>%
    rename(prebid_mod = 4)%>%
    mutate(prebid_mod = as.numeric(sub("%", "", prebid_mod))/100)%>%
    select(date, prebid_mod)%>%
    fill(prebid_mod)%>%
    fill(prebid_mod, .direction = "up")
  
  
  AdX_df <- AdX_df_1%>%
    mutate(date = mdy(Date))%>%
    rename(adX_mod = 4)%>%
    mutate(adX_mod = as.numeric(sub("%", "", adX_mod))/100)%>%
    select(date, adX_mod)%>%
    fill(adX_mod)%>%
    fill(adX_mod, .direction = "up")
  
  
  
  
  EBDA_df <- EBDA_df_1%>%
    mutate(date = mdy(Date))%>%
    rename(ebda_mod = 4)%>%
    mutate(ebda_mod = as.numeric(sub("%", "", ebda_mod))/100)%>%
    select(date, ebda_mod)%>%
    fill(ebda_mod)%>%
    fill(ebda_mod, .direction = "up")
  
  
  
  df_1 <- merge(df_1, AdSense_df, by = "date")
  df_1 <- merge(df_1, prebid_df, by = "date")
  df_1 <- merge(df_1, AdX_df, by = "date")
  df_1 <- merge(df_1, EBDA_df, by = "date")
  
  df_final_keep <- df_1%>%
    mutate(rev1 = adsenseRevenue * adSense_mod )%>%
    mutate(timestamp = ymd_h(paste(date_og, hour)))%>%
    mutate(rev2 = dfpRevenue * 1.0)%>%
    mutate(rev3 = backfillRevenue * adX_mod)%>%
    mutate(rev = (rev1 + rev2 + rev3)*ebda_mod)%>%
    rename(ses = sessions)%>%
    select(date, timestamp, landingContentGroup2, operatingSystem, deviceCategory, country, ses, rev)
  
  return(df_final_keep)
  
}