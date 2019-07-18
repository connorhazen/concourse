pull_script <- function(days){
  
  end.date <- as.character(Sys.Date())
  start.date <- as.character(Sys.Date()-days)
  
  dimensions = "ga:date,ga:hour,ga:operatingSystem,ga:deviceCategory,ga:country,ga:landingContentGroup2"
  metrics = "ga:sessions,ga:adsenseRevenue,ga:dfpRevenue,ga:backfillRevenue"
  filters = "ga:sourceMedium=~facebook / cpc;ga:sessions>0;ga:country=~Canada|United States|Australia|New Zealand|United Kingdom;ga:deviceCategory=~mobile|tablet;ga:sourcePropertyDisplayName=~Fame10;ga:landingContentGroup2!~(not set)"
  
  speadsheetID = "1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M"
  
  format = 'csv'
  
  options(googleAuth.scopes.selected = c("https://www.googleapis.com/auth/analytics.readonly", 
                                         "https://www.googleapis.com/auth/spreadsheets.readonly", 
                                         "https://docs.google.com/feeds"))
  
  service_token <- gar_auth_service(json_file="Service_client.json")
  
  
  
  ga.df <- analyticsDB_pull(start.date, end.date, dimensions, metrics, filters, service_token)
  
  
  
  raw_sheet_data <-content(GET(paste("https://sheets.googleapis.com/v4/spreadsheets/", speadsheetID, "?access_token=",service_token$credentials$access_token, sep = "" )))$sheets
  
  sheet_id_df = dplyr::bind_rows(lapply(1:length(raw_sheet_data), FUN = function(i) data.frame(raw_sheet_data[[i]]$properties, stringsAsFactors = FALSE  )))
  
  
  
  
  sheet_df = lapply(1:nrow(sheet_id_df), FUN = function(i) readr::read_csv(paste0('https://docs.google.com/spreadsheets/export?id=',speadsheetID,'&format=',format, '&gid=',sheet_id_df[[1]][i])))
  
  
  
  fin_sheet_df <- sheet_df%>%
    purrr::reduce(full_join, by = c("Date", "Month", "Day"))%>%
    mutate(date = mdy(Date))%>%
    select(-Date,-Month, -Day)%>%
    mutate(AdSense = as.numeric(sub("%", "", AdSense))/100)%>%
    mutate(Prebid = as.numeric(sub("%", "", Prebid))/100)%>%
    mutate(AdX = as.numeric(sub("%", "", AdX))/100)%>%
    mutate(EBDA = as.numeric(sub("%", "", EBDA))/100)%>%
    fill(AdSense)%>%
    fill(AdSense, .direction = "up")%>%
    fill(Prebid)%>%
    fill(Prebid, .direction = "up")%>%
    fill(AdX)%>%
    fill(AdX, .direction = "up")%>%
    fill(EBDA)%>%
    fill(EBDA, .direction = "up")
  
  

  
  df_1<-ga.df%>%
    rename(date_og = date)%>%
    mutate(date = ymd(date_og))
  
  df_1 <- merge(df_1, fin_sheet_df, by = "date")
 
  
  df_final_keep <- df_1%>%
    mutate(rev1 = adsenseRevenue * AdSense )%>%
    mutate(timestamp = ymd_h(paste(date_og, hour)))%>%
    mutate(rev2 = dfpRevenue * 1.0)%>%
    mutate(rev3 = backfillRevenue * AdX)%>%
    mutate(rev = (rev1 + rev2 + rev3)*EBDA)%>%
    rename(ses = sessions)%>%
    select(date, timestamp, landingContentGroup2, operatingSystem, deviceCategory, country, ses, rev)
  
  return(df_final_keep)
  
}



gar_auth_service <- function(json_file, scope = getOption("googleAuth.scopes.selected")){
  
  stopifnot(file.exists(json_file))
  
  endpoint <- httr::oauth_endpoints("google")
  
  secrets  <- jsonlite::fromJSON(json_file)
  scope <- paste(scope, collapse=" ")
  
  if(is.null(secrets$private_key)){
    stop("$private_key not found in JSON - have you downloaded the correct JSON file? 
         (Service Account Keys, not service account client)")
  }
  
  google_token <- httr::oauth_service_token(endpoint, secrets, scope)
  
  return(google_token)
  
}



analyticsDB_pull <- function(start.date_inp, end.date_inp, dimensions_inp, metrics_inp, filters_inp, service_token){
  library(RGoogleAnalytics)
  
  trys<-0
  if(exists("view_id_list")){
    print("shit")
  }
  while(!exists("view_id_list")&trys <4){
    trys<-trys+1
    try(view_id_list <- GetProfiles(service_token))
  }
  
  view_id <- view_id_list[[1]]
  
  
  
  query.list <- Init(start.date = start.date_inp,
                     end.date = end.date_inp,
                     dimensions = dimensions_inp,
                     metrics = metrics_inp,
                     table.id = paste("ga:",view_id, sep = ""), 
                     max.results = 10000, 
                     filters = filters_inp)
  
  ga.query <- QueryBuilder(query.list)
  if(exists("df_pull_ret")){
    
    rm(df_pull_ret)
  }
  
  while(!exists("df_pull_ret") & trys <4){
    
    trys<-trys+1
    try(df_pull_ret <-GetReportData(ga.query, service_token, paginate_query = TRUE), silent = FALSE)
  }
  if(trys >0){
    print("got new data")
  }
  return(df_pull_ret)
}

