library(dplyr)
library(imputeTS)
library(forecast) 
library(xts)
library(checkmate)
library(data.table)
library(lubridate)
library(logger)
library(system1)

#' This is the script to run predictions and data pulls
#' @return data frame of concourse RPS with predictions
#' @param config_loc location of config.yml file
#' @param spread_id google spreadsheet id for modifiers. 
f10_roas_predic <- function(config_loc, spread_id, view_id){
  
  pred_date <- Sys.Date()
  #Pulling and merging data sets ---------------------------------------------------------------------
  
  # Connection to Snowflake database
  
  my_db <-  tryCatch(dbr::db_query(paste('select * 
                                         from "DATA_SCIENCE"."CONCOURSE"."CONCOURSE_SELL_SIDE_HAZEN" 
                                         where "DATE" >to_date(\'', Sys.Date()-30, '\')', 
                                         sep = ""), db = 'snowflake'), error = function(e)e)
  

  # Insures the table is in snowflake, if not it creates a new one
  if (!is(my_db, "error")){
    
    #Make names consitent, weird habit of going all uppercase when pushing to snowflake, so I just followed suit....
    names(my_db) <- tolower(names(my_db))
    
    # make sure table has right date metrics. 
    df_sf <- my_db %>%
      data.frame() %>%
      mutate(date = as.Date(date), timestamp = as.POSIXct(timestamp)) %>%
      arrange(desc(date))
    
    #Most recent date in snowflake, this tells me how much to pull
    new_data_start <- df_sf$date[1]
    
    # Find number of days to refresh, 
    # the +5 is in case modifiers updated or data comes in after the fact. 
    # not the most elegant but should work
    days <- as.numeric(Sys.Date() - new_data_start) + 5
  
    log_info(paste("Found snowflake table, pulling", days, "days of new data from google analytics"))
    
    # Pull New data from concourse, ga360
    df_new <- pull_script(days, config_loc, spread_id, view_id) %>%
      filter(rev >= .01)
    
    names(df_new) <- tolower(names(df_new))
    
    # Combine with snowflake table, fill rev, ses with new pull, recalculate rps. 
    df_total <- merge(df_sf, df_new, 
                      by = c("timestamp", "landingcontentgroup2", 
                             "country", "devicecategory", "operatingsystem", "date"),
                      all = TRUE)%>%
      mutate(rev = case_when(!is.na(rev.y)~rev.y, 
                             TRUE~rev.x)) %>%
      mutate(ses = case_when(!is.na(ses.y)~ses.y, 
                             TRUE~ses.x)) %>%
      mutate(rps = case_when(rev > 0 & ses > 0 ~ rev / ses,
                             TRUE~as.numeric(NA))) %>%
      select(date, timestamp, landingcontentgroup2, country, devicecategory, 
             operatingsystem, ses, rev, rps, hpred, dpred, avg) %>%
      mutate(date = date(timestamp))
    
  }else{
    
    # If no snowflake present, pull maximum amount of data.
    # In case table is deleted.
    df_total <- pull_script(40, config_loc, spread_id, view_id) %>%
      filter(rev >= .1) %>%
      mutate(rps = case_when(rev > 0 & ses > 0 ~ rev / ses,
                             TRUE~as.numeric(NA))) %>%
      mutate(hpred = as.numeric(NA), dpred = as.numeric(NA), avg = as.numeric(NA)) %>%
      arrange(date)
    
    names(df_total) <- tolower(names(df_total))
  }
  
  # This is the table agregated into days not hours
  df_total_day <- df_total %>%
    group_by(date, landingcontentgroup2, operatingsystem, devicecategory, country) %>%
    summarise(ses = sum(ses, na.rm = TRUE), rev = sum(rev, na.rm = TRUE), 
              avg = mean(avg, na.rm = TRUE), dpred = mean(dpred, na.rm = TRUE)) %>%
    mutate(rps = case_when(rev > 0 & ses > 0 ~ rev / ses,
                           TRUE~as.numeric(NA)))
  
  # This table is what dictates which ad sets get predictions. Its based of the concourse current spreadsheet.
  df_amt <- df_total_day %>%
    filter(date < pred_date) %>%
    filter(date >= pred_date - 2) %>%
    filter(ses >= 100) %>%
    group_by(landingcontentgroup2, country, devicecategory, operatingsystem) %>%
    summarise(count = n()) %>%
    arrange(desc(count)) %>%
    select(-count)

  
  
  
  
  
  
  
  
  # Recreating concourse 2day moving average, predicts current day
  log_info("starting naive predictions")

  
  #This was used for backtesting or if I missed a day
  #mcresults <- mclapply(seq(from = pred_date, to = Sys.Date()-1, by = "day"),
  #                      FUN=function(i) f10_predictor(df_total_day, i),
  #                      mc.cores = numCores-1)
  #res <- bind_rows(mcresults)
  
  res <- df_total_day %>%
    filter(date < pred_date) %>%
    filter(date >= pred_date - 2) %>%
    filter(ses >= 100) %>%
    group_by(landingcontentgroup2, country, devicecategory, operatingsystem) %>%
    summarise(avg = mean(rps, na.rm = TRUE)) %>%
    mutate(date = pred_date)
  
  #Add results to df_total_day
  df_total_day <- merge(df_total_day, res, 
                        by = c("date", "landingcontentgroup2", "country", 
                               "devicecategory", "operatingsystem"), 
                        all = TRUE) %>%
    mutate(avg = case_when(!is.na(avg.y)~avg.y,
                           TRUE~avg.x)) %>%
    select(-avg.x, -avg.y)

  
  
  
  
  
  
  
  # 12 day expontialy weighted moving average model for today
  log_info("starting daily predictions")

  # Used for backtesting
  #mcresults <- mclapply(seq(from = pred_date, to = Sys.Date()-1, by = "day"),
  #                      FUN=function(i) predictor_day(df_total_day, i),
  #                      mc.cores = numCores-1)
  #mcresults <-bind_rows(mcresults)
  
  # function for lapply to loop over to create 12 day EMA
  inner_func_day <- function(x){
    name1 <- df_amt[x,]
    
    #filter for ad set, pad date for full 12 days
    df_1 <- df_total_day %>%
      filter(landingcontentgroup2 == name1$landingcontentgroup2, 
             country == name1$country,
             devicecategory == name1$devicecategory, 
             operatingsystem == name1$operatingsystem) %>%
      filter(date > pred_date - 13, date < pred_date) %>%
      pad(start_val = pred_date - 12, end_val = pred_date - 1, interval = "day") %>%
      arrange(desc(date))
    
    # create weights for EMA
    weights <- rep(.6, 12) ^ seq(1:12)
    
    # add weights to data.frame
    df_exp <- data.frame(df_1, weights) %>%
      mutate(rps_adj = rps * weights) %>%
      filter(!is.na(rps_adj))
    
    #calculate prediction
    dval <- sum(df_exp$rps_adj) / sum(df_exp$weights)
    
    if (dval < 0){
      dval <- 0
    }
    
    return(mutate(name1, dpred = dval))
    
  }
  
  # Call to inner func to make 12 day EMA
  mcresults <- mclapply(1:nrow(df_amt),
           FUN = function(i) inner_func_day(i),
           mc.cores = detectCores(),
           mc.cleanup = TRUE,
           mc.preschedule = FALSE)
  
  # Bind results into dataframe
  mcresults <- bind_rows(mcresults) %>%
    mutate(date = pred_date)
  
  #Add results to df_total_day
  df_total_day <- merge(df_total_day, mcresults, 
                        by = c("date", "landingcontentgroup2", 
                               "country", "devicecategory", "operatingsystem"), 
                        all = TRUE) %>%
    mutate(dpred = case_when(!is.na(dpred.y)~dpred.y,
                             TRUE~dpred.x)) %>%
    select(-dpred.x, -dpred.y)

  
  
  
  
  
  
  
  
  
  
  # Tbats on hourly data 

  
  log_info("starting hourly predictions")
  pred_time <- Sys.time() - 12 * 60 * 60
  
  #start timer for tbats, just for monitoring
  total_timer <- Sys.time()
  
  #lapp_res <- mclapply(seq(from = pred_date, to = Sys.Date()-1, by = "day"),
  #                    FUN=function(i) predictor_hour(df_total, i),
  #                    mc.cores = 1)
  
  #lapp_res <-bind_rows(lapp_res)
  
  #Helper function for lapply
  inner_func_hour <- function(row){
    
    name1 <- df_amt[row,]
    
    #Filter for ad set
    df_1 <- df_total %>%
      filter(landingcontentgroup2 == name1$landingcontentgroup2, 
             country == name1$country,
             devicecategory == name1$devicecategory, 
             operatingsystem == name1$operatingsystem) %>%
      filter(date > pred_date - 31) %>%
      filter(date < pred_date) %>%
      arrange(timestamp) %>%
      select(timestamp, rps, ses) %>%
      rename(metric = 2, weight = 3)
    
    #run tbats
    df_hour_tbats <- fit_tbats_concourse(df_1, current_time = pred_time, outliers = 'tsoutliers', na.impute = 'na.interp') %>%
      mutate(date = date(timestamp)) %>%
      filter(timestamp >= pred_time) %>%
      rename(hpred = 2) %>%
      select(timestamp, hpred, date) %>%
      mutate(landingcontentgroup2 = name1$landingcontentgroup2, 
             country = name1$country,
             devicecategory = name1$devicecategory, 
             operatingsystem = name1$operatingsystem) %>%
      select(date, timestamp, landingcontentgroup2, country, devicecategory, operatingsystem, hpred)
    
    return(df_hour_tbats)
    
  }
  #loop over helper function for all ad sets in df_amt
  mcresults <- mclapply(1:nrow(df_amt),
                        FUN = function(i) inner_func_hour(i),
                        mc.cores = detectCores(),
                        mc.cleanup = TRUE,
                        mc.preschedule = FALSE)
  
  #bind list into df
  res < -bind_rows(mcresults)
  
  log_info(paste("hour tbats total time = ", Sys.time() - total_timer))

  #Add new predictions to df_total
  df_total <- merge(df_total, res, 
                    by = c("date", "timestamp", "landingcontentgroup2", "country", 
                           "devicecategory", "operatingsystem"), 
                    all = TRUE) %>%
    mutate(hpred = case_when(!is.na(hpred.y)~hpred.y,
                             TRUE~hpred.x)) %>%
    select(-hpred.x, -hpred.y)
  
  
  
  
  
  
  
  
  
  
  # Combining day and hourly estimates and pushing to snow flake 
  
  
  
  # Merging day and hourly data, if hour prediction is 
  # negative (which happens if I think the data is too old), then it is replaced 
  # with the day estimate
  df_total <- merge(select(df_total_day, -ses, -rev, -rps), 
                    df_total, 
                    by = c("date", "landingcontentgroup2", "country", 
                           "devicecategory", "operatingsystem"), 
                    all = TRUE) %>%
    mutate(dpred = case_when(!is.na(dpred.y)~dpred.y,
                             TRUE ~ dpred.x)) %>%
    mutate(avg = case_when(!is.na(avg.y) ~ avg.y,
                           TRUE ~ avg.x)) %>%
    mutate(hpred = case_when(hpred <= 0 ~ dpred,
                             TRUE ~ hpred)) %>%
    select(date, timestamp, landingcontentgroup2, country, devicecategory, 
           operatingsystem, ses, rev, rps, hpred, dpred, avg)
  
  names(df_total) <- toupper(names(df_total))
  
  #This is becuase I pull extra data from ga360 in order to replace stale data. 
  #Might want to change at somepoint, but this way I can svae data but if more comes in, then it gets updated
  delete_date <- new_data_start - 5
   
  
  log_info("pushing back to snowflake")
  
  #Data to push back to snowflake
  df_append <- df_total %>%
    filter(DATE > delete_date)
  
  sf_con <- dbr::db_connect('snowflake')
  
  #delete rows that might duplicate
  DBI::dbSendStatement(sf_con, paste("delete from DATA_SCIENCE.CONCOURSE.CONCOURSE_SELL_SIDE_HAZEN
                                     where \"DATE\" > to_date('", delete_date, "')" ))
  
  #Use concourse schema
  DBI::dbSendStatement(sf_con, "use DATA_SCIENCE.CONCOURSE")
  
  #Append values
  dbr::db_append(db = sf_con, table = "CONCOURSE_SELL_SIDE_HAZEN", df = df_append)
  
  log_info("DONE")
  return(df_total)
  
}
























# DATA PULL ------------------------------------------------------------------------------------------
#' The following functions are used for the ga360 data pull and google sheet modifier pull. 
#' The location of modifiers is : https://docs.google.com/spreadsheets/d/1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M/edit#gid=0
#' The current spreadsheet cocnourse is using is : https://docs.google.com/spreadsheets/d/1N87gk5rPEbRK578OM264JsyoEFbbX7VXIVfrXm30tCE/edit#gid=113482794
#' The ga360 view is : https://analytics.google.com/analytics/web/#/report-home/a35484318w97213639p101353932

#' Script to accumulate data from analytics 360 and google sheets
#' @author Connor-Hazen
#' @param days number of days into past to pull
#' @param config_path location of config.yaml file for google api token
#' @param spread_id id of spreadsheet with concourse modifiers.
pull_script <- function(days, config_loc, spread_id, view_id){
  
  end.date <- as.character(Sys.Date() - 1)
  start.date <- as.character(Sys.Date() - days)
  
  dimensions <- "ga:date,ga:hour,ga:operatingSystem,ga:deviceCategory,ga:country,ga:landingContentGroup2"
  metrics <- "ga:sessions,ga:adsenseRevenue,ga:dfpRevenue,ga:backfillRevenue"
  filters <- "ga:sourceMedium=~facebook / cpc;ga:sessions>0;ga:country=~Canada|United States|Australia|New Zealand|United Kingdom;ga:deviceCategory=~mobile|tablet;ga:sourcePropertyDisplayName=~Fame10;ga:landingContentGroup2!~(not set)"
  
  
  #Spredsheet of councourse modifiers, could change
  speadsheetID <- spread_id
  
  format <- 'csv'
  
  
  options(googleAuth.scopes.selected = c("https://www.googleapis.com/auth/analytics.readonly", 
                                         "https://www.googleapis.com/auth/spreadsheets.readonly", 
                                         "https://docs.google.com/feeds"))
  
  service_token <- gar_auth_service(config = config_loc)
  
  
  
  ga.df <- analyticsDB_pull(start.date, end.date, dimensions, metrics, filters, service_token, view_id)
  
  
  
  raw_sheet_data <- content(GET(paste("https://sheets.googleapis.com/v4/spreadsheets/", speadsheetID, "?access_token=", service_token$credentials$access_token, sep = "" )))$sheets
  
  sheet_id_df <- dplyr::bind_rows(lapply(1:length(raw_sheet_data), FUN = function(i) data.frame(raw_sheet_data[[i]]$properties, stringsAsFactors = FALSE  )))
  
  
  
  
  sheet_df <- lapply(1:nrow(sheet_id_df), FUN = function(i) readr::read_csv(paste0('https://docs.google.com/spreadsheets/export?id=', 
                                                                                   speadsheetID, '&format=', format, 
                                                                                   '&gid=', sheet_id_df[[1]][i])))
  
  
  
  fin_sheet_df <- sheet_df %>%
    purrr::reduce(full_join, by = c("Date", "Month", "Day")) %>%
    mutate(date = mdy(Date)) %>%
    select(-Date, -Month, -Day) %>%
    mutate(AdSense = as.numeric(sub("%", "", AdSense)) / 100) %>%
    mutate(Prebid = as.numeric(sub("%", "", Prebid)) / 100) %>%
    mutate(AdX = as.numeric(sub("%", "", AdX)) / 100) %>%
    mutate(EBDA = as.numeric(sub("%", "", EBDA)) / 100) %>%
    fill(AdSense) %>%
    fill(AdSense, .direction = "up") %>%
    fill(Prebid) %>%
    fill(Prebid, .direction = "up") %>%
    fill(AdX) %>%
    fill(AdX, .direction = "up") %>%
    fill(EBDA) %>%
    fill(EBDA, .direction = "up")
  
  
  
  
  df_1 <- ga.df %>%
    mutate(date = ymd(date))
  
  df_1 <- merge(df_1, fin_sheet_df, by = "date")
  
  
  df_final_keep <- df_1 %>%
    mutate(rev1 = adsenseRevenue * AdSense) %>%
    mutate(timestamp = ymd_h(paste(date, hour), tz = 'America/Toronto')) %>%
    mutate(rev2 = dfpRevenue * 1.0) %>%
    mutate(rev3 = backfillRevenue * AdX) %>%
    mutate(rev = (rev1 + rev2 + rev3) * EBDA) %>%
    rename(ses = sessions) %>%
    select(date, timestamp, landingContentGroup2, operatingSystem, deviceCategory, country, ses, rev)
  
  return(df_final_keep)
  
}



#' Create OAuth2 token for google API usage
#' @author Connor-Hazen
#' @param config location of config.yaml file - needs to change when I place my creditials in the global config.yml file
#' @param scope scopes of api acsess. 
#' This function come from googleAuthR, botor and dbr. It is a combination of various elements. 

gar_auth_service <- function(config, scope = getOption("googleAuth.scopes.selected")){
  
  id <- "ga360"
  
  withclass <- function(class) {
    force(class)
    function(x) structure(x, class = class)
  }
  secrets <- yaml::yaml.load_file(
    config,
    ## keep classes
    handlers = list('aws_kms'       = withclass('aws_kms'),
                    'aws_kms_file'  = withclass('aws_kms_file'),
                    'aws_parameter' = withclass('aws_parameter')))
  
  
  ## secrets was loaded in zzz.R at pkg load time
  hasName(secrets, id) || stop('Secret ', id, ' not found, check the config.yml')
  
  
  secrets <- lapply(secrets[[id]], function(secret) {
    switch(
      class(secret),
      'aws_kms' = botor::kms_decrypt(secret),
      ## default
      secret)
    })
  
  
  endpoint <- httr::oauth_endpoints("google")
  
  scope <- paste(scope, collapse = " ")
  
  if (is.null(secrets$private_key)){
    stop("$private_key not found in JSON - have you downloaded the correct JSON file? 
         (Service Account Keys, not service account client)")
  }
  
  
  while (!exists("google_token")) {
    try(google_token <- httr::oauth_service_token(endpoint, secrets, scope))
  }
  
  
  
  return(google_token)
  
}



#' Data query to Analytics 360
#' @author Connor-Hazen
#' @param start.date_inp query start date as string
#' @param end.date_inp query end date as string
#' @param dimensions_inp query dimensions as string
#' @param metrics_inp query metrics as string
#' @param filters_inp query filter as string
#' @param service_token OAuth2 token create by calling gar_auth_service
#' @param view_id view id from analytics360 account


analyticsDB_pull <- function(start.date, end.date, dimensions, metrics, filters, service_token, view_id){
  
  query.list <- list("start.date"  = start.date,
                     "end.date"    = end.date,
                     "dimensions"  = dimensions,
                     "metrics"     = metrics,
                     "segment"     = NULL,
                     "sort"        = NULL,
                     "filters"     = filters,
                     "max.results" = "10000",
                     "start.index" = "1",
                     "table.id"    = paste("ga:", view_id, sep = ""),
                     "access_token" = service_token$credentials$access_token)
  
  # Hit One Query
  message("Getting data starting at row ", query.list$start.index)
  query.uri <- toURI(query.list)
  ga.list <- Get_Data_Feed(query.uri)
  if (is.null(ga.list)){
    return(NULL)
  }
  # Convert ga.list into a dataframe
  
  ga.list.df <- data.frame()
  ga.list.df <- rbind(ga.list.df, do.call(rbind, as.list(ga.list$rows)))
  
  col.headers <- ga.list$columnHeaders
  
  # Check if pagination is required
  
  if (length(ga.list$rows) < ga.list$totalResults) {
    number.of.pages <- ceiling(ga.list$totalResults / length(ga.list$rows))
    
    # Clamp Number of Pages to 100 in order to enforce upper limit for pagination as 1M rows
    if (number.of.pages > 100) {
      number.of.pages <- 100
      message("too big of query")
    }
    
    # Call Pagination Function
    
    pagenation_loop <- function(i) {
      
      dataframe.param <- data.frame()
      start.index <- (i * as.numeric(query.list$max.results)) + 1
      message("Getting data starting at row ", start.index)
      query.list$start.index <- as.character(start.index)
      query.uri <- toURI(query.list)
      ga.list <- Get_Data_Feed(query.uri)
      dataframe.param <- rbind(dataframe.param,
                               do.call(rbind, as.list(ga.list$rows)))
      col.headers <- ga.list$columnHeaders
      return(dataframe.param)
      
    }
    
    page.df <- mclapply(1:(number.of.pages - 1), 
                        function(i) pagenation_loop(i),
                        mc.cores = 1,
                        mc.cleanup = TRUE,
                        mc.preschedule = FALSE)

    # Collate Results and convert to Dataframe
    inter.df <- bind_rows(ga.list.df, page.df)
    final.df <- set_col_names(col.headers, inter.df)

    message("The API returned ", nrow(final.df), " results.")
    return(final.df)
  } else {
    return(set_col_names(col.headers, ga.list.df))
  } 
  
}

#' Function to create uri for api queries
#' @param query.list list created in analyticsDB_pull
#' @return uri for query

toURI <- function(query.list){
  
  query <- query.list
  
  uri <- "https://www.googleapis.com/analytics/v3/data/ga?"
  for (name in names(query)) {
    uri.name <- switch(name,
                       start.date  = "start-date",
                       end.date    = "end-date",
                       dimensions  = "dimensions",
                       metrics     = "metrics",
                       segment     = "segment",
                       sort        = "sort",
                       filters     = "filters",
                       max.results = "max-results",
                       start.index = "start-index",
                       table.id    = "ids",
                       access_token = "access_token")
    if (!is.null(query[[name]])) {
      uri <- paste(uri,
                   URLencode(uri.name, reserved = TRUE),
                   "=",
                   URLencode(query[[name]], reserved = TRUE),
                   "&",
                   sep = "",
                   collapse = "")
    }
  }
  # remove the last '&' that joins the query parameters together.
  uri <- sub("&$", "", uri)
  # remove any spaces that got added in from bad input.
  uri <- gsub("\\s", "", uri)
  return(uri)
}



#' Function to send queries and record errors.
#' @param query.uri uri returned from toURI
#' @return list of data or error if bad api request


Get_Data_Feed <- function(query.uri){
  
  ga.Data <- GET(query.uri)  
  
  api.response.list <- content(ga.Data, as = "parsed")  
  check.param <- regexpr("error", api.response.list)
  if (check.param[1] != -1) {
    ga.list <- list(code = api.response.list$error$code,
                    message = api.response.list$error$message)
  } else {
    code <- NULL
    ga.list <- api.response.list
  }   
  
  
  if (!is.null( ga.list$code)) {
    stop(paste("code :",
               ga.list$code,
               "Reason :",
               ga.list$message))
  }
  if (is.null(ga.list$rows)) {
    warning("Your query matched 0 results. Please verify your query using the Query Feed Explorer and re-run it.")
    return(NULL)
    # break
  } else {
    return (ga.list)
  }
}

#' Sets data frame column names and data types. 
#' @param ga.list.param.columnHeaders list of column headers and types. 
#' @param dataframe.param data frame to change types
set_col_names <- function(ga.list.param.columnHeaders, dataframe.param){
  
  
  column.param <- t(sapply(ga.list.param.columnHeaders, 
                           '[',
                           1 : max(sapply(ga.list.param.columnHeaders,
                                          length))))
  col.name <- gsub('ga:', '', as.character(column.param[, 1]))
  col.datatype <- as.character(column.param[, 3])
  colnames(dataframe.param) <- col.name
  
  dataframe.param <- as.data.frame(dataframe.param)
  
  
  
  for (i in 1:length(col.datatype)) {
    if (col.datatype[i] == "STRING") {
      dataframe.param[, i] <- as.character(dataframe.param[, i]) 
    } else {
      dataframe.param[, i] <- as.numeric(as.character(dataframe.param[, i])) 
    }
  }
  return(dataframe.param)

}








# TBATS Predictions ------------------------------------------------------------------------------------




#' function that runs tbats on campaigns, adapted from system1 fit tbats package
#' the reason I am not using the fit_tbats package is, 
#' 1: I dont want to push to s3, 
#' 2: the camapigns are very short and sparse so the function needed to be more lenient. 
#' 
#' @param df with timestamp, metric, weights
#' @param current_date date representing "current date" can be used to backtest if set to previous date
#' @param forecast length of forecast window
#' @return data frame with timestamp and prediction
#' It currently predicted from the last observed data point to 24 hours after runtime. 
#' This means that when automated, all periods with have a prediction and we can account for the data delays. 

fit_tbats_concourse <- function(df, label, id, extra = list(),
                                minweight = 0, maxsparsity = 0.25,
                                aggfun = function(metric, weight) weighted.mean(metric, weight, na.rm = TRUE),
                                aggintervals = c(1, 2, 3, 4),
                                interval = '1 hour', frequency = c(24, 24 * 7),
                                forecast = NULL,
                                outliers = c('tsoutliers', NA),
                                na.impute = c('na.interp', 'na.kalman'),
                                current_time = Sys.time()) {
  
  
  
  
  
  outliers <- match.arg(outliers)
  na.impute <- match.arg(na.impute)
  
  assert_data_frame(df, min.rows = 1)
  assert_colname(df, c('timestamp', 'metric', 'weight'))
  
  
  ## parse interval
  interval_value <- as.numeric(strsplit(interval, ' ')[[1]][1])
  interval_unit  <- strsplit(interval, ' ')[[1]][2]
  
  ## make sure it's a data.table object
  df <- data.table(df)
  setorder(df, timestamp)
  
  
  ## pre-aggregate metrics
  agginterval <- head(aggintervals, 1)
  df[, aggtimestamp := floor_date(timestamp, unit = paste(agginterval, interval_unit))] # nolint
  df[, metric := aggfun(metric, weight), by = aggtimestamp] # nolint
  df[, weight := sum(weight, na.rm = TRUE), by = aggtimestamp] # nolint
  df[, aggtimestamp := NULL] # nolint
  
  ## get rid of meaningless rows (so that the next checks do not fail on NAs)
  df <- df[!is.na(metric) & is.finite(metric)] # nolint
  
  ## the most recent ZERO values are most likely to be actually missing data
  if (nrow(df) > 0 && tail(df$metric, 1) == 0) {
    ## so drop all trailing ZERO metrics
    df <- head(df, -1 * (min(which(cumsum(rev(df$metric)) != 0)) - 1))
  }
  
  
  placeholder <- -1
  
  forecast <- as.numeric(difftime(floor_date(current_time + 24 * 60 * 60, unit = "hour"), max(df$timestamp, na.rm = TRUE), units = c("hours")))
  #system(paste("echo '\n length",forecast,"'"))
  #system(paste("echo '\n max",max(df$timestamp, na.rm = TRUE),"'"))
  
  
  #this statment checks for recent enugh data. 
  if (forecast > 60){
    predf <- data.frame(timestamp = seq(max(df$timestamp, na.rm = TRUE) + 60 * 60,
                                        by = interval,
                                        length.out = forecast),
                        rep(placeholder, forecast))
    return(predf)
    
  }
  
 
  ## remove values with low weights
  df <- df[weight > minweight] # nolint
  
  ## remove further outliers
  if (nrow(df) > 0 && !is.na(outliers) && outliers == 'tsoutliers') {
    tryCatch(
      df[tsoutliers(ts(df$metric, frequency = frequency[1]))$index,
         c('metric', 'weight') := NA],
      error = function(e) {
        # print(
        #   'Outlier removal failed on model id {id} due to {shQuote(e$message)} ',
        #   'using {frequency[1]} frequency on the following vector: ',
        #   paste(capture.output(dput(df$metric)), collapse = ' '))
      })
  }
  ## outliers <- AnomalyDetectionTs(
  ##     x = data.frame(df[, .(timestamp, metric)]),
  ##     plot = FALSE,
  ##     max_anoms = 0.05, direction = 'pos')
  ## if (nrow(outliers$anoms) > 0) {
  ##     TODO
  ## }
  
  ## find gaps in the time series
  if (nrow(na.omit(df)) > 0) {
    df  <- data.table(suppressMessages(pad(df, interval = interval, by = 'timestamp')))
  }
  ndf <- nrow(df) # nolint
  
  ## fall back to most recent records if full data is too sparse
  recent_sparsity <- original_sparsity <- sparsity <- df[, round(mean(is.na(metric)), 4)] # nolint
  recent_df <- df
  while (recent_sparsity > maxsparsity &&
         ## allow at least 4 periods to figure out at least some seasonality
         nrow(recent_df) > frequency[1] * 4) {
    
    ## drop 1 frequency of data
    recent_df <- tail(recent_df, -frequency[1])
    
    ## drop leading NAs
    if (!all(is.na(recent_df$metric)) && is.na(recent_df$metric[1])) {
      recent_df <- tail(recent_df, -1 * (which(cumsum(!is.na(recent_df$metric)) != 0)[1] - 1))
    }
    
    recent_sparsity <- recent_df[, round(mean(is.na(metric)), 4)] # nolint
    if (recent_sparsity < sparsity) {
      
      df       <- recent_df
      sparsity <- recent_sparsity
    }
    
  }
  
  
  if (is.finite(sparsity) && sparsity > maxsparsity) {
    
    ## fall back to further interval aggregates if data is still too sparse
    if (length(aggintervals) > 1) {
      
      mc <- match.call()
      mc$aggintervals <- tail(aggintervals, -1)
      return(eval(mc, envir = parent.frame()))
      
    } else {
      
      ## time to give up -- by dropping all records
      
      # print(paste(
      #   ' after dropping all records from the original {ndf} ',
      #   'with {original_sparsity * 100}% original sparsity ',
      #   'that we could not fix with aggregation and other tweaks'))
      
      
      predf <- data.frame(timestamp = seq(max(df$timestamp, na.rm = TRUE) + 60 * 60,
                                          by = interval,
                                          length.out = forecast),
                          rep(placeholder, forecast))
      return(predf)
      
    }
    
  }
  
  ## number of missing values
  dfna  <- df[is.na(metric), .N] # nolint
  
  ## impute missing intervals
  tsobj <- xts(df$metric, order.by = df$timestamp)
  if (dfna > 0) {
    
    if (na.impute == 'na.kalman') {
      tsobj <- tryCatch(
        ## Kalman smoothing & structural time series models
        na.kalman(tsobj), error = function(e)
          ## use last observation if Kalman fails
          tryCatch(na.locf(tsobj), error = function(e) tsobj))
      df <- merge(setnames(as.data.table(tsobj), c('timestamp', 'metric')),
                  df[, .(timestamp, weight)], by = 'timestamp', all.x = TRUE)
    }
    
    if (na.impute == 'na.interp') {
      df[, metric := tryCatch(
        as.numeric(na.interp(ts(metric, frequency = frequency[1]))),
        error = function(e)
          as.numeric(na.locf(ts(metric, frequency = frequency[1]))))]
      tsobj <- xts(df$metric, order.by = df$timestamp)
    }
    
  }
  
  ## build model using all frequencies
  model <- tryCatch(qc_tbats(tbats(msts(tsobj, seasonal.periods = frequency))),
                    error = function(e) e)
  
  ## fall back to less seasonal effects if no seasonality found
  if (hasName(model, 'seasonal.periods') && length(model$seasonal.periods) < 1 &&
      length(frequency) > 1) {
    frequency <- head(frequency, -1)
    model <- tryCatch(qc_tbats(tbats(msts(tsobj, seasonal.periods = frequency))), error = function(e) e)
  }
  
  ## fall back to further interval aggregates if
  if (( # nolint
    ## no seasonality found
    (hasName(model, 'seasonal.periods') && length(model$seasonal.periods) < 1) |
    ## low number of observations to capture a real seasonal effect
    (nrow(na.omit(tsobj)) < frequency[1] * 4)) &&
    ## there are further aggintervals available
    length(aggintervals) > 1) {
    mc <- match.call()
    mc$aggintervals <- tail(aggintervals, -1)
    return(eval(mc, envir = parent.frame()))
  }
  
  if(hasName(model, 'seasonal.periods') && length(model$seasonal.periods) < 1){
    
    predf <- data.frame(timestamp = seq(max(df$timestamp, na.rm = TRUE) + 60 * 60,
                                        by = interval,
                                        length.out = forecast),
                        rep(placeholder, forecast))
    return(predf)
  }
  
  ## make sure we fail the model on low number of (unique) observations
  if (nrow(na.omit(tsobj)) < frequency[1] | length(unique(na.omit(tsobj))) < 3 ) {
    model <- structure(
      list(message = "failing on low number of observations"),
      class = 'error')
  }
  
  
  if (!inherits(model, 'error')) {
    
    ## bump number of periods to forecast based on the current hour
    forecast_adj <- forecast
  
    
    
    preds <- forecast(model, h = forecast_adj)
    print(preds)
    predf <- as.data.table(preds)
    predp <- predf[['Point Forecast']]
    pred  <- round(head(predp, 1), 2) # nolint
    
    
    ## add timestamps to the predictions for JSON array
    predf <- data.frame(timestamp = seq(max(df$timestamp, na.rm = TRUE) + 60 * 60,
                                             by = interval,
                                             length.out = forecast),
                        predf)
    
    
    
    
    
    
  }else{
    
    predf <- data.frame(timestamp = seq(max(df$timestamp, na.rm = TRUE) + 60 * 60,
                                        by = interval,
                                        length.out = forecast),
                        rep(placeholder, forecast))
    
    
    
  }
  
  return(predf)
  ## return
  
}


log_layout(layout_glue_colors)
log_threshold(TRACE)

# Time zone for consitent predictions and timings 
Sys.setenv(TZ = 'America/Toronto')

# spreadsheet id for concourse modifiers
spread_id <- "1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M"

#location of config.yml file
config_loc <- "./model/concourse/config.yml"

#View id for googleAnalytics
view_id <- "101353932"

f10_roas_predic(config_loc, spread_id, view_id)


