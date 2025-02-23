


#' Script to accumulate data from analytics 360 and google sheets
#' @author Connor-Hazen
#' @param days number of days into past to pull
pull_script <- function(days){
  
  end.date <- as.character(Sys.Date())
  start.date <- as.character(Sys.Date()-days)
  
  dimensions = "ga:date,ga:hour,ga:operatingSystem,ga:deviceCategory,ga:country,ga:landingContentGroup2"
  metrics = "ga:sessions,ga:adsenseRevenue,ga:dfpRevenue,ga:backfillRevenue"
  filters = "ga:sourceMedium=~facebook / cpc;ga:sessions>0;ga:country=~Canada|United States|Australia|New Zealand|United Kingdom;ga:deviceCategory=~mobile|tablet;ga:sourcePropertyDisplayName=~Fame10;ga:landingContentGroup2!~(not set)"
  
  
  #Spredsheet of councourse modifiers, could change
  speadsheetID = "1jkPloX3qsaWhU9uq7AkUJiGifuMvVIdxot-W5ySJj1M"
  
  format = 'csv'
  
  
  options(googleAuth.scopes.selected = c("https://www.googleapis.com/auth/analytics.readonly", 
                                         "https://www.googleapis.com/auth/spreadsheets.readonly", 
                                         "https://docs.google.com/feeds"))
  
  service_token <- gar_auth_service(config="config.yml")
  
  
  
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










#' Create OAuth2 token for google API usage
#' @author Connor-Hazen
#' @param config location of config.yaml file - needs to change when I place my creditials in the global config.yml file
#' @param scope scopes of api acsess. 
#' This function come from googleAuthR, botor and dbr. It is a combination of various elements. 

gar_auth_service <- function(config, scope = getOption("googleAuth.scopes.selected")){
  
  id = "ga360"
  
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
      secret)})
  
  
  endpoint <- httr::oauth_endpoints("google")
  
  scope <- paste(scope, collapse=" ")
  
  if(is.null(secrets$private_key)){
    stop("$private_key not found in JSON - have you downloaded the correct JSON file? 
         (Service Account Keys, not service account client)")
  }
  
  
  trys<-0
  while(!exists("google_token") & trys <4){
    
    trys<-trys+1
    try(google_token <- httr::oauth_service_token(endpoint, secrets, scope), silent = FALSE)
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

#' Function to run 3day moving average predictions
#' @param df data frame must contain date, landingContentGroup2, country, deviceCategory, operatingSystem, ses, rev, rps, avg
#' @param current_date date representing "current date" can be used to backtest by setting date in the past
#' @return data frame containg new avg predictions
#' 
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

#' Function to run predictions on daily level
#' @param df data frame must contain date, landingContentGroup2, country, deviceCategory, operatingSystem, ses, rev, rps, dpred, avg
#' @param current_date date representing "current date" can be used to backtest by setting date in the past
#' @return data frame containg new dpred predictions

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

#' Function to run predictions on hourly level
#' @param df data frame must contain date, timestamp, landingContentGroup2, country, deviceCategory, operatingSystem, ses, rev, rps hpred, dpred, avg
#' @param current_date date representing "current date" can be used to backtest by setting date in the past
#' @return data frame containg new hpred predictions
predictor_hour <- function(df, current_date) {
  numCores <- detectCores()-1 # get the number of cores available
  
  #system(paste("echo '\n",current_date,"'"))
  
  
  
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
    
    print(nrow(df_hour_tbats))
    
    
    if(nrow(df_hour_tbats)!=24){
      
      df_1<-df%>%
        filter(landingContentGroup2 == name1$landingContentGroup2 , country == name1$country ,
               deviceCategory == name1$deviceCategory, operatingSystem == name1$operatingSystem)%>%
        filter(date > current_date-30)%>%
        filter(date <= current_date)%>%
        arrange(timestamp)%>%
        select(timestamp, rps, ses)%>%
        rename(metric = 2, weight = 3)
      
      
      
      
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
                          mc.cores = numCores,
                          mc.cleanup = TRUE,
                          mc.preschedule = FALSE)
  
  
  mcresults <-bind_rows(mcresults)
  
  
  
  return(mcresults)
  
}


#' fucntion that runs tbats on campaigns, adapted from system1 fit tbats package
#' @param df with timestamp, metric, weights
#' @param current_date date representing "current date" can be used to backtest if set to previous date
#' @param forecast length of forecast window

fit_tbats_concourse <- function(df, label, id, extra = list(),
                                minweight = 0, maxsparsity = 0.25,
                                aggfun = function(metric, weight) weighted.mean(metric, weight, na.rm = TRUE),
                                aggintervals = c(1, 2, 3, 4),
                                interval = '1 hour', frequency = c(24, 24 * 7),
                                forecast = NULL,
                                outliers = c('tsoutliers', NA),
                                na.impute = c('na.interp', 'na.kalman'),
                                current_date = NULL) {
  
  
  
  
  
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
  
  if(as.numeric(difftime(as.POSIXct(current_date+1),df[, max(timestamp)], units = c("hours")))>=12){
    #print(paste("not recent data: ", df[, max(timestamp)]))
    
    
    
    predf <- data.frame(timestamp = seq(from = as.POSIXct(current_date+1),
                                        by = interval,
                                        length.out = forecast),
                        rep(placeholder, forecast))
    return(predf)
    
  }
  
  
  if(difftime( max(df$timestamp, na.rm = TRUE) , min(df$timestamp, na.rm = TRUE), units = c("hours")) < frequency[1] *4){
    predf <- data.frame(timestamp = seq(from = as.POSIXct(current_date+1),
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
      
      
      predf <- data.frame(timestamp = seq(from = as.POSIXct(current_date+1),
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
    predf <- as.data.table(preds)
    predp <- predf[['Point Forecast']]
    pred  <- round(head(predp, 1), 2) # nolint
    
    ## add timestamps to the predictions for JSON array
    
    predf <- data.frame(timestamp = tail(seq(from = df[, max(timestamp)],
                                             by = interval,
                                             length.out = length(predp) + 1), -1),
                        predf)
    
    
    
    
    
    
  }else{
    
    predf <- data.frame(timestamp = seq(from = as.POSIXct(current_date+1),
                                        by = interval,
                                        length.out = forecast),
                        rep(placeholder, forecast))
    
    
    
  }
  
  return(predf)
  ## return
  
}

