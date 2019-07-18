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
                          mc.cores = numCores,
                          mc.cleanup = TRUE,
                          mc.preschedule = FALSE)
  

  mcresults <-bind_rows(mcresults)
  
  
 
  return(mcresults)
  
}

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
