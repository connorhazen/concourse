query.list <- list("start.date"  = as.character(Sys.Date()-2),
  "end.date"    = as.character(Sys.Date()-1),
  "dimensions"  = "ga:date,ga:hour,ga:operatingSystem,ga:deviceCategory,ga:country,ga:landingContentGroup2",
  "metrics"     = "ga:sessions,ga:adsenseRevenue,ga:dfpRevenue,ga:backfillRevenue",
  "segment"     = NULL,
  "sort"        = NULL,
  "filters"     = "ga:sourceMedium=~facebook / cpc;ga:sessions>0;ga:country=~Canada|United States|Australia|New Zealand|United Kingdom;ga:deviceCategory=~mobile|tablet;ga:sourcePropertyDisplayName=~Fame10;ga:landingContentGroup2!~(not set)",
  "max.results" = "10000",
  "start.index" = "1",
  "table.id"    = paste("ga:","101353932", sep = ""),
  "access_token" = service_token$credentials$access_token)

api_query(query.list)

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


api_query <- function(query.list){
  # Hit One Query
  query.uri <- toURI(query.list)
  ga.list <- Get_Data_Feed(query.uri)
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
    
    page.df <- mclapply(1:number.of.pages-1, 
                        function(i) pagenation_loop(i),
                        mc.cores = detectCores(),
                        mc.cleanup = TRUE,
                        mc.preschedule = FALSE))
    
    # Collate Results and convert to Dataframe
    inter.df <- bind_rows(ga.list.df, page.df)
    final.df <- set_col_names(col.headers, inter.df)
    
    message("The API returned ", nrow(final.df), " results.")
    return(final.df)
  } else {
    return(set_col_names(col.headers, ga.list.df))
  } 
}



Get_Data_Feed <- function(query.uri){
  
  ga.Data <- GET(query.uri)  
 
  api.response.list <- content(ga.Data,as="parsed")  
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


set_col_names <- function(ga.list.param.columnHeaders, dataframe.param){
  
  
  column.param <- t(sapply(ga.list.param.columnHeaders, 
                           '[',
                           1 : max(sapply(ga.list.param.columnHeaders,
                                          length))))
  col.name <- gsub('ga:', '', as.character(column.param[, 1]))
  col.datatype <- as.character(column.param[, 3])
  colnames(dataframe.param) <- col.name
  
  dataframe.param <- as.data.frame(dataframe.param)
  dataframe.param <- set_col_type(col.datatype, col.name, dataframe.param)
  
  return(dataframe.param)
}

set_col_type <-function(col.datatype, col.name, dataframe.param){
  for(i in 1:length(col.datatype)) {
    if (col.datatype[i] == "STRING") {
      dataframe.param[, i] <- as.character(dataframe.param[, i]) 
    } else {
      dataframe.param[, i] <- as.numeric(as.character(dataframe.param[, i])) 
    }
  }
  return(dataframe.param)
}

