
pull <- function(start.date_inp, end.date_inp, dimensions_inp, metrics_inp, filters_inp){
  library(RGoogleAnalytics)
  
  library(googleAuthR)
  options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/analytics.readonly", "https://spreadsheets.google.com/feeds", "https://docs.google.com/feeds"))
  
  trys<-0
  while(!exists("service_token")&trys <4){
    trys<-trys+1
    try(service_token <- gar_auth_service(json_file="~/Desktop/Service_client.json"), silent = TRUE   )
  }
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
    try(df_pull_ret <-GetReportData(ga.query, service_token, paginate_query = TRUE), silent = TRUE)
  }
  if(trys >0){
    print("got new data")
  }
  return(df_pull_ret)
}




