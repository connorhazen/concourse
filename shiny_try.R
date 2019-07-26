library(shiny)
library(shinydashboard)

my_db <-  tryCatch(dbr::db_query(paste('select * 
                                         from "DATA_SCIENCE"."CONCOURSE"."CONCOURSE_SELL_SIDE_HAZEN" 
                                         where "DATE" >to_date(\'', Sys.Date()-30,'\')', 
                                       sep = ""), db = 'snowflake'), error = function(e)e)

names(my_db) <- tolower(names(my_db))
# Pull old table
df_sf<- tbl(my_db, "CONCOURSE_SELL_SIDE_HAZEN")%>%
  data.frame()%>%
  mutate(date = as.Date(date), timestamp=as.POSIXct(timestamp))%>%
  arrange(desc(date))%>%
  mutate(resid_f10 = avg - rps)%>%
  mutate(resid_me = hpred - rps)%>%
  filter(date > Sys.Date()-20)

df_round <- df_sf%>%
  mutate(rps = round(rps,2), avg = round(avg,2), hpred = round(hpred,2))%>%
  mutate(resid_f10 = rps - avg)%>%
  mutate(resid_me = rps - hpred)

df_amt2 <- df_round%>%
  group_by(landingContentGroup2, country, deviceCategory, operatingSystem)%>%
  summarise(ses = sum(ses, na.rm = TRUE), count = n(), rev = sum(rev, na.rm = TRUE))%>%
  arrange(desc(ses),desc(count))


mase1 <- function(df_tester){
  
  
  df_mase <- df_tester%>%
    filter(!is.na(resid_f10)&!is.na(resid_me))
  
  return(sum(abs(df_mase$resid_me))/sum(abs(df_mase$resid_f10)))
}
mase2 <- function(df_tester){
  
  
  df_mase <- df_tester%>%
    filter(!is.na(resid_f10)&!is.na(resid_me))%>%
    mutate(resid_f10 = resid_f10*ses, resid_me = resid_me*ses)
  
  num <-  sum(abs(df_mase$resid_me))/sum(df_mase$ses)
  den <- sum(abs(df_mase$resid_f10))/sum(df_mase$ses)
  return(num/den)
}

# num<-1
# 
# name1 <- as.character(df_amt2[num,])
# 
# df_tester<-df_round%>%
#   filter(as.character(landingContentGroup2) == name1[1] & as.character(country) == name1[2] &
#            as.character(deviceCategory) == name1[3], as.character(operatingSystem) == name1[4])%>%
#   arrange(timestamp)


  
  
  

ui <- dashboardPage(
  dashboardHeader(),
  
  ## Sidebar content
  dashboardSidebar(
    sidebarMenu(
      menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
      menuItem("DATA", tabName = "rawData", icon = icon("th"))
    )
  ),
  
  ## Body content
  dashboardBody(
    tabItems(
      # First tab content
      tabItem(tabName = "dashboard",
              fluidRow(
                box(plotOutput("plot1", height = 450, width = "100%"), width = 10  ),
                
                box(
                  title = "Controls",
                  
                  numericInput("campNumber", "Campaign number:", 1, 1, nrow(df_amt2), 1)
                  
                )
              )
      ),
      
      # Second tab content
      tabItem(tabName = "rawData",
              h2("Raw Data")
      )
    )
  )
)


server <- function(input, output) {
  output$plot1 <- renderPlot({
    name1 <- as.character(df_amt2[input$campNumber,])
    
    df_tester<-df_round%>%
      filter(as.character(landingContentGroup2) == name1[1] & as.character(country) == name1[2] &
               as.character(deviceCategory) == name1[3], as.character(operatingSystem) == name1[4])%>%
      arrange(timestamp)

    
    ggplot(df_tester, aes(x = timestamp))+
      geom_line(aes(y = rps, color = "rps"), size =  1.1)+
      geom_point(aes(y = rps, color = "rps"), size = .6)+
      geom_line(aes(y = avg, color = "3day"))+
      geom_line(aes(y = hpred, color = "pred"))+
      scale_x_datetime(minor_breaks  = "1 day")+
      scale_colour_manual("", 
                          breaks = c("rps", "3day", "pred"),
                          values = c("blue", "red", "black")) +
      ggtitle(paste("campaign: ", substring(name1[1], first = 1,last = 50), 
                    "\n",name1[2],name1[3],name1[4], "\n", "mase comparison: ", round(mase2(df_tester),4), sep = " " ))
  })
}

shinyApp(ui, server)
