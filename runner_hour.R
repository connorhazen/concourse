source("~/rstudio-workspace/f10/concourse_hour_backtest_runner.R")
source("~/rstudio-workspace/packages/mase.R")

df <- f10_hour()

df<- read.csv("~/concourse_hour.csv")%>%
  mutate(timestamp = as.POSIXct(timestamp))%>%
  mutate(landingContentGroup2 = as.character(landingContentGroup2))%>%
  mutate(country = as.character(country))%>%
  mutate(deviceCategory = as.character(deviceCategory))%>%
  mutate(operatingSystem = as.character(operatingSystem))%>%
  mutate(date = as.Date(date))%>%
  select(-X)%>%
  rename(hour_pred = pred)





my_db <- src_snowflakedb(user = "CONNOR_HAZEN",password = "FFg-nvU-83U-nYh",account = "system1",
                         opts = list(warehouse = "S1_DS", db = "DATA_SCIENCE",schema = "CONCOURSE"))


df_sf<- tbl(my_db, "CONCOURSE_SELL_SIDE_DAY")

df_sf <-data.frame(df_sf)%>%
  mutate(date = as.Date(date))%>%
  arrange(desc(date))




df_total <- merge(df, df_sf, by = c("date","landingContentGroup2", "country", "deviceCategory", "operatingSystem"), all = TRUE)%>%
  mutate(avg = avg.y)%>%
  select(-avg.x, -avg.y)


df <- df_total%>%
  mutate(hour_pred = case_when(hour_pred<0~day_pred,
                               hour_pred==0~as.numeric(NA),
                               TRUE~hour_pred))

df_day<- df_total%>%
  group_by(date,landingContentGroup2, country,deviceCategory,operatingSystem)%>%
  summarise(hour_pred_min = min(hour_pred, na.rm = TRUE), hour_pred_max = max(hour_pred, na.rm = TRUE))%>%
  mutate(new_pred_indic = case_when(hour_pred_min==hour_pred_max~1,
                                    TRUE~0))%>%
  select(-hour_pred_min, -hour_pred_max)

df_fin <-  merge(df, df_day, by = c("date","landingContentGroup2", "country", "deviceCategory", "operatingSystem"), all = TRUE)%>%
  mutate(hour_pred = case_when(new_pred_indic == 1~day_pred,
                               TRUE~hour_pred))




write.csv(df_fin, "~/concourse.csv")

#Random
library(lubridate)

library(ggplot2)
df_graph <- df_fin%>%
  mutate(resid_f10 = avg - rps)%>%
  mutate(resid_me = hour_pred - rps)%>%
  mutate(off_calc_f10 = resid_f10 *ses)%>%
  mutate(off_calc_me = resid_me *ses)%>%
  filter(date>Sys.Date()-20, date < Sys.Date()-7)

df_graph_day <- df_fin%>%
  group_by(date,landingContentGroup2, country,deviceCategory,operatingSystem)%>%
  summarise(ses = sum(ses, na.rm = TRUE),rev = sum(rev, na.rm = TRUE),avg = mean(avg, na.rm = TRUE), day_pred = mean(day_pred, na.rm = TRUE))%>%
  mutate(rps = case_when(rev>0&ses>0~rev/ses,
                         TRUE~as.numeric(NA)))%>%
  filter(rev>100)%>%
  mutate(resid_f10 = avg - rps)%>%
  mutate(resid_me = day_pred - rps)%>%
  filter(date>Sys.Date()-30)




sum(df_graph_day$ses, na.rm = TRUE)
sum(df_graph_day$rev, na.rm = TRUE)


df_round <- df_graph%>%
  mutate(rps = round(rps,2), avg = round(avg,2), hour_pred = round(hour_pred,2))%>%
  mutate(resid_f10 = rps - avg)%>%
  mutate(resid_me = rps - hour_pred)








df_amt2 <- df_round%>%
  group_by(landingContentGroup2, country, deviceCategory, operatingSystem)%>%
  summarise(ses = sum(ses, na.rm = TRUE), count = n(), rev = sum(rev, na.rm = TRUE))%>%
  arrange(desc(ses),desc(count))


num <- 13

name1 <- as.character(df_amt2[num,])


df_tester<-df_round%>%
  filter(as.character(landingContentGroup2) == name1[1] & as.character(country) == name1[2] &
           as.character(deviceCategory) == name1[3], as.character(operatingSystem) == name1[4])%>%
  arrange(timestamp)

ggplot(df_tester, aes(x = timestamp))+
  geom_line(aes(y = rps), size =  1.2, color  = "black")+
  scale_x_datetime(minor_breaks  = "1 day")+
  ggtitle(paste("campaign: ", substring(name1[1], first = 1,last = 50),
                "\n",name1[2],name1[3],name1[4],  sep = " " ))

 
ggplot(df_tester, aes(x = timestamp))+
  geom_line(aes(y = rps, color = "rps"), size =  1.2)+
  geom_line(aes(y = avg, color = "3day"))+
  scale_x_datetime(minor_breaks  = "1 day")+
  scale_colour_manual("",
                      breaks = c("rps", "3day" ),
                      values = c("blue", "black" )) +
  ggtitle(paste("campaign: ", substring(name1[1], first = 1,last = 50),
                "\n",name1[2],name1[3],name1[4], "\n", "MAE : ", round(mean(abs(df_tester$resid_f10), na.rm = TRUE),4), sep = " " ))

ggplot(df_tester, aes(x = timestamp))+
  geom_line(aes(y = rps, color = "rps"), size =  1.1)+
  geom_line(aes(y = avg, color = "3day"))+
  geom_line(aes(y = hour_pred, color = "pred"))+
  scale_x_datetime(minor_breaks  = "1 day")+
  scale_colour_manual("", 
                      breaks = c("rps", "3day", "pred"),
                      values = c("blue", "red", "black")) +
  ggtitle(paste("campaign: ", substring(name1[1], first = 1,last = 50), 
                "\n",name1[2],name1[3],name1[4], "\n", "mase comparison: ", round(mase1(df_tester),4), sep = " " ))
#
# ggplot(df_tester, aes(x = date))+
#   geom_line(aes(y = round(rps,2)), size =  1.2)+
#   geom_line(aes(y = round(avg,2)), color = "blue")+
#   geom_line(aes(y = round(pred,2)), color = "red")+
#   scale_x_date(minor_breaks  = "1 day")


df_round <- df_graph%>%
  mutate(rps = round(rps,2), avg = round(avg,2), pred = round(pred,2))%>%
  mutate(resid_f10 = rps - avg)%>%
  mutate(resid_me = rps - pred)


mase(actual = df_tester$rps, predicted = df_tester$pred, step_size = 24)


ggplot(df_tester, aes(x = timestamp))+
  geom_point(aes(y = resid_f10))+
  geom_point(aes(y = resid_me), color = "red")


source("~/rstudio-workspace/packages/mase.R")
first <- TRUE

for(x in 1:20){
  
  
  name1<-as.character(df_amt2[x,])
  
  
  df_tester<-df_round%>%
    filter(as.character(landingContentGroup2) == name1[1] & as.character(country) == name1[2] &
             as.character(deviceCategory) == name1[3], as.character(operatingSystem) == name1[4])%>%
    arrange(date)
  
  
  if(first){
    mas_1 <- mase2(df_tester)*sum(df_tester$ses, na.rm = TRUE)
    count <- sum(df_tester$ses, na.rm = TRUE)
    first <- FALSE
  }
  else{
    if(is.na(mase2(df_tester))){
      next
    }
    if(is.infinite(mase2(df_tester))){
      next
    }
    mas_1 <- mas_1 + mase2(df_tester)*sum(df_tester$ses, na.rm = TRUE)
    count <- count + sum(df_tester$ses, na.rm = TRUE)
  }
  
  
  #print(  paste(  mase(df_tester$rps, df_tester$pred),  mase(df_tester$rps, df_tester$avg), sep = "--"))
  #print(mase(df_tester$rps, df_tester$pred) - mase(df_tester$rps, df_tester$avg))
  print(mase2(df_tester))
  
  
}

print(mas_1/count)
mase1(df_graph)

mase1(df_round)




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


# df_over_f10 <- df_graph%>%
#   filter(!is.na(resid_f10),!is.na(resid_me))%>%
#   filter(off_calc_f10>0)
# 
# df_under_f10 <- df_graph%>%
#   filter(!is.na(resid_f10),!is.na(resid_me))%>%
#   filter(off_calc_f10<0)
# 
# df_over_me <- df_graph%>%
#   filter(!is.na(resid_f10),!is.na(resid_me))%>%
#   filter(off_calc_me>0)
# 
# df_under_me <- df_graph%>%
#   filter(!is.na(resid_f10),!is.na(resid_me))%>%
#   filter(off_calc_me<0)
# 
# sum(df_over_f10$off_calc_f10)
# 
# sum(df_under_f10$off_calc_f10)
# 
# sum(df_over_me$off_calc_me)
# 
# sum(df_under_me$off_calc_me)
# 
# sum(df_graph$rev, na.rm = TRUE)
# 
# 
# 

# 

