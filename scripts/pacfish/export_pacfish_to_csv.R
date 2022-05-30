# Author: Saeesh Mangwani
# Date: 2021-05-20

# Description: A script that exports a CSV file from postgres containing the pacfish data to be posted to depth2water

# ==== Loading libraries ====
library(DBI)
library(RPostgres)
library(lubridate)
library(rjson)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(readr)

# ==== Paths and global variables ====

# Path to credentials
dbase_credentials_path <- 'credentials.json'

# Path to exported csv
csv_path <- 'csv/pacfish'

# ==== Opening database connection ====

# Credentials files
creds <- fromJSON(file = 'credentials.json')

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Daily data ====

# Data query
query <- format_simple_query('pacfish', 'hourly', 'Date', 
                             '2022-04-01', '2022-05-30')

# Reading data
df <- dbGetQuery(conn, query)

# Splitting table by datatype
dfs <- df %>%
  filter(Parameter != 'Air Temperature') %>% 
  group_by(Parameter) %>%
  group_split() %>% 
  map(., ~{
    param <- unique(.x$Parameter)[1] %>% 
      tolower() %>% 
      str_replace_all(., ' ', '_')
    .x %>% 
      select(-numObservations, -Parameter) %>% 
      setNames(c('station_number', 'station_name', 'datetime', param))
  })

# Joining to a single dataframe
daily <- dfs[[1]]
walk(dfs[2:length(dfs)], ~{
  daily <<- full_join(daily, 
                      .x %>% select(-station_name), 
                      by = c('station_number', 'datetime'))
})

# Writing to csv
write_csv(daily, file.path(csv_path, 'pacfish-daily.csv'))
rm(daily)
gc()
# ==== Hourly data ====

# Data query
query <- format_simple_query('pacfish', 'hourly')

# Reading data
df <- dbGetQuery(conn, query)

# Splitting table by datatype
dfs <- df %>%
  filter(Parameter != 'Air Temperature') %>% 
  group_by(Parameter) %>%
  group_split() %>% 
  map(., ~{
    param <- unique(.x$Parameter)[1] %>% 
      tolower() %>% 
      str_replace_all(., ' ', '_')
    .x %>% 
      select(-Parameter, -Comments) %>% 
      mutate(Code = ifelse(Code == 21, param, NA_character_)) %>% 
      setNames(c('station_number', 'station_name', 'date', 'time', param, 
                 paste0('code_', param)))
  })

# Joining to a wide table
hourly <- dfs[[1]]
walk(dfs[2:length(dfs)], ~{
  hourly <<- full_join(hourly, 
                       .x %>% select(-station_name), 
                       by = c('station_number', 'date', 'time'))
})

# Joining all estimated columns into 1 indicating which data columns contain
# estimated data
hourly <- hourly %>% 
  mutate(comments = ifelse(
    is.na(code_pressure) & is.na(code_sensor_depth) & is.na(code_water_level) & is.na(code_water_temperature),
    NA_character_,
    paste0('Estimated: ', paste(code_pressure, code_sensor_depth, code_water_level, code_water_temperature, sep = ','))
  )) %>% 
  select(-contains('code')) %>% 
  mutate(comments = str_remove_all(comments, 'NA,?\\s?')) %>% 
  mutate(comments = str_remove_all(comments, ',$'))

# Writing to disk
write_csv(hourly, file.path(csv_path, 'pacfish-hourly.csv'))
