# Author: Saeesh Mangwani
# Date: 2021-05-20

# Description: Exporting pacfish data to CSV format based on a given data range.

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
library(optparse)
source('scripts/help_funcs.R')

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-s", "--startdate"), type="character", default=(Sys.Date() - 30), 
              help="A year month combination indicating the start date for data download. [Default= %default]", 
              metavar="character")
)

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== Paths and global variables ====

# Path to credentials
dbase_credentials_path <- 'credentials.json'

# Path to exported csvs
csv_path <- 'data/csv/pacfish'
dir_check_create(csv_path)

# ==== Opening database connection ====

# Credentials files
creds <- fromJSON(file = 'credentials.json')

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Daily data ====

# Data query
query <- format_simple_query('pacfish', 'daily', 
                             date_col = 'Date', start_date = ymd(opt$startdate))

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

# # Data query
# query <- format_simple_query('pacfish', 'hourly', 
#                              date_col = 'Date', start_date = ymd(opt$startdate))
# 
# # Reading data
# df <- dbGetQuery(conn, query)
# 
# # Splitting table by datatype
# dfs <- df %>%
#   filter(Parameter != 'Air Temperature') %>% 
#   group_by(Parameter) %>%
#   group_split() %>% 
#   map(., ~{
#     param <- unique(.x$Parameter)[1] %>% 
#       tolower() %>% 
#       str_replace_all(., ' ', '_')
#     .x %>% 
#       select(-Parameter, -Comments) %>% 
#       mutate(Code = ifelse(Code == 21, param, NA_character_)) %>% 
#       setNames(c('station_number', 'station_name', 'date', 'time', param, 
#                  paste0('code_', param)))
#   })
# 
# # Joining to a wide table
# hourly <- dfs[[1]]
# walk(dfs[2:length(dfs)], ~{
#   hourly <<- full_join(hourly, 
#                        .x %>% select(-station_name), 
#                        by = c('station_number', 'date', 'time'))
# })
# 
# # Joining all estimated columns into 1 indicating which data columns contain
# # estimated data
# hourly <- hourly %>% 
#   mutate(comments = ifelse(
#     is.na(code_pressure) & is.na(code_sensor_depth) & is.na(code_water_level) & is.na(code_water_temperature),
#     NA_character_,
#     paste0('Estimated: ', paste(code_pressure, code_sensor_depth, code_water_level, code_water_temperature, sep = ','))
#   )) %>% 
#   select(-contains('code')) %>% 
#   mutate(comments = str_remove_all(comments, 'NA,?\\s?')) %>% 
#   mutate(comments = str_remove_all(comments, ',$'))
# 
# # Writing to disk
# write_csv(hourly, file.path(csv_path, 'pacfish-hourly.csv'))

# Closing database connection
dbDisconnect(conn)