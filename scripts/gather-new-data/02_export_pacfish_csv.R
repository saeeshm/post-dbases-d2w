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
              help="A year month combination indicating the start date for data to post to depth2water. Defaults to 30 days before today. [Default= %default]", 
              metavar="character"),
  make_option(c("-e", "--enddate"), type="character", default=(Sys.Date()), 
              help="A year month combination indicating the end date for data to post to depth2water. Defaults to the current date [Default= %default]", 
              metavar="character")
)

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== Paths and global variables ====

# Database credentials
creds <- fromJSON(file = 'options/dbase_credentials.json')

# Filepaths for gathering functions
fpaths <- fromJSON(file='options/filepaths.json')

# Ensuring directory exists for holding posting data data
dir_check_create(fpaths$`daily-post-dir`)
# Creating a sub-directory for EC climate, and deleting any prior contents if
# they exist
out_dir <- file.path(fpaths$`daily-post-dir`, 'pacfish')
if(dir.exists(out_dir)) unlink(out_dir, recursive=T)
dir_check_create(out_dir)

# ==== Opening database connection ====

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Daily data ====

# Data query
query <- format_simple_query('pacfish', 'daily', 
                             date_col = 'Date', 
                             start_date = ymd(opt$startdate),
                             end_date = ymd(opt$enddate))

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

# If there are no dfs, it means no new data are available. Skipping the rest...
# Writing if there are data, otherwise printing a nodata message
if(length(dfs) > 0){
  # Joining to a single dataframe
  daily <- dfs[[1]]
  walk(dfs[2:length(dfs)], ~{
    daily <<- full_join(daily, 
                        .x %>% select(-station_name), 
                        by = c('station_number', 'datetime'))
  })
  
  # Writing to csv
  write_csv(daily, file.path(out_dir, 'pacfish-daily.csv'))
  rm(daily)
  gc()
}else{
  print(
    paste0(
      'No new data available for Pacfish between ',
      as.character(opt$startdate),
      ' and ',
      as.character(opt$enddate),
      '. No CSV exported.'
    )
  )
}

# Closing database connection
dbDisconnect(conn)
