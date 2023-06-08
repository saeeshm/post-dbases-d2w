# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Exporting Hydat data to CSV format based on a provided
# date range

# ==== Libraries ====
library(DBI)
library(RPostgres)
library(rjson)
library(readr)
library(dplyr)
library(optparse)
library(lubridate)
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
out_dir <- file.path(fpaths$`daily-post-dir`, 'hydat')
if(dir.exists(out_dir)) unlink(out_dir, recursive=T)
dir_check_create(out_dir)

# ==== Opening database connection ====

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Reading data ====

# Flow data
query <- format_simple_query('bchydat', 'flow', 
                             date_col = 'Date', 
                             start_date = ymd(opt$startdate),
                             end_date = ymd(opt$enddate))
flow <- dbGetQuery(conn, query)

# Level data
query <- format_simple_query('bchydat', 'level', 
                             date_col = 'Date', 
                             start_date = ymd(opt$startdate),
                             end_date = ymd(opt$enddate))
level <- dbGetQuery(conn, query)

# Joining to a single table
hydat <- flow %>% 
  select(-Parameter, -Symbol) %>% 
  rename('flow' = Value) %>% 
  full_join(level %>% 
              select(STATION_NUMBER, Date, 'level' = Value, pub_status),
            by = c('STATION_NUMBER', 'Date')) %>%
  mutate(pub_status = case_when(
    is.na(pub_status.x) & is.na(pub_status.y) ~ NA_character_,
    is.na(pub_status.y) ~ pub_status.x,
    is.na(pub_status.x) ~ pub_status.y,
    T ~ pub_status.x
  )) %>% 
  select(-pub_status.x, -pub_status.y) %>% 
  # Converting pub-status to a boolean
  mutate(pub_status = ifelse(pub_status == 'Published', T, F))

# ==== Writing to CSV ====

# Writing if there are data, otherwise printing a nodata message
if(nrow(hydat) > 0){
  # Writing to csv
  write_csv(hydat, file.path(out_dir, 'hydat-daily.csv'))
}else{
  print(
    paste0(
      'No new data available for Hydat between ',
      as.character(opt$startdate),
      ' and ',
      as.character(opt$enddate),
      '. No CSV exported.'
    )
  )
}

# Closing database connection
dbDisconnect(conn)
