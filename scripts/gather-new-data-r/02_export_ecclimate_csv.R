# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Exporting EC Climate data to CSV format based on a provided
# date range

# ==== Libraries ====
library(DBI)
library(RPostgres)
library(rjson)
library(readr)
library(optparse)
library(lubridate)
source('scripts/help_funcs.R')

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-s", "--startdate"), type="character", default=(Sys.Date() - 31), 
              help="A year month combination indicating the start date for data to post to depth2water. Defaults to 31 days before today. [Default= %default]", 
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
out_dir <- fpaths$`update-data-dir`
dir_check_create(out_dir)

# ==== Opening database connection ====

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Daily data ====

# Data query
query <- format_simple_query('ecclimate', 'daily', 
                             date_col = 'datetime', 
                             start_date = ymd(opt$startdate), 
                             end_date = ymd(opt$enddate))

# Reading data
daily <- dbGetQuery(conn, query)

# Writing to CSV (if there are data) ----------

# Writing if there are data, otherwise printing a nodata message
if(nrow(daily) > 0){
  # Writing to csv
  write_csv(daily, file.path(out_dir, 'ecclimate-daily.csv'))
}else{
  print(
    paste0(
      'No new data available for EC-Climate between ',
      as.character(opt$startdate),
      ' and ',
      as.character(opt$enddate),
      '. No CSV exported.'
    )
  )
}

# Closing database connection
dbDisconnect(conn)
