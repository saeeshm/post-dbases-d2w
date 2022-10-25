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
csv_path <- 'data/csv/ecclimate'
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
query <- format_simple_query('ecclimate', 'daily', 
                             date_col = 'datetime', start_date = ymd(opt$startdate))

# Reading data
daily <- dbGetQuery(conn, query)

# Writing to csv
write_csv(daily, file.path(csv_path, 'ecclimate-daily.csv'))
rm(daily)
gc()

# ==== Hourly data ====

# Data query
# query <- format_simple_query('ecclimate', 'hourly', 
#                              date_col = 'datetime', start_date = ymd(opt$startdate))
# 
# # Reading data
# hourly <- dbGetQuery(conn, query)
# 
# # Writing to csv
# write_csv(hourly, file.path(csv_path, 'ecclimate-hourly.csv'))

# Closing database connection
dbDisconnect(conn)
