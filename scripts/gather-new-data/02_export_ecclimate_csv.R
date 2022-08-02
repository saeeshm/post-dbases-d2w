# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Exporting EC Climate data to CSV format based on a provided
# date range

# ==== Libraries ====
library(DBI)
library(RPostgres)
library(rjson)
library(readr)
source('scripts/help_funcs.R')

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
query <- format_simple_query('ecclimate', 'daily')

# Reading data
daily <- dbGetQuery(conn, query)

# Writing to csv
write_csv(daily, file.path(csv_path, 'ecclimate-daily.csv'))
rm(daily)
gc()

# ==== Hourly data ====

# Data query
query <- format_simple_query('ecclimate', 'hourly')

# Reading data
hourly <- dbGetQuery(conn, query)

# Writing to csv
write_csv(hourly, file.path(csv_path, 'ecclimate-hourly.csv'))

# Closing database connection
dbDisconnect(conn)
