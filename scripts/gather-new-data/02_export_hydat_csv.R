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
source('scripts/help_funcs.R')

# ==== Paths and global variables ====

# Path to credentials
dbase_credentials_path <- 'credentials.json'

# Path to exported csvs
csv_path <- 'data/csv/hydat'
dir_check_create(csv_path)

# ==== Opening database connection ====

# Credentials files
creds <- fromJSON(file = 'credentials.json')

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Reading data ====

# Flow data
query <- format_simple_query('bchydat', 'flow')
flow <- dbGetQuery(conn, query)

# Level data
query <- format_simple_query('bchydat', 'level')
level <- dbGetQuery(conn, query)

# Joining to a single table
hydat <- flow %>% 
  select(-Parameter) %>% 
  rename('flow' = Value) %>% 
  full_join(level %>% 
              select(STATION_NUMBER, Date, 'level' = Value),
            by = c('STATION_NUMBER', 'Date'))

# ==== Writing to CSV ====

# Writing to csv
write_csv(hydat, file.path(csv_path, 'bchydat.csv'))

# Closing database connection
dbDisconnect(conn)