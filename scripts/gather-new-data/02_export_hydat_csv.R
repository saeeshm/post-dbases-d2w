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
query <- format_simple_query('bchydat', 'flow', 
                             date_col = 'Date', start_date = ymd(opt$startdate))
flow <- dbGetQuery(conn, query)

# Level data
query <- format_simple_query('bchydat', 'level', 
                             date_col = 'Date', start_date = ymd(opt$startdate))
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

# Writing to csv
write_csv(hydat, file.path(csv_path, 'bchydat-daily.csv'))

# Closing database connection
dbDisconnect(conn)
