# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Copies station metadata files from the relevant locations

# ==== Libraries ====
library(DBI)
library(RPostgres)
library(rjson)
library(readr)
library(dplyr)
source('scripts/help_funcs.R')

# ==== File paths ====

# Database credentials
creds <- fromJSON(file = 'options/dbase_credentials.json')

# Reading file paths from JSON
fpaths <- fromJSON(file='options/filepaths.json')

# ==== Opening database connection ====

# Opening database connection
conn <- dbConnect(RPostgres::Postgres(),
                  host = creds$host, dbname = creds$dbname,
                  user = creds$user, password = creds$password)

# ==== Exporting pacfish metadata ====

# Data query
query <- format_simple_query('pacfish', 'station_metadata')

# Reading data from dbase
pacfish <- dbGetQuery(conn, query)

# Exporting CSV to metadata folder
pacfish %>% 
  # Removing stations with missing location data - can't do anything with these
  filter(!is.na(long) | !is.na(lat)) %>% 
  # Writing to disk
  write_csv(fpaths$`pacfish-metadata`)

# ==== Exporting EC climate metadata ====

# Data query
query <- format_simple_query('ecclimate', 'station_metadata')

# Reading data from dbase
ecclimate <- dbGetQuery(conn, query)

# Exporting CSV
write_csv(ecclimate, fpaths$`ecclimate-metadata`)

# ==== Exporting hydat station metadata ====

# Data query
query <- format_simple_query('bchydat', 'station_metadata')

# Reading data from dbase
hydat <- dbGetQuery(conn, query)

# Copying hydat - first formatting the columns since they change often and the
# I/O needs to be controlled
hydat_formatted <- hydat %>% 
  select(STATION_NUMBER, STATION_NAME, 
         'STATION_STATUS' = HYD_STATUS,
         DRAINAGE_AREA_GROSS, DRAINAGE_AREA_EFFECT,
         RHBN, REAL_TIME,
         LONGITUDE, LATITUDE, DATUM_ID) %>% 
  mutate(STATION_STATUS = ifelse(STATION_STATUS == 'ACTIVE-REALTIME', 
                                 'ACTIVE', 
                                 STATION_STATUS))
# Writing the formatted version
write_csv(hydat_formatted, fpaths$`hydat-metadata`)

# Closing database connection
dbDisconnect(conn)
