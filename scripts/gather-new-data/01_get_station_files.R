# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Copies station metadata files from the relevant locations

# ==== Libraries ====
library(rjson)
library(readr)
library(dplyr)

# ==== File paths ====

# Reading file paths from JSON
fpaths <- fromJSON(file='options/filepaths.json')

# ==== Copying station metadata files ====

# Copying pacfish
pacfish <- read_csv(fpaths$`pacfish-metadata-infile`)
pacfish %>% 
  # Removing stations with missing location data - can't do anything with these
  filter(!is.na(long) | !is.na(lat)) %>% 
  # Writing to disk
  write_csv(fpaths$`pacfish-metadata`)

# Copying ecclimate
file.copy(from = fpaths$`ecclimate-metadata-infile`, 
          to = fpaths$`ecclimate-metadata`, 
          overwrite = T)

# Copying hydat - first formatting the columns since they change often and the
# I/O needs to be controlled
hydat <- read_csv(fpaths$`hydat-metadata-infile`)
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

