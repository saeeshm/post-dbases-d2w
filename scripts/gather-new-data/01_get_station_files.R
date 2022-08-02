# Author: Saeesh Mangwani
# Date: 2022-06-15

# Description: Copies station metadata files from the relevant locations

# ==== Libraries ====
library(rjson)

# ==== Paths ====

# Station file paths JSON
station_path_file <- 'scripts/gather-new-data/station-file-paths.json'

# Path to output
out_path <- 'data/stations'

# ==== Copying station metadata files ====

# Reading paths
fpaths <- fromJSON(file = station_path_file)

# Copying pacfish
file.copy(from = fpaths$pacfish, 
          to = file.path(out_path, 'pacfish_station_data.csv'), 
          overwrite = T)

# Copying ecclimate
file.copy(from = fpaths$ecclimate, 
          to = file.path(out_path, 'ecclimate_station_data.csv'), 
          overwrite = T)

# Copying hydat
file.copy(from = fpaths$hydat, 
          to = file.path(out_path, 'hydat_station_data.csv'), 
          overwrite = T)
