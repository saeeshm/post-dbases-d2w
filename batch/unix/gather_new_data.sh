# Gathering station metadata
Rscript scripts/gather-new-data/01_get_station_files.R

# Exporting newly updated daily data. All of these scripts can take -s and -e arguments to specific the start and end date respectively (in YYYY-MM-DD format). Defaults to starting 31 days before the current date
Rscript scripts/gather-new-data/02_export_hydat_csv.R
Rscript scripts/gather-new-data/02_export_pacfish_csv.R
Rscript scripts/gather-new-data/02_export_ecclimate_csv.R