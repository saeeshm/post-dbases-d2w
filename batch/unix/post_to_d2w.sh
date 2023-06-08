# Scripts for posting newly downloaded data. All of these scripts can  take -s and -e arguments to specific the start and end date respectively (in YYYY-MM-DD format). Defaults to starting 31 days before the current date - the same as the data gathering scripts. If using a custom date range for data gathering, make sure to provide the same date range here.

# Activating the approriate conda environment
# conda activate depth2water

# Posting new pacfish data to d2w
python scripts/post-to-d2w/post_pacfish_to_d2w.py

# Posting new EC Climate data to d2w
python scripts/post-to-d2w/post_ecclimate_to_d2w.py

# Posting new Hydat data to d2w
python scripts/post-to-d2w/post_hydat_to_d2w.py