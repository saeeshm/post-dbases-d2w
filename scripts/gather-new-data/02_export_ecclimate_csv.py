# Author: Saeesh Mangwani
# Date: 02/08/2023

# Description: Exporting EC Climate data to CSV format based on a provided date range

# %% ===== Loading libraries =====
import os
import sys
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
sys.path.append(os.getcwd())
from json import load
from optparse import OptionParser
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

#%% Initializing option parsing
parser = OptionParser()
parser.add_option(
    "-s", "--startdate", 
    dest="startdate",
    default=(datetime.today() - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00"),
    help="The start date of the date range for which data are being posted. Defaults to 31 days before today")
parser.add_option(
    "-e", "--enddate", 
    dest="enddate",
    default=datetime.today().strftime("%Y-%m-%dT00:00:00-00:00"),
    help="The end date of the date range for which data are being posted. Defaults to today")
(options, args) = parser.parse_args()

# %% ===== Paths and global variables =====

# Client credentials from JSON
creds = load(open('options/dbase_credentials.json',))

# Filepaths
fpaths = load(open('options/filepaths.json', ))

# Ensuring directory exists for holding posting data data
out_dir = fpaths['update-data-dir']
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# %% ===== Initializing database connection =====

# Database connection
conn = psycopg2.connect(
    host=creds['host'], 
    port=creds['port'],
    database=creds['dbname'],
    user= creds['user'],
    password=creds['password']
)
cursor = conn.cursor()

# %% ===== Initializing update options =====
start_date = options.startdate
end_date = options.enddate
# start_date = (datetime.today() - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.today().strftime("%Y-%m-%dT00:00:00-00:00")

# %% ==== Gathering update data ====

# Query options
schema = 'ecclimate'
table = 'daily'
datecol = 'datetime'
# Building query
query = 'select * from {0}.{1} where "{2}" >= \'{3}\' and "{2}" <= \'{4}\''.format(
    schema,
    table,
    datecol,
    start_date,
    end_date
)

# Getting data
cursor.execute(query)
col_names = [i[0] for i in cursor.description]
daily = pd.DataFrame(cursor.fetchall(), columns=col_names)


# %%  ==== Exporting to CSV ====
if daily.shape[0] == 0:
    print("No new data available for EC-Climate between {} and {}. No CSV exported".format(start_date, end_date))
else:
    print("Exporting data to CSV")
    daily.to_csv(out_dir + '/ecclimate-daily.csv', index=False)



