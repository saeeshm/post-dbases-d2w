# Author: Saeesh Mangwani
# Date: 02/08/2023

# Description: Copies station metadata files from postgres to prepare for posting

# %% ===== Loading libraries =====
import os
import sys
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
sys.path.append(os.getcwd())
from json import load
import psycopg2
import pandas as pd

# %% ===== Paths and global variables =====

# Client credentials from JSON
creds = load(open('options/dbase_credentials.json',))

# Filepaths
fpaths = load(open('options/filepaths.json', ))

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

# %% ==== Exporting pacfish metadata ====

# Creating query
query = "select * from pacfish.station_metadata"

# Getting metadata
cursor.execute(query)

# Reading the result
col_names = [i[0] for i in cursor.description]
pacfish = pd.DataFrame(cursor.fetchall(), columns=col_names)

# Removing stations with missing location data
pacfish = pacfish.dropna(subset=['long', 'lat'])

# Writing to csv
pacfish.to_csv(fpaths['pacfish-metadata'], index=False)

# %% ==== Exporting EC Climate metadata ====

# Creating query
query = "select * from ecclimate.station_metadata"

# Getting metadata
cursor.execute(query)

# Reading the result
col_names = [i[0] for i in cursor.description]
ecclimate = pd.DataFrame(cursor.fetchall(), columns=col_names)

# Ensuring the station ID column is a string
ecclimate.loc[:,'Station ID'] = pd.to_numeric(ecclimate['Station ID'], downcast='integer').astype(str)

# Writing to csv
ecclimate.to_csv(fpaths['ecclimate-metadata'], index=False)

# %% ==== Exporting Hydat metadata ====

# Creating query
query = "select * from bchydat.station_metadata"

# Getting metadata
cursor.execute(query)

# Reading the result
col_names = [i[0] for i in cursor.description]
hydat = pd.DataFrame(cursor.fetchall(), columns=col_names)

# List of only relevant columns
collist = ['STATION_NUMBER', 'STATION_NAME', 'STATION_STATUS', 'DRAINAGE_AREA_GROSS', 'DRAINAGE_AREA_EFFECT','RHBN', 'REAL_TIME','LONGITUDE', 'LATITUDE', 'DATUM_ID']

# Renaming and subsetting columns
hydat.rename(columns={'HYD_STATUS':'STATION_STATUS'}, inplace=True)
hydat = hydat[collist]

# Editing station status to either be active or discontinued
hydat['STATION_STATUS'] = ['ACTIVE' if status == 'ACTIVE-REALTIME' else status for status in hydat['STATION_STATUS']]

# Writing to csv
hydat.to_csv(fpaths['hydat-metadata'], index=False)

# %% Closing connections
conn.close()
