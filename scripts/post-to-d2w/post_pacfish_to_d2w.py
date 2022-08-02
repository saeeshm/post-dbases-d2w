# Author: Saeesh Mangwani
# Date: 19/05/2022

# Description: Exporting pacfish data to CSV and posting to depth2water

# %% ===== Loading libraries =====
import os
os.chdir('../..')
import logging
import pandas as pd
from numpy import isnan
from datetime import datetime
from dateutil.parser import parse
from depth2water import create_client, get_surface_water_mapping, get_surface_water_station_mapping

# %% ===== Paths and global variables =====

# Path to station file
station_file_path = '/Users/saeesh/code/GWProjects/databases/post-dbases-d2w/data/stations/pacfish_station_data.csv'

# Path to daily data file
daily_data_path = '/Users/saeesh/code/GWProjects/databases/post-dbases-d2w/data/csv/pacfish/pacfish-daily.csv'

# Path to temporary directory for storing posting files
data_temp_path = '/Users/saeesh/code/GWProjects/databases/post-dbases-d2w/data/temp/pacfish'

# %% ===== Initializing update parameters =====
#Date range of update data
start_date = str(parse("1950-01-01T00:00:00-08:00"))
end_date = str(parse("2022-07-10T00:00:00-08:00"))

# %% ===== Initializing parameters =====

# Setting up logging
# logging.basicConfig(level=logging.DEBUG)

# Initializing application parameters
USERNAME = "gwadmin"
PASSWORD = "kowe#0485"
CLIENT_ID = 'x4u9RdFzzSfYs4Dau1c2bdEZ66RtGMuRUe7OWX1L'
CLIENT_SECRET = 'qM3s44Uoyi6CePPLHKV6WG359JVVHtaDelSyQz40QNU1SFcVit2ApXqsS9djxdnDxLiTUA77wxb4TmUM2bpA4mqB0GTj2Lq5Vw3DIU8CuhLDIxPlDSwXMcSA6GQm6u25'
TEST_USER_ID = 24

# Creating a client
client = create_client(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, host = 'localhost:8000', scheme='http')

# %% ===== Reading data =====

# Reading the most recent station metadata file
metadata = pd.read_csv(station_file_path)

# Daily data CSV file
daily = pd.read_csv(daily_data_path, parse_dates=[2])

# Getting unique station IDs:
stat_ids = metadata['station_id'].unique()

# %% ===== Checking stations on d2w =====
for stat in stat_ids:
    # Checking if the station is present
    result = client.get_station_by_station_id(stat)
    # If not, creating it
    if len(result) == 0:
        print('Creating station ' + stat)
        station_mapping = get_surface_water_station_mapping({
            'station_id': stat,
            'location_name': metadata.loc[metadata['station_id'] == stat].iloc[0]['station_name'],
            'longitude': metadata.loc[metadata['station_id'] == stat].iloc[0]['long'],
            'latitude': metadata.loc[metadata['station_id'] == stat].iloc[0]['lat'],
            'prov_terr_state_lc': 'BC',
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')

# %% ===== Helper functions for comparing update to existing data =====

# A helper function to read multipage responses from the get-function
def get_surface_water_data_multipage(station_id, start_date=None, end_date=None, url=None):
    resp = client.get_surface_water_data(station_id=station_id, start_date=start_date, end_date=end_date, url=url)
    outdata = resp['results']
    if resp['next'] is not None:
        print('There is another page, getting its data...')
        outdata.extend(get_surface_water_data_multipage(station_id, url=resp['next']))
    return outdata

# A helper function that formats the 'station' subdictionary within each get request - note that this function only makes in-place changes, so the underlying list does not need to be reassigned
def format_get_station_metadata(datadict, keylist):
    if 'station' in datadict.keys():
        updatedict = {key: datadict['station'][key] for key in keylist}
        datadict.pop('station')
        datadict.update(updatedict)
    return datadict

# A helper function to format the retreived station data to the upload data format
def format_curr_data_df(currdf):
    if currdf.shape[1] == 0 : return currdf
    # Selecting relevant columns
    outdf = currdf[
        ['station_id', 'location_name', 'datetime', 'barometric_pressure_m', 'water_level_compensated_m', 'water_level_staff_gauge_calibrated', 'temperature_c']
    ]
    # Renaming to match the upload data table
    outdf.columns = ['station_number', 'station_name', 'datetime', 'pressure', 'sensor_depth', 'water_level', 'water_temperature']

    # Setting variable types
    dtype_dict = {
        'station_number': 'str',
        'station_name': 'str',
        'datetime': 'datetime64',
        'pressure': 'float64',
        'sensor_depth': 'float64',
        'water_level': 'float64',
        'water_temperature': 'float64'
    }
    outdf = outdf.astype(dtype_dict)
    outdf['datetime'] = pd.to_datetime(outdf.datetime, utc=True)
    return outdf

# Function that returns an empty string if the input is NaN
def emptyIfNan(x):
    if type(x) == str: return x
    return '' if isnan(x) else x

# %% ===== Categorizing new data for update or post =====

# Getting the station of ids of all stations included in the current update dataset
stat_ids = daily['station_id'].unique()

for stat in stat_ids:
    print(stat)

    # Getting all the new data for this station
    updatedf = daily[daily['station_number'] == stat]
    
    # Getting all current data for the station within the data range
    curr_data = get_surface_water_data_multipage(
        station_id=stat, 
        start_date=start_date, 
        end_date=end_date
    )

    # If there is no current data present, just pushing new data directly to a csv to be posted (i.e no direct database updates required)
    if len(curr_data) == 0:
        # Writing all new data to csv for posting
        if updatedf.shape[0] > 0:
            fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
            updatedf.to_csv(fpath, index=False, na_rep='NA')
        else:
            print('No rows to post for station ' + stat)
        # Skipping iteration to the next station, as no updates are needed
        continue

    # If there is current data, getting only relevant columns
    keylist = ['station_id','location_name']
    for i in range(0, len(curr_data)): format_get_station_metadata(curr_data[i], keylist)

    # Converting to dataframe
    currdf = pd.DataFrame(curr_data, index = None)

    # Formatting to new data structure
    currdf = format_curr_data_df(currdf)

    # Using an indicator left join to see which rows are new and which already exist
    left_joined = updatedf.merge(currdf, how='left', indicator=True, on=['station_number', 'datetime'])

    # Those that say left-only new, and therefore need to be directly uploaded
    addrows = left_joined[left_joined._merge == 'left_only'][['station_number', 'datetime']]
    addrows = addrows.merge(updatedf, how='left', on=['station_number', 'datetime'])

    # Those that say both need to be updated/edited on the server directly
    updaterows = left_joined[left_joined._merge == 'both'][['station_number', 'datetime']]
    updaterows = updaterows.merge(updatedf, how='left', on=['station_number', 'datetime'])
    # Of the update rows, joining on all columns to only include for update those rows where some value has changed (i.e there are differences between the downloaded and stored versions). In this case, the newly downloaded version takes precedence
    left_joined = updaterows.merge(currdf, how='left', indicator=True)
    updaterows = left_joined[left_joined._merge == 'left_only'].drop('_merge', axis = 1)

    # For the rows that need updating
    for i in range(0, updaterows.shape[0]):
        # Getting the date of the update row
        querydate = updaterows.iloc[i,]['datetime']
        print('Updating data on date: ' + querydate.strftime('%Y-%m-%d'))
        
        # Obtaining the data dictionary already stored on the server for this date
        updict = dict()
        for row in curr_data:
            if row['datetime'] == querydate.strftime('%Y-%m-%dT%H:%M:%SZ'):
                updict = row
                break
        
        # Getting the new values to edit in this datapoint as a dictionary
        valuedict = dict(updaterows.iloc[i,][3:7])

        # Updating values
        updict['water_level_staff_gauge_calibrated'] = emptyIfNan(valuedict['water_level'])
        updict['water_level_compensated_m'] = emptyIfNan(valuedict['sensor_depth'])
        updict['temperature_c'] = emptyIfNan(valuedict['water_temperature'])
        updict['barometric_pressure_m'] = emptyIfNan(valuedict['pressure'])
        
        # Posting updates
        client.update_surface_water_data(updict['id'], updict)
    else:
        print(str(updaterows.shape[0]) + ' rows updated for station ' + stat)

    # For those that are simple additions, writing to csv for posting
    if addrows.shape[0] > 0:
        fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
        addrows.to_csv(fpath, index=False, na_rep='')
        print(str(addrows.shape[0]) + ' rows to post for station ' + stat)
    else:
        print('0 rows to post for station ' + stat)

# %% ===== Posting newly data csvs =====

# Defining column mappings
file_mappings = {
    'station_id':'station_number',
    'datetime': 'datetime',
    'water_level_staff_gauge_calibrated': 'water_level',
    'water_level_compensated_m': 'sensor_depth',
    'temperature_c': 'water_temperature',
    'barometric_pressure_m': 'pressure',
    'owner': TEST_USER_ID,
    'comments': ''
}
# File names of posting csvs
fnames = os.listdir(data_temp_path)

# If no files are present, posting a message and closing
if len(fnames) == 0:
    print('No new data files to post')
else:
    # Calling the client to post each file
    for name in fnames:
        print('Uploaded new data from file: ' + name)
        fpath = data_temp_path + '/' + name
        client.post_csv_file(fpath, get_surface_water_mapping(file_mappings))

    # Cleaning temporary directories
    for name in fnames:
        os.remove(data_temp_path + '/' + name)