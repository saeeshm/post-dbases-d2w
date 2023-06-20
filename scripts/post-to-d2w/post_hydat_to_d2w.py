# Author: Saeesh Mangwani
# Date: 19/05/2022

# Description: Exporting Hydat data to CSV and posting to depth2water

# %% ===== Loading libraries =====
import os
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
import logging
import pandas as pd
from json import load
from numpy import isnan
from optparse import OptionParser
from datetime import datetime, timedelta
from depth2water import create_client, get_surface_water_mapping, get_surface_water_station_mapping

#%% Initializing option parsing
parser = OptionParser()
parser.add_option(
    "-s", "--startdate", 
    dest="startdate", action="store", 
    default=(datetime.today() - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00"),
    help="The start date of the date range for which data are being posted. Defaults to 31 days before today")
parser.add_option(
    "-e", "--enddate", 
    dest="enddate", action="store", 
    default=datetime.today().strftime("%Y-%m-%dT00:00:00-00:00"),
    help="The end date of the date range for which data are being posted. Defaults to today")
(options, args) = parser.parse_args()

# %% ===== Paths and global variables =====

# Getting client credentials from JSON
creds = load(open('options/client_credentials.json',))

# Getting file paths from JSON
fpaths = load(open('options/filepaths.json',))

# Path to station file
station_file_path = fpaths['hydat-metadata']

# Path to daily data file
daily_data_path = fpaths['update-data-dir'] + '/hydat-daily.csv'

# Path to temporary directory for storing posting files
data_temp_path = fpaths['temp-dir'] + '/hydat'

# %% ===== Initializing update parameters =====
start_date = options.startdate
end_date = options.enddate

#%% Manual data inputs - for use when script testing
# start_date = (datetime.today() - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.today().strftime("%Y-%m-%dT00:00:00-00:00")

# %% ===== Initializing parameters =====

# Setting up logging
# logging.basicConfig(level=logging.DEBUG)

# Setting owner ID for this database
OWNER_ID = 7

# Creating a client
client = create_client(
    username=creds['username'], 
    password=creds['password'], 
    client_id=creds['client_id'], 
    client_secret=creds['client_secret'], 
    host=creds['host'], 
    scheme=creds['scheme']
)

# %% ===== Reading data =====

# Station metadata file
metadata = pd.read_csv(station_file_path)
dtype_dict = {
    'STATION_NUMBER': 'str',
    'STATION_NAME': 'str',
    'STATION_STATUS': 'str',
    'DRAINAGE_AREA_GROSS': 'float64',
    'DRAINAGE_AREA_EFFECT': 'float64',
    'RHBN': 'str',
    'REAL_TIME': 'str',
    'LATITUDE': 'float64',
    'LONGITUDE': 'float64',
    'DATUM_ID':'float64'
}
metadata = metadata.astype(dtype_dict)

# Daily data CSV file
try:
    daily = pd.read_csv(daily_data_path, parse_dates=[2])
    # Setting types for relevant variables
    dtype_dict = {
        'STATION_NUMBER': 'str',
        'Date': 'datetime64',
        'flow': 'float64',
        'level': 'float64',
        'pub_status': 'str'
    }
    daily = daily.astype(dtype_dict)
    # Filtering daily dataset to only include stations reference in the metadata file (this ensures that data with no stations are excluded)
    daily = daily[daily['STATION_NUMBER'].isin(metadata['STATION_NUMBER'])]
except:
    print('No daily dataset found. This script will only complete a metadata update')
    daily = None

# %% ===== Script helper functions =====

# Helper function to quickly access values from the downloaded metadata file for a station
def pull_metadata(statid, varname):
    return metadata.loc[metadata['STATION_NUMBER'] == statid].iloc[0][varname]

# Helper function to quickly access values from a station or timeseries "result" dictionary, obtained from a d2w query
def pull_statquery(resultobj, varname):
    return resultobj['results'][0][varname]

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
    # If the df is empty, returning as-is
    if currdf.shape[1] == 0 : return currdf

    # Selecting relevant columns
    outdf = currdf[
        ['station_id', 'datetime', 'water_level_staff_gauge_calibrated', 'water_flow_calibrated_mps']
    ]
    # Renaming to match the upload data table
    outdf.columns = ['STATION_NUMBER', 'Date', 'level', 'flow']

    # Setting variable types
    dtype_dict = {
        'STATION_NUMBER': 'str',
        'Date': 'datetime64',
        'flow': 'float64',
        'level': 'float64'
    }
    outdf = outdf.astype(dtype_dict)
    # Formatting the date column as a datetime object with only dates available (i.e no timestamps, since data are aggregated as daily means)
    outdf['Date'] = pd.to_datetime(outdf['Date'].dt.date)
    return outdf

# Function that returns an empty string if the input is NaN
def emptyIfNan(x):
    if type(x) == str: 
        return '' if x == 'nan' else x
    return '' if isnan(x) else x

# %% ===== Checking stations on d2w =====

# Getting unique station IDs:
stat_ids = metadata['STATION_NUMBER'].unique()

for stat in stat_ids:
    # Checking if the station is present
    result = client.get_station_by_station_id(stat)
    # Removing results that are not surface water stations
    result['results'] = [station for station in result['results'] if station['monitoring_type'] == 'SURFACE_WATER']
    # If not, creating it
    if len(result['results']) == 0:
        print('Creating station ' + stat)
        station_mapping = get_surface_water_station_mapping({
            'station_id': stat,
            'owner': OWNER_ID,
            'location_name': pull_metadata(stat, 'STATION_NAME'),
            'longitude': pull_metadata(stat, 'LONGITUDE'),
            'latitude': pull_metadata(stat, 'LATITUDE')
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')
        # If any of the parameters are not the same between metadata and those stored on file, updating
        isdiscrepant = any([
            pull_metadata(stat, 'STATION_STATUS') != pull_statquery(result, 'monitoring_status'),
            round(pull_metadata(stat, 'LONGITUDE'), 5) != round(pull_statquery(result, 'longitude'), 5),
            round(pull_metadata(stat, 'LATITUDE'), 5) != round(pull_statquery(result, 'latitude'), 5)
        ])
        if isdiscrepant:
            print('Station status has changed - updating...')
            updict = result['results'][0]
            updict['monitoring_status'] = emptyIfNan(pull_metadata(stat, 'STATION_STATUS'))
            updict['longitude'] = emptyIfNan(pull_metadata(stat, 'LONGITUDE'))
            updict['latitude'] = emptyIfNan(pull_metadata(stat, 'LATITUDE'))
            client.update_station(id=updict['id'], data=updict)
        else:
            print('No changes made to station ' + stat)

print('Station updates complete')

# %% ===== Categorizing new data for update or post =====

if daily is None:
    print('No daily data available in this time range. Skipping data update...')
else:
    # Getting the station of ids of all stations included in the current update dataset
    stat_ids = daily['STATION_NUMBER'].unique()
    # stat_ids = daily['STATION_NUMBER'].head(100).unique()
    # stat = stat_ids[0]
    for stat in stat_ids:
    
        # Getting all the new data for this station
        updatedf = daily[daily['STATION_NUMBER'] == stat]

        # Getting all current data for the station within the data range
        curr_data = get_surface_water_data_multipage(
            station_id=stat, 
            start_date=(pd.to_datetime(start_date) - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-00:00"), 
            end_date=end_date
        )

        # If there is no current data present, just pushing new data directly to a csv to be posted (i.e no direct database updates required)
        if len(curr_data) == 0:
            # Writing all new data to csv for posting
            if updatedf.shape[0] > 0:
                print('No data in this time period for station ' + stat + '. Writing new data to post...')
                fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
                updatedf.to_csv(fpath, index=False, na_rep='',  )
            else:
                print('No rows to post for station ' + stat)
            # Skipping iteration to the next station, as no updates are needed
            continue

        # Removing unncessary station information
        keylist = ['station_id','location_name']
        for i in range(0, len(curr_data)): format_get_station_metadata(curr_data[i], keylist)

        # Converting to dataframe
        currdf = pd.DataFrame(curr_data, index = None)

        # Formatting to upload style
        currdf = format_curr_data_df(currdf)
        # Ensuring datetime format is not specified as anything and is only set to a date, not a datetime (since hydat is published without times for daily data)
        currdf['Date'] = currdf['Date'].dt.tz_localize(None)

        # Using an indicator left join to see which rows are new and which already exist
        left_joined = updatedf.merge(currdf, how='left', indicator=True, on=['STATION_NUMBER', 'Date'])

        # Those that say left-only are new, and therefore need to be directly uploaded
        addrows = left_joined[left_joined._merge == 'left_only'][['STATION_NUMBER', 'Date']]
        addrows = addrows.merge(updatedf, how='left', on=['STATION_NUMBER', 'Date'])

        # Those that say both need to be updated/edited on the server directly
        updaterows = left_joined[left_joined._merge == 'both'][['STATION_NUMBER', 'Date']]
        updaterows = updaterows.merge(updatedf, how='left', on=['STATION_NUMBER', 'Date'])
        # Of the update rows, joining on all columns to only include for update those rows where some value has changed (i.e there are differences between the downloaded and stored versions). In this case, the newly downloaded version takes precedence
        left_joined = updaterows.merge(currdf, how='left', indicator=True, on=['STATION_NUMBER', 'Date', 'flow', 'level']) 
        updaterows = left_joined[left_joined._merge == 'left_only'].drop('_merge', axis = 1)

        # For the rows that need updating
        for i in range(0, updaterows.shape[0]):
            # Getting the date of the update row
            querydate = updaterows.iloc[i,]['Date']
            
            # Obtaining the data dictionary already stored on the server for this date
            updict = dict()
            for row in curr_data:
                if pd.to_datetime(row['datetime']).strftime('%Y-%m-%d') == querydate.strftime('%Y-%m-%d'):
                    updict = row
                    break
            
            # Getting the new values to edit in this datapoint as a dictionary
            valuedict = valuedict = updaterows.to_dict('records')[i]

            # Updating values
            updict['water_flow_calibrated_mps'] = emptyIfNan(valuedict['flow'])
            updict['water_level_staff_gauge_calibrated'] = emptyIfNan(valuedict['level'])
            updict['published'] = emptyIfNan(valuedict['pub_status'])
            
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
    print('Completed station data updates')

# %% ===== Posting new data csvs =====

# Defining column mappings
file_mappings = {
    'station_id':'STATION_NUMBER',
    'date': 'Date',
    'water_flow_calibrated_mps': 'flow', 
    'water_level_staff_gauge_calibrated': 'level', 
    'published': 'pub_status', 
    'owner': OWNER_ID
}

# File names of posting csvs
fnames = [file for file in os.listdir(data_temp_path) if file.endswith('csv')]

# Empty list to store the filenames of cleaning CSV
fclean = []
errstats = []

if len(fnames) == 0:
    print('No new data files to post. Process complete.')
else:
    # Calling the client to post each file
    for name in fnames:
        fpath = data_temp_path + '/' + name
        try:
            client.post_csv_file(fpath, get_surface_water_mapping(file_mappings))
            fclean.extend([name])
            print('Uploaded new data from file: ' + name)
        except:
            print('Error with station: ' + name)
            errstats.extend([name])
    print('Completed new data posting')

# Cleaning temporary directories
for name in fclean:
    print('Cleaning file: ' + name)
    os.remove(data_temp_path + '/' + name)

# %%
