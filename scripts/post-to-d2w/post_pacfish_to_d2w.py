# Author: Saeesh Mangwani
# Date: 19/05/2022

# Description: Exporting pacfish data to CSV and posting to depth2water

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
creds = load(open('options/client_credentials.json',))

# Filepaths
fpaths = load(open('options/filepaths.json', ))

# Path to station file
station_file_path = fpaths['pacfish-metadata']

# Path to daily data file
daily_data_path = fpaths['update-data-dir'] + '/pacfish-daily.csv'

# Path to temporary directory for storing posting files
data_temp_path = fpaths['temp-dir'] + '/pacfish'


# %% ===== Initializing update parameters =====
start_date = options.startdate
end_date = options.enddate
print('Start Date: ' + start_date)
print('End Date: ' + end_date)

#%% Manual data inputs - for use when script testing
# start_date = (datetime.today() - timedelta(days=331)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.today().strftime("%Y-%m-%dT00:00:00-00:00")

# %% ===== Initializing client =====

# Setting up logging
# logging.basicConfig(level=logging.DEBUG)

# Setting owner ID for this database
OWNER_ID = 9

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

# Reading the most recent station metadata file
metadata = pd.read_csv(station_file_path)

# Daily data CSV file
try:
    daily = pd.read_csv(daily_data_path, parse_dates=[2])
    # Filtering daily dataset to only include stations reference in the metadata file (this ensures that data with no stations are excluded)
    daily = daily[daily['station_number'].isin(metadata['station_id'])]
except:
    print('No daily dataset found')
    daily = None

# %% ===== Script helper functions =====

# Helper function to quickly access values from the downloaded metadata file for a station
def pull_metadata(statid, varname):
    return metadata.loc[metadata['station_id'] == statid].iloc[0][varname]

# Helper function to quickly access values from a "result" dictionary, obtained from a station-specific d2w query
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
    # Ensuring the datetime column is a string to begin with
    outdf.datetime = outdf.datetime.astype('string')
    # Enjorcing types
    outdf = outdf.astype(dtype_dict)
    # Ensuring datetime is correctly formatted as a date
    outdf['datetime'] = pd.to_datetime(outdf.datetime, utc=True)
    return outdf

# Function that returns an empty string if the input is NaN
def emptyIfNan(x):
    if type(x) == str: 
        return '' if x == 'nan' else x
    return '' if isnan(x) else x

# %% ===== Checking stations on d2w =====

# Getting unique station IDs from the metadata file:
stat_ids = metadata['station_id'].unique()

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
            'location_name': pull_metadata(stat, 'station_name'),
            'longitude': pull_metadata(stat, 'long'),
            'latitude': pull_metadata(stat, 'lat'),
            'prov_terr_state_lc': 'BC'
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')
        # Calculating station active status (giving a 1-year leeway period)
        last_yr = pull_metadata(stat, 'end_date')
        # If any of the last years are greater than or equal to the 1-minus this year, changing active status
        isactive = datetime.strptime(last_yr, '%Y/%m/%d %H:%M').year >= (datetime.today().year - 1)
        # Getting either active/discontinued status from isactive
        curr_station_status = 'ACTIVE' if isactive else 'DISCONTINUED'
        # If any of the parameters are not the same between metadata and those stored on file, updating
        isdiscrepant = any([
            curr_station_status != pull_statquery(result, 'monitoring_status'),
            round(pull_metadata(stat, 'long'), 5) != round(pull_statquery(result, 'longitude'), 5),
            round(pull_metadata(stat, 'lat'), 5) != round(pull_statquery(result, 'latitude'), 5)
        ])
        if isdiscrepant:
            print('Station status has changed - updating...')
            updict = result['results'][0]
            # updict['owner'] = OWNER_ID
            updict['monitoring_status'] = emptyIfNan(curr_station_status)
            updict['longitude'] = emptyIfNan(pull_metadata(stat, 'long'))
            updict['latitude'] = emptyIfNan(pull_metadata(stat, 'lat'))
            client.update_station(id=updict['id'], data=updict)
        else:
            print('No changes made to station ' + stat)

print('Station updates complete')

# %% ===== Categorizing new data for update or post =====
if daily is None:
    print('No daily data available in this time range. Skipping data update...')
else:
    # Getting the station of ids of all stations included in the current update dataset
    stat_ids = daily['station_number'].unique()
    for stat in stat_ids:
        print(stat)
        # Getting all the new data for this station
        updatedf = daily[daily['station_number'] == stat]
        
        # Getting all current data for the station within the data range
        curr_data = get_surface_water_data_multipage(
            station_id=stat, 
            # Start date as 1 before the current date (prevents edge issues created repeated data points)
            start_date=(pd.to_datetime(start_date) - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-00:00"), 
            end_date=end_date
        )

        # If there is no current data present, just pushing new data directly to a csv to be posted (i.e no direct database updates required)
        if len(curr_data) == 0:
            # Writing all new data to csv for posting
            if updatedf.shape[0] > 0:
                print('No data in this time period for station ' + stat + '. Writing new data to post...')
                fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
                updatedf.to_csv(fpath, index=False, na_rep='NA')
            else:
                print('No rows to post for station ' + stat)
            # Skipping iteration to the next station, as no updates are needed
            continue

        # If there is current data, getting only relevant columns
        keylist = ['station_id','location_name']
        for i in range(0, len(curr_data)): 
            format_get_station_metadata(curr_data[i], keylist)

        # Converting to dataframe
        currdf = pd.DataFrame(curr_data, index = None)

        # Formatting to new data structure
        currdf = format_curr_data_df(currdf)

        # Using an indicator left join to see which rows are new and which already exist
        left_joined = updatedf.merge(currdf, how='left', indicator=True, on=['station_number', 'datetime'])

        # Those that say left-only are new, and therefore need to be directly uploaded
        addrows = left_joined[left_joined._merge == 'left_only'][['station_number', 'datetime']]
        addrows = addrows.merge(updatedf, how='left', on=['station_number', 'datetime'])

        # Those that say both may need to be updated/edited on the server directly
        updaterows = left_joined[left_joined._merge == 'both'][['station_number', 'datetime']]
        updaterows = updaterows.merge(updatedf, how='left', on=['station_number', 'datetime'])
        # Of the update rows, joining on all columns to only include for update those rows where some value has changed (i.e there are differences between the downloaded and stored versions). In this case, the newly downloaded version takes precedence (note that station name is dropped as it is not a value column)
        left_joined = updaterows.drop('station_name', axis = 1).merge(currdf.drop('station_name', axis = 1), how='left', indicator=True)
        updaterows = left_joined[left_joined._merge == 'left_only'].drop('_merge', axis = 1)

        # For the rows that need updating
        for i in range(0, updaterows.shape[0]):
            # Getting the date of the update row
            querydate = updaterows.iloc[i,]['datetime']
            
            # Obtaining the data dictionary already stored on the server for this date
            updict = dict()
            for row in curr_data:
                if pd.to_datetime(row['datetime']).strftime('%Y-%m-%d') == querydate.strftime('%Y-%m-%d'):
                    updict = row
                    break
            
            # Getting the new values to edit in this datapoint as a dictionary
            valuedict = updaterows.to_dict('records')[i]

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

    print('Time series updates complete')

# %% ===== Posting new data csvs =====

# Defining column mappings
file_mappings = {
    'station_id':'station_number',
    'datetime': 'datetime',
    'water_level_staff_gauge_calibrated': 'water_level',
    'water_level_compensated_m': 'sensor_depth',
    'temperature_c': 'water_temperature',
    'barometric_pressure_m': 'pressure',
    'owner': OWNER_ID,
    'comments': '',
    'published': True
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
