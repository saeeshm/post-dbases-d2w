# Author: Saeesh Mangwani
# Date: 19/05/2022

# Description: Exporting EC Climate data to CSV and posting to depth2water

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
from depth2water import create_client, get_climate_mapping, get_climate_station_mapping

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
station_file_path = fpaths['ecclimate-metadata']

# Path to daily data file
daily_data_path = fpaths['update-data-dir'] + '/ecclimate-daily.csv'

# Path to temporary directory for storing posting files
data_temp_path = fpaths['temp-dir'] + '/ecclimate'

# %% ===== Initializing update parameters =====
start_date = options.startdate
end_date = options.enddate

#%% Manual data inputs - for use when script testing
# start_date = (datetime.today() - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.today().strftime("%Y-%m-%dT00:00:00-00:00")

# %% ===== Initializing parameters =====

# Setting up logging
logging.basicConfig(level=logging.DEBUG)

# Setting owner ID for this database
OWNER_ID = 8

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
dtype_dict = {
    'Name': 'str',
    'Province': 'str',
    'Climate ID': 'str',
    'Station ID': 'str',
    'WMO ID': 'str',
    'TC ID': 'str',
    'Latitude (Decimal Degrees)': 'float64',
    'Longitude (Decimal Degrees)': 'float64',
    'Latitude': 'float64',
    'Longitude': 'float64',
    'Elevation (m)': 'float64',
    'First Year': 'int64',
    'Last Year': 'int64',
    'HLY First Year': 'float64',
    'HLY Last Year': 'float64',
    'DLY First Year': 'float64',
    'DLY Last Year': 'float64',
    'MLY First Year': 'float64',
    'MLY Last Year': 'float64',
}
metadata = metadata.astype(dtype_dict)

# Daily data CSV file
try:
    daily = pd.read_csv(daily_data_path, parse_dates=[2])
    # Setting types for relevant variables
    dtype_dict = {
        'ec_station_id': 'str',
        'station_name': 'str',
        'datetime': 'datetime64',
        'max_temp': 'float64',
        'max_temp_flag': 'str',
        'min_temp': 'float64',
        'min_temp_flag': 'str',
        'mean_temp': 'float64',
        'mean_temp_flag': 'str',
        'heat_deg_days': 'float64', 
        'heat_deg_days_flag': 'str',
        'cool_deg_days': 'float64', 
        'cool_deg_days_flag': 'str', 
        'total_rain': 'float64', 
        'total_rain_flag': 'str',  
        'total_snow': 'float64', 
        'total_snow_flag': 'str', 
        'total_precip': 'float64', 
        'total_precip_flag': 'str', 
        'snow_on_grnd': 'float64', 
        'snow_on_grnd_flag': 'str', 
        'dir_of_max_gust': 'float64', 
        'dir_of_max_gust_flag': 'str', 
        'spd_of_max_gust': 'float64', 
        'spd_of_max_gust_flag': 'str'
    }
    daily = daily.astype(dtype_dict)
    # Removing 'nan' string values that are populated
    daily = daily.applymap(lambda x: '' if (type(x) == str) & (x == 'nan') else x)
    # Filtering daily dataset to only include stations reference in the metadata file (this ensures that data with no stations are excluded)
    daily = daily[daily['ec_station_id'].isin(metadata['Station ID'])]
except:
    print('No daily dataset found. This script will only complete a metadata update')
    daily = None

# %% ===== Script helper functions =====

# Helper function to quickly access values from the downloaded metadata file for a station
def pull_metadata(statid, varname):
    return metadata.loc[metadata['Station ID'] == statid].iloc[0][varname]

# Helper function to quickly access values from a "result" dictionary, obtained from a station-specific d2w query
def pull_statquery(resultobj, varname):
    return resultobj['results'][0][varname]

# A helper function to read multipage responses from the get-function
def get_climate_data_multipage(station_id, start_date=None, end_date=None, url=None):
    resp = client.get_climate_data(station_id=station_id, start_date=start_date, end_date=end_date, url=url)
    outdata = resp['results']
    if resp['next'] is not None:
        print('There is another page, getting its data...')
        outdata.extend(get_climate_data_multipage(station_id, url=resp['next']))
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
        ['station_id', 'location_name', 'datetime', 
        'max_temperature_c', 'max_temp_flag', 
        'min_temperature_c', 'min_temperature_flag', 'mean_temperature_c', 'mean_temperature_flag', 'heat_degree_days_c', 'heat_degree_days_flag', 'cool_degree_days_c', 'cool_degree_days_flag', 
        'total_rain_mm', 'total_rain_flag', 
        'total_snow_cm', 'total_snow_flag', 
        'total_precipitation_mm', 'total_precipitation_flag', 'snow_on_ground_cm', 'snow_on_ground_flag', 'direction_max_gust_tens_degree', 'direction_max_gust_flag', 'speed_max_gust_kmh', 'speed_max_gust_flag']
    ]
    # Renaming to match the upload data table
    outdf.columns = ['ec_station_id', 'station_name', 'datetime', 
    'max_temp', 'max_temp_flag', 
    'min_temp', 'min_temp_flag',
    'mean_temp', 'mean_temp_flag',
    'heat_deg_days', 'heat_deg_days_flag',
    'cool_deg_days', 'cool_deg_days_flag',
    'total_rain', 'total_rain_flag', 
    'total_snow', 'total_snow_flag',
    'total_precip', 'total_precip_flag',
    'snow_on_grnd', 'snow_on_grnd_flag',
    'dir_of_max_gust', 'dir_of_max_gust_flag',
    'spd_of_max_gust', 'spd_of_max_gust_flag'
    ]

    # Setting variable types
    dtype_dict = {
        'ec_station_id': 'str',
        'station_name': 'str',
        'datetime': 'datetime64',
        'max_temp': 'float64',
        'max_temp_flag': 'str',
        'min_temp': 'float64',
        'min_temp_flag': 'str',
        'mean_temp': 'float64',
        'mean_temp_flag': 'str',
        'heat_deg_days': 'float64', 
        'heat_deg_days_flag': 'str',
        'cool_deg_days': 'float64', 
        'cool_deg_days_flag': 'str', 
        'total_rain': 'float64', 
        'total_rain_flag': 'str',  
        'total_snow': 'float64', 
        'total_snow_flag': 'str', 
        'total_precip': 'float64', 
        'total_precip_flag': 'str', 
        'snow_on_grnd': 'float64', 
        'snow_on_grnd_flag': 'str', 
        'dir_of_max_gust': 'float64', 
        'dir_of_max_gust_flag': 'str', 
        'spd_of_max_gust': 'float64', 
        'spd_of_max_gust_flag': 'str'
    }
    outdf = outdf.astype(dtype_dict)
    outdf['datetime'] = pd.to_datetime(outdf.datetime, utc=True)
    # Replacing "None" character values that randomly get created with the empty string
    outdf.replace('None', '', inplace=True)
    return outdf

# Function that returns an empty string if the input is NaN
def emptyIfNan(x):
    if type(x) == str: 
        return '' if x == 'nan' else x
    return '' if isnan(x) else x
# %% ===== Checking stations on d2w =====

# Getting unique station IDs:
stat_ids = metadata['Station ID'].unique()

for stat in stat_ids:
    # Checking if the station is present
    result = client.get_station_by_station_id(stat)
    # Removing results that are not climate stations
    result['results'] = [station for station in result['results'] if station['monitoring_type'] == 'CLIMATE']
    # If not, creating it
    if len(result['results']) == 0:
        print('Creating station ' + stat)
        station_mapping = get_climate_station_mapping({
            'station_id': stat,
            'owner': OWNER_ID,
            'location_name': pull_metadata(stat, 'Name'),
            'longitude': pull_metadata(stat, 'Longitude (Decimal Degrees)'),
            'latitude': pull_metadata(stat, 'Latitude (Decimal Degrees)'),
            'prov_terr_state_lc': 'BC',
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')
        # Calculating station active status (giving a 1-year leeway period)
        last_yrs = list(pull_metadata(stat, ['DLY Last Year', 'HLY Last Year']))
        # If any of the last years are greater than or equal to the last year, setting isactive to true (This gives a 1-year leeway period, useful to ignore long periods of missing data/station inactivity)
        isactive = any([yr >= (datetime.today().year - 1) for yr in last_yrs])
        # Getting either active/discontinued status from isactive
        curr_station_status = 'ACTIVE' if isactive else 'DISCONTINUED'
        # If any of the parameters are not the same between metadata and those stored on file, updating
        isdiscrepant = any([
            curr_station_status != pull_statquery(result, 'monitoring_status'),
            pull_metadata(stat, 'Elevation (m)') != pull_statquery(result, 'ground_elevation_m'),
            round(pull_metadata(stat, 'Longitude (Decimal Degrees)'), 5) != round(pull_statquery(result, 'longitude'), 5),
            round(pull_metadata(stat, 'Latitude (Decimal Degrees)'), 5) != round(pull_statquery(result, 'latitude'), 5)
        ])
        if isdiscrepant:
            print('Station status has changed - updating...')
            updict = result['results'][0]
            updict['monitoring_status'] = emptyIfNan(curr_station_status)
            updict['ground_elevation_m'] = emptyIfNan(pull_metadata(stat, 'Elevation (m)'))
            updict['longitude'] = emptyIfNan(pull_metadata(stat, 'Longitude (Decimal Degrees)'))
            updict['latitude'] = emptyIfNan(pull_metadata(stat, 'Latitude (Decimal Degrees)'))
            # Fixing an issue with the measurement frequency if relevant
            updict['measurement_frequency'] = emptyIfNan(None if updict['measurement_frequency'] is None else str.replace(updict['measurement_frequency'], '-', '/'))
            client.update_station(id=updict['id'], data=updict)
        else:
            print('No changes made to station ' + stat)

print('Station updates complete')

# %%

# %% ===== Categorizing new data for update or post =====

if daily is None:
    print('No daily data available in this time range. Skipping data update...')
else:
    # Getting the station of ids of all stations included in the current update dataset 
    stat_ids = daily['ec_station_id'].unique()
    # temp = stat_ids
    # stat_ids = stat_ids[0:10]
    for stat in stat_ids:
    
        # Getting all the new data for this station
        updatedf = daily[daily['ec_station_id'] == stat]

        # Getting all current data for the station within the data range
        curr_data = get_climate_data_multipage(
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
        # Ensuring datetime format is not specified as anything
        currdf['datetime'] = pd.to_datetime(currdf['datetime'].dt.date)

        # Using an indicator left join to see which rows are new and which already exist
        left_joined = updatedf.merge(currdf, how='left', indicator=True, on=['ec_station_id', 'datetime'])

        # Those that say left-only new, and therefore need to be directly uploaded
        addrows = left_joined[left_joined._merge == 'left_only'][['ec_station_id', 'datetime']]
        addrows = addrows.merge(updatedf, how='left', on=['ec_station_id', 'datetime'])

        # Those that say both need to be updated/edited on the server directly
        updaterows = left_joined[left_joined._merge == 'both'][['ec_station_id', 'datetime']]
        updaterows = updaterows.merge(updatedf, how='left', on=['ec_station_id', 'datetime'])
        # Of the update rows, joining on all columns to only include for update those rows where some value has changed (i.e there are differences between the downloaded and stored versions). In this case, the newly downloaded version takes precedence
        left_joined = updaterows.merge(currdf, how='left', indicator=True)
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
            updict['max_temperature_c'] = emptyIfNan(valuedict['max_temp'])
            updict['max_temp_flag'] = emptyIfNan(valuedict['max_temp_flag'])
            updict['min_temperature_c'] = emptyIfNan(valuedict['min_temp'])
            updict['min_temperature_flag'] = emptyIfNan(valuedict['min_temp_flag'])
            updict['mean_temperature_c'] = emptyIfNan(valuedict['mean_temp'])
            updict['mean_temperature_flag'] = emptyIfNan(valuedict['mean_temp_flag'])
            updict['heat_degree_days_c'] = emptyIfNan(valuedict['heat_deg_days'])
            updict['heat_degree_days_flag'] = emptyIfNan(valuedict['heat_deg_days_flag'])
            updict['cool_degree_days_c'] = emptyIfNan(valuedict['cool_deg_days'])
            updict['cool_degree_days_flag'] = emptyIfNan(valuedict['cool_deg_days_flag'])
            updict['total_rain_mm'] = emptyIfNan(valuedict['total_rain'])
            updict['total_rain_flag'] = emptyIfNan(valuedict['total_rain_flag'])
            updict['total_snow_cm'] = emptyIfNan(valuedict['total_snow'])
            updict['total_snow_flag'] = emptyIfNan(valuedict['total_snow_flag'])
            updict['total_precipitation_mm'] = emptyIfNan(valuedict['total_precip'])
            updict['total_precipitation_flag'] = emptyIfNan(valuedict['total_precip_flag'])
            updict['snow_on_ground_cm'] = emptyIfNan(valuedict['snow_on_grnd'])
            updict['snow_on_ground_flag'] = emptyIfNan(valuedict['snow_on_grnd_flag'])
            updict['direction_max_gust_tens_degree'] = emptyIfNan(valuedict['dir_of_max_gust'])
            updict['direction_max_gust_flag'] = emptyIfNan(valuedict['dir_of_max_gust_flag'])
            updict['speed_max_gust_kmh'] = emptyIfNan(valuedict['spd_of_max_gust'])
            updict['speed_max_gust_flag'] = emptyIfNan(valuedict['spd_of_max_gust_flag'])
            
            # Posting updates
            client.update_climate_data(updict['id'], updict)
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

# %% ===== Posting newly data csvs =====

# Defining column mappings
file_mappings = {
    'station_id':'ec_station_id',
    'datetime': 'datetime',
    # 'station': 'STATION_NAME',
    'max_temperature_c': 'max_temp', 
    'max_temp_flag': 'max_temp_flag', 
    'min_temperature_c': 'min_temp', 
    'min_temperature_flag': 'min_temp_flag', 
    'mean_temperature_c': 'mean_temp', 
    'mean_temperature_flag': 'mean_temp_flag', 
    'heat_degree_days_c': 'heat_deg_days', 
    'heat_degree_days_flag': 'heat_deg_days_flag', 
    'cool_degree_days_c': 'cool_deg_days', 
    'cool_degree_days_flag': 'cool_deg_days_flag', 
    'total_rain_mm': 'total_rain', 
    'total_rain_flag': 'total_rain_flag', 
    'total_snow_cm': 'total_snow', 
    'total_snow_flag': 'total_snow_flag', 
    'total_precipitation_mm': 'total_precip', 
    'total_precipitation_flag': 'total_precip_flag', 
    'snow_on_ground_cm': 'snow_on_grnd', 
    'snow_on_ground_flag': 'snow_on_grnd_flag', 
    'direction_max_gust_tens_degree': 'dir_of_max_gust', 
    'direction_max_gust_flag': 'dir_of_max_gust_flag', 
    'speed_max_gust_kmh': 'spd_of_max_gust', 
    'speed_max_gust_flag': 'spd_of_max_gust_flag',
    'owner': OWNER_ID,
    'published': True,
    # Extra params that are unnecessary
    'water_temperature_c': '',
    'water_temperature_flag': '',
    'comments': ''
}

# File names of posting csvs
fnames = [file for file in os.listdir(data_temp_path) if file.endswith('csv')]

# Empty list to store the filenames of cleaning CSV
fclean = []
errstats = []

if len(fnames) == 0:
    print('No new data files to post')
else:
    # Calling the client to post each file
    for name in fnames:
        fpath = data_temp_path + '/' + name
        try:
            client.post_csv_file(fpath, get_climate_mapping(file_mappings))
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
