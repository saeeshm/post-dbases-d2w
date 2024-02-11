# %% ===== Loading libraries =====
import os
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
import logging
import pandas as pd
from json import load
from optparse import OptionParser
from datetime import datetime, timedelta
from scripts.post_to_d2w.PostD2W import PostD2W
from scripts.post_to_d2w.post_utils import *
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

# Which database is being update?
schema = 'ecclimate'

# The D2W owner ID for this database
OWNER_ID = 8

# Client credentials from JSON
creds = load(open('options/client_credentials.json',))

# Filepaths
fpaths = load(open('options/filepaths.json', ))

# Path to station file
station_file_path = fpaths[schema + '-metadata']

# Path to daily data file
daily_data_path = fpaths['update-data-dir'] + '/' + schema + '-daily.csv'

# Path to temporary directory for storing posting files
data_temp_path = fpaths['temp-dir'] + '/' + schema


# %% ===== Initializing posting class =====
postd2w = PostD2W(
    # Basic attributes
    schema=schema,
    monitoring_type='CLIMATE',
    # Paths to metadata and update/posting data
    metadata_path = station_file_path,
    postdf_path = daily_data_path,
    # Type specifications for metadata and posting data
    metadata_dtypes = {
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
    },
    postdf_dtypes={
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
    },
    # Column that uniquely identifies stations in the metadata table
    metadata_statcol='Station ID',
    # Column that uniquely identifies stations in the posting table
    postdf_statcol='ec_station_id',
    # Column that uniquely identifies dates in the posting table
    postdf_datecol='datetime',
    # A mapping dictionary connecting column names in the d2w server to column names in the posting file. 
    ps_col_mappings={
        'station_id':'ec_station_id',
        'datetime': 'datetime',
        'location_name': 'station_name',
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
    }
)
# %% ===== Initializing client =====

# Setting up logging
# logging.basicConfig(level=logging.DEBUG)

# Creating a client
client = create_client(
    username=creds['username'], 
    password=creds['password'], 
    client_id=creds['client_id'], 
    client_secret=creds['client_secret'], 
    host=creds['host'], 
    scheme=creds['scheme']
)

#%% Manual data inputs - for use when script testing
# start_date = (datetime.today() - timedelta(days=331)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.today().strftime("%Y-%m-%dT00:00:00-00:00")
# start_date = (datetime.strptime('2023-05-01', '%Y-%m-%d') - timedelta(days=31)).strftime("%Y-%m-%dT00:00:00-00:00")
# end_date =datetime.strptime('2023-05-01', '%Y-%m-%d').strftime("%Y-%m-%dT00:00:00-00:00")

#%% Setting update daterange
start_date = options.startdate
end_date = options.enddate
print('Start Date: ' + start_date)
print('End Date: ' + end_date)

# %% ===== Checking stations on d2w =====

# Getting unique station IDs from the metadata file:
stat_ids = postd2w.metadata[postd2w.metadata_statcol].unique()

for stat in stat_ids:
    # Checking if the station is present,
    result = client.get_station_by_station_id(stat, monitoring_type=postd2w.monitoring_type)
     # Removing results that are not the right type
    # result['results'] = [station for station in result['results'] if station['monitoring_type'] == postd2w.monitoring_type]
    # If not, creating it
    if len(result['results']) == 0:
        print('Creating station ' + stat)
        station_mapping = get_climate_station_mapping({
            'station_id': stat,
            'owner': OWNER_ID,
            'location_name': postd2w.pull_from_metadata(stat, 'Name'),
            'longitude': postd2w.pull_from_metadata(stat, 'Longitude (Decimal Degrees)'),
            'latitude': postd2w.pull_from_metadata(stat, 'Latitude (Decimal Degrees)'),
            'prov_terr_state_lc': 'BC'
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')
        # Calculating station active status (giving a 1-year leeway period)
        last_yrs = list(postd2w.pull_from_metadata(stat, ['DLY Last Year', 'HLY Last Year']))
        # If any of the years are greater than or equal to the previous year, setting isactive to true (This gives a 1-year leeway period, useful to ignore long periods of missing data/station inactivity)
        isactive = any([yr >= (datetime.today().year - 1) for yr in last_yrs])
        # Getting relevant parameters from the local metadata file for comparison
        metaparams = {
            # Getting either active/discontinued status from isactive
            'station_status': 'ACTIVE' if isactive else 'DISCONTINUED',
            'lat': postd2w.pull_from_metadata(stat, 'Latitude (Decimal Degrees)'),
            'long': postd2w.pull_from_metadata(stat, 'Longitude (Decimal Degrees)')
        }
        # If any of the parameters are not the same between metadata and those stored on file, updating
        isdiscrepant = any([
            metaparams['station_status'] != pull_from_query(result, 'monitoring_status'),
            metaparams['long'] != pull_from_query(result, 'longitude'),
            metaparams['lat'] != pull_from_query(result, 'latitude')
        ])
        if isdiscrepant:
            print('Station status has changed - updating...')
            updict = result['results'][0]
            # updict['owner'] = OWNER_ID
            # updict['monitoring_status'] = emptyIfNan(metaparams['station_status'])
            # updict['longitude'] = emptyIfNan(metaparams['long'])
            # updict['latitude'] = emptyIfNan(metaparams['lat'])
            updict['monitoring_status'] = metaparams['station_status']
            updict['longitude'] = metaparams['long']
            updict['latitude'] = metaparams['lat']
            client.update_station(id=updict['id'], data=updict)
        else:
            print('No changes made to station ' + stat)

print('Station updates complete')

# %% ===== Categorizing new data for update or post =====
if postd2w.postdf is None:
    print('No daily data available in this time range. Skipping data update...')
else:
    # Getting the station of ids of all stations included in the current update dataset
    stat_ids = postd2w.postdf[postd2w.postdf_statcol].unique()
    for stat in stat_ids:
        print(stat)
        # Getting all the new data for this station
        updatedf = postd2w.postdf[postd2w.postdf[postd2w.postdf_statcol] == stat]
        
        # Getting all current data for the station within the data range
        raw_resp = get_server_data_multipage(
            client=client,
            monitoring_type=postd2w.monitoring_type,
            station_id=stat, 
            start_date=(pd.to_datetime(start_date) - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-00:00"), 
            end_date=(pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-00:00")
        )

        # If there is no current data present, just pushing new data directly to a csv to be posted (i.e no direct database updates required)
        if len(raw_resp) == 0:
            # Writing all new data to csv for posting
            if updatedf.shape[0] > 0:
                print('No existing data in this time period for station ' + stat + '. Writing all new data to post...')
                fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
                updatedf.to_csv(fpath, index=False)
            else:
                print('No rows to post for station ' + stat)
            # Skipping iteration to the next station, as no updates are needed
            continue

        # Simplifying the response data dictionary
        keylist = ['station_id','location_name']
        curr_data = [simplify_queried_dict(datadict, keylist) for datadict in raw_resp]

        # Converting to dataframe
        querydf = pd.DataFrame(curr_data, index = None)

        # Formatting to match the update data
        querydf = format_queried_df(
            querydf=querydf, 
            cols_dict=postd2w.ps_col_mappings,
            dtype_dict=postd2w.postdf_dtypes,
            dtime_col=postd2w.postdf_datecol
        )

        # Separating rows that are totally new and need to be added (via a post) from those that already exist but have changed (need to be updated)
        (addrows, updaterows) = separate_add_vs_update_rows(
            updatedf=updatedf, 
            querydf=querydf, 
            statid_col=postd2w.postdf_statcol, 
            dtime_col=postd2w.postdf_datecol,
            collist = list(postd2w.ps_col_mappings.values()),
            statname_col = postd2w.ps_col_mappings['location_name']
        )

        # For each rows that needs updating:
        for i in range(0, updaterows.shape[0]):
            # Getting the date of the update row
            querydate = updaterows.iloc[i,][postd2w.postdf_datecol]
            
            # Obtaining the data dictionary already stored on the server for this date
            updict = dict()
            for row in curr_data:
                if pd.to_datetime(row['datetime']).strftime('%Y-%m-%d') == querydate.strftime('%Y-%m-%d'):
                    updict = row
                    break
            
            # Converting the update row to a dictionary (easier to pull out values)
            valuedict = updaterows.to_dict('records')[i]
            # Removing the ID and date columns - don't want these to constantly change.
            valuedict.pop(postd2w.postdf_statcol)
            valuedict.pop(postd2w.postdf_datecol)
            # Also removing the location name column, as this is set by the station table and so updates here are redundant
            if postd2w.ps_col_mappings['location_name'] in valuedict.keys():
                valuedict.pop(postd2w.ps_col_mappings['location_name'])

            # Updating values for every shared column (based on the provided mappings dictionary)
            for key, value in postd2w.ps_col_mappings.items():
                if(value not in valuedict.keys()): continue
                # updict[key] = emptyIfNan(valuedict[value])
                updict[key] = valuedict[value]
            
            # Posting updates
            client.update_climate_data(updict['id'], updict)
        else:
            print(str(updaterows.shape[0]) + ' rows updated for station ' + stat)

        # For those that are simple additions, writing to csv for posting
        if addrows.shape[0] > 0:
            fpath = data_temp_path + '/' + stat + '_' + datetime.today().strftime('%Y-%m-%d') + '.csv'
            addrows.to_csv(fpath, index=False)
            print(str(addrows.shape[0]) + ' rows to post for station ' + stat)
        else:
            print('0 rows to post for station ' + stat)

    print('Time series updates complete')

# %% ===== Posting new data csvs =====

# Defining column mappings
file_mappings = postd2w.ps_col_mappings.copy()
file_mappings.update({
    'owner': OWNER_ID,
    'comments': '',
    # Extra columns
    # 'water_temperature_c': '',
    # 'water_temperature_flag': '',
    'published': True
})

# File names of posting csvs
fnames = [file for file in os.listdir(data_temp_path) if file.endswith('csv')]

# # Empty list to store the filenames of cleaning CSV
# fclean = []
# errstats = []

# if len(fnames) == 0:
#     print('No new data files to post. Process complete.')
# else:
#     # Calling the client to post each file
#     for name in fnames:
#         fpath = data_temp_path + '/' + name
#         try:
#             client.post_csv_file(fpath, get_climate_mapping(file_mappings))
#             fclean.extend([name])
#             print('Uploaded new data from file: ' + name)
#         except:
#             print('Error with station: ' + name)
#             errstats.extend([name])
#     print('Completed new data posting')

# # Cleaning temporary directories
# for name in fclean:
#     print('Cleaning file: ' + name)
#     os.remove(data_temp_path + '/' + name)
# %%
