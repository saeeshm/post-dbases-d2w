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

# Which database is being update?
schema = 'hydat'

# The D2W owner ID for this database
OWNER_ID = 7

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
    monitoring_type='SURFACE_WATER',
    # Paths to metadata and update/posting data
    metadata_path = station_file_path,
    postdf_path = daily_data_path,
    # Type specifications for metadata and posting data
    metadata_dtypes = {
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
    },
    postdf_dtypes={
        'STATION_NUMBER': 'str',
        'Date': 'datetime64',
        'flow': 'float64',
        'level': 'float64',
        'pub_status': 'str'
    },
    # Column that uniquely identifies stations in the metadata table
    metadata_statcol='STATION_NUMBER',
    # Column that uniquely identifies stations in the posting table
    postdf_statcol='STATION_NUMBER',
    # Column that uniquely identifies dates in the posting table
    postdf_datecol='Date',
    # A mapping dictionary connecting column names in the d2w server to column names in the posting file. 
    ps_col_mappings={
        'station_id':'STATION_NUMBER',
        'datetime': 'Date',
        'water_flow_calibrated_mps': 'flow', 
        'water_level_staff_gauge_calibrated': 'level', 
        'published': 'pub_status'
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
    # Checking if the station is present
    result = client.get_station_by_station_id(stat)
     # Removing results that are not surface water stations
    result['results'] = [station for station in result['results'] if station['monitoring_type'] == postd2w.monitoring_type]
    # If not, creating it
    if len(result['results']) == 0:
        print('Creating station ' + stat)
        station_mapping = get_surface_water_station_mapping({
            'station_id': stat,
            'owner': OWNER_ID,
            'location_name': postd2w.pull_from_metadata(stat, 'STATION_NAME'),
            'longitude': postd2w.pull_from_metadata(stat, 'LONGITUDE'),
            'latitude': postd2w.pull_from_metadata(stat, 'LATITUDE'),
            'prov_terr_state_lc': 'BC'
        })
        new_station = client.create_station(station_mapping)
    else:
        print('Station ' + stat + ' already present')
        # Getting relevant parameters from the local metadata file for comparison
        metaparams = {
            # Getting either active/discontinued status from isactive
            'station_status': postd2w.pull_from_metadata(stat, 'STATION_STATUS'),
            'lat': postd2w.pull_from_metadata(stat, 'LATITUDE'),
            'long': postd2w.pull_from_metadata(stat, 'LONGITUDE')
        }
         # If any of the parameters are not the same between metadata and those stored on file, updating
        isdiscrepant = any([
            metaparams['station_status'] != pull_from_query(result, 'monitoring_status'),
            metaparams['long'] != pull_from_query(result, 'longitude'),
            metaparams['lat']!= pull_from_query(result, 'latitude')
        ])
        if isdiscrepant:
            print('Station status has changed - updating...')
            updict = result['results'][0]
            # updict['owner'] = OWNER_ID
            updict['monitoring_status'] = emptyIfNan(metaparams['station_status'])
            updict['longitude'] = emptyIfNan(metaparams['long'])
            updict['latitude'] = emptyIfNan(metaparams['lat'])
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
                updatedf.to_csv(fpath, index=False, na_rep='NA')
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
            statname_col = None
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
                updict[key] = emptyIfNan(valuedict[value])
            
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
file_mappings = postd2w.ps_col_mappings.copy()
file_mappings.update({
    'owner': OWNER_ID,
    'comments': ''
})

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
