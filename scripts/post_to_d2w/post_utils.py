from numpy import isnan, nan
import pandas as pd

# Helper function to quickly access values from a "result" dictionary, obtained from a station-specific d2w query
def pull_from_query(resultobj, varname, roundfigs=5):
    value = resultobj['results'][0][varname]
    if isinstance(value, (int, float)):
        value = round(value, roundfigs)
    return(value)

# Function that returns an empty string if the input is NaN
def emptyIfNan(x):
    if isinstance(x, (float, int)): 
        return '' if isnan(x) else x
    elif isinstance(x, str):
        return '' if x == 'nan' else x
    else:
        return x

def get_server_data_multipage(client, monitoring_type, station_id, start_date=None, end_date=None, url=None):
    # Different get function for different data types
    if(monitoring_type == 'SURFACE_WATER'):    
        resp = client.get_surface_water_data(station_id=station_id, start_date=start_date, end_date=end_date, url=url)
    elif(monitoring_type == 'CLIMATE'):
        resp = client.get_climate_data(station_id=station_id, start_date=start_date, end_date=end_date, url=url)
    outdata = resp['results']
    # Calling the function recursively to get the next page if present
    if resp['next'] is not None:
        print('There is another page, getting its data...')
        outdata.extend(get_server_data_multipage(client, monitoring_type, station_id, url=resp['next']))
    # Returning the full queried data dictionary
    return outdata

# Individual data points are returned as dictionaries when the d2w server is queried, and each dictionary contains a subdirectory called "station", where the metadata is stored. This function simplifies a d2w data dictionary to a non-nested, by removing only important attributes from station, dropping the rest, and placing the attributes back at the same level as the rest of the data. 
def simplify_queried_dict(datadict, keylist):
    # Making a copy prior to manipulation
    rawdat = dict(datadict)
    # Removing requested attributes from the "station" sub-dictionary, as specified by the keylist
    updatedict = {key: rawdat['station'][key] for key in keylist}
    # Dropping the station sub-dictionary
    rawdat.pop('station')
    # Adding back the extracted attributes
    rawdat.update(updatedict)
    # Returning
    return rawdat

def format_queried_df(querydf, cols_dict, dtype_dict, dtime_col):
    qdf = querydf.copy()
    # If there are no rows, returning as-is
    if qdf.shape[1] == 0 : return qdf
    # Selecting only relevant columns
    qdf = qdf[list(cols_dict.keys())]
    # Renaming all columns with names consistent with the update dataset
    qdf.columns = list(cols_dict.values())
    # Replacing all "None" values with empty strings - None is the default NA value returned by the server
    qdf.replace('None', '', inplace=True)
    # Setting column types - datetime set as a string initially
    dtype_dict[dtime_col] = 'str'
    qdf = qdf.astype(dtype_dict)
    # Ensuring datetime is correctly formatted as a date
    qdf.loc[:, dtime_col] = pd.to_datetime(pd.to_datetime(qdf[dtime_col], utc=False).dt.date)
    return qdf

def separate_add_vs_update_rows(updatedf, querydf, statid_col, dtime_col, collist, statname_col=None, roundfigs=5):
    # Keeping only columns named in the mappings col-list - these are the only ones that see updates and changes between the new data and the server data
    updatedf = updatedf[collist]
    querydf = querydf[collist]
    
    # Rounding all numbers to 5 significant figures to make comparisons less subject to small variations
    updatedf = updatedf.round(roundfigs)
    querydf = querydf.round(roundfigs)

    # Replacing all "None" characters with empty strings for the sake of comparison
    querydf = querydf.replace('None', '')
    updatedf = updatedf.replace('None', '')

    # Using an indicator left join to see which rows from the update table are new and which already exist
    left_joined = updatedf.merge(querydf, how='left', indicator=True, on=[statid_col, dtime_col])

    # Those that are "left-only" only exist in the update table, and therefore need to be directly uploaded. First indexing only the id and datetime columns for these rows
    addrows = left_joined.loc[left_joined._merge == 'left_only', ].loc[:, [statid_col, dtime_col]]
    # Then taking the rest of the information from the update table
    addrows = addrows.merge(updatedf, how='left', on=[statid_col, dtime_col])

     # Those that say "both" may need to be updated/edited if any data has changed, not associated with the date/time. Separating only these rows now
    update_index_table = left_joined.loc[left_joined._merge == 'both', ].loc[:, [statid_col, dtime_col]]
    update_index_table = update_index_table.merge(updatedf, how='left', on=[statid_col, dtime_col])

    # On these rows, performing a join using all columns to flag which rows had a value change (i.e there are differences between the stored and new/updated versions). In this case, the newly downloaded version takes precedence.
    if statname_col is None:
        update_index_table = update_index_table.merge(querydf, how='left', indicator=True)
    else:
         update_index_table = update_index_table.drop(statname_col, axis=1).merge(querydf.drop(statname_col, axis=1), how='left', indicator=True)
    # Looking for ones that say left_only - these are the ones that have changed. Everything that says both has remained the same.
    updaterows = update_index_table[update_index_table._merge == 'left_only'].drop('_merge', axis = 1)

    # Returning add and update rows as a tuple
    return (addrows, updaterows)