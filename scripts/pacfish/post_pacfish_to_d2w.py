# Author: Saeesh Mangwani
# Date: 19/05/2022

# Description: Exporting pacfish data to CSV and posting to depth2water

#%% Loading libraries
# import os
# os.chdir('../..')
import logging
import numpy as np
from scripts.help_funcs import get_query_as_df
from depth2water import create_client, get_surface_water_mapping

# %% Initializing parameters

# Setting up logging
logging.basicConfig(level=logging.DEBUG)

# Initializing application parameters
USERNAME = "gwadmin"
PASSWORD = "kowe#0485"
CLIENT_ID = 'x4u9RdFzzSfYs4Dau1c2bdEZ66RtGMuRUe7OWX1L'
CLIENT_SECRET = 'qM3s44Uoyi6CePPLHKV6WG359JVVHtaDelSyQz40QNU1SFcVit2ApXqsS9djxdnDxLiTUA77wxb4TmUM2bpA4mqB0GTj2Lq5Vw3DIU8CuhLDIxPlDSwXMcSA6GQm6u25'
TEST_USER_ID = 24

# Creating a client
client = create_client(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, host = 'localhost:8000', scheme='http')

# %% Posting daily data



# Creating file mappings for these data
file_mappings = {
    'station_id':'station_id',
    # 'station': 'STATION_NAME',
    'datetime': 'datetime',
    'water_level_staff_gauge_calibrated': 'water_level',
    'water_level_compensated_m': 'sensor_depth',
    'temperature_c': 'water_temp',
    'barometric_pressure_m': 'pressure',
    'owner': TEST_USER_ID,
    'comments': ''
}

# Getting appropriate base mapping
surface_water_mapping = get_surface_water_mapping(file_mappings)

client.post_csv_file(
    'bedwell_test.csv',
    surface_water_mapping
)
# %%
