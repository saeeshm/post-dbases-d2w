
#%% Importing modules
from json import load
import pandas as pd
import psycopg2

#%% Function that exports the results of a database query to csv
def export_query_to_csv(creds_path, output_path, query):
    # Reading local database credentials
    creds = load(open(creds_path, ))

    # Connecting to dbase
    conn = psycopg2.connect(
        host = creds['host'],
        database = creds['dbname'],
        user = creds['user'],
        password = creds['password']
    )
    cur = conn.cursor()

    # Output query
    outputquery = 'copy ({0}) to stdout with csv header'.format(query)

    # Writing to csv
    with open(output_path, 'w') as file:
        cur.copy_expert(outputquery, file)
    # Closing connection
    conn.close()

#%% A function that gets the result of a query as a dataframe object
def get_query_as_df(creds_path, query):
    # Reading local database credentials
    creds = load(open(creds_path, ))

    # Connecting to dbase
    conn = psycopg2.connect(
        host = creds['host'],
        database = creds['dbname'],
        user = creds['user'],
        password = creds['password']
    )

    # Getting data
    df = pd.read_sql(query, conn)
    
    conn.close()
    return df





# %%
