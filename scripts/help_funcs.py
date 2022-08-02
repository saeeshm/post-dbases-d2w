
#%% Importing modules
from json import load
import pandas as pd
import psycopg2

#%% Formats a simple get query including date filtering if required
def format_simple_query(schema, table, date_col = None, start_date = None, end_date = None):
    # Base query
    query = 'select * from ' + schema + '.' + table
    if (start_date is not None) and (end_date is not None):
        query = {
            query + '\n' + 
            'where "' + date_col + '" >= \'' + start_date + '\'' +
            'and "' + date_col + '" <= \'' + end_date + '\''
        }
        query = query.pop()
    elif start_date is not None:
        query = {
            query + '\n' + 
            'where "' + date_col + '" >= \'' + start_date + '\''
        }
        query = query.pop()
    elif end_date is not None:
        query = {
            query + '\n' + 
            'where "' + date_col + '" <= \'' + end_date + '\''
        }
        query = query.pop()
    return query


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