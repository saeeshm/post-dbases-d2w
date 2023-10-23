import pandas as pd

class PostD2W:
    def __init__(self, schema, monitoring_type, metadata_path, metadata_dtypes, postdf_path, postdf_dtypes, metadata_statcol, postdf_statcol, postdf_datecol, ps_col_mappings):        
        # Setting attributes 
        self.schema = schema
        self.monitoring_type = monitoring_type
        self.metadata_statcol = metadata_statcol
        self.postdf_statcol = postdf_statcol
        self.postdf_datecol = postdf_datecol
        self.postdf_dtypes = postdf_dtypes

        # Setting the base column mappings attribute
        self.ps_col_mappings = ps_col_mappings

        # Reading metadata
        self.metadata = pd.read_csv(
            metadata_path, 
            # Splitting out non-date columns
            dtype={key:value for key, value in metadata_dtypes.items() if value != 'datetime64'}, 
            # Passing datetime columns to parse dates
            parse_dates=[key for key, value in metadata_dtypes.items() if value == 'datetime64']
        )
        self.metadata = self.metadata.astype(metadata_dtypes)

        # Reading table of data to post/update
        try:
            self.postdf = pd.read_csv(postdf_path)
            # Setting types - ensuring the date column is initially a string
            postdf_dtypes[postdf_datecol] = 'str'
            self.postdf = self.postdf.astype(postdf_dtypes)
            # Converting the date column tx`o acorrectly formatted YMD date
            self.postdf.loc[:, postdf_datecol] = pd.to_datetime(pd.to_datetime(self.postdf[postdf_datecol], utc=False).dt.date)
            # Filtering daily dataset to only include stations reference in the metadata file (this ensures that data with no stations are excluded)
            self.postdf = self.postdf[self.postdf[self.postdf_statcol].isin(self.metadata[self.metadata_statcol])]
            # Removing 'nan' string values that are populated
            self.postdf = self.postdf.applymap(lambda x: '' if (type(x) == str) & (x == 'nan') else x)
        except:
            # If the daily data read fails, printing a status message and saving this as None.
            print('No daily dataset found')
            self.postdf = None
    
    def __str__(self):
        outstr = "Posting D2W object for database: " + self.schema + '\n' + 'Metadata station ID: ' + self.metadata_statcol + '\n' + 'Posting table station ID: ' + self.postdf_statcol
        return(outstr)

    # Helper function to quickly access values from the downloaded metadata file for a station
    def pull_from_metadata(self, statid, varname, roundfigs=5):
        value = self.metadata.loc[self.metadata[self.metadata_statcol] == statid].iloc[0][varname]
        if isinstance(value, (int, float)):
            value = round(value, roundfigs)
        return(value)