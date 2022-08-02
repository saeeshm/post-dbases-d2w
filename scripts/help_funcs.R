# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: Helper functions for posting databases to Depth2water

# ==== Libraries ====

# ==== Helper functions ====

# Formats a simple get query using a schema, table and date range. Pulls all
# columns
format_simple_query <- function(schema, table, date_col = NULL, 
                                start_date = NULL, end_date = NULL){
  # Base query
  query <- paste0('select * from ', schema, '.', table)
  # If dates are given
  if(!is.null(start_date) & !is.null(end_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" >= \'', start_date, '\'',
      ' and "', date_col, '" <= \'', end_date, '\''
    )
  }else if(!is.null(start_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" >= \'', start_date, '\''
    )
  }else if(!is.null(end_date)){
    query <- paste0(
      query, '\n',
      'where "', date_col, '" <= \'', end_date, '\''
    )
  }
  return(query)
}

# Infix function for returning a new value if current value is NULL (thank you
# Hadley Wikham)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# Function for creating an new directory if it doesn't exist, and returning a
# statement if it does
dir_check_create <- function(path, reset = F){
  if (!dir.exists(path)) {
    dir.create(path)
    return(paste('Created', path))
  }else if (reset){
    unlink(path, recursive = T)
    dir.create(path)
    return(paste('Reset directory', path))
  }else{
    return(paste(path, 'already exists'))
  }
}

