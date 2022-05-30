# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: Helper functions for posting databases to Depth2water

# ==== Libraries ====

# ==== Helper functions ====
format_simple_query <- function(schema, table, date_col = NULL, start_date = NULL, end_date = NULL){
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
