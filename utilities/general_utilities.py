# load libraries
import pandas as pd
import re, datetime, dateutil, random, math, statistics
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# Set broadcast timeout
# Set broadcast timeout longer than default
broadcast_timeout = 1800
print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)


# define functions
# define miscellaneous UDFs
# Median function
def median(value_list):
  # Return None if not a valid non-zero-length list
  if(not(isinstance(value_list, list)) or (value_list is None) or (len(value_list) < 1)):
    return None
  # If valid list, return median
  return statistics.median(value_list)

# Median UDF
median_udf = F.udf(median, FloatType())


# Clean string before mapping concept
# Function to get cleaned string
def clean_string(input_string):
  """Clean an arbitrary input string

  Parameters:
  input_string (str): An arbitrary input string

  Returns:
  str: Cleaned string

  """
  # Return empty string if input_string is None
  if input_string is None:
    return ''
  
  # Characters to remove
  remove_char = ['.', '*', ':', '-', '(', ')']
  
  # Remove non-alphanumeric characters
  cleaned_string = input_string
  for char_ in remove_char:
    cleaned_string = cleaned_string.replace(char_, '')
  
  # Lowercase, remove leading and trailing whitespace, and replace duplicate spaces within the string
  cleaned_string = re.sub(' +', ' ', cleaned_string.strip().lower())
  
  return cleaned_string

# Create a UDF
clean_string_udf = F.udf(clean_string, StringType())


# Zero-pad the front of an arbitrary string/integer
def zero_pad_front(id_, total_length=3):
  """Zero-pad the front of an arbitrary string/int input to a specified length

  Parameters:
  id_ (str or int): An arbitrary input string or integer
  total_length (int): Truncate (from the front) the result to this length

  Returns:
  str: Zero-padded string or integer, truncated to total_length

  """
  string_id = str(id_)
  return (total_length*'0' + string_id)[-total_length:]

# Define UDF
zero_pad_front_udf = F.udf(zero_pad_front, StringType())

# Register UDF so it can be used in SQL statements
spark.udf.register("zero_pad_front", zero_pad_front)


# Get a random file name (for caching)
def get_random_file_name():
  """Generate a random file name based on current time stamp and a random number
  
  Parameters:
  None
  
  Returns:
  str: random file name
  
  """
  # Generate string based on current timestamp
  current_timestamp = list(datetime.datetime.now().timetuple())[:6]
  current_timestamp = [
    str(t) if i == 0 else zero_pad_front(t, 2) for i, t in enumerate(current_timestamp)]
  current_timestamp = ''.join(current_timestamp)
  
  # Get a random number to use in filename
  random_number = str(random.random()).split('.')[-1]
  
  return '_'.join([current_timestamp, random_number])

  # Get list of permanent tables in the specified sandbox
  def get_permanent_tables_list(sandbox_db='rdp_phi_sandbox'):
  """Get list of permanent tables in specified database
  
  Parameters:
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  
  Returns:
  list: List of permanent tables in specified sandbox db
  
  """
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  permanent_tables_df = spark.sql(
    "SHOW TABLES IN {}".format(sandbox_db)).where(F.col('isTemporary') == False)
  permanent_tables_list = [row.tableName for row in permanent_tables_df.collect()]
  
  return permanent_tables_list


  # Get only clinical concept records for a provided dataframe
  def get_clinical_concept_records(df, cc_list):
  """From the provided dataframe, return only records for which one or more of the clinical concept
  values for the provided list of clinical concept labels is not False AND not null/None
  
  Parameters:
  df (PySpark df): A dataframe containing clinical concept columns specified in cc_list
  cc_list (list): A list of clinical concept labels. Note that df must contain columns for all
                  clinical concepts in the list
  
  Returns:
  PySpark df: A dataframe containing only records for which one or more of the clinical concept values
              is not False and not null/None
  
  """
  # Make sure cc_list is a list
  cc_list = cc_list if isinstance(cc_list, list) else [cc_list]
  
  # Make sure that df contains the required cc columns
  assert(all([s in df.columns for s in cc_list]))
  
  # Generate a query string to get only specified clinical concept conditions
  dtype_map = {k: v for k, v in df.dtypes}
  cc_query_list = ["({} = TRUE)".format(label)
                   if dtype_map[label] == 'boolean'
                   else "({} IS NOT NULL)".format(label)
                   for label in cc_list]
  cc_filter_query = """{}""".format(' OR '.join(cc_query_list))
  
  # Filter and return records corresponding to one or more clinical concepts
  results_df = df.where(cc_filter_query)
  return results_df


  # Adjust WHO score based on CRRT, ECMO, and pressor
  def get_adjusted_who_score(df, who_nominal_column, crrt_column, ecmo_column, pressor_column, discharge_disposition_column, new_column_name='who_score'):
  """Add a column to the provided dataframe with adjusted WHO score
  
  Parameters
  df (PySpark dataframe): Dataframe to which to add an adjusted WHO score column.
  who_nominal_column (str): Column containing unadjusted (i.e. based only on oxygen support) WHO
                            scores
  crrt_column (str): Column containing CRRT records (should be None or empty if CRRT not being used)
  ecmo_column (str): Column containing ECMO records (should be None or empty if ECMO not being used)
  pressor_column (str): Column containing count of pressor medication orders (None or 0 indicates the
                        patient is not on pressor medication)
  discharge_disposition_column (str): Column containing discharge disposition
  new_column_name (str): Name of new column to be added. Default is 'who_score'
  
  Returns:
  (PySpark dataframe): The original dataframe with an additional column for adjusted who score
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain appropriate columns
  assert(s in df.columns for s in [who_nominal_column, crrt_column, ecmo_column, pressor_column,
                                   discharge_disposition_column])
  
  # New column name must be a valid string
  assert(isinstance(new_column_name, str))
  
  # Utility function to use in determining adjusted WHO score
  def get_adjusted_who_score(who_nominal, crrt, ecmo, pressor, disch_disposition):
    # Assign 8 if discharge disposition is 'Expired'
    if(not(disch_disposition is None) and ('expired' in disch_disposition.lower())):
      return 8
    elif(who_nominal == 6):
      on_crrt = not(crrt is None) and not(crrt == '')
      on_ecmo = not(ecmo is None) and not(ecmo == '')
      on_pressor = not(pressor is None) and (pressor > 0)
      if(on_crrt|on_ecmo|on_pressor):
        return 7
      else:
        return 6
    else:
      return who_nominal
  
  # Create a UDF
  get_adjusted_who_score_udf = F.udf(get_adjusted_who_score, IntegerType())
  
  # Add WHO score column and return dataframe
  results_df = df \
    .withColumn(new_column_name,
      get_adjusted_who_score_udf(
        who_nominal_column, crrt_column, ecmo_column, pressor_column, discharge_disposition_column))
  
  return results_df


  # Aggregate data over partition columns
  def aggregate_data(df, partition_columns, aggregation_columns, order_by=None):
  """Aggregate data over specified partition columns
  
  Parameters:
  df (PySpark): Dataframe to aggregate
  partition_columns (str or list): Field(s) in df on which to partition. If partitioning on only one
                                   column, the column name can be provided as a str rather than a
                                   list
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  order_by (str or list): Field(s) in df to use for ordering records in each partition. If None, do
                          not order. If ordering on only one column, the column name can be provided
                          as a str rather than a list
  
  Result:
  PySpark df: Dataframe containing the aggregated results
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain specified partition columns
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  assert(all([s in df.columns for s in partition_columns]))
    
  # Perform validity checks on aggregation_columns
  assert(isinstance(aggregation_columns, dict))
  assert(all([s in df.columns for s in list(aggregation_columns.keys())]))
  valid_agg_functions = ['avg', 'collect_list', 'collect_set', 'concat_ws', 'count', 'first', 'last', 'max', 'mean', 'median', 'min', 'stddev', 'sum']
  for k in list(aggregation_columns.keys()):
    v = aggregation_columns[k]
    aggregation_columns[k] = v if isinstance(v, list) else [v]
    assert(all([s in valid_agg_functions for s in aggregation_columns[k]]))
  
  # order_by (if not None) must contain valid column names
  if(not(order_by is None)):
    order_by = order_by if isinstance(order_by, list) else [order_by]
    assert(all([s in df.columns for s in order_by]))
  
  # Define partition window
  w = Window.partitionBy(partition_columns)
  if(not(order_by is None)):
    w = w.orderBy(order_by).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  
  # Add aggregate columns
  results_df = df; new_columns = []
  for col, agg_func in aggregation_columns.items():
    for s in agg_func:
      # Check for boolean field (must be converted to 0/1 for some aggregation functions)
      bool_type = (dict(results_df.dtypes)[col] == 'boolean')
      
      # Apply aggregation function
      col_name = '_'.join([col, s])
      new_columns = new_columns + [col_name]
      print("Adding new column '{}'...".format(col_name))
      if(s in ['avg', 'mean']):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate avg/mean...".format(col))
          results_df = results_df.withColumn(col_name, F.avg(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.avg(col).over(w))
      elif(s == 'collect_list'):
        results_df = results_df.withColumn(col_name, F.collect_list(col).over(w))
      elif(s == 'collect_set'):
        results_df = results_df.withColumn(col_name, F.collect_set(col).over(w))
      elif(s == 'concat_ws'):
        results_df = results_df.withColumn(col_name, F.concat_ws(';', F.collect_list(col).over(w)))
      elif(s == 'count'):
        results_df = results_df.withColumn(col_name, F.count(col).over(w))
      elif(s == 'first'):
        results_df = results_df.withColumn(col_name, F.first(col).over(w))
      elif(s == 'last'):
        results_df = results_df.withColumn(col_name, F.last(col).over(w))
      elif(s == 'max'):
        results_df = results_df.withColumn(col_name, F.max(col).over(w))
      elif(s == 'min'):
        results_df = results_df.withColumn(col_name, F.min(col).over(w))
      elif(s == 'median'):
        results_df = results_df.withColumn(col_name, median_udf(F.collect_list(col).over(w)))
      elif(s == 'stddev'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate stddev...".format(col))
          results_df = results_df.withColumn(col_name,
                                             F.stddev(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.stddev(col).over(w))
      elif(s == 'sum'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate sum...".format(col))
          results_df = results_df.withColumn(col_name, F.sum(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.sum(col).over(w))
  
  # Process the final dataframe for return
  final_columns = partition_columns + new_columns
  results_df = results_df.select(final_columns).dropDuplicates()
    
  return results_df


  # Add a time window index column to a dataframe
  def add_time_window_column(df, datetime_column, new_column_name, type='year', reference_datetime_column=None, period_bounds=[0]):
  """Add a new time period column based on year, month, day, hour or time period index (starting
  from 0)
  
  Parameters:
  df (PySpark): Dataframe to which to add a new time period column
  datetime_column (str): Date or date/time column to be mapped to a time index
  new_column_name (str): Name of the new time period column to be added
  type (str): Type of time window column to add: 'year', 'month', 'day', '12-hour', '6-hour', 'hour',
              'defined'
  reference_datetime_column (str): Reference date or date/time column to use for 'defined' option
                                   (corresponds to time=0)
  period_bounds (list): List of boundaries (in days relative to reference_datetime_column) defining
                        time periods for the 'defined' option (not used for the other type options)
  
  Returns:
  PySpark df: Original dataframe with an additional column with time periods
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain column datetime_column
  assert(datetime_column in df.columns)
  
  # New column name must be a valid string
  assert(isinstance(new_column_name, str))
  
  # type must be one of a pre-defined set of possible strings
  assert(type in ['year', 'month', 'day', '12-hour', '6-hour', 'hour', 'defined'])
  
  # Add new time period column of specified type
  if(type == 'year'):
    # Add new column with year portion of specified datetime column
    df = df.withColumn(new_column_name, F.year(datetime_column))
  elif(type == 'month'):
    # Add new column with year and month portions of specified datetime column
    df = df.withColumn(
      new_column_name,
      F.concat_ws('-', F.year(datetime_column),
                  zero_pad_front_udf(F.month(datetime_column), F.lit(2))))
  elif(type == 'day'):
    # Add new column with year, month, and day portions of specified datetime column
    df = df.withColumn(
      new_column_name,
      F.concat_ws('-', F.year(datetime_column),
                  zero_pad_front_udf(F.month(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.dayofmonth(datetime_column), F.lit(2))))
  elif(type == '12-hour'):
    # Add new column with year, month, day portions and 12-hour index of specified datetime column.
    # 0 corresponds to hours 0-11, and 1 corresponds to hours 12-23 of the day
    df = df.withColumn(
      new_column_name,
      F.concat_ws('-', F.year(datetime_column),
                  zero_pad_front_udf(F.month(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.dayofmonth(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.floor(F.hour(datetime_column)/12), F.lit(2))))
  elif(type == '6-hour'):
    # Add new column with year, month, day portions and 6-hour index of specified datetime column.
    # 0 corresponds to hours 0-5, 1 corresponds to hours 6-11, 2 corresponds to hours 12-17, and 3
    # correponds to hours 18-23 of the day
    df = df.withColumn(
      new_column_name,
      F.concat_ws('-', F.year(datetime_column),
                  zero_pad_front_udf(F.month(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.dayofmonth(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.floor(F.hour(datetime_column)/6), F.lit(2))))
  elif(type == 'hour'):
    # Add new column with year, month, day, and hour portions of specified datetime column
    df = df.withColumn(
      new_column_name,
      F.concat_ws('-', F.year(datetime_column),
                  zero_pad_front_udf(F.month(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.dayofmonth(datetime_column), F.lit(2)),
                  zero_pad_front_udf(F.hour(datetime_column), F.lit(2))))
  elif(type == 'defined'):
    # Input dataframe must contain column reference_datetime_column
    assert(reference_datetime_column in df.columns)
    
    # period_bounds must have at least one value
    if((period_bounds is None) | (len(period_bounds) < 1)):
      period_bounds = [0]
    
    # Period bounds must be a monotonically increasing list of integers
    assert(all([isinstance(s, int) for s in period_bounds]))
    period_bounds.sort()
    
    # Create temporary column with difference between date and reference date
    df = df.withColumn('temp_date_difference',
                       F.datediff(datetime_column, reference_datetime_column))
    
    # Function to map index values to defined time periods, starting with 0 and counting up
    def map_to_index(date_difference):
      if(date_difference is None):
        return None
      
      index = -1
      # Assign values 1 or greater
      for i, v in enumerate(period_bounds):
        if(date_difference >= v):
          index = i+1
      
      # Assign 0 value
      if(index == -1):
        index = 0
      
      return index
    
    # Create UDF from mapping function
    map_to_index_udf = F.udf(map_to_index, IntegerType())
    
    # Add time index column and drop temporary date difference column
    df = df.withColumn(new_column_name, map_to_index_udf(F.col('temp_date_difference')))
    df = df.drop('temp_date_difference')
  
  return df


  # Aggregate data over partition columns and time index column
  def aggregate_data_by_time_index(df, partition_columns, time_index_column, aggregation_columns, order_by=None, max_expanded_columns=100, time_index_names=None):
  """Aggregate data over specified partition columns and time index column
  
  Parameters:
  df (PySpark): Dataframe to aggregate
  partition_columns (str or list): Field(s) in df on which to partition (not including the time
                                   index column). If partitioning on only one column, the column
                                   name can be provided as a str rather than a list
  time_index_column (str): Field to use for the time index partition.
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  order_by (str or list): Field(s) in df to use for ordering records in each partition. If None, do
                          not order. If ordering on only one column, the column name can be provided
                          as a str rather than a list
  max_expanded_columns (int): If the number of distinct time index values times the number of
                              aggregation columns is <= max_expanded_columns, separate columns will
                              be created containing the aggregate results for each time index.
                              Otherwise (or if max_expanded_columns is None), each aggregate result
                              will only have a single column.
  time_index_names (list): If provided, this is a list of names to be used for time indexes 0 up to 
                           len(time_index_names)-1
  
  Result:
  PySpark df: Dataframe containing the aggregated results
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain specified partition columns
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  assert(all([s in df.columns for s in partition_columns]))
  
  # Partition column list should not include the time index column (automatically taken into account)
  while(time_index_column in partition_columns):
    partition_columns.remove(time_index_column)
  
  # Input dataframe must contain the specified time_index_column
  assert(time_index_column in df.columns)
    
  # Perform validity checks on aggregation_columns
  assert(isinstance(aggregation_columns, dict))
  assert(all([s in df.columns for s in list(aggregation_columns.keys())]))
  valid_agg_functions = ['avg', 'collect_list', 'collect_set', 'concat_ws', 'count', 'first', 'last',
                         'max', 'mean', 'median', 'min', 'stddev', 'sum']
  for k in list(aggregation_columns.keys()):
    v = aggregation_columns[k]
    aggregation_columns[k] = v if isinstance(v, list) else [v]
    assert(all([s in valid_agg_functions for s in aggregation_columns[k]]))
  
  # order_by (if not None) must contain valid column names
  if(not(order_by is None)):
    order_by = order_by if isinstance(order_by, list) else [order_by]
    assert(all([s in df.columns for s in order_by]))
  
  # max_expanded_columns must be an integer
  max_expanded_columns = max_expanded_columns if isinstance(max_expanded_columns, int) else 0
  
  # Convenience function to get time index label for a given index
  def get_time_index_label(idx):
    if((time_index_names is None) or (idx is None)):
      return idx
    
    if(idx < len(time_index_names)):
      label = str(time_index_names[idx])
    else:
      label = str(idx)
    return label
  
  # UDF for getting time index labels
  get_time_index_label_udf = F.udf(get_time_index_label, StringType())
  
  # If provided, time_index_names must be a list
  if(not(time_index_names is None)):
    time_index_names = time_index_names if isinstance(time_index_names, list) else [time_index_names]
  
  # Add time index labels (if provided) as an additional column
  time_index_label_column = '{}_label'.format(time_index_column)
  results_df = df.withColumn(time_index_label_column,
                             get_time_index_label_udf(F.col(time_index_column)))
  
  # Define partition window
  w = Window.partitionBy(partition_columns + [time_index_column, time_index_label_column])
  if(not(order_by is None)):
    w = w.orderBy(order_by).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  
  # Add aggregate columns
  new_columns = []
  for col, agg_func in aggregation_columns.items():
    for s in agg_func:
      # Check for boolean field (must be converted to 0/1 for some aggregation functions)
      bool_type = (dict(results_df.dtypes)[col] == 'boolean')
      
      # Apply aggregation function
      col_name = '_'.join([col, s])
      new_columns = new_columns + [col_name]
      print("Adding new column '{}'...".format(col_name))
      if(s in ['avg', 'mean']):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate avg/mean...".format(col))
          results_df = results_df.withColumn(col_name, F.avg(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.avg(col).over(w))
      elif(s == 'collect_list'):
        results_df = results_df.withColumn(col_name, F.collect_list(col).over(w))
      elif(s == 'collect_set'):
        results_df = results_df.withColumn(col_name, F.collect_set(col).over(w))
      elif(s == 'concat_ws'):
        results_df = results_df.withColumn(col_name, F.concat_ws(';', F.collect_list(col).over(w)))
      elif(s == 'count'):
        results_df = results_df.withColumn(col_name, F.count(col).over(w))
      elif(s == 'first'):
        results_df = results_df.withColumn(col_name, F.first(col).over(w))
      elif(s == 'last'):
        results_df = results_df.withColumn(col_name, F.last(col).over(w))
      elif(s == 'max'):
        results_df = results_df.withColumn(col_name, F.max(col).over(w))
      elif(s == 'min'):
        results_df = results_df.withColumn(col_name, F.min(col).over(w))
      elif(s == 'median'):
        results_df = results_df.withColumn(col_name, median_udf(F.collect_list(col).over(w)))
      elif(s == 'stddev'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate stddev...".format(col))
          results_df = results_df.withColumn(col_name,
                                             F.stddev(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.stddev(col).over(w))
      elif(s == 'sum'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate sum...".format(col))
          results_df = results_df.withColumn(col_name, F.sum(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.sum(col).over(w))
  
  # Get list of unique time index values
  time_index_list = results_df \
    .select(time_index_column).where(F.col(time_index_column).isNotNull()) \
    .dropDuplicates().rdd.flatMap(lambda x: x).collect()
  time_index_list.sort()
  
  # Expand new columns over distinct time index values
  if(len(time_index_list) * len(new_columns) <= max_expanded_columns): 
    
    # Get distinct records over the partition columns
    expanded_results_df = results_df.select(partition_columns).dropDuplicates()
    for i in time_index_list:
      
      # Get all records for the current time index
      current_time_index_df = results_df \
        .where(F.col(time_index_column) == i) \
        .select(partition_columns + new_columns).dropDuplicates()
      
      # Rename columns (append the time index)
      for col in new_columns:
        new_col_name = '_'.join([col, get_time_index_label(i)])
        print("Adding column: {}".format(new_col_name))
        current_time_index_df = current_time_index_df.withColumnRenamed(col, new_col_name)
      
      # Join all columns
      expanded_results_df = expanded_results_df \
        .join(current_time_index_df, partition_columns, how='left')
    
    results_df = expanded_results_df
  else:
    # Do not expand new columns by time index
    final_columns = partition_columns + [time_index_column, time_index_label_column] + new_columns
    results_df = results_df.select(final_columns).dropDuplicates()
    
  return results_df


  # aggregate_by_patient
  def aggregate_by_patient(df, partition_columns, aggregation_columns, order_by=None):
  """Wrapper for the aggregate_data function to aggregate data on a per-patient basis
  
  Parameters:
  df (PySpark): Dataframe to aggregate (must contain pat_id and instance columns)
  partition_columns (str or list): Field(s) in df on which to partition. If pat_id and instance are
                                   not included, they will automatically be included
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  order_by (str or list): Field(s) in df to use for ordering records in each partition. If None, do
                          not order. If ordering on only one column, the column name can be provided
                          as a str rather than a list
  
  Result:
  PySpark df: Dataframe containing the aggregated results
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # The dataframe must contain pat_id and instance columns
  assert(all([s in df.columns for s in ['pat_id', 'instance']]))
  
  # Make sure that partition_columns is a list
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  
  # Make sure that partition_columns contains pat_id and instance
  partition_columns = partition_columns if ('instance' in partition_columns) else ['instance'] + partition_columns
  partition_columns = partition_columns if ('pat_id' in partition_columns) else ['pat_id'] + partition_columns
  
  # Aggregate records on partition columns
  results_df = aggregate_data(df, partition_columns, aggregation_columns, order_by)
  
  return results_df


  # aggregate_by_regular_time_windows
  def aggregate_by_regular_time_windows(df, partition_columns, aggregation_columns, datetime_column, type='year', time_index_column_name=None):
  """Aggregate records over regular (i.e. yearly, monthly, daily, hourly, 12-hourly, or 6-hourly) time
  intervals and specified partition columns
  
  Parameters:
  df (PySpark): Dataframe to aggregate
  partition_columns (str or list): Field(s) in df on which to partition (not including the time
                                   index column). If partitioning on only one column, the column
                                   name can be provided as a str rather than a list
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  datetime_column (str): Date or date/time column to be mapped to a time index (e.g. 'year', 'month',
                         'day', etc) and used for partitioning
  type (str): Type of time window column to add: 'year', 'month', 'day', '12-hour', '6-hour', 'hour'
  time_index_column_name (str): Name of added time index column (used for partitioning). Default is
                                None, in which case, a column name is automatically generated by 
                                concatenating 'time_index' and the time window type (with hyphens
                                replaced by underscores for '12-hour' and '6-hour')
  
  Result:
  PySpark df: Dataframe containing the results aggregated on regular time windows
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain specified partition columns
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  assert(all([s in df.columns for s in partition_columns]))
  
  # Input dataframe must contain column datetime_column
  assert(datetime_column in df.columns)
  
  # type must be one of a pre-defined set of possible strings
  assert(type in ['year', 'month', 'day', '12-hour', '6-hour', 'hour'])
  
  # Time index column name must be valid
  time_index_column_name = time_index_column_name if not(time_index_column_name is None) else \
                           "time_index_{}".format(type.replace('-', '_'))
  
  # Add a time index column to be used for time-window-based aggregation
  results_df = add_time_window_column(df, datetime_column, time_index_column_name, type)
  
  # Aggregate records on time index windows and partition columns
  results_df = aggregate_data_by_time_index(results_df, partition_columns, time_index_column_name,
                                            aggregation_columns, order_by=datetime_column)
  
  return results_df


 # aggregate_by_custom_time_windows
 def aggregate_by_custom_time_windows(df, partition_columns, aggregation_columns, datetime_column, reference_datetime_column, period_bounds=[0], time_index_column_name='time_index_custom', max_expanded_columns=100, time_index_names=None):
  """Aggregate records over custom-defined time windows (relative to a specified reference date) and
  specified partition columns
  
  Parameters:
  df (PySpark): Dataframe to aggregate
  partition_columns (str or list): Field(s) in df on which to partition (not including the time
                                   index column). If partitioning on only one column, the column
                                   name can be provided as a str rather than a list
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  datetime_column (str): Date or date/time column to be mapped to a custom-defined time index.
  reference_datetime_column (str): Reference date or date/time column to use for custom-time-window
                                   aggregation (corresponds to time=0)
  period_bounds (list): List of boundaries (in days relative to reference_datetime_column) defining
                        time periods for custom-time-window aggregation. Default is [0], which means
                        records will be aggregated into pre-reference-date and post-reference-date
                        time windows.
  time_index_column_name (str): Name of added time index column used for partitioning. Default is
                                'time_index_custom'. Note that a user should typically not specify
                                this. The primary reason it is provided is so the user can specify a
                                name to avoid column naming conflicts (which will only happen if df
                                has a column named 'time_index_column')
  max_expanded_columns (int): If the number of distinct time index values times the number of
                              aggregation columns is <= max_expanded_columns, separate columns will
                              be created containing the aggregate results for each time index.
                              Otherwise (or if max_expanded_columns is None), each aggregate result
                              will only have a single column.
  time_index_names (list): If provided, this list of names will be used as labels for time indexes 0
                           up to len(time_index_names)-1
  
  Result:
  PySpark df: Dataframe containing the results aggregated on custom-defined time windows
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain specified partition columns
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  assert(all([s in df.columns for s in partition_columns]))
  
  # Input dataframe must contain column datetime_column
  assert(datetime_column in df.columns)
  
  # Time index column name must be a string
  assert(isinstance(time_index_column_name, str))
  
  # Add a time index column to be used for custom-time-window-based aggregation
  results_df = add_time_window_column(df, datetime_column, time_index_column_name, type='defined',
                                      reference_datetime_column=reference_datetime_column,
                                      period_bounds=period_bounds)
  
  # Aggregate records on time index windows and partition columns
  results_df = aggregate_data_by_time_index(results_df, partition_columns, time_index_column_name,
                                            aggregation_columns, order_by=datetime_column,
                                            max_expanded_columns=max_expanded_columns,
                                            time_index_names=time_index_names)
  
  return results_df


# Write dataframe to sandbox table
def write_data_frame_to_sandbox(df, table_name, sandbox_db='rdp_phi_sandbox', replace=False):
  """Write the provided dataframe to a sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be written to the sandbox
  table_name (str): Name of table to write
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  replace (bool): Whether to replace (True) the table if it exists
  
  Returns:
  (bool): True/False value indicating whether the table was successfully written
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    if(replace):
      # Drop existing table
      print("Deleting existing table '{}'...".format(full_table_name))
      spark.sql("""drop table {}""".format(full_table_name))
    else:
      # If table exists and replace is False, issue a warning and return False (do not overwrite)
      print("Warning: Table '{}' already exists!".format(full_table_name))
      print("If you are sure you want to replace, rerun with 'replace' set to True.")
      return False
  
  # Make sure there is no folder remaining from past save attempts
  db_path = sandbox_db.replace('_', '-')
  #dummy = dbutils.fs.rm("abfss://{0}@datalakestorage321.dfs.core.windows.net/{1}.db/{2}".format(db_path, sandbox_db, table_name), True)
    
  # Arrival at this point means the table does not exist. Write the new table.
  broadcast_timeout = 7200
  print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
  print("Writing table '{0}'...".format(full_table_name))
  spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
  df.write.saveAsTable(full_table_name)
  
  # Run commands to optimize new table
  spark.sql("REFRESH {}".format(full_table_name))
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True


# Insert records from dataframe into sandbox table
def insert_data_frame_into_table(df, table_name, sandbox_db='rdp_phi_sandbox'):
  """Insert/merge records in provided dataframe into an existing sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be merged into sandbox table
  table_name (str): Name of table into which to merge records
  sandbox_db (str): Name of sandbox database (generally, this should be rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully merged
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Merge records into table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    broadcast_timeout = 7200
    print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
    spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
    print("Inserting records into table '{}'...".format(full_table_name))
    df.write.insertInto(".".join([sandbox_db, table_name]), overwrite=False)
  else:
    print("Specified table '{}' does not exist. Cannot insert records.".format(full_table_name))
    return False
    
  # Run commands to optimize new table
  spark.sql("REFRESH {}".format(full_table_name))
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True


# Write dataframe to a sandbox cache table
def write_data_frame_to_sandbox_cache_table(df, sandbox_db='rdp_phi_sandbox'):
  """Write the provided dataframe to a sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be written to a sandbox cache table
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  
  Returns:
  (str): Returns the name of the cache table
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Generate a name for the sandbox cache table
  cache_table = "cache_table___{}".format(get_random_file_name())
  
  # Write dataframe to a cache table
  successful = write_data_frame_to_sandbox(df, cache_table, sandbox_db, replace=True)
  
  # Return the cache table name, if successful
  if(successful):
    return cache_table
  else:
    return None


 # Write dataframe to sandbox delta table
 def write_data_frame_to_sandbox_delta_table(
  df, table_name, sandbox_db='rdp_phi_sandbox', replace=False):
  """Write the provided dataframe to a sandbox delta table
  
  Parameters:
  df (PySpark df): A dataframe to be written to the sandbox
  table_name (str): Name of table to write
  sandbox_db (str): Name of sandbox database to write to (generally, this should be rdp_phi_sandbox)
  replace (bool): Whether to replace (True) the table if it exists
  
  Returns:
  (bool): True/False value indicating whether the table was successfully written
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    if(replace):
      # Drop existing table
      print("Deleting existing table '{}'...".format(full_table_name))
      spark.sql("""drop table {}""".format(full_table_name))
    else:
      # If table exists and replace is False, issue a warning and return False (do not overwrite)
      print("Warning: Table '{}' already exists!".format(full_table_name))
      print("If you are sure you want to replace, rerun with 'replace' set to True.")
      return False
  
  # Make sure there is no folder remaining from past save attempts
  db_path = sandbox_db.replace('_', '-')
  full_table_path = \
    "abfss://{0}@datalakestorage321.dfs.core.windows.net/{1}.db/{2}".format(
    db_path, sandbox_db, table_name)
  dummy = dbutils.fs.rm(full_table_path, True)
    
  # Arrival at this point means the table does not exist. Write the new table.
  broadcast_timeout = 1200
  print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
  print("Writing table '{0}'...".format(full_table_name))
  spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
  
  # Write the delta table
  df.write.format("delta").save(full_table_path)
  spark.sql("CREATE TABLE {0} USING DELTA LOCATION \'{1}\'".format(
    full_table_name, full_table_path))
  
  # Run commands to optimize new table
#   print("Optimizing table...")
#   spark.sql("OPTIMIZE {}".format(full_table_name))
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True


# Insert records into sandbox delta table
def insert_data_frame_into_delta_table(df, table_name, sandbox_db='rdp_phi_sandbox'):
  """Insert/merge records in provided dataframe into an existing sandbox table
  
  Parameters:
  df (PySpark df): A dataframe to be merged into sandbox table
  table_name (str): Name of table into which to merge records
  sandbox_db (str): Name of sandbox database (generally, this should be rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully merged
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Get full file path to data files
  db_path = sandbox_db.replace('_', '-')
  full_table_path = \
    "abfss://{0}@datalakestorage321.dfs.core.windows.net/{1}.db/{2}".format(
    db_path, sandbox_db, table_name)
  
  # Merge records into table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    broadcast_timeout = 1200
    print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
    spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
    print("Inserting records into table '{}'...".format(full_table_name))
    df.write.format("delta").mode("append").save(full_table_path)
  else:
    print("Specified table '{}' does not exist. Cannot insert records.".format(full_table_name))
    return False
    
  # Run commands to optimize new table
#   print("Optimizing table...")
#   spark.sql("OPTIMIZE {}".format(full_table_name))
  print("Refreshing table...")
  spark.sql("REFRESH TABLE {}".format(full_table_name))
  print("Analyzing table...")
  spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS".format(full_table_name))
  
  return True


# Delete table from sandbox
def delete_table_from_sandbox(table_name, sandbox_db='rdp_phi_sandbox'):
  """Delete the specified table from the sandbox
  
  Parameters:
  table_name (str): Name of table to delete
  sandbox_db (str): Name of sandbox database containing the table (generally, this should be
                    rdp_phi_sandbox)
  
  Returns:
  (bool): True/False value indicating whether the table was successfully deleted
  
  """
  # Provided table name must be a string
  assert(isinstance(table_name, str))
  
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Delete table if it actually exists
  full_table_name = '.'.join([sandbox_db, table_name])
  permanent_tables_list = get_permanent_tables_list(sandbox_db)
  if(table_name in permanent_tables_list):
    # Drop existing table
    print("Deleting specified table '{}'...".format(full_table_name))
    spark.sql("""drop table {}""".format(full_table_name))
  else:
    # If table does not exist, print a notification
    print("NOTE: Table '{}' does not exist. Cannot delete.".format(full_table_name))
    return False
  
  # Make sure there is no folder remaining after delete
  db_path = sandbox_db.replace('_', '-')
  dummy = dbutils.fs.rm("abfss://{0}@datalakestorage321.dfs.core.windows.net/{1}.db/{2}".format(db_path, sandbox_db, table_name), True)
  
  return True


# Delete all cache tables from sandbox
def delete_cache_tables_from_sandbox(sandbox_db='rdp_phi_sandbox'):
  """Delete all cache tables from the sandbox
  
  Parameters:
  sandbox_db (str): Name of sandbox database containing the cache tables to be deleted
  
  Returns:
  (bool): True/False value indicating if all cache tables have been deleted
  
  """
  # Sandbox database name must be a string
  assert(isinstance(sandbox_db, str))
  
  # Get full list of cache tables (assumes cache tables start with 'cache_table')
  cache_table_list = [s for s in sqlContext.tableNames(sandbox_db) if s.startswith('cache_table___')]
  
  # Delete cache tables
  for table in cache_table_list:
    delete_table_from_sandbox(table, sandbox_db)
  
  # Return True if all cache tables have been deleted
  cache_table_list = [s for s in sqlContext.tableNames(sandbox_db) if s.startswith('cache_table___')]
  if(len(cache_table_list) < 1):
    return True
  else:
    print("Warning: Not all cache tables were deleted from '{}'!".format(sandbox_db))
    return False

