# import libraries
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# define utility function to get default columns to omit
def basic_cohort_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the cohort dataframe
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing patient table fields to omit by default
  
  """
  # Patient table columns to omit
  patient_columns_to_omit = ['birth_datetime', 'first_name', 'middle_name', 'last_name', 'full_name', 'curr_loc_id', 'zip']
  
  # patientrace table columns to omit
  patientrace_columns_to_omit = []
  
  columns_to_omit_table_map = {
    'patient': patient_columns_to_omit,
    'patientrace': patientrace_columns_to_omit
  }
  
  # Ensure that table is a valid list
  if(table_list is None):
    table_list = list(columns_to_omit_table_map.keys())
  else:
    table_list = table_list if isinstance(table_list, list) else [table_list]
    assert(all([s in list(columns_to_omit_table_map.keys()) for s in table_list]))
  
  # Get columns to omit
  columns_to_omit = []
  for t in table_list:
    columns_to_omit = columns_to_omit + columns_to_omit_table_map[t]
  
  return columns_to_omit


# define function to get patient cohort based on specified filter criteria
def get_basic_cohort(filter_string=None, include_race=True, omit_columns=basic_cohort_default_columns_to_omit()):
  """Get patient cohort based on specified filter criteria (e.g. demographic information)
  
  Parameters:
  filter_string (string): String to use as a WHERE clause filter/condition
  include_race (bool): If true, join to patientrace table (aggregating to ensure 1-to-1 join)
  omit_columns (list): List of columns/fields to exclude
  
  Returns:
  PySpark df: Dataframe containing patients satisfying specified filter criteria
  
  """
  
  # By default, get all patient records (filter, if provided, is subsequently applied)
  print('Selecting all patient records...')
  results_df = spark.sql(
    """
    SELECT
      pat_id,
      instance,
      birthdate as birth_date,
      birthdatetime as birth_datetime,
      deathdate as death_date,
      first as first_name,
      middle as middle_name,
      last as last_name,
      fullname as full_name,
      sex,
      ethnicgroup as ethnic_group,
      curr_loc_id,
      zip
    FROM rdp_phi.patient""")
  
  # Include patient race, if include_race is True
  if(include_race):
    print('Joining patientrace table (aggregating race to ensure 1-to-1 join)...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          COLLECT_SET(race) as race
        FROM rdp_phi.patientrace
        GROUP BY instance, pat_id
        """), ['pat_id', 'instance'], how='left')
  
  # Exclude specified columns
  if(omit_columns is not None):
    print('Omitting the following columns:')
    print(omit_columns)
    results_df = results_df.select([s for s in results_df.columns if s not in omit_columns])
  
  # Apply filter to get subset of records
  if(filter_string is not None):
    print('Applying specified filter to get subset of records...')
    print(filter_string)
    results_df = results_df.where(filter_string)
  
  return results_df

