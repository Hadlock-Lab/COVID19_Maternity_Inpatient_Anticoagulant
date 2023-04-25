# load environment
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.basic_cohort
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medications_cc_utilities


# Utility functions to get default columns to omit
def medication_orders_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the medication orders table and other
  tables that are optionally joined
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing medication-order-related table fields to omit by default
  
  """
  # Medication order table columns to omit
  medication_order_columns_to_omit = ['authorizing_prov_id', 'department_id', 'ord_prov_id', 'order_priority', 'order_set', 'requested_instant_utc']
  
  # Medication table columns to omit
  medication_columns_to_omit = ['controlled']
  
  # Optionally-joined table columns to omit
  medication_administration_columns_to_omit = ['due_datetime', 'scheduled_for_datetime', 'scheduled_on_datetime']
  
  columns_to_omit_table_map = {
    'medicationorders': medication_order_columns_to_omit,
    'medication': medication_columns_to_omit,
    'medicationadministration': medication_administration_columns_to_omit
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


# Get medication order records based on specified filter criteria
def get_medication_orders(cohort_df=None, include_cohort_columns=None, join_table=None, filter_string=None, omit_columns=medication_orders_default_columns_to_omit(), add_cc_columns=[]):
  """Get medication order records based on specified filter criteria
  
  Parameters:
  cohort_df (PySpark df): Optional cohort dataframe for which to get medication order records
                          (if None, get all patients in the RDP). This dataframe *must* include
                          pat_id and instance columns for joining
  include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None, in which case all columns from cohort dataframe are kept.
  join_table (string): Optionally specify table to join to medication orders (this can only be one
                       of the following: 'medicationadministration'). Default is None
                       (i.e. do not join any additional table)
  filter_string (string): String to use as a WHERE clause filter/condition. Default is None.
  omit_columns (list): List of columns/fields to exclude from the final dataframe.
  add_cc_columns (list): List of medication cc columns to add (e.g. 'antidepressant',
                         'antihypertensive', 'vasopressor')
  
  Returns:
  PySpark df: Dataframe containing medication orders satisfying specified filter criteria
  
  """
  # If not None, filter_string must be a string
  if(filter_string is not None):
    assert(isinstance(filter_string, str))
  
  # 'Omit' columns input must be a list
  omit_columns_from_final = [] if omit_columns is None else omit_columns
  assert(isinstance(omit_columns_from_final, list))
  
  # add_cc_columns must be a list of valid medication cc labels
  cc_columns_list = add_cc_columns if isinstance(add_cc_columns, list) else [add_cc_columns]
  assert(all([s in get_medication_cc_list() for s in cc_columns_list]))
  
  # If no cohort input is provided, use all patients in the RDP
  results_df = cohort_df if cohort_df is not None else get_basic_cohort(include_race=True)
  assert(isinstance(results_df, DataFrame))
  
  # 'Include cohort' columns input must be a list
  keep_cohort_columns = results_df.columns if include_cohort_columns is None else include_cohort_columns
  assert(isinstance(keep_cohort_columns, list))
  
  # Patient cohort dataframe must also include 'pat_id' and 'instance'
  for s in ['instance', 'pat_id']:
    if(s not in keep_cohort_columns):
      keep_cohort_columns = [s] + keep_cohort_columns
  
  # Make sure cohort columns to keep are all present
  assert(all([s in results_df.columns for s in keep_cohort_columns]))
  
  # Get initial columns from patient cohort dataframe
  print('Getting initial columns from patient cohort dataframe...')
  results_df = results_df.select(keep_cohort_columns)
  
  # Join patient cohort to medication orders table
  print('Joining medication orders table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        pat_id,
        instance,
        order_med_id,
        pat_enc_csn_id,
        start_date,
        end_date,
        medication_id,
        orderdescription as order_description,
        orderingdatetime as ordering_datetime,
        dosage,
        quantity,
        sig,
        route,
        orderclass as order_class,
        ordermode as order_mode,
        orderset as order_set,
        orderstatus as order_status,
        orderpriority as order_priority,
        requestedinstantutc as requested_instant_utc,
        prn_yn,
        authrzing_prov_id as authorizing_prov_id,
        department_id
      FROM rdp_phi.medicationorders"""), ['pat_id', 'instance'], how='left')
  
  # If birth_date is present in the cohort table, calculate age at ordering datetime
  if('birth_date' in results_df.columns):
    print('Adding age (at ordering datetime) to results...')
    results_df = results_df.withColumn(
      'age_at_order_dt',
      F.round(F.datediff(F.col('ordering_datetime'), F.col('birth_date'))/365.25, 1))
  
  # Join medication table to get medication name/description
  print('Joining medication table to get name/description...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        medication_id,
        instance,
        name,
        shortname as short_name,
        controlled
      FROM rdp_phi.medication
      """).hint("broadcast"), ['medication_id', 'instance'], how='left')
  
  # Join medication RxNorm codes
  rxnorm_codes_df = get_rxnorm_from_medication_ids(results_df, use_column='medication_id')
  print('Joining RxNorm codes...')
  results_df = results_df.join(F.broadcast(rxnorm_codes_df), ['instance', 'medication_id'], how='left')
  
  # Optionally join medication administration table
  if(join_table == 'medicationadministration'):
    print('Joining medication administration table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          order_med_id,
          administrationdatetime as administration_datetime,
          actiontaken as action_taken,
          istimely as is_timely,
          recordeddatetime as recorded_datetime,
          duedatetime as due_datetime,
          scheduledondatetime as scheduled_on_datetime,
          scheduledfordatetime as scheduled_for_datetime
        FROM rdp_phi.medicationadministration
        """), ['pat_id', 'instance', 'order_med_id'], how='left')
  
  # Apply filter to get subset of records
  if(filter_string is not None):
    print('Applying specified filter to get subset of records...')
    filter_string_for_print = filter_string if (len(filter_string) <= 1000) else '{}...'.format(filter_string[:1000])
    print(filter_string_for_print)
    results_df = results_df.where(filter_string)
  
  # Add medication cc columns
  for cc_label in cc_columns_list:
    print("Adding column for '{}' medications clinical concept...".format(cc_label))
    results_df = add_medication_cc_column(results_df, cc_label, search_column='medication_id')
  
  # Exclude specified columns
  if(len(omit_columns_from_final) > 0):
    print('Omitting the following columns:')
    print(omit_columns_from_final)
    results_df = results_df.select(
      [s for s in results_df.columns if s not in omit_columns_from_final])
  
  return results_df
