# import libraries
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.basic_cohort
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.labs_cc_utilities


# define utility functions to get default columns to omit
def procedure_orders_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the procedure orders table and other
  tables that are optionally joined
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing procedure-order-related table fields to omit by default
  
  """
  # Procedure orders table columns to omit
  procedureorders_columns_to_omit = ['parent_order_id', 'order_priority', 'order_set', 'referral_class', 'referral_expiration_date', 'referral_number_of_visits', 'referral_priority', 'referral_reason', 'referral_type', 'referred_to_department_id', 'referred_to_facility_id', 'referred_to_specialty', 'requested_instant_utc', 'future_or_stand', 'authorizing_prov_id']
  
  # Optionally-joined table columns to omit
  cardiovascularfinding_columns_to_omit = ['ord_date_real', 'finding_id']
  labresult_columns_to_omit = ['organism_id', 'organism_name', 'organism_quantity', 'organism_quantity_unit', 'compon_lnc_id', 'default_lnc_id', 'fulladd_lresult_comment']
  
  columns_to_omit_table_map = {
    'procedureorders': procedureorders_columns_to_omit,
    'cardiovascularfinding': cardiovascularfinding_columns_to_omit,
    'labresult': labresult_columns_to_omit
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


  # define functions to get procedure order records based on specified filter criteria
  def get_procedure_orders(cohort_df=None, include_cohort_columns=None, join_table=None, filter_string=None, omit_columns=procedure_orders_default_columns_to_omit(), add_cc_columns=[]):
  """Get procedure order records based on specified filter criteria
  
  Parameters:
  cohort_df (PySpark df): Optional cohort dataframe for which to get procedure order records
                          (if None, get all patients in the RDP). This dataframe *must* include
                          pat_id and instance columns for joining
  include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None, in which case all columns from cohort dataframe are kept.
  join_table (string): Optionally specify table to join to procedure orders (this can only be one
                       of the following: 'cardiovascularfinding', 'labresult'). Default is None
                       (i.e. do not join any additional table)
  filter_string (string): String to use as a WHERE clause filter/condition. Default is None.
  omit_columns (list): List of columns/fields to exclude from the final dataframe.
  
  Returns:
  PySpark df: Dataframe containing procedure orders satisfying specified filter criteria
  
  """
  # If not None, filter_string must be a string
  if(filter_string is not None):
    assert(isinstance(filter_string, str))
  
  # 'Omit' columns input must be a list
  omit_columns_from_final = [] if omit_columns is None else omit_columns
  assert(isinstance(omit_columns_from_final, list))
  
  # add_cc_columns must be a list of valid device cc labels
  cc_columns_list = add_cc_columns if isinstance(add_cc_columns, list) else [add_cc_columns]
  assert(all([s in get_lab_cc_list() for s in cc_columns_list]))
  
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
  
  # Join patient cohort to procedure orders table
  print('Joining procedure orders table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        pat_id,
        instance,
        order_proc_id,
        parent_order_id,
        pat_enc_csn_id,
        category,
        ordername as order_name,
        orderdescription as order_description,
        orderingdatetime as ordering_datetime,
        orderclass as order_class,
        ordermode as order_mode,
        ordertype as order_type,
        orderset as order_set,
        orderstatus as order_status,
        orderpriority as order_priority,
        abnormal_yn,
        referralclass as referral_class,
        referralexpirationdate as referral_expiration_date,
        referralnumberofvisits as referral_number_of_visits,
        referralpriority as referral_priority,
        referralreason as referral_reason,
        referraltype as referral_type,
        referredtodepartmentid as referred_to_department_id,
        referredtofacilityid as referred_to_facility_id,
        referredtospecialty as referred_to_specialty,
        requestedinstantutc as requested_instant_utc,
        future_or_stand,
        authrzing_prov_id as authorizing_prov_id,
        department_id
      FROM rdp_phi.procedureorders"""), ['pat_id', 'instance'], how='left')
  
  # If birth_date is present in the cohort table, calculate age at ordering datetime
  if('birth_date' in results_df.columns):
    print('Adding age (at ordering datetime) to results...')
    results_df = results_df.withColumn(
      'age_at_order_dt',
      F.round(F.datediff(F.col('ordering_datetime'), F.col('birth_date'))/365.25, 1))
  
  # Optionally join an additional table
  if(join_table == 'cardiovascularfinding'):
    print('Joining cardiovascular finding table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          order_proc_id,
          type,
          findingtime as finding_time,
          ord_date_real,
          finding_id
        FROM rdp_phi.cardiovascularfinding
        """), ['pat_id', 'instance', 'order_proc_id'], how='left')
  elif(join_table == 'labresult'):
    print('Joining lab result table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          instance,
          order_proc_id,
          basename as base_name,
          commonname as common_name,
          observationdatetime as observation_datetime,
          resultcomment as result_comment,
          resultdatetime as result_datetime,
          resultname as result_name,
          resultvalue as result_value,
          lowthreshold as low_threshold,
          highthreshold as high_threshold,
          type,
          unit,
          flaggedas as flagged_as,
          status,
          organism_id,
          organismname as organism_name,
          organismquantity as organism_quantity,
          organismquantityunit as organism_quantity_unit,
          snomed,
          loinccode as loinc_code,
          defaultloinccode as default_loinc_code,
          compon_lnc_id,
          default_lnc_id,
          fulladdlresultcomment as fulladd_lresult_comment
        FROM rdp_phi.labresult
        """), ['instance', 'order_proc_id'], how='left')
    
    # Add mapped LOINC codes
    results_df = add_mapped_loinc_code_column(results_df)
    
    # Add lab cc columns
    for cc_label in cc_columns_list:
      print("Adding column for '{}' lab clinical concept...".format(cc_label))
      results_df = add_lab_cc_column(results_df, cc_label)
  
  # Apply filter to get subset of records
  if(filter_string is not None):
    print('Applying specified filter to get subset of records...')
    filter_string_for_print = filter_string if (len(filter_string) <= 1000) else '{}...'.format(filter_string[:1000])
    print(filter_string_for_print)
    results_df = results_df.where(filter_string)
  
  # Exclude specified columns
  if(len(omit_columns_from_final) > 0):
    print('Omitting the following columns:')
    print(omit_columns_from_final)
    results_df = results_df.select(
      [s for s in results_df.columns if s not in omit_columns_from_final])
  
  return results_df

