# import libraries
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from datetime import date

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.procedure_orders


# define functions
def get_sars_cov_2_cohort(date_bounds=('2020-01-01', None)):
  """Get all patients tested for SARS-CoV-2, whether positive or negative
  
  Parameters:
  date_bounds (tuple): Tuple of date strings defining inclusive bounds for lab order datetimes.
                       Default is a start date of '2020-01-01' and the current date for the end date.
  
  Returns:
  PySpark df: Dataframe containing patient test results and demographic information
  
  """
  # Set broadcast timeout longer than default
  broadcast_timeout = 1800
  print("Setting broadcast timeout to {} seconds.".format(broadcast_timeout))
  spark.conf.set("spark.sql.broadcastTimeout", broadcast_timeout)
  
  # Check for valid date bounds
  assert(isinstance(date_bounds, tuple))
  assert(len(date_bounds) >= 2)
  
  # Get all patients in the database
  basic_cohort_df = get_basic_cohort()
  
  # Specify bounds for lab result records
  start_date = '2020-01-01' if (date_bounds[0] is None) else date_bounds[0]
  end_date = date.today().strftime("%Y-%m-%d") if (date_bounds[1] is None) else date_bounds[1]
  query_string = \
  """
  DATE(ordering_datetime) >= '{0}' AND
  DATE(ordering_datetime) <= '{1}'
  """.format(start_date, end_date)
  
  # Define clinical concepts list
  cc_list = ['sars_cov_2_pcr_naat']
  
  # Get SARS-CoV-2 lab results
  print('Getting SARS-CoV-2 lab results for all patients...')
  results_df = get_procedure_orders(
    cohort_df=basic_cohort_df,
    include_cohort_columns=basic_cohort_df.columns,
    join_table='labresult',
    filter_string=query_string,
    add_cc_columns=cc_list)
  
  # Keep only SARS-CoV-2 records
  results_df = get_clinical_concept_records(results_df, cc_list)
  
  # Add column with test result as 'positive', 'negative', 'see report or comment', or 'unknown'
  print('Adding column for cleaned test result...')
  results_df = add_pos_neg_lab_result_column(results_df, 'result_value', 'result_short')
  
  # Columns on which to partition records for aggregation
  partition_columns = basic_cohort_df.columns
  partition_columns.remove('birth_date')
  
  # Specify how to aggregate non-clinical-concept columns
  agg_columns = {
    'pat_enc_csn_id': 'collect_list',
    'ordering_datetime': 'collect_list',
    'observation_datetime': 'collect_list',
    'result_short': 'collect_list',
    'flagged_as': 'collect_list',
    'age_at_order_dt': 'collect_list',
    'order_name': 'collect_list'
  }
  
  # Get per-patient aggregated results
  print('Generating aggregate test results columns...')
  results_df = aggregate_data(results_df, 
                              partition_columns=partition_columns,
                              aggregation_columns=agg_columns,
                              order_by='ordering_datetime')
  
  # Add a column categorizing each individual into one of the following categories: '>= 1 positive',
  # '>= 1 negative (no positive)', or 'inconclusive or unknown'
  print("Adding results category column with values '>= 1 positive', '>= 1 negative (no positive)', or 'inconclusive or unknown')...")
  results_df = add_lab_results_category_column(
    results_df, 'result_short_collect_list', 'results_category')
  
  # Add column with first (earliest) ordering datetime
  results_df = results_df \
    .withColumn('first_ordering_datetime', F.element_at('observation_datetime_collect_list', 1))
  
  return results_df
