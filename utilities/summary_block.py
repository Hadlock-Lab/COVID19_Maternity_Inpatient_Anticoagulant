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


# Utility functions to get default columns to omit
def summary_blocks_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the summary block table and other tables
  that are optionally joined
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing summary-block-related table fields to omit by default
  
  """
  # Summary block table columns to omit
  summaryblock_columns_to_omit = ['labor_onset_datetime', 'induction_datetime', 'ob_sticky_note_text', 'pregravid_bmi', 'pregravid_weight']
  
  # Optionally-joined table columns to omit
  summaryblockdeliverysummary_columns_to_omit = ['ob_time_rom_to_delivery', 'start_pushing_instant', 'delivery_birth_instant', 'ob_delivery_induction_date', 'antibiotics_during_labor', 'delivery_placental_instant', 'ob_delivery_1st_stage_hours', 'ob_delivery_1ststage_minutes', 'ob_delivery_2nd_stage_hours', 'ob_delivery_2nd_stage_minutes', 'ob_delivery_3rd_stage_hours', 'ob_delivery_3rd_stage_minutes', 'ob_delivery_blood_loss_ml', 'ob_delivery_total_delivery_blood_loss_ml', 'ob_delivery_dilation_complete_date']
  summaryblockobhistory_columns_to_omit = ['ob_hx_order']
  
  columns_to_omit_table_map = {
    'summaryblock': summaryblock_columns_to_omit,
    'summaryblockdeliverysummary': summaryblockdeliverysummary_columns_to_omit,
    'summaryblockobhistory': summaryblockobhistory_columns_to_omit
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


# Get summary block records based on specified filter criteria
def get_summary_block_records(cohort_df=None, include_cohort_columns=None, join_delivery_summary=False, join_ob_history=False, filter_string=None, omit_columns=summary_blocks_default_columns_to_omit()):
  """Get summary block records based on specified filter criteria
  
  Parameters:
  cohort_df (PySpark df): Optional cohort dataframe for which to get summary block records
                          (if None, get all patients in the RDP). This dataframe *must* include
                          pat_id and instance columns for joining
  include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None, in which case all columns from cohort dataframe are kept.
  join_delivery_summary (bool): If True, join the summaryblockdeliverysummary table
  join_ob_history (bool): If True, join the summaryblockobhistory table
  filter_string (string): String to use as a WHERE clause filter/condition. Default is None.
  omit_columns (list): List of columns/fields to exclude from the final dataframe.
  
  Returns:
  PySpark df: Dataframe containing summary block records satisfying specified filter criteria
  
  """
  # If not None, filter_string must be a string
  if(filter_string is not None):
    assert(isinstance(filter_string, str))
  
  # 'Omit' columns input must be a list
  omit_columns_from_final = [] if omit_columns is None else omit_columns
  assert(isinstance(omit_columns_from_final, list))
  
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
  
  # Join patient cohort to summary block table
  print('Joining summary block table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        pat_id,
        instance,
        episode_id,
        name,
        type,
        status,
        startdate as start_date,
        enddate as end_date,
        laboronsetdatetime as labor_onset_datetime,
        workingdeliverydate as working_delivery_date,
        inductiondatetime as induction_datetime,
        numberoffetuses as number_of_fetuses,
        obstickynotetext as ob_sticky_note_text,
        pregravidbmi as pregravid_bmi,
        pregravidweight as pregravid_weight
      FROM rdp_phi.summaryblock"""), ['pat_id', 'instance'], how='left')
    
  # If birth_date is present in the cohort table, calculate age at episode start date
  if('birth_date' in results_df.columns):
    print('Adding age (at episode start date) to results...')
    results_df = results_df.withColumn(
      'age_at_start_dt',
      F.round(F.datediff(F.col('start_date'), F.col('birth_date'))/365.25, 1))
  
  # Optionally join summaryblockdeliverysummary table
  if(join_delivery_summary):
    print('Joining summaryblockdeliverysummary table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          summary_block_id,
          instance,
          obdeliverypregnancyepisode as ob_delivery_pregnancy_episode,
          obdeliverydeliverydate as ob_delivery_delivery_date,
          deliverydeliverymethod as delivery_delivery_method,
          obdeliveryepisodetype as ob_delivery_episode_type,
          obdeliverydeliverycsnmom as ob_delivery_delivery_csn_mom,
          obdeliverybirthcsn_baby as ob_delivery_birth_csn_baby,
          obdeliveryrecordbabyid as ob_delivery_record_baby_id,
          obdeliverylaboronsetdate as ob_delivery_labor_onset_date,
          obtimeromtodelivery as ob_time_rom_to_delivery,
          startpushinginstant as start_pushing_instant,
          deliverybirthinstant as delivery_birth_instant,
          obdeliveryinductiondate as ob_delivery_induction_date,
          antibioticsduringlabor as antibiotics_during_labor,
          deliverybirthcomments as delivery_birth_comments,
          deliveryinfantbirthlengthin as delivery_infant_birth_length_in,
          deliveryinfantbirthweightoz as delivery_infant_birth_weight_oz,
          deliveryplacentalinstant as delivery_placental_instant,
          obdelivery1ststagehours as ob_delivery_1st_stage_hours,
          obdelivery1ststageminutes as ob_delivery_1ststage_minutes,
          obdelivery2ndstagehours as ob_delivery_2nd_stage_hours,
          obdelivery2ndstageminutes as ob_delivery_2nd_stage_minutes,
          obdelivery3rdstage_hours as ob_delivery_3rd_stage_hours,
          obdelivery3rdstageminutes as ob_delivery_3rd_stage_minutes,
          obdeliverybloodlossml as ob_delivery_blood_loss_ml,
          obdeliverytotaldeliverybloodlossml as ob_delivery_total_delivery_blood_loss_ml,
          obdeliverydepartment as ob_delivery_department,
          obdeliverydilationcompletedate as ob_delivery_dilation_complete_date
        FROM rdp_phi.summaryblockdeliverysummary
        """).withColumnRenamed('summary_block_id', 'episode_id'), ['episode_id', 'instance'], how='left')
  
  # Optionally join summaryblockobhistory table
  if(join_ob_history):
    print('Joining summaryblockobhistory table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          episode_id,
          instance,
          obhistorylastknownlivingstatus as ob_history_last_known_living_status,
          obhxgestationalagedays as ob_hx_gestational_age_days,
          obhxdeliverysite as ob_hx_delivery_site,
          obhxdeliverysitecomment as ob_hx_delivery_site_comment,
          obhxinfantsex as ob_hx_infant_sex,
          obhxlivingstatus as ob_hx_living_status,
          obhxorder as ob_hx_order,
          obhxoutcome as ob_hx_outcome
        FROM rdp_phi.summaryblockobhistory
        """), ['episode_id', 'instance'], how='left')
    
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