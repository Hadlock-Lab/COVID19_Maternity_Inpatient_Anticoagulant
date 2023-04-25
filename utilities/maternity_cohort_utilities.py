# load environment
from datetime import date
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import numpy as np

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.maternity_cohort_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.summary_block


# Get patients that have delivered and associated information
def get_maternity_cohort_custom():
  """Get all patients that have delivered and associated information
  
  Parameters: None
  
  Returns:
  PySpark df: Dataframe containing patients that have delivered (there is a separate record for
              each delivery for patients who have delivered more than once)
  
  """
  # Define filter string to get summary block records of interest
  summary_block_filter_string = \
  """
  type IN ('PREGNANCY', 'DELIVERY', 'DELIVERY SUMMARY', 'STORK CONVERSION HBD')
  """
  
  # Specify columns to keep in the patient table
  include_cohort_columns = ['birth_date', 'ethnic_group', 'race']
  
  # Get summary block records
  additional_columns = ['ob_sticky_note_text', 'pregravid_bmi',
                        'ob_delivery_total_delivery_blood_loss_ml']
  omit_columns = [c for c in summary_blocks_default_columns_to_omit() 
                  if not(c in additional_columns)]
  results_df = get_summary_block_records(
    include_cohort_columns=include_cohort_columns,
    join_delivery_summary = True,
    join_ob_history = True,
    filter_string=summary_block_filter_string,
    omit_columns=omit_columns)
  
  # Get child episode records
  print('Getting child episode records...')
  child_episodes_df = results_df \
    .withColumnRenamed('episode_id', 'child_episode_id') \
    .select('child_episode_id', 'instance',
            F.col('type').alias('child_type'), \
            F.col('ob_delivery_episode_type').alias('child_ob_delivery_episode_type'),
            F.col('ob_delivery_pregnancy_episode').alias('episode_id'),
            'delivery_delivery_method',
            F.col('ob_delivery_delivery_date').alias('child_ob_delivery_delivery_date'),
            'ob_delivery_birth_csn_baby',
            'ob_delivery_record_baby_id',
            F.col('ob_delivery_labor_onset_date').alias('child_ob_delivery_labor_onset_date'),
            'delivery_birth_comments',
            'delivery_infant_birth_length_in',
            'delivery_infant_birth_weight_oz',
            'ob_delivery_department',
            'ob_history_last_known_living_status',
            'ob_hx_gestational_age_days',
            'ob_hx_delivery_site',
            'ob_hx_delivery_site_comment',
            'ob_hx_infant_sex',
            'ob_hx_living_status',
            'ob_hx_outcome',
            F.col('ob_sticky_note_text').alias('child_ob_sticky_note_text'),
            F.col('ob_delivery_total_delivery_blood_loss_ml') \
              .alias('child_ob_delivery_total_delivery_blood_loss_ml')) \
    .where(F.col('type').isin('DELIVERY', 'DELIVERY SUMMARY', 'STORK CONVERSION HBD'))
  
  # Load cord vessels table
  cord_vessels_df = spark.sql("SELECT * FROM rdp_phi.summaryblockdeliverycordvessels") \
    .withColumnRenamed('summary_block_id', 'child_episode_id') \
    .select('child_episode_id', 'instance',
            #F.col('deliverycordcomplications').alias('delivery_cord_complications'),
            F.col('deliverycordvessels').alias('delivery_cord_vessels'))
 
  # Join cord vessels df to the child episode df
  print("Joining delivery cord vessels records...")
  child_episodes_df = child_episodes_df \
    .join(cord_vessels_df, ['child_episode_id', 'instance'], how='left')
  
  # Omit columns from the mother records
  results_df = results_df \
    .where(F.col('type') == 'PREGNANCY') \
    .drop('ob_delivery_pregnancy_episode') \
    .drop('delivery_delivery_method') \
    .drop('ob_delivery_birth_csn_baby') \
    .drop('ob_delivery_record_baby_id') \
    .drop('delivery_birth_comments') \
    .drop('delivery_infant_birth_length_in') \
    .drop('delivery_infant_birth_weight_oz') \
    .drop('ob_delivery_department') \
    .drop('ob_history_last_known_living_status') \
    .drop('ob_hx_gestational_age_days') \
    .drop('ob_hx_delivery_site') \
    .drop('ob_hx_delivery_site_comment') \
    .drop('ob_hx_infant_sex') \
    .drop('ob_hx_living_status') \
    .drop('ob_hx_outcome')
  
  # Perform left join to match child to mother records
  # A left join preserves mother records in instances where the child has not been born
  print('Joining child to mother episode records...')
  results_df = results_df.join(child_episodes_df, ['episode_id', 'instance'], how='left')
  
  # Add a column for gestational age
  print('Adding a column for gestational age...')
  results_df = results_df.withColumn(
    'gestational_days',
    F.lit(280) + F.datediff(F.col('ob_delivery_delivery_date'), F.col('working_delivery_date')))
  
  # Function to map to term, late preterm, moderate preterm, very preterm, and extremely preterm
  def map_to_preterm_labels(gestational_age):
    if(gestational_age is None):
      return None
    elif(gestational_age >= 259):
      return 'term'
    elif(gestational_age >= 238):
      return 'late_preterm'
    elif(gestational_age >= 224):
      return 'moderate_preterm'
    elif(gestational_age >= 196):
      return 'very_preterm'
    else:
      return 'extremely_preterm'
  
  # Define UDF function for mapping gestational age to term, late preterm, etc
  map_to_preterm_labels_udf = F.udf(lambda ga: map_to_preterm_labels(ga), StringType())
    
  # Add column with labels: term, late preterm, moderate preterm, very preterm, and extremely preterm
  print("Adding column to map gestational age to 'term', 'late preterm', 'moderate preterm', etc...")
  results_df = results_df.withColumn('preterm_category',
                                     map_to_preterm_labels_udf(F.col('gestational_days')))
  
  # Add a column for last menstrual period (LMP)
  print('Adding a column for last menstrual period (LMP)...')
  results_df = results_df.withColumn(
    'lmp', F.expr("date_sub(ob_delivery_delivery_date, gestational_days)"))
  
  return results_df
  