# load environment
import re
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame 

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.vitals_cc_registry


# Get list of vital sign clinical concepts
def get_vitals_cc_list():
  """Get the full list of vital sign clinical concept labels
  
  Returns:
  list: List of all available vital sign clinical concept labels
  
  """
  return list(vitals_cc_registry.keys())


# Get SNOMED codes for vital sign clinical concept
def get_vitals_cc_codes(vitals_cc, include_descendant_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get SNOMED codes with descriptions for a specified vital sign clinical concept
  
  Parameters:
  vitals_cc (str): A string corresponding to a vital sign clinical concept (e.g. 'blood_pressure', 'heart_rate')
  include_descendant_codes (bool): If True, return clinical concept SNOMED codes and descendant codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing two columns: 'snomed_code', 'name'.
              
  """
  # Validate vitals_cc string
  valid_cc_strings = get_vitals_cc_list()
  assert(vitals_cc in valid_cc_strings)
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Convert to PySpark dataframe
  print("Getting SNOMED codes for '{}' vitals clinical concept...".format(vitals_cc))
  vitals_cc_codes = vitals_cc_registry[vitals_cc]
  codes_pd_df = pd.DataFrame(vitals_cc_codes, columns=['snomed_code'])
  schema = [StructField('snomed_code', StringType(), True)]
  results_df = spark.createDataFrame(codes_pd_df, schema=StructType(schema))
  
  # Get SNOMED OMOP primary concept codes
  omop_primary_concepts_df = spark.sql(
    """
    SELECT
      concept_code as snomed_code,
      concept_name as name,
      concept_id as concept_id_1
    FROM {database}.omop_concept_{version}
    WHERE vocabulary_id = 'SNOMED'
    """.format(database=omop_tables_location, version=omop_version))
  
  # Join descriptions and optionally get descendant codes
  if(not include_descendant_codes):
    # Join SNOMED code descriptions
    results_df = results_df \
      .join(F.broadcast(omop_primary_concepts_df), ['snomed_code'], how='left') \
      .select('snomed_code', 'name')
  else:
    # Get concept ID mapping
    concept_id_mapping_df = spark.sql(
      """
      SELECT
        ancestor_concept_id as concept_id_1,
        descendant_concept_id as concept_id_2
      FROM {database}.omop_concept_ancestor_{version}
      """.format(database=omop_tables_location, version=omop_version))
    
    # Get SNOMED OMOP secondary concept codes
    omop_secondary_concepts_df = spark.sql(
      """
      SELECT
        concept_id as concept_id_2,
        concept_name as descendant_snomed_name,
        concept_code as descendant_snomed_code
      FROM {database}.omop_concept_{version}
      WHERE vocabulary_id = 'SNOMED'
      """.format(database=omop_tables_location, version=omop_version))
    
    # Get descendant SNOMED codes and descriptions
    print('Getting expanded list of SNOMED codes...')
    results_df = results_df \
      .join(F.broadcast(omop_primary_concepts_df), ['snomed_code'], how='left') \
      .join(F.broadcast(concept_id_mapping_df), ['concept_id_1'], how='inner') \
      .join(F.broadcast(omop_secondary_concepts_df), ['concept_id_2'], how='inner')
    
    # Prepare final results
    results_df = results_df \
      .select('snomed_code', 'name') \
      .union(
        results_df.select(
          F.col('descendant_snomed_code').alias('snomed_code'),
          F.col('descendant_snomed_name').alias('name'))) \
      .dropDuplicates()
  
  return results_df


# Add categorical column for vitals cc
def add_vitals_cc_column(vitals_df, cc_label, search_column='code', values_column='value'):
  """Add a column that contains vital sign values for a specified vitals cc category. This column
  will be a numerical value if a given record is in the specified vitals cc category, otherwise it
  will be None (null) for records not in the specified vitals cc category.
  
  Parameters:
  vitals_df (PySpark df): Dataframe containing vital sign records. By default, this Dataframe must
                          contain a column labeled 'code'.
  cc_label (str): A string corresponding to a vital sign clinical concept (e.g. 'blood_pressure',
                  'heart_rate'). This cc_label will be used as the name of the newly-added column.
  search_column (str): Name of column to search for the clinical concept codes. The provided Dataframe
                       (vitals_df) must contain this column.
  values_column (str): Name of column in vitals_df containing the vital sign record value. The default
                       is 'value'
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the vital sign values for the
              specified vitals cc category (None i.e. null for records not in the cc category)
  
  """
  # vitals_df must be a PySpark dataframe
  assert(isinstance(vitals_df, DataFrame))
  
  # cc_label must be a valid vital sign clinical concept string
  assert(cc_label in get_vitals_cc_list())
  
  # vitals_df must contain the following columns: search_column, values_column
  assert(all([s in vitals_df.columns for s in [search_column, values_column]]))
  
  # Get vitals cc codes
  vitals_cc_codes = vitals_cc_registry[cc_label]
  
  # Define utility function for checking if a record is in the specified cc category
  def get_value_if_in_cc_category(codes, value):
    if(codes is None):
      return None
    
    if(value is None):
      return None
    
    # Make sure codes is a list
    codes = codes if isinstance(codes, list) else [codes]
    
    # Check if any codes are in the specified cc category
    in_cc_category = any([s in vitals_cc_codes for s in codes])
    if(in_cc_category):
      return str(value)
    else:
      return None
  
  # Create UDF for utility function
  get_value_if_in_cc_category_udf = F.udf(get_value_if_in_cc_category, StringType())
  
  # Add column with vital sign record value if record is in cc category (otherwise, None)
  results_df = vitals_df \
    .withColumn(cc_label, get_value_if_in_cc_category_udf(search_column, values_column))
  
  return results_df
  
