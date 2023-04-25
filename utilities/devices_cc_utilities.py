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
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.devices_cc_registry


# Get list of device clinical concepts
def get_device_cc_list():
  """Get the full list of device clinical concept labels
  
  Returns:
  list: List of all available device clinical concept labels
  
  """
  return list(devices_cc_registry.keys())


# Get SNOMED codes for device clinical concept
def get_device_cc_codes(device_cc, include_descendant_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get SNOMED codes with descriptions for a specified device clinical concept
  
  Parameters:
  device_cc (str): A string corresponding to a device clinical concept (e.g. 'oxygen_equipment')
  include_descendant_codes (bool): If True, return clinical concept SNOMED codes and descendant codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing two columns: 'snomed_code', 'name'.
              
  """
  # Validate device_cc string
  valid_cc_strings = get_device_cc_list()
  assert(device_cc in valid_cc_strings)
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Convert to PySpark dataframe
  print("Getting SNOMED codes for '{}' device clinical concept...".format(device_cc))
  device_cc_codes = devices_cc_registry[device_cc]
  codes_pd_df = pd.DataFrame(device_cc_codes, columns=['snomed_code'])
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


# Add categorical column for device cc
def add_device_cc_column(devices_df, cc_label, search_column='code', values_column='value'):
  """Add a column that contains device values for a specified device cc category. This column could be
  a numerical value, or a string indicating a categorical result (e.g. 'nasal cannula'). The value
  will be None (null) for records not in the specified device cc category.
  
  Parameters:
  devices_df (PySpark df): Dataframe containing device records. By default, this Dataframe must
                           contain a column labeled 'code'.
  cc_label (str): A string corresponding to a device clinical concept (e.g. 'crrt'). This cc_label
                  will be used as the name of the newly-added column.
  search_column (str): Name of column to search for the clinical concept codes. The provided Dataframe
                       (devices_df) must contain this column.
  values_column (str): Name of column in devices_df containing the device record value. The default is
                       'value'
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the device values for the
              specified device cc category (None i.e. null for records not in the cc category)
  
  """
  # devices_df must be a PySpark dataframe
  assert(isinstance(devices_df, DataFrame))
  
  # cc_label must be a valid device clinical concept string
  assert(cc_label in get_device_cc_list())
  
  # devices_df must contain the following columns: search_column, values_column
  assert(all([s in devices_df.columns for s in [search_column, values_column]]))
  
  # Get device cc codes
  device_cc_codes = devices_cc_registry[cc_label]
  
  # Define utility function for checking if a record is in the specified cc category
  def get_value_if_in_cc_category(codes, value):
    if(codes is None):
      return None
    
    if(value is None):
      return None
    
    # Make sure codes is a list
    codes = codes if isinstance(codes, list) else [codes]
    
    # Check if any codes are in the specified cc category
    in_cc_category = any([s in device_cc_codes for s in codes])
    if(in_cc_category):
      return str(value)
    else:
      return None
  
  # Create UDF for utility function
  get_value_if_in_cc_category_udf = F.udf(get_value_if_in_cc_category, StringType())
  
  # Add column with device record value if record is in cc category (otherwise, None)
  results_df = devices_df \
    .withColumn(cc_label, get_value_if_in_cc_category_udf(search_column, values_column))
  
  return results_df


# Add a column with device values mapped to WHO scores
def add_device_who_score_column(df, value_column, new_column_name):
  """Add new column with device record values mapped to WHO scores (to be used with oxygen_device
  clinical concept)
  
  Parameters:
  df (PySpark df): Dataframe containing oxygen device/equipment results.
  value_column (str): Name of column to search for oxygen device records. The provided Dataframe
                      (devices_df) must contain this column.
  new_column_name (str): Name of the new column to be added to the dataframe
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the mapped WHO score
  
  """
  # devices_df must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # results_column must be a column in devices_df
  assert(value_column in df.columns)
  
  # new_column_name must be a string
  assert(isinstance(new_column_name, str))
  
  # Cleaned strings classified as WHO score 6
  who_6_strings = ['bag valve mask', 'et tube', 'ett', 'manual bag ventilation',
                   'mechanical ventilation', 'o2/bag valve ventilation', 'o2/ventilator',
                   'transport vent', 'ventilator']
  
  # Cleaned strings classified as WHO score 5
  who_5_strings = ['aerosol mask', 'aerosol trach', 'bipap', 'bi-pap', 'bubble cpap',
                   'continuous aerosol', 'cpap', 'hfnc', 'high flow nasal cannula', 'high flow nc',
                   'high-flow nasal cannula', 'high frequency ventilation', 'laryngeal mask airway',
                   'mask-aerosol', 'mask-nrb', 'nasal prongs', 'ncpap', 'non-invasive ventilation',
                   'non-invasive ventilation (i.e. bi-level)', 'non-invasive ventilation (niv)',
                   'nonrebreather mask', 'non-rebreather mask', 'nrb mask', 'tracheostomy collar',
                   'vapotherm', 'venti-mask', 'venturi mask']
  
  # Cleaned strings classified as WHO score 4
  who_4_strings = ['cannula', 'face tent', 'mask-simple', 'nasal cannula', 'nasal mask', 'nc',
                   'o2 via face mask', 'o2 via nasal cannula', 'o2/cannula', 'o2/simple mask',
                   'open oxygen mask', 'oxygen hood', 'oxygen tent', 'oxymask',
                   'partial rebreather mask', 'simple face mask', 'simple mask', 'trach mask']
  
  # Cleaned strings classified as WHO score 3
  who_3_strings = ['none - ra', 'none (room air)', 'room air', 'room air (none)', 'room air']
  
  # Utility function for value mapping
  def value_mapping(value_string):
    if(value_string is None):
      return None
    
    # Make value_string lower case
    value_string_lower = value_string.lower()

    # Return mapped value
    if(any([s in value_string_lower for s in who_6_strings])):
      return 6
    elif(any([s in value_string_lower for s in who_5_strings])):
      return 5
    elif(any([s in value_string_lower for s in who_4_strings])):
      return 4
    elif(any([s in value_string_lower for s in who_3_strings])):
      return 3
    else:
      return None
  
  # Make value mapping UDF
  value_mapping_udf = F.udf(lambda value_str: value_mapping(value_str), IntegerType())
  
  # Add column with mapped WHO score: 6, 5, 4, 3, or None
  results_df = df.withColumn(new_column_name, value_mapping_udf(F.col(value_column)))
  
  return results_df
  
