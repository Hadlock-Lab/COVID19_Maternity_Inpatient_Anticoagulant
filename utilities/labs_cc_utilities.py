# load libraries
import re
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame 

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.labs_cc_registry


# define functions
# Get list of lab clinical concepts that have string search definitions
def get_lab_cc_list_string_search():
  return list(cc_lab_search_strings.keys())


# Get list of all lab clinical concepts
def get_lab_cc_list():
  """Get the full list of lab clinical concept labels
  
  Returns:
  list: List of all available lab clinical concept labels
  
  """
  search_string_list = get_lab_cc_list_string_search()
  return list(labs_cc_registry.keys()) + search_string_list


# Get LOINC codes for lab clinical concept
def get_lab_cc_codes(lab_cc, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get LOINC codes with descriptions for a specified lab clinical concept
  
  Parameters:
  lab_cc (str): A string corresponding to a lab clinical concept (e.g. 'sars_cov_2')
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing two columns: 'loinc_code', 'name'.
              
  """
  # Validate lab_cc string
  valid_cc_strings = get_lab_cc_list()
  assert(lab_cc in valid_cc_strings)
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Convert to PySpark dataframe
  print("Getting LOINC codes with descriptions for '{}' lab clinical concept...".format(lab_cc))
  lab_cc_codes = labs_cc_registry[lab_cc]
  codes_pd_df = pd.DataFrame(lab_cc_codes, columns=['loinc_code'])
  schema = [StructField('loinc_code', StringType(), True)]
  results_df = spark.createDataFrame(codes_pd_df, schema=StructType(schema))
  
  # Get LOINC OMOP primary concept codes
  omop_primary_concepts_df = spark.sql(
    """
    SELECT
      concept_code as loinc_code,
      concept_name as name,
      concept_id as concept_id_1
    FROM {database}.omop_concept_{version}
    WHERE vocabulary_id = 'LOINC'
    """.format(database=omop_tables_location, version=omop_version))
  
  # Join LOINC code descriptions
  results_df = results_df \
    .join(F.broadcast(omop_primary_concepts_df), ['loinc_code'], how='left').select('loinc_code', 'name')  
    
  return results_df

# Utility functions for add_lab_cc_column
# Check if a record is in the specified cc category based on LOINC codes
def get_value_if_in_cc_category(cc_label, loinc_codes, result_value):
  if(loinc_codes is None):
    return None

  if(result_value is None):
    return None
  
  # Get lab cc codes
  lab_cc_codes = labs_cc_registry[cc_label]
  
  # Make sure loinc_codes is a list
  codes = loinc_codes if isinstance(loinc_codes, list) else [loinc_codes]

  # Check if any loinc codes are in the specified cc category
  in_cc_category = any([s in lab_cc_codes for s in codes])
  if(in_cc_category):
    return str(result_value)
  else:
    return None

# Create UDF for utility function
get_value_if_in_cc_category_udf = F.udf(get_value_if_in_cc_category, StringType())


# Check if a record is in the specified cc category based on string searches
def get_value_if_string_match(cc_label, result_value, *search_key_vals):
  if(result_value is None):
    return None
  
  # Get search term dictionary
  search_terms = cc_lab_search_strings[cc_label]
  
  # Check for string matches
  assert(len(search_key_vals) % 2 == 0)
  key_vals = [(search_key_vals[2*i], search_key_vals[2*i+1]) for i in range(int(len(search_key_vals)/2))]
  match = all([any([(s in v) for s in search_terms[k]]) for k, v in key_vals])
  
  # Return result value if search terms match
  if (match):
    return str(result_value)
  else:
    return None

# Create UDF for utility function
get_value_if_string_match_udf = F.udf(get_value_if_string_match, StringType())


 # Check if a record is in the specified cc category based on LOINC codes
def get_value_if_in_cc_category(cc_label, loinc_codes, result_value):
  if(loinc_codes is None):
    return None

  if(result_value is None):
    return None
  
  # Get lab cc codes
  lab_cc_codes = labs_cc_registry[cc_label]
  
  # Make sure loinc_codes is a list
  codes = loinc_codes if isinstance(loinc_codes, list) else [loinc_codes]

  # Check if any loinc codes are in the specified cc category
  in_cc_category = any([s in lab_cc_codes for s in codes])
  if(in_cc_category):
    return str(result_value)
  else:
    return None

# Create UDF for utility function
get_value_if_in_cc_category_udf = F.udf(get_value_if_in_cc_category, StringType())


# Check if a record is in the specified cc category based on string searches
def get_value_if_string_match(cc_label, result_value, *search_key_vals):
  if(result_value is None):
    return None
  
  # Get search term dictionary
  search_terms = cc_lab_search_strings[cc_label]
  
  # Check for string matches
  assert(len(search_key_vals) % 2 == 0)
  key_vals = [(search_key_vals[2*i], search_key_vals[2*i+1]) for i in range(int(len(search_key_vals)/2))]
  match = all([any([(s in v) for s in search_terms[k]]) for k, v in key_vals])
  
  # Return result value if search terms match
  if (match):
    return str(result_value)
  else:
    return None

# Create UDF for utility function
get_value_if_string_match_udf = F.udf(get_value_if_string_match, StringType())


# add categorical column for lab cc
def add_lab_cc_column(labs_df, cc_label, search_column='mapped_loinc_codes', values_column='result_value'):
  """Add a column that contains lab result values for a specified lab cc category. This column can be
  a numerical value, or it can be a string indicating a categorical lab test result (e.g. positive,
  negative, etc). The value will be None (null) for lab results not in the specified lab cc category.
  
  Parameters:
  labs_df (PySpark df): Dataframe containing lab result records. By default, this Dataframe must
                        contain a column labeled 'mapped_loinc_codes'.
  cc_label (str): A string corresponding to a lab clinical concept (e.g. 'sars_cov_2'). This cc_label
                  will be used as the name of the newly-added column.
  search_column (str): Name of column to search for the clinical concept codes. The provided Dataframe
                       (labs_df) must contain this column.
  values_column (str): Name of column in labs_df containing the lab result value. The default is
                       'result_value'
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the lab result values for the
              specified device cc category (None i.e. null for lab results not in the cc category)
  
  """
  # labs_df must be a PySpark dataframe
  assert(isinstance(labs_df, DataFrame))
  
  # Get list of all possible lab clinial concept labels
  string_search_lab_labels = get_lab_cc_list_string_search()
  all_lab_labels = get_lab_cc_list()
  
  # cc_label must be a valid lab clinical concept string
  assert(cc_label in all_lab_labels)
    
  # Add column with lab result value if record is in cc category (otherwise, None)
  if (cc_label in string_search_lab_labels):
    # Get names of fields to search
    search_field_names = list(cc_lab_search_strings[cc_label].keys())
    search_key_vals = [item_ for f in search_field_names for item_ in [F.lit(f), F.col(f)]]
    
    # labs_df must contain the following columns: search_column, values_column
    assert(all([s in labs_df.columns for s in search_field_names]))
    
    results_df = labs_df \
      .withColumn(cc_label, get_value_if_string_match_udf(F.lit(cc_label), F.col(values_column), *search_key_vals))
  else:
    # labs_df must contain the following columns: search_column, values_column
    assert(all([s in labs_df.columns for s in [search_column, values_column]]))
    
    results_df = labs_df \
      .withColumn(cc_label, get_value_if_in_cc_category_udf(F.lit(cc_label), search_column, values_column))
  
  return results_df


# Utility function to add mapped LOINC code column
def add_mapped_loinc_code_column(df):
  """When joining the labresult table, use this function to aggregate all available loinc codes by
  the result_name column and then join those aggregated loinc codes as a new column
  
  Parameters:
  df (PySpark df): Dataframe to which to add an aggregated loinc codes column. This Dataframe must
                   contain the following labresult table columns: loinc_code, default_loinc_code,
                   result_name. It adds a column labeled: 'mapped_loinc_codes'
  
  Returns:
  PySpark df: The original Dataframe with an additional column (called 'mapped_loinc_codes') with
              aggregated loinc codes that correspond to the result_name field
  
  """
  # df must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Confirm that df has the needed columns
  assert(all([s in df.columns for s in ['loinc_code', 'default_loinc_code', 'result_name']]))
  
  # Add a temporary column that contains available loinc codes
  results_df = df.withColumn(
    'temp_loinc_or_default_code', F.when(F.col('loinc_code').isNull(),
                                         F.col('default_loinc_code')).otherwise(F.col('loinc_code')))
  
  # Convenience function for removing leading X from loinc codes
  def process_loinc_code(code):
    if(code is None):
      return None

    processed_code = code
    if(processed_code[0] == 'X'):
      processed_code = processed_code[1:]
    return processed_code

  # Create UDF for LOINC-processing functions
  process_loinc_code_udf = F.udf(process_loinc_code, StringType())

  # Remove leading X from LOINC codes
  results_df = results_df \
    .withColumn('loinc_or_default_code',
                process_loinc_code_udf(F.col('temp_loinc_or_default_code'))) \
    .drop('temp_loinc_or_default_code')

  # Generate result_name to LOINC code mapping
  print('Adding mapped LOINC codes column...')
  result_name_to_loinc_mapping_df = results_df \
    .groupBy('result_name') \
    .agg(F.collect_set('loinc_or_default_code').alias('mapped_loinc_codes'))

  # Join the mapping to results_df
  results_df = results_df.join(result_name_to_loinc_mapping_df.hint("broadcast"), ['result_name'], how='left')
  
  return results_df


 # Add a column with positive/negative lab results mapped 
 def add_pos_neg_lab_result_column(lab_results_df, results_column, new_column_name):
  """Add new column with cleaned positive/negative lab result string
  
  Parameters:
  lab_results_df (PySpark df): Dataframe containing positive/negative-type lab results.
  results_column (str): Name of column to search for lab results. The provided Dataframe
                        (lab_results_df) must contain this column.
  new_column_name (str): Name of the new column to be added to the dataframe
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the cleaned lab result string
  
  """
  # lab_results_df must be a PySpark dataframe
  assert(isinstance(lab_results_df, DataFrame))
  
  # results_column must be a column in lab_results_df
  assert(results_column in lab_results_df.columns)
  
  # new_column_name must be a string
  assert(isinstance(new_column_name, str))
  
  # Cleaned strings classified as positive results
  positive_strings = ['positive', 'detected', 'presumptive pos', 'detected see scanned report',
                      'detcted', 'presumptive positive']
  
  # Cleaned strings classified as negative results
  negative_strings = ['none detected', 'undetected', 'not dectected', 'negative', 'not detected',
                      'not deteced', 'not detected see scanned result', 'not detectd',
                      'not detected see scanned report', 'negatiev', 'not detected',
                      'not detected see media', 'non detected', 'not dtected',
                      'not detected see scanned results', 'not detecte', 'none detected']
  
  # Cleaned strings classified as neither positive nor negative
  other_strings = ['see scanned report', 'comment', 'see scanned result',
                   'abnormal see scanned report', 'see report', 'see scanned results',
                   'see comments', 'see scanned report covid19', 'see comment',
                   'please see scanned report', 'see note', 'separate report to follow',
                   'see attached report', 'refer to separate reference lab report for results']
  
  # Utility function for result mapping
  def result_mapping(result_string):
    # Clean the resultvalue string
    resultvalue_cleaned = clean_string(result_string)

    # Return mapped string
    if(resultvalue_cleaned in positive_strings):
      return 'positive'
    elif(resultvalue_cleaned in negative_strings):
      return 'negative'
    elif(resultvalue_cleaned in other_strings):
      return 'see report or comment'
    else:
      return 'unknown'
  
  # Make result mapping UDF
  result_mapping_udf = F.udf(lambda result_str: result_mapping(result_str), StringType())
  
  # Add column with test result as 'positive', 'negative', 'see report or comment', or 'unknown'
  results_df = lab_results_df.withColumn(new_column_name, result_mapping_udf(F.col(results_column)))
  
  return results_df


 # Add lab results category column (based on 1 or more test results)
 def add_lab_results_category_column(lab_results_df, results_column, new_column_name):
  """Add new column with a results category column containing one of the following values:
  '>= 1 positive', '>= 1 negative (no positive)', or 'inconclusive or unknown'
  
  Parameters:
  lab_results_df (PySpark df): Dataframe containing positive/negative-type lab results.
  results_column (str): Name of column to search for list of lab results. The provided Dataframe
                        (lab_results_df) must contain this column.
  new_column_name (str): Name of the new column to be added to the dataframe
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the results category
  
  """
  # lab_results_df must be a PySpark dataframe
  assert(isinstance(lab_results_df, DataFrame))
  
  # results_column must be a column in lab_results_df
  assert(results_column in lab_results_df.columns)
  
  # new_column_name must be a string
  assert(isinstance(new_column_name, str))
    
  # Utility function for mapping a list of test results to a results category
  def results_category_map(result_list):
    # Make sure result_list is actually a list
    result_list = result_list if isinstance(result_list, list) else [result_list]
    
    # Categorize patient based on one or more tests
    if 'positive' in result_list:
      return '>= 1 positive'
    elif ('negative' in result_list) and ('positive' not in result_list):
      return '>= 1 negative (no positive)'
    else:
      return 'inconclusive or unknown'
  
  # Make results category map UDF
  results_category_map_udf = F.udf(
    lambda results_list: results_category_map(results_list), StringType())
  
  # Add column with results category value
  results_df = lab_results_df.withColumn(
    new_column_name, results_category_map_udf(F.col(results_column)))
  
  return results_df