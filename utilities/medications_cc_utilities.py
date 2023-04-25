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
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medications_cc_registry


# Get list of medication clinical concepts
def get_medication_cc_list():
  """Get the full list of medication clinical concept labels
  
  Returns:
  list: List of all available medication clinical concept labels
  
  """
  return list(medications_cc_registry.keys())


  # Get RxNorm codes for medication clinical concept
  def get_medication_cc_codes(medication_cc, include_related_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get RxNorm codes with descriptions for a specified medication clinical concept
  
  Parameters:
  medication_cc (str): A string corresponding to a medication clinical concept (e.g. 'antidepressant',
                       'antihypertensive', 'vasopressor')
  include_related_codes (bool): If True, return clinical concept RxNorm codes and related codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing two columns: 'rxnorm_code', 'name'.
              
  """
  # Validate medication_cc string
  valid_cc_strings = get_medication_cc_list()
  assert(medication_cc in valid_cc_strings)
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Convert to PySpark dataframe
  print("Getting RxNorm codes for '{}' medication clinical concept...".format(medication_cc))
  medication_cc_codes = medications_cc_registry[medication_cc]
  codes_pd_df = pd.DataFrame(medication_cc_codes, columns=['rxnorm_code'])
  schema = [StructField('rxnorm_code', StringType(), True)]
  results_df = spark.createDataFrame(codes_pd_df, schema=StructType(schema))
  
  # Get RxNorm OMOP primary concept codes
  omop_primary_concepts_df = spark.sql(
    """
    SELECT
      concept_code as rxnorm_code,
      concept_name as name,
      concept_id as concept_id_1
    FROM {database}.omop_concept_{version}
    WHERE domain_id = 'Drug' AND vocabulary_id = 'RxNorm'
    """.format(database=omop_tables_location, version=omop_version))
  
  # Join descriptions and optionally get related codes
  if(not include_related_codes):
    # Join RxNorm code descriptions
    results_df = results_df \
      .join(F.broadcast(omop_primary_concepts_df), ['rxnorm_code'], how='left') \
      .select('rxnorm_code', 'name')
  else:
    # Get concept ID mapping
    concept_id_mapping_df = spark.sql(
      """
      SELECT
        concept_id_1,
        concept_id_2
      FROM {database}.omop_concept_relationship_{version}
      """.format(database=omop_tables_location, version=omop_version))
    
    # Get RxNorm OMOP secondary concept codes
    omop_secondary_concepts_df = spark.sql(
      """
      SELECT
        concept_id as concept_id_2,
        concept_name as secondary_rxnorm_name,
        concept_code as secondary_rxnorm_code
      FROM {database}.omop_concept_{version}
      WHERE domain_id = 'Drug' AND vocabulary_id = 'RxNorm'
      """.format(database=omop_tables_location, version=omop_version))

    # Use OMOP concept tables to get expanded list of RxNorm codes
    print('Getting expanded list of related RxNorm codes from initial list...')
    results_df = results_df \
      .join(F.broadcast(omop_primary_concepts_df), ['rxnorm_code'], how='left') \
      .join(F.broadcast(concept_id_mapping_df), ['concept_id_1'], how='inner') \
      .join(F.broadcast(omop_secondary_concepts_df), ['concept_id_2'], how='inner')

    # Prepare final results
    results_df = results_df \
      .select('rxnorm_code', 'name') \
      .union(
        results_df.select(
          F.col('secondary_rxnorm_code').alias('rxnorm_code'),
          F.col('secondary_rxnorm_name').alias('name'))) \
      .dropDuplicates()
  
  return results_df


# Get medication_id list from RxNorm codes
def get_medication_ids_from_rxnorm(rxnorm_codes_df, use_column='rxnorm_code', include_descriptions=False):
  """Get medication_id values from a provided list of RxNorm codes

  Parameters:
  rxnorm_codes_df (PySpark df): Dataframe containing RxNorm codes
  use_column (str): Label of column containing RxNorm codes. Default is 'rxnorm_code'
  include_descriptions (bool): True to include medication descriptions
  
  Returns:
  PySpark df: Dataframe containing medication_id values corresponding to provided RxNorm codes

  """
  # rxnorm_codes_df must be a PySpark dataframe
  assert(isinstance(rxnorm_codes_df, DataFrame))
  
  # use_column must be a string
  assert(isinstance(use_column, str))
  
  # Join medicationrxnorm table to get medication_id values
  print('Getting medication_id values for provided RxNorm code list...')
  results_df = rxnorm_codes_df \
    .select(use_column).where(F.col(use_column).isNotNull()).dropDuplicates() \
    .join(spark.sql(
        """
        SELECT
          rxnormcode, medication_id, instance,
          termtype as term_type,
          codelevel as code_level
        FROM rdp_phi.medicationrxnorm
        """).withColumnRenamed('rxnormcode', use_column), [use_column], how='left')
  
  # Join medication table to get descriptions
  if(include_descriptions):
    print('Joining medication table to get descriptions...')
    results_df = results_df \
      .join(spark.sql(
        """
        SELECT medication_id, instance, name, shortname as short_name
        FROM rdp_phi.medication
        """), ['medication_id', 'instance'], how='inner')
  
  return results_df


# Get medication_id list for medication clinical concept
def get_medication_ids_for_medication_cc(medication_cc, include_related_codes=True):
  """Get medication_id values for a specified medication clinical concept

  Parameters:
  medication_cc (str): A string corresponding to a medication clinical concept (e.g. 'antidepressant',
                       'antihypertensive', 'vasopressor')
  include_related_codes (bool): If True, use expanded list of related RxNorm codes for the specified
                                clinical concept
  
  Returns:
  PySpark df: Dataframe containing two columns: 'medication_id', 'instance'

  """
  # Validate medication_cc string
  valid_cc_strings = get_medication_cc_list()
  assert(medication_cc in valid_cc_strings)
  
  # Get RxNorm codes for medication clinical concept
  medication_cc_codes_df = get_medication_cc_codes(
    medication_cc, include_related_codes=include_related_codes)

  # Get medication_id values for RxNorm codes
  medication_ids_df = get_medication_ids_from_rxnorm(rxnorm_codes_df=medication_cc_codes_df, use_column='rxnorm_code')
  
  return medication_ids_df.select('medication_id', 'instance').dropDuplicates()


# Generate query filter string for medication clinical concept
def get_filter_string_for_medication_cc(medication_cc, include_related_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Generate a query filter string for a specified medication clinical concept that
    can be used in a 'WHERE' clause
  
  Parameters:
  medication_cc (str or list): A string corresponding to one or more medication clinical concepts
                               (e.g. 'antidepressant', 'antihypertensive', 'vasopressor')
  include_related_codes (bool): If True, expand RxNorm codes to include related codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the
                            OMOP concept table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  str: A query filter string properly formatted for a 'WHERE' clause
  
  """
  # Convert medication_cc to a list, if necessary
  medication_cc_list = medication_cc if isinstance(medication_cc, list) else [medication_cc]
  
  # Validate medication_cc strings
  valid_cc_strings = get_medication_cc_list()
  assert(all([s in valid_cc_strings for s in medication_cc_list]))
  
  # Get individual query strings for each medication cc
  print("Generating medication query string...")
  query_string_list = []
  for medication_cc_label in medication_cc_list:
    print("Getting query string for '{}' medication clinical concept...".format(medication_cc_label))
    
    # Get RxNorm codes for medication clinical concept
    medication_cc_codes_df = get_medication_cc_codes(medication_cc_label, include_related_codes, omop_table_version, omop_tables_location)
    
    # Get medication_id values for RxNorm codes
    medication_ids_df = get_medication_ids_from_rxnorm(rxnorm_codes_df=medication_cc_codes_df, use_column='rxnorm_code')
    
    # Generate 'WHERE' clause filter string with medication_id values
    medications_query_string = get_filter_string_from_medication_id_list(medication_ids_df)
    
    # Concatenate individual medication query strings
    query_string_list = query_string_list + ["({})".format(medications_query_string)]
    query_string = " OR ".join(query_string_list)
  
  return medications_query_string


# Generate query filter string from medication_id list
def get_filter_string_from_medication_ids(medication_ids_df):
  """Generate a query filter string that can be used in a 'WHERE' clause.

  Parameters:
  medication_ids_df (PySpark df): Dataframe containing a list of medication_id values.
  These must be contained in a 'medication_id' column. There must also be an
  'instance' column.
  
  Returns:
  str: A query filter string properly formatted for a 'WHERE' clause

  """
  # medication_ids_df must be a PySpark dataframe
  assert(isinstance(medication_ids_df, DataFrame))
  
  # The dataframe must contain the following columns: 'medication_id', 'instance'
  assert(all([s in medication_ids_df.columns for s in ['medication_id', 'instance']]))
  
  # Get list of instance values
  instances = medication_ids_df.select('instance').dropDuplicates().orderBy('instance').collect()
  instances = [instance.instance for instance in instances]
  
  # Generate query filter string
  filter_string_list = []
  for instance in instances:
    # Get medication IDs for the current instance
    medication_ids = medication_ids_df.where(F.col('instance') == instance) \
      .select('medication_id').dropDuplicates().collect()
    
    # Print number of medication_id values for the current instance
    print("Number of medication_id values for instance {0}: {1}".format(instance, len(medication_ids)))
    
    # Get medication IDs as Python list and join with commas for query string
    medication_ids = ["'{}'".format(id_.medication_id) for id_ in medication_ids]
    medication_ids = ', '.join(medication_ids)
    
    # Generate portion of query string for the current instance
    filter_string = """(instance = '{0}' AND medication_id IN ({1}))""".format(instance, medication_ids)
    filter_string_list.append(filter_string)
  
  filter_string = ' OR '.join(filter_string_list)
  
  return filter_string


# Get RxNorm codes from medication_id list
def get_rxnorm_from_medication_ids(medication_ids_df, use_column='medication_id', omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get RxNorm codes from a provided list of medication_id values

  Parameters:
  medication_ids_df (PySpark df): Dataframe containing medication_id values. This dataframe *must*
                                  include use_column and 'instance' columns for joining
  use_column (str): Label of column containing medication_id values. Default is 'medication_id'
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing RxNorm codes corresponding to provided medication_id codes

  """
  # medication_ids_df must be a PySpark dataframe
  assert(isinstance(medication_ids_df, DataFrame))
  
  # use_column must be a string
  assert(isinstance(use_column, str))
  
  # medication_ids_df must include use_column and 'instance'
  assert(all([s in medication_ids_df.columns for s in ['instance', use_column]]))
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Join medicationrxnorm table to get RxNorm codes
  print('Getting RxNorm codes for provided list of medication_id values...')
  results_df = medication_ids_df \
    .select(use_column, 'instance').where(F.col(use_column).isNotNull()).dropDuplicates() \
    .join(
      spark.sql("""SELECT * FROM rdp_phi.medicationrxnorm""") \
        .withColumnRenamed('medication_id', use_column) \
        .withColumnRenamed('rxnormcode', 'rxnorm_code') \
        .where(F.col('termtype').isin(['Ingredient', 'Precise Ingredient', 'Multiple Ingredients'])) \
        .select('rxnorm_code', 'instance', use_column, 'termtype').hint("broadcast"), [use_column, 'instance'], how='left')
  
  # Join OMOP concept table to get descriptions
  print('Joining OMOP concept table to get descriptions...')
  results_df = results_df.join(
    F.broadcast(spark.sql(
      """
      SELECT
        concept_name as medication_description,
        concept_code as rxnorm_code
      FROM {database}.omop_concept_{version}
      WHERE domain_id = 'Drug' AND vocabulary_id = 'RxNorm'
      """.format(database=omop_tables_location, version=omop_version))), ['rxnorm_code'], how='left')
  
  # Aggregate RxNorm codes so that there is only one row for each medication_id
  print('Aggregating RxNorm codes so each medication_id has a single record...')
  results_df = results_df \
    .groupBy('instance', use_column) \
    .agg(
      F.collect_list('medication_description').alias('medication_description'),
      F.collect_list('termtype').alias('term_type'),
      F.collect_list('rxnorm_code').alias('rxnorm_code'))
  
  return results_df


# Add categorical column for medication cc
def add_medication_cc_column(medication_orders_df, cc_label, search_column='medication_id'):
  """Add categorical column indicating if a medication order record is in a specified medication cc
  category. True indicates it is in the category, False indicates it is not.
  
  Parameters:
  medication_orders_df (PySpark df): Dataframe containing medication order records. By default, this
                                     Dataframe must contain a column labeled 'medication_id'. It must
                                     also contain an 'instance' column.
  cc_label (str): A string corresponding to a medication clinical concept (e.g. 'antidepressant',
                  'antihypertensive', 'vasopressor'). This cc_label will be used as the name of the
                  newly-added column.
  search_column (str): Name of column to search for the clinical concept codes. The provided Dataframe
                       (medication_orders_df) must contain this column.
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the categorical True/False values,
              indicating whether the medication order record is in the specified device cc category
  
  """
  
  # medication_orders_df must be a PySpark dataframe
  assert(isinstance(medication_orders_df, DataFrame))
  
  # cc_label must be a valid medication clinical concept string
  assert(cc_label in get_medication_cc_list())
  
  # medication_orders_df must contain the following columns: search_column, 'instance'
  assert(all([s in medication_orders_df.columns for s in [search_column, 'instance']]))
  
  # Get medication_id values for specified medication cc and add column with True values
  temp_column = '{}_temp'.format(cc_label)
  cc_medication_ids = get_medication_ids_for_medication_cc(cc_label) \
    .withColumn(temp_column, F.lit(True))
  
  # Join medication_orders_df to medication medication_ids and fill nulls in cc column with False
  results_df = medication_orders_df \
    .join(F.broadcast(cc_medication_ids), [search_column, 'instance'], how='left') \
    .withColumn(cc_label, F.when(F.col(temp_column), F.col(temp_column)).otherwise(F.lit(False))) \
    .drop(temp_column)
  
  return results_df