# load libraries
import re
import pandas as pd
from io import StringIO
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame 

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import conditions registry
load COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_registry


# Get list of condition clinical concepts
def get_condition_cc_list():
  """Get the full list of condition clinical concept labels
  
  Returns:
  list: List of all available condition clinical concept labels
  
  """
  return list(conditions_cc_registry.keys())


# Get SNOMED codes for condition clinical concept
def get_condition_cc_codes(condition_cc, include_descendant_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get SNOMED codes with descriptions for a specified condition clinical concept
  
  Parameters:
  condition_cc (str): A string corresponding to a condition clinical concept (e.g. 'asthma',
                       'cardiac_arrhythmia', 'chronic_lung_disease', 'coronary_artery_disease')
  include_descendant_codes (bool): If True, return clinical concept SNOMED codes and descendant codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing two columns: 'snomed_code', 'name'.
              
  """
  # Validate condition_cc string
  valid_cc_strings = get_condition_cc_list()
  assert(condition_cc in valid_cc_strings)
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Convert to PySpark dataframe
  print("Getting SNOMED codes for '{}' condition clinical concept...".format(condition_cc))
  condition_cc_codes = conditions_cc_registry[condition_cc]
  codes_pd_df = pd.DataFrame(condition_cc_codes, columns=['snomed_code'])
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


# Get dx_id list from SNOMED codes
def get_dx_ids_from_snomed(snomed_codes_df, use_column='snomed_code', include_descriptions=False):
  """Get dx_id values from a provided list of SNOMED codes

  Parameters:
  snomed_codes_df (PySpark df): Dataframe containing SNOMED codes
  use_column (str): Label of column containing SNOMED codes. Default is 'snomed_code'
  include_descriptions (bool): True to include diagnosis descriptions
  
  Returns:
  PySpark df: Dataframe containing dx_id values corresponding to provided SNOMED codes

  """
  # snomed_codes_df must be a PySpark dataframe
  assert(isinstance(snomed_codes_df, DataFrame))
  
  # use_column must be a string
  assert(isinstance(use_column, str))
  
  # Join externalconceptmapping table to get dx_id values
  print('Getting dx_id values for provided SNOMED code list...')
  results_df = snomed_codes_df \
    .select(use_column).where(F.col(use_column).isNotNull()).dropDuplicates() \
    .join(
      spark.sql("""SELECT * FROM rdp_phi.externalconceptmapping""") \
        .withColumnRenamed('value', 'dx_id') \
        .withColumn('snomed_code', F.split('concept', '#').getItem(1)) \
        .withColumn('code_type', F.split('concept', '#').getItem(0)) \
        .where(F.col('code_type') == 'SNOMED') \
        .withColumnRenamed('snomed_code', use_column)
        .select('dx_id', 'instance', use_column, 'code_type'), [use_column], how='left')
  
  # Join diagnosis table to get descriptions
  if(include_descriptions):
    print('Joining diagnosis table to get descriptions...')
    results_df = results_df \
      .join(F.broadcast(spark.sql(
        """
        SELECT dx_id, instance, name as description
        FROM rdp_phi.diagnosis
        """)), ['dx_id', 'instance'], how='inner')
  
  return results_df


# Get dx_id list for condition clinical concept
def get_dx_ids_for_condition_cc(condition_cc, include_descendant_codes=True):
  """Get dx_id values for a specified condition clinical concept

  Parameters:
  condition_cc (str): A string corresponding to a condition clinical concept (e.g. 'asthma',
                      'depression', 'obesity')
  include_descendant_codes (bool): If True, use expanded list of descendant SNOMED codes for the
                                   specified clinical concept
  
  Returns:
  PySpark df: Dataframe containing two columns: 'dx_id', 'instance'

  """
  # Validate condition_cc string
  valid_cc_strings = get_condition_cc_list()
  assert(condition_cc in valid_cc_strings)
  
  # Get SNOMED codes for condition clinical concept
  condition_cc_codes_df = get_condition_cc_codes(
    condition_cc, include_descendant_codes=include_descendant_codes)

  # Get dx_id values for SNOMED codes
  dx_ids_df = get_dx_ids_from_snomed(snomed_codes_df=condition_cc_codes_df, use_column='snomed_code')
  
  return dx_ids_df.select('dx_id', 'instance').dropDuplicates()


# Generate query filter string for condition clinical concept
def get_filter_string_for_condition_cc(condition_cc, include_descendant_codes=False, omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Generate a query filter string for a specified condition clinical concept that
    can be used in a 'WHERE' clause
  
  Parameters:
  condition_cc (str or list): A string corresponding to one or more condition clinical concepts
                              (e.g. 'asthma', 'depression', 'obesity')
  include_descendant_codes (bool): If True, expand SNOMED codes to include descendant codes
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the
                            OMOP concept table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  str: A query filter string properly formatted for a 'WHERE' clause
  
  """
  # Convert condition_cc to a list, if necessary
  condition_cc_list = condition_cc if isinstance(condition_cc, list) else [condition_cc]
  
  # Validate condition_cc strings
  valid_cc_strings = get_condition_cc_list()
  assert(all([s in valid_cc_strings for s in condition_cc_list]))
  
  # Get individual query strings for each condition cc
  print("Generating condition query string...")
  query_string_list = []
  for condition_cc_label in condition_cc_list:
    print("Getting query string for '{}' condition clinical concept...".format(condition_cc_label))
    
    # Get SNOMED codes for condition clinical concept
    condition_cc_codes_df = get_condition_cc_codes(condition_cc_label, include_descendant_codes, omop_table_version, omop_tables_location)
    
    # Get dx_id values for SNOMED codes
    dx_ids_df = get_condition_ids_from_snomed(snomed_codes_df=condition_cc_codes_df, use_column='snomed_code')
    
    # Generate 'WHERE' clause filter string with medication_id values
    conditions_query_string = get_filter_string_from_dx_id_list(dx_ids_df)
    
    # Concatenate individual condition query strings
    query_string_list = query_string_list + ["({})".format(conditions_query_string)]
    query_string = " OR ".join(query_string_list)
  
  return conditions_query_string


# Generate query filter string from dx_id list
def get_filter_string_from_dx_ids(diagnosis_ids_df):
  """Generate a query filter string that can be used in a 'WHERE clause'.

  Parameters:
  diagnosis_ids_df (PySpark df): Dataframe containing a list of dx_id values. These must be contained
  in a 'dx_id' column. There must also be an 'instance' column.
  
  Returns:
  str: A query filter string properly formatted for a 'WHERE' clause

  """
  # dx_ids_df must be a PySpark dataframe
  assert(isinstance(diagnosis_ids_df, DataFrame))
  
  # The dataframe must contain the following columns: 'dx_id', 'instance'
  assert(all([s in diagnosis_ids_df.columns for s in ['dx_id', 'instance']]))
  
  # Get list of instance values
  instances = diagnosis_ids_df.select('instance').dropDuplicates().orderBy('instance').collect()
  instances = [instance.instance for instance in instances]
  
  # Generate query filter string
  filter_string_list = []
  for instance in instances:
    # Get diagnosis IDs for the current instance
    diagnosis_ids = diagnosis_ids_df.where(F.col('instance') == instance) \
      .select('dx_id').dropDuplicates().collect()
    
    # Print number of dx_id values for the current instance
    print("Number of dx_id values for instance {0}: {1}".format(instance, len(diagnosis_ids)))
    
    # Get diagnosis IDs as Python list and join with commas for query string
    diagnosis_ids = ['{}'.format(id_.dx_id) for id_ in diagnosis_ids]
    diagnosis_ids = ', '.join(diagnosis_ids)
    
    # Generate portion of query string for the current instance
    filter_string = """(instance = '{0}' AND dx_id IN ({1}))""".format(instance, diagnosis_ids)
    filter_string_list.append(filter_string)
  
  filter_string = ' OR '.join(filter_string_list)
  
  return filter_string


# Get SNOMED codes from dx_id list
def get_snomed_from_dx_ids(dx_ids_df, use_column='dx_id', omop_table_version='2020-08-05', omop_tables_location='rdp_phi_sandbox'):
  """Get SNOMED codes from provided list of dx_id values

  Parameters:
  dx_ids_df (PySpark df): Dataframe containing dx_id values. This dataframe *must* include use_column
                          and 'instance' columns for joining
  use_column (str): Label of column containing dx_id values. Default is 'dx_id'
  omop_table_version (str): A date-formatted string (yyyy-mm-dd) corresponding to the OMOP concept
                            table version to use
  omop_tables_location (str): Database location of the OMOP tables
  
  Returns:
  PySpark df: Dataframe containing SNOMED codes corresponding to provided dx_id values

  """
  # dx_ids_df must be a PySpark dataframe
  assert(isinstance(dx_ids_df, DataFrame))
  
  # use_column must be a string
  assert(isinstance(use_column, str))
  
  # dx_ids_df must include use_column and 'instance'
  assert(all([s in dx_ids_df.columns for s in ['instance', use_column]]))
  
  # OMOP table location must be a string
  assert(isinstance(omop_tables_location, str))
  
  # OMOP table version must be a properly-formatted date string
  version_validator = "(20[0-9]{2})-([0-1][0-9])-([0-3][0-9])"
  match = re.match(version_validator, omop_table_version)
  assert(bool(match))
  
  # Replace hyphens with underscores in omop version string
  omop_version = '_'.join([match[1], match[2], match[3]])
  
  # Join externalconceptmapping table to get SNOMED codes
  print('Getting SNOMED codes for provided list of dx_id values...')
  results_df = dx_ids_df \
    .select(use_column, 'instance').where(F.col(use_column).isNotNull()).dropDuplicates() \
    .join(
      spark.sql("""SELECT * FROM rdp_phi.externalconceptmapping""") \
        .withColumnRenamed('value', use_column) \
        .withColumn('snomed_code', F.split('concept', '#').getItem(1)) \
        .withColumn('code_type', F.split('concept', '#').getItem(0)) \
        .where(F.col('code_type') == 'SNOMED') \
        .select('snomed_code', 'instance', use_column), [use_column, 'instance'], how='left')
  
  # Join OMOP concept table to get descriptions
  print('Joining OMOP concept table to get descriptions...')
  results_df = results_df.join(
    F.broadcast(spark.sql(
      """
      SELECT
        concept_name as condition_description,
        concept_code as snomed_code
      FROM {database}.omop_concept_{version}
      WHERE vocabulary_id = 'SNOMED'
      """.format(database=omop_tables_location, version=omop_version))), ['snomed_code'], how='left')
  
  # Aggregate SNOMED concepts so that there is only one row for each dx_id
  print('Aggregating SNOMED concepts so each dx_id has a single record...')
  results_df = results_df \
    .groupBy('instance', use_column) \
    .agg(
      F.collect_list('condition_description').alias('condition_description'),
      F.collect_list('snomed_code').alias('snomed_code'))
  
  return results_df


# Add categorical column for condition cc
def add_condition_cc_column(conditions_df, cc_label, search_column='dx_id'):
  """Add categorical column indicating if a condition record is in a specified condition cc category.
    True indicates it is in the category, False indicates it is not.
  
  Parameters:
  conditions_df (PySpark df): Dataframe containing condition (diagnosis) records. By default, this
                              Dataframe must contain a column labeled 'dx_id'. It must also contain
                              an 'instance' column
  cc_label (str): A string corresponding to a condition clinical concept (e.g. 'asthma',
                  'cardiac_arrhythmia', 'chronic_lung_disease'). This cc_label will be used as the
                  name of the newly-added column.
  search_column (str): Name of column to search for the clinical concept codes. The provided
                       Dataframe (conditions_df) must contain this column.
  
  Returns:
  PySpark df: The original Dataframe with an additional column with the categorical True/False
              values, indicating whether the condition (diagnosis) record is in the specified
              condition cc category
  
  """
  
  # conditions_df must be a PySpark dataframe
  assert(isinstance(conditions_df, DataFrame))
  
  # cc_label must be a valid conditions clinical concept string
  assert(cc_label in get_condition_cc_list())
  
  # conditions_df must contain the following columns: search_column, 'instance'
  assert(all([s in conditions_df.columns for s in [search_column, 'instance']]))
  
  # Get dx_id values for specified condition cc and add column with True values
  temp_column = '{}_temp'.format(cc_label)
  cc_dx_ids = get_dx_ids_for_condition_cc(cc_label) \
    .withColumn(temp_column, F.lit(True))
  
  # Join conditions_df to condition dx_ids and fill nulls in cc column with False
  results_df = conditions_df \
    .join(F.broadcast(cc_dx_ids), [search_column, 'instance'], how='left') \
    .withColumn(cc_label, F.when(F.col(temp_column), F.col(temp_column)).otherwise(F.lit(False))) \
    .drop(temp_column)
  
  return results_df


# Add a single clinical concept column using mapping table
def add_condition_cc_column_from_mapping(diagnoses_df, cc_label, cc_codes, mapping_table_name, db='rdp_phi_sandbox', include_metadata_columns=False):
  """Add a new condition clinical concept column to a dataframe containing
  diagnosis records (e.g. problem list or encounter diagnoses) based on a specified
  cc label and diagnosis mapping table.
  
  Parameters:
  diagnoses_df (PySpark df): A dataframe with diagnosis records (problem list or
                             encounter diagnosis). Must contain columns dx_id and
                             instance.
  cc_label (str): Condition clinical concept for which to add a T/F column
  cc_codes (list): List of SNOMED codes
  mapping_table_name (str): Name of mapping table
  db (str): Name of sandbox database (default is rdp_phi_sandbox)
  include_metadata_columns (bool): If True, include the metadata columns from the
                                    mapping table (condition_description and
                                    snomed_code)
  
  Returns:
  (PySpark df): Conditions dataframe with T/F column indicating whether each
                record is in the condition cc category.
  
  """
  # Validate inputs
  assert(isinstance(diagnoses_df, DataFrame))
  assert(isinstance(cc_label, str))
  assert(isinstance(cc_codes, list))
  assert(isinstance(mapping_table_name, str))
  assert(isinstance(db, str))
  
  # Conditions dataframe must contain dx_id and instance
  assert(all([c in diagnoses_df.columns for c in ['dx_id', 'instance']]))
  
  # Condition clinical concept label must be in the registry
  assert(cc_label in list(conditions_cc_registry.keys()))
  
  # Generate SNOMED column labels (in mapping table)
  cc_code_labels = ["SNOMED_{}".format(code) for code in cc_codes]
  
  # Load mapping table and generate temporary view
  mapping_df = spark.sql(
    "SELECT * FROM {db}.{table}".format(db=db, table=mapping_table_name))
  mapping_df.createOrReplaceTempView('mapping_table')
  
  # Get SNOMED columns from mapping table
  snomed_columns = [c for c in mapping_df.columns if ('SNOMED' in c)]
  
  # Only include cc code labels if they are in the mapping table
  cc_code_labels = [
    label for label in cc_code_labels if (label in snomed_columns)]
  
  # Return diagnosis df without modification if labels not found in mapping table
  if(len(cc_code_labels) < 1):
    warning_message = \
    """
    Warning: None of the codes associated with the specified clinical concept
    ('{}') were found in the mapping table.
    """.format(cc_label)
    print(warning_message)
    
    # Add column with all False values
    diagnoses_df = diagnoses_df \
      .withColumn(cc_label, F.lit(False))
    
    return diagnoses_df
  
  # Columns to include from mapping table
  include_columns = ['dx_id', 'instance']
  if(include_metadata_columns):
    include_columns = include_columns + ['condition_description', 'snomed_code']
  
  # Add clinical concept column to mapping table
  print("Adding '{}' clinical concept column...".format(cc_label))
  snomed_mapping_df = spark.sql(
    """
    SELECT {include_columns}, ({code_list}) as {cc_label}
    FROM mapping_table
    """.format(include_columns=', '.join(include_columns),
               code_list=' OR '.join(cc_code_labels), cc_label=cc_label))
  
  # Join new condition cc mapping column to the diagnosis record df
  diagnoses_df = diagnoses_df \
    .join(snomed_mapping_df, ['dx_id', 'instance'], how='left')
  
  return diagnoses_df


# Add multiple clinical concept columns using mapping table
def add_condition_cc_columns_from_mapping(diagnoses_df, cc_list, mapping_table_name, db='rdp_phi_sandbox'):
  """Add one or more condition clinical concept columns from cc label list
  
  Parameters:
  diagnoses_df (PySpark df): A dataframe with diagnosis records (problem list or
                             encounter diagnosis). Must contain columns dx_id and
                             instance.
  cc_list (str, list, or dict): One or more condition clinical concept labels. If a string is
                                provided, it is converted to a 1-item list. A list is assumed to
                                be a list of clinical concept labels found in the condition cc
                                registry. If a dictionary is provided, the keys should be desired
                                column labels and the values should be lists of SNOMED codes.
  mapping_table_name (str): Name of mapping table
  db (str): Name of sandbox database (default is rdp_phi_sandbox)
  
  Returns:
  (PySpark df): Conditions dataframe with T/F column indicating whether each
                record is in the condition cc category.
  
  """
  # Validate inputs
  assert(isinstance(diagnoses_df, DataFrame))
  assert(isinstance(mapping_table_name, str))
  assert(isinstance(db, str))
  
  # Validate cc_list and generate cc dictionary
  if (isinstance(cc_list, dict)):
    # If cc_list is a dictionary, keys must be strings and values must be lists
    assert(all([isinstance(k, str) for k in list(cc_list.keys())]))
    assert(all([isinstance(v, list) for v in list(cc_list.values())]))
    cc_dictionary = cc_list
  elif (isinstance(cc_list, str)):
    # Condition clinical concept label must be in the registry
    assert(cc_list in list(conditions_cc_registry.keys()))
    cc_dictionary = {cc_list: conditions_cc_registry[cc_list]}
  elif (isinstance(cc_list, list)):
    cc_list = [str(cc) for cc in cc_list]
    assert([cc in list(conditions_cc_registry.keys()) for cc in cc_list])
    cc_dictionary = {cc: conditions_cc_registry[cc] for cc in cc_list}
  
  # Add clinical concept columns to mapping table
  for label, codes in cc_dictionary.items():
    diagnoses_df = add_condition_cc_column_from_mapping(
      diagnoses_df=diagnoses_df,
      cc_label=label,
      cc_codes=codes,
      mapping_table_name=mapping_table_name,
      db=db,
      include_metadata_columns=False)
  
  return diagnoses_df


# Generate cc label to SNOMED list dictionary
def generate_dict_from_cc_list(cc_list):
  """Add one or more condition clinical concept columns from cc label list
  
  Parameters:
  cc_list (str, list, or dict): One or more condition clinical concept labels. If a string is
                                provided, it is converted to a 1-item list. A list is assumed to
                                be a list of clinical concept labels found in the condition cc
                                registry.
  
  Returns:
  (dict): Keys of dictionary are clinical concept labels. Values are corresponding lists of
          SNOMED codes.
  
  """  
  # Validate cc_list and generate cc dictionary
  if (isinstance(cc_list, str)):
    # Condition clinical concept label must be in the registry
    assert(cc_list in list(conditions_cc_registry.keys()))
    cc_dictionary = {cc_list: conditions_cc_registry[cc_list]}
  elif (isinstance(cc_list, list)):
    cc_list = [str(cc) for cc in cc_list]
    assert([cc in list(conditions_cc_registry.keys()) for cc in cc_list])
    cc_dictionary = {cc: conditions_cc_registry[cc] for cc in cc_list}
  else:
    print("Warning: cc_list must be a string or a list. Returning empty dict.")
    
  return cc_dictionary


# Generate dx_id to cc label mapping
def generate_dx_id_cc_label_mapping(mapping_table, cc_dict, db='rdp_phi_sandbox'):
  """Generate a dx_id to cc label mapping table from a provided dx_id to SNOMED mapping table.
  
  Parameters:
  mapping_table_name (str): Name of mapping table
  cc_dict (dict): A dictionary of clinical concept labels (keys) and corresponding lists of SNOMED
                  codes (values).
  db (str): Name of sandbox database (default is rdp_phi_sandbox)
  
  Returns:
  (PySpark df): A new mapping table with T/F column indicating whether each dx_id is in the
                condition cc category.
  
  """
  # Validate inputs
  assert(isinstance(mapping_table, str))
  assert(isinstance(db, str))
  
  # Get list of clinical concept labels from registry
  registry_cc_labels = list(conditions_cc_registry.keys())
  
  # Validate cc dictionary keys
  assert(isinstance(cc_dict, dict))
  labels = list(cc_dict.keys())
  assert(all([label in registry_cc_labels for label in labels]))
  
  # Load dx_id to SNOMED mapping table and create a temporary view
  mapping_df = spark.sql("SELECT * FROM {0}.{1}".format(db, mapping_table))
  mapping_df.createOrReplaceTempView('snomed_mapping_table')
  
  # Make sure all cc dictionary values are lists of strings
  valid_labels = mapping_df.columns
  make_label = lambda label: "SNOMED_{}".format(label)
  for label in labels:
    v = cc_dictionary[label]
    v = v if isinstance(v, list) else [v]
    cc_dictionary[label] = [make_label(c) for c in v if (make_label(c) in valid_labels)]
  
  # Generate select statement string for creating new mapping columns  
  concat_code_list = lambda s: 'FALSE' if (len(s) < 1) else ' OR '.join(s)
  select_statement_list = [
    "({concat_codes}) as {label}".format(concat_codes=concat_code_list(codes), label=label)
    for label, codes in cc_dictionary.items()]
  select_statement_string = ', '.join(select_statement_list)
  
  # Generate new dx_id to cc mapping table
  new_mapping_df = spark.sql(
  """
  SELECT dx_id, instance, condition_description, snomed_code, {mapping_columns}
  FROM snomed_mapping_table
  """.format(mapping_columns=select_statement_string))
  
  return new_mapping_df
