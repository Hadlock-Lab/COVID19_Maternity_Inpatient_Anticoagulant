# Define schemas for generated tables
table_field_values_schema = StructType([
    StructField('row_id', IntegerType(), True),
    StructField('table_name', StringType(), True),
    StructField('field_name', StringType(), True),
    StructField('field_value', StringType(), True),
    StructField('secondary_field_name', StringType(), True),
    StructField('secondary_field_value', StringType(), True),
    StructField('instance', IntegerType(), True),
    StructField('value_count', IntegerType(), True)])

table_details_schema = StructType([
    StructField('table_name', StringType(), True),
    StructField('records_cnt', LongType(), True),
    StructField('field_name', StringType(), True),
    StructField('data_type', StringType(), True),
    StructField('unique_cnt', LongType(), True),
    StructField('instance_unique_cnt', LongType(), True),
    StructField('null_cnt', LongType(), True)])


# Get distinct values for one or two table fields
def get_table_field_values(table_fields, table_location='rdp_phi', filter_string=None):
  """Get the values and counts of specified` table fields

  Parameters:
  table_fields (dict): Keys are table names and values are lists of field names
  table_location (str): Database location of the table
  filter_string (str): 'WHERE' clause filter string to apply to the specified table

  Returns:
  PySpark df: Dataframe containing table field values

  """
  assert(isinstance(table_fields, dict))
  if table_location is not None:
    assert(isinstance(table_location, str))
  
  # Pre-process table_location
  if (table_location is None) | (table_location == ''):
    location = ''
  else:
    location = '{}.'.format(table_location)
  
  # Create empty PySpark dataframe
  schema = table_field_values_schema
  result_df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
  
  for table, fields in table_fields.items():
    assert(isinstance(fields, list))
    
    # Get all table fields to check for an instance column
    table_description = spark.sql(
      """describe {location}{table}""".format(table=table, location=location)).orderBy('col_name').collect()
    all_table_fields = [r.col_name.lower() for r in table_description]    
    
    for field in fields:
      # Get primary and secondary fields
      if(isinstance(field, str)):
        field_pair = [field]
      elif(isinstance(field, list) or isinstance(field, tuple)):
        if len(field) < 1:
          field_pair = None
        elif len(field) == 1:
          field_pair = [field[0]]
        else:
          field_pair = [field[0], field[1]]
      else:
        field_pair = None
      
      # Skip if invalid field(s) provided
      if field_pair is None:
        continue
    
      # Modify portions of query depending on if there is an instance column
      fields=', '.join(field_pair)
      order_by_section = "{}, instance".format(fields) if ('instance' in all_table_fields) else fields
      select_instance_value = "instance" if ('instance' in all_table_fields) else "NULL as instance"
      group_by_section = "instance, {}".format(fields) if ('instance' in all_table_fields) else fields
      
      # Set substitution strings
      primary_field = field_pair[0]
      secondary_field_name = "\'\'" if len(field_pair) < 2 else "\'{}\'".format(field_pair[1])
      secondary_field_value = "\'\'" if len(field_pair) < 2 else field_pair[1]
      where_clause = 'TRUE' if filter_string is None else filter_string
      
      # Query for field information
      print("Querying table '{}', field '{}'...".format(table, field))
      current_query_string = \
      """
      SELECT
        ROW_NUMBER() OVER (ORDER BY {order_by}) as row_id,
        '{table}' as table_name,
        '{primary_field}' as field_name,
        {primary_field} as field_value,
        {secondary_field_name} as secondary_field_name,
        {secondary_field_value} as secondary_field_value,
        {select_instance},
        COUNT(*) as value_count
      FROM (SELECT * FROM {table_location}{table} WHERE {where_clause})
      GROUP BY {group_by}
      """.format(order_by=order_by_section, table=table, primary_field=primary_field, secondary_field_name=secondary_field_name,
                 secondary_field_value=secondary_field_value, select_instance=select_instance_value,
                 table_location=location, where_clause=where_clause, group_by=group_by_section)
      print(current_query_string)
      result_df = result_df.union(spark.sql(current_query_string))
  
  return result_df.orderBy('table_name', 'field_name', 'secondary_field_name', 'row_id')


# Get table field unique counts and other statistics
# Utility function to generate query string
def generate_query_strings(table, table_location='rdp_phi'):
  """Utility function to generate query strings used by get_details_for_table

  Parameters:
  table (str): Table for which counts/statistics should be generated
  table_location (str): Database location of the table

  Returns:
  list: List of query strings corresponding to fields in the specified table

  """
  assert(isinstance(table, str))
  if table_location is not None:
    assert(isinstance(table_location, str))
  
  # Pre-process table_location
  if (table_location is None) | (table_location == ''):
    location = ''
  else:
    location = '{}.'.format(table_location)
  
  # Get table fields and types
  table_description = spark.sql(
    """describe {location}{table}""".format(table=table, location=location)).orderBy('col_name').collect()
  fields = [r.col_name.lower() for r in table_description]
  types = [r.data_type for r in table_description]
  
  # Instance unique count is NULL if there is no 'instance' column
  instance_unique_select = 'COUNT(DISTINCT(CONCAT(instance, CAST({f} AS STRING))))' if ('instance' in fields) else 'NULL'
  
  # Convenience function for generating strings
  def query_string(_table, _field, _type, _instance_unique):
    instance_unique = _instance_unique.format(f=_field)
    qstring = \
    """
    SELECT
      '{t}' as table,
      COUNT(*) as records,
      '{f}' as field,
      '{type}' as data_type,
      COUNT(DISTINCT({f})) as unique_cnt,
      {instance_unique} as instance_unique_cnt,
      SUM(CASE WHEN {f} is NULL then 1 else 0 END) as null_cnt
    FROM {table_location}{t}
    """.format(t=_table, f=_field, type=_type, table_location=location, instance_unique=instance_unique)
    return qstring
  
  return [(f, query_string(table, f, t, instance_unique_select)) for f, t in list(zip(fields, types))]

# Get details for a table
def get_details_for_table(table, table_location='rdp_phi'):
  """Get counts/statistics for all fields in the specified table

  Parameters:
  table (str): Table for which counts/statistics should be generated
  table_location (str): Database location of the table

  Returns:
  PySpark df: Dataframe containing counts/statistics for all fields in the specified table

  """
  # Make sure that table is a string
  assert(isinstance(table, str))
  
  # Generate query strings for all fields
  query_strings = generate_query_strings(table, table_location=table_location)
  assert(len(query_strings) > 0)
  
  # Create empty PySpark dataframe
  schema = table_details_schema
  result_df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
  
  # Use queries to get field information
  for i in range(len(query_strings)):
    current_field, current_query = query_strings[i]
    print("Analyzing table '{table}', field '{field}'...".format(table=table, field=current_field))
    result_df = result_df.union(spark.sql(current_query))
    
  return result_df
  