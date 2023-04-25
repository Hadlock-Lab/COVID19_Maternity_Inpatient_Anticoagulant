# Author: Yeon Mi Hwang
# Date: 3/17/22

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.encounters_utilities


# functions for GPAL extraction
def find_GPAL(note):
  import re
  if re.search(r"G\d{1}P\d{4}", note):
    return re.search(r"G\d{1}P\d{4}", note).group()
  elif re.search(r"G\d{1}\s+P\d{1}\s+T\d{1}\s+P\d{1}\s+A\d{1}", note):
    return re.search(r"G\d{1}\s+P\d{1}\s+T\d{1}\s+P\d{1}\s+A\d{1}", note).group()
  elif re.search(r"G\d{1}P\d{1}", note):
    return re.search(r"G\d{1}P\d{1}", note).group()
  elif re.search(r"G\d{1}\s{1}P\d{1}", note):
    return re.search(r"G\d{1}\s{1}P\d{1}", note).group()
  else:
    return None


def get_G(GPAL):
  import re
  if GPAL is not None:
    if re.search(r"G\d{1}", GPAL):
      return re.search(r"G\d{1}", GPAL).group()[-1]
  else:
    return None


def get_P(GPAL):
  import re
  if GPAL is not None: 
    if re.search(r"P\d{1}", GPAL):
      return re.search(r"P\d{1}", GPAL).group()[-1]
  else:
    return None


def get_preterm(GPAL):
  import re
  if GPAL is not None: 
    if re.search(r"P\d{4}", GPAL):
      return re.search(r"P\d{4}", GPAL).group()[-3]
    elif len(re.findall(r"P\d{1}", GPAL)) == 2:
      return re.findall(r"P\d{1}", GPAL)[1][-1]
  else:
    return None


# call functions
find_GPAL_udf = F.udf(lambda note: find_GPAL(note), StringType())  
get_G_udf = F.udf(lambda GPAL: get_G(GPAL), StringType())  
get_P_udf = F.udf(lambda GPAL: get_P(GPAL), StringType())  
get_preterm_udf = F.udf(lambda GPAL: get_preterm(GPAL), StringType())  


# define application functions
def load_notes_join_save(cohort, df_name):
  notes = spark.sql("SELECT * FROM rdp_phi.notes") # this part takes time 
  notes_joined = cohort.join(notes, ['pat_id','instance'] ,how = 'left').where("CONTACT_DATE <= date_add(ob_delivery_delivery_date,3) AND CONTACT_DATE > date_add(ob_delivery_delivery_date,3) - interval '280' day")
  notes_to_save = notes_joined.select(['pat_id','instance', 'contact_date','ob_delivery_delivery_date','FULL_NOTE_TEXT'])
  write_data_frame_to_sandbox(notes_to_save, df_name, sandbox_db='rdp_phi_sandbox', replace=True)


def add_GPP_columns(df_name):
  notes_joined = spark.sql("SELECT * FROM rdp_phi_sandbox.{0}".format(df_name))
  GPAL = notes_joined.withColumn('GPAL', find_GPAL_udf(F.col('full_note_text')))
  partition_by = ['pat_id','instance', 'ob_delivery_delivery_date']
  aggregate_by = {'GPAL' : 'max'}
  GPAL_aggregated = aggregate_data(GPAL, partition_columns = partition_by , aggregation_columns = aggregate_by)
  GPAL_aggregated = GPAL_aggregated.withColumn('Gravidity', get_G_udf(F.col('GPAL_max')))
  GPAL_aggregated = GPAL_aggregated.withColumn('Parity', get_P_udf(F.col('GPAL_max')))
  GPAL_aggregated = GPAL_aggregated.withColumn('Preterm_history', get_preterm_udf(F.col('GPAL_max')))
  return GPAL_aggregated 