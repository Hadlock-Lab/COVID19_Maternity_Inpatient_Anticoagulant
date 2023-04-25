# Author: Samantha Piekos
# Date: 3/17/22
# load environment
from datetime import date
import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import *


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.cohort_covid_pregnancy_functions
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.add_GPAL


# define functions
# Drug Use
# 1 - at least one Yes associated with the patient
# 0 - Never or Quit

def get_social_history_df(df_pat):
  df_social_history = df_pat.join(
  spark.sql(
    """
    SELECT
      pat_id,
      instance,
      istobaccouser as is_tobacco_user,
      isillegaldruguser as is_illegal_drug_user,
      isivdruguser as is_iv_drug_user,
      years_education
    FROM rdp_phi.socialhistory"""), ['pat_id', 'instance'], how='left')
  df_social_history = df_social_history.withColumnRenamed('pat_id', 'social_history_pat_id')
  df_social_history = df_social_history.withColumnRenamed('instance', 'social_history_instance')
  df_social_history.createOrReplaceTempView("social_history")
  return(df_social_history)


def get_not_drug_users(df_pat, df_drug_users, col_name):
  df_drug_users.createOrReplaceTempView("drug_users")
  df_temp = spark.sql(
  """
  SELECT p.*
  FROM pat AS p
  WHERE NOT EXISTS 
    (SELECT * 
     FROM drug_users AS du
     WHERE du.pat_id = p.pat_id AND du.instance = p.instance)
  """)
  df_temp = df_temp.withColumn(col_name, lit(0))
  return(df_temp)


def get_smoker_status(df_pat, df_social_history):
  df_smokers = spark.sql(
  """
  SELECT p.*
  FROM pat as p
  INNER JOIN social_history AS sh
  ON p.pat_id = sh.social_history_pat_id 
    AND p.instance = sh.social_history_instance
  WHERE sh.is_tobacco_user == 'Yes'
  """)
  df_smokers = df_smokers.withColumn('smoker', lit(1))
  df_smokers = df_smokers.union(get_not_drug_users(df_pat, df_smokers, 'smoker'))
  df_smokers = df_smokers.distinct()
  return(df_smokers)


def get_illegal_drug_user_status(df_pat, df_social_history):
  df_drug_users = spark.sql(
  """
  SELECT p.*
  FROM pat as p
  INNER JOIN social_history AS sh
  ON p.pat_id = sh.social_history_pat_id 
    AND p.instance = sh.social_history_instance
  WHERE sh.is_illegal_drug_user == 'Yes'
  """)
  df_drug_users = df_drug_users.withColumn('illegal_drug_user', lit(1))
  df_drug_users = df_drug_users.union(get_not_drug_users(df_pat, df_drug_users, 'illegal_drug_user'))
  df_drug_users = df_drug_users.distinct()
  return(df_drug_users)


def get_iv_drug_user_status(df_pat, df_social_history):
  df_iv_drug_users = spark.sql(
  """
  SELECT p.*
  FROM pat AS p
  INNER JOIN social_history AS sh
  ON p.pat_id = sh.social_history_pat_id 
    AND p.instance = sh.social_history_instance
  WHERE sh.is_iv_drug_user == 'Yes'
  """)
  df_iv_drug_users = df_iv_drug_users.withColumn('iv_drug_user', lit(1))
  df_iv_drug_users = df_iv_drug_users.union(get_not_drug_users(df_pat, df_iv_drug_users, 'iv_drug_user'))
  df_iv_drug_users = df_iv_drug_users.distinct()
  return(df_iv_drug_users)


def add_drug_use_to_df(df):
  print('Retrieving patient social history info...')
  df.createOrReplaceTempView("pat")
  df_social_history = get_social_history_df(df)
  print('Adding smoker status...')
  df = get_smoker_status(df, df_social_history)
  print('Adding illegal drug user status...')
  df.createOrReplaceTempView("pat")
  df = get_illegal_drug_user_status(df, df_social_history)
  print('Adding iv drug user status...')
  df.createOrReplaceTempView("pat")
  df = get_iv_drug_user_status(df, df_social_history)
  return(df)


def populate_dict(df):
  d = {}
  l = list(df.toPandas().pat_id.unique())
  for i in l:
    d[i] = 0
  return(d)


def add_parity(df_pat, df_mom):
  dict_parity = populate_dict(df_pat)
  df_parity = spark.sql(
    """
    SELECT p.pat_id, p.child_episode_id
    FROM pat as p
    INNER JOIN mom AS m
    ON p.pat_id = m.mom_pat_id
    WHERE p.gestational_days >= 168 
    """)
  df_parity = df_parity.dropna().distinct().toPandas()
  for index, row in df_parity.iterrows():
    dict_parity[row['pat_id']] += 1
  return(dict_parity)


def add_gravidity(df_pat, df_mom):
  dict_gravidity = populate_dict(df_pat)
  df_gravidity = spark.sql(
    """
    SELECT p.pat_id, p.child_episode_id
    FROM pat as p
    INNER JOIN mom AS m
    ON p.pat_id = m.mom_pat_id
    """)
  df_gravidity = df_gravidity.dropna().distinct().toPandas()
  for index, row in df_gravidity.iterrows():
    dict_gravidity[row['pat_id']] += 1
  return(dict_gravidity)


def convert_dict_to_pyspark_df(d, column_name):
  data = []
  for k, v in d.items():
    data.append({'temp_pat_id': k, column_name: v})
  df = spark.createDataFrame(data)
  return(df)


def add_dict_to_pyspark_df(df, d, column_name):
  df_temp = convert_dict_to_pyspark_df(d, column_name)
  df_temp.createOrReplaceTempView("temp")
  df = spark.sql(
    """
    SELECT *
    FROM pat as p
    INNER JOIN temp AS t
    ON p.pat_id = t.temp_pat_id
    """)
  df = df.drop('temp_pat_id')
  return(df)
  

def add_previous_pregnancies_to_df(df):
  print('Retrieving previous pregnancies info...')
  df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_maternity")
  df_cohort_maternity = df_cohort_maternity.withColumnRenamed('pat_id', 'mom_pat_id')
  df_cohort_maternity.createOrReplaceTempView("mom")
  print('Adding parity...')
  df.createOrReplaceTempView("pat")
  dict_parity = add_parity(df, df_cohort_maternity)
  df = add_dict_to_pyspark_df(df, dict_parity, 'parity')
  print('Adding gravidity...')
  df.createOrReplaceTempView("pat")
  dict_gravidity = add_gravidity(df, df_cohort_maternity)
  df = add_dict_to_pyspark_df(df, dict_gravidity, 'gravidity')
  return(df)
