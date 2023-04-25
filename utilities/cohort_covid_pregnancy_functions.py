# Author: Samantha Piekos
# Date: 3/17/22

# import functions from another notebook
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_registry


# define functions
def write_all_pat_conditions_df(df_pat, df_name):
  df_temp =  df_pat.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          dx_id,
          date_of_entry
        FROM rdp_phi.problemlist"""), ['pat_id', 'instance'], how='left')
  write_data_frame_to_sandbox(df_temp, df_name, sandbox_db='rdp_phi_sandbox', replace=True)


  def generate_condition_df(df_pat_conditions, condition):
  df_condition_dx_ids = get_dx_ids_for_condition_cc(condition)
  df_condition_dx_ids.createOrReplaceTempView("condition")
  df_condition = spark.sql(
  """
  SELECT pc.pat_id, pc.instance, pc.date_of_entry
  FROM pat_conditions as pc
  INNER JOIN condition as c
  ON pc.dx_id = c.dx_id 
    AND pc.instance = c.instance
  """)
  df_condition = df_condition.distinct()
  return(df_condition)

# /*+ RANGE_JOIN(c, 50) */
def get_pat_with_condition(df_pat, df_condition, condition):
  df_temp = spark.sql(
  """
  SELECT p.*
  FROM pat as p
  INNER JOIN condition AS c
  ON p.pat_id = c.pat_id 
    AND p.instance = c.instance
    AND p.ob_delivery_delivery_date BETWEEN c.date_of_entry AND c.date_of_entry + interval '280' day
  """)
  df_temp = df_temp.distinct()
  df_temp = df_temp.withColumn('disease_status', lit(1))
  return(df_temp)

def get_condition_status(df_pat, df_pat_conditions, condition):
  df_condition = generate_condition_df(df_pat_conditions, condition)
  print('Getting patient\'s condition status...')
  df_condition.createOrReplaceTempView("condition")
  df_temp = get_pat_with_condition(df_pat, df_condition, condition) # get patients with disease
  df_temp.createOrReplaceTempView("temp")
  df_temp_2 = spark.sql(
  """
  SELECT p.*
  FROM pat AS p
  WHERE NOT EXISTS 
    (SELECT * 
     FROM temp AS t
     WHERE t.pat_id = p.pat_id AND t.instance = p.instance)
  """)
  df_temp_2 = df_temp_2.withColumn('disease_status', lit(0))
  df_pat_data_condition = df_temp.union(df_temp_2)
  df_pat_data_condition = df_pat_data_condition.withColumnRenamed('disease_status', condition + '_status')
  return(df_pat_data_condition)

def add_condition_to_df(df, df_pat_conditions, condition):
  print('Adding status of ' + condition + '...')
  df.createOrReplaceTempView("pat")
  df = get_condition_status(df, df_pat_conditions, condition)
  return(df)


def add_pregnancy_related_conditions_to_df(df_cohort, df_pat_conditions):
  print('Adding patient status of pregnancy related conditions...')
  df_pat_conditions.createOrReplaceTempView("pat_conditions")
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'chronic_diabetes_with_pregnancy')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'chronic_hypertension_with_pregnancy')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'gestational_diabetes')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'gestational_hypertension')
  #df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia_no_hypertension')
  #df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia_with_hypertension')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'severe_preeclampsia')
  print('All pregnancy related condition statuses obtained!')
  return(df_cohort)


def add_chronic_conditions_during_pregnancy_to_df(df_cohort, df_pat_conditions):
  print('Adding patient status of chronic conditions present during pregnancy...')
  df_pat_conditions.createOrReplaceTempView("pat_conditions")
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'diabetes_type_2')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'chronic_diabetes_with_pregnancy')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'chronic_hypertension_with_pregnancy')
  print('All chronic condition statuses obtained!')
  return(df_cohort)


def add_pregnancy_gestational_conditions_to_df(df_cohort, df_pat_conditions):
    print('Adding patient status of pregnancy gestational conditions...')
    df_pat_conditions.createOrReplaceTempView("pat_conditions")
    df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'gestational_diabetes')
    df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'gestational_hypertension')
    print('All pregnancy gestational conditions statuses obtained!')
    return(df_cohort)


 def add_preeclampsia_conditions_to_df(df_cohort, df_pat_conditions):
  print('Adding patient status of preeclampsia conditions...')
  df_pat_conditions.createOrReplaceTempView("pat_conditions")
  #df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia_no_hypertension')
  #df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia_with_hypertension')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'preeclampsia')
  df_cohort = add_condition_to_df(df_cohort, df_pat_conditions, 'severe_preeclampsia')
  print('All preeclampsia condition statuses obtained!')
  return(df_cohort)


def determine_trimester(days):
  if days <= 91:
    return('1st trimester')
  elif days <= 189:
    return('2nd trimester')
  else:
    return('3rd trimester')


def get_covid_completed_pregnancy_cohorts():
  # identify women with covid infections during each trimester
  # gets completed pregnancies only
  import pandas as pd
  import numpy as np
  df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_covid_maternity")  # load cohort
  df_covid_mom = df_covid_mom.toPandas()
  cols = df_covid_mom.columns
  c = 0
  first_trimester, second_trimester, third_trimester = 0, 0, 0
  df_cohort_covid_maternity = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_1st_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_2nd_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_3rd_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  for index, row in df_covid_mom.iterrows():
    delivery_date = row['ob_delivery_delivery_date']
    count = 0
    if row['insurance'] == 'Medicaid' or row['insurance'] == 'Commercial':
      for item in row['observation_datetime_collect_list']:
        days_covid_test_before_birth = (delivery_date - item)/np.timedelta64(1, 'D')
        if row['result_short_collect_list'][count] == 'positive' and days_covid_test_before_birth >= 0  and days_covid_test_before_birth <= row['gestational_days']:
          df_cohort_covid_maternity = df_cohort_covid_maternity.append(row)
          trimester = determine_trimester(row['gestational_days'] - days_covid_test_before_birth)
          if trimester == '1st trimester':
            first_trimester += 1
            df_cohort_covid_1st_trimester_completed_pregnancy = df_cohort_covid_1st_trimester_completed_pregnancy.append(row)
          elif trimester == '2nd trimester':
            second_trimester += 1
            df_cohort_covid_2nd_trimester_completed_pregnancy = df_cohort_covid_2nd_trimester_completed_pregnancy.append(row)
          elif trimester == '3rd trimester':
            third_trimester += 1
            df_cohort_covid_3rd_trimester_completed_pregnancy = df_cohort_covid_3rd_trimester_completed_pregnancy.append(row)
          c += 1
          count += 1
  print('Number of covid infection during a completed pregnancy: ' + str(c))
  print('Number of covid infection during the 1st trimester of a completed pregnancy: ' + str(first_trimester))
  print('Number of covid infection during the 2nd trimester of a completed pregnancy: ' + str(second_trimester))
  print('Number of covid infection during the 3rd trimester of a completed pregnancy: ' + str(third_trimester))
  return(df_cohort_covid_maternity, df_cohort_covid_1st_trimester_completed_pregnancy, df_cohort_covid_2nd_trimester_completed_pregnancy, df_cohort_covid_3rd_trimester_completed_pregnancy)


  def get_covid_completed_pregnancy_cohorts_covid_records():
  # identify women with covid infections during each trimester
  # gets completed pregnancies only
  import pandas as pd
  import numpy as np
  df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_covid_maternity")  # load cohort
  cols = df_covid_mom.columns
  c = 0
  first_trimester, second_trimester, third_trimester = 0, 0, 0
  df_cohort_covid_maternity = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_1st_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_2nd_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  df_cohort_covid_3rd_trimester_completed_pregnancy = pd.DataFrame(columns=df_covid_mom.columns)
  for index, row in df_covid_mom.iterrows():
    delivery_date = row['ob_delivery_delivery_date']
    count = 0
    for item in row['observation_datetime_collect_list']:
      days_covid_test_before_birth = (delivery_date - item)/np.timedelta64(1, 'D')
      if row['result_short_collect_list'][count] == 'positive' and days_covid_test_before_birth >= 0  and days_covid_test_before_birth <= row['gestational_days']:
        df_cohort_covid_maternity = df_cohort_covid_maternity.append(row)
        trimester = determine_trimester(row['gestational_days'] - days_covid_test_before_birth)
        if trimester == '1st trimester':
          first_trimester += 1
          df_cohort_covid_1st_trimester_completed_pregnancy = df_cohort_covid_1st_trimester_completed_pregnancy.append(row)
        elif trimester == '2nd trimester':
          second_trimester += 1
          df_cohort_covid_2nd_trimester_completed_pregnancy = df_cohort_covid_2nd_trimester_completed_pregnancy.append(row)
        elif trimester == '3rd trimester':
          third_trimester += 1
          df_cohort_covid_3rd_trimester_completed_pregnancy = df_cohort_covid_3rd_trimester_completed_pregnancy.append(row)
        c += 1
        count += 1
  print('Number of covid infection during a completed pregnancy: ' + str(c))
  print('Number of covid infection during the 1st trimester of a completed pregnancy: ' + str(first_trimester))
  print('Number of covid infection during the 2nd trimester of a completed pregnancy: ' + str(second_trimester))
  print('Number of covid infection during the 3rd trimester of a completed pregnancy: ' + str(third_trimester))
  return(df_cohort_covid_maternity, df_cohort_covid_1st_trimester_completed_pregnancy, df_cohort_covid_2nd_trimester_completed_pregnancy, df_cohort_covid_3rd_trimester_completed_pregnancy)



  def determine_covid_postnatal_cohort(df_covid_mom_all):
  import pandas as pd
  import numpy as np
  c = 0
  df_covid_mom_all = df_covid_mom_all.toPandas()
  cols = df_covid_mom_all.columns
  df_cohort_covid_postnatal = pd.DataFrame(columns=cols)
  for index, row in df_covid_mom_all.iterrows():
    count = 0
    delivery_date = row['ob_delivery_delivery_date']
    if pd.isnull(delivery_date):
      continue
    else:
      for item in row['observation_datetime_collect_list']:
        if row['result_short_collect_list'][count] == 'positive':
          days_after_birth = (item - delivery_date) / np.timedelta64(1, 'D')
          if 0 < days_after_birth <= 42:
            c += 1
            df_cohort_covid_postnatal = df_cohort_covid_postnatal.append(row)
            break
          count+=1
  return(df_cohort_covid_postnatal)


def determine_covid_days_from_conception_all_pregnancies(row):  # identify the number of days into pregnancy the first covid positive test was observed
  count = 0
  working_delivery_date = row['working_delivery_date']
  delivery_date = row['ob_delivery_delivery_date']
  lmp = row['lmp']
  if pd.isnull(delivery_date):
    if pd.isnull(working_delivery_date):
      if pd.isnull(lmp):
        return(None)
      else:
        for item in row['observation_datetime_collect_list']:
          if row['result_short_collect_list'][count] == 'positive':
            days_from_conception = (item - lmp)/np.timedelta64(1, 'D')
            if 0 < days_from_conception < 280:
              return(days_from_conception)
          count += 1
    else:
      for item in row['observation_datetime_collect_list']:
        birth_covid_delta = (working_delivery_date - item)/np.timedelta64(1, 'D')
        if row['result_short_collect_list'][count] == 'positive' and birth_covid_delta >= 0:
          days_from_conception = 280 - birth_covid_delta
          return(days_from_conception)
        count += 1
  else:
    return(determine_covid_days_from_conception(row))
  return(None)


def determine_covid_days_from_conception_alt(row, c):  # identify the number of days into pregnancy the first covid positive test was observed
  count = 0
  delivery_date = row['ob_delivery_delivery_date']
  gestational_days = row['gestational_days']
  testing_date = row['covid_ordering_datetime']
  birth_covid_delta = (delivery_date - testing_date)/np.timedelta64(1, 'D')
  days_from_conception = row['gestational_days'] - birth_covid_delta
  c+=1
  return(days_from_conception, c)

def determine_covid_days_from_conception(row):  # identify the number of days into pregnancy the first covid positive test was observed
  count = 0
  delivery_date = row['ob_delivery_delivery_date']
  gestational_days = row['gestational_days']
  check_date = datetime.datetime(2021, 2, 14, 0, 0, 0)
  for item in row['ordering_datetime_collect_list']:
    if row['result_short_collect_list'][count] == 'positive' and (check_date - item).total_seconds() > 0:
      days_from_conception = row['gestational_days'] - (delivery_date - item)/np.timedelta64(1, 'D')
      if 0 <= days_from_conception <= row['gestational_days']:
        return(days_from_conception)
    count += 1
  return(None)

def verify_covid_test_during_pregnancy(row):
  delivery_date = row['ob_delivery_delivery_date']
  check_date = datetime.datetime(2021, 2, 14, 0, 0, 0)
  count = 0
  for item in row['ordering_datetime_collect_list']:
    if (check_date - item).total_seconds() > 0:
      days_from_conception = row['gestational_days'] - (delivery_date - item)/np.timedelta64(1, 'D')
      if 0 <= days_from_conception <= row['gestational_days']:
        return(True)
    count += 1
  return(False)


def get_earliest_test(df):
  dict_date = {}
  list_del_index = []
  cols = list(df.columns)
  for index, row in df.iterrows():
    pat_id, date = row.pat_id, row.covid_ordering_datetime
    if pat_id not in dict_date:
      dict_date[pat_id] = date
    elif date < dict_date[pat_id]:
      dict_date[pat_id] = date
    else:
      list_del_index.append(index)
  for index in list_del_index:
    df = df.drop(index=[index])
  return(df)


def determine_covid_maternity_cohorts_alt(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = get_earliest_test(df.toPandas())
  cols = df.columns
  c, first_trimester, second_trimester, third_trimester = 0, 0, 0, 0
  df_maternity_all = pd.DataFrame(columns=cols)
  df_1st_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_2nd_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_3rd_trimester_pregnancy = pd.DataFrame(columns=cols)
  for index, row in df.iterrows():
    days_from_conception, c = determine_covid_days_from_conception(row, c)
    if days_from_conception:
      df_maternity_all = df_maternity_all.append(row)
      trimester = determine_trimester(days_from_conception)
      if trimester == '1st trimester':
        first_trimester += 1
        df_1st_trimester_pregnancy = df_1st_trimester_pregnancy.append(row)
      elif trimester == '2nd trimester':
        second_trimester += 1
        df_2nd_trimester_pregnancy = df_2nd_trimester_pregnancy.append(row)
      elif trimester == '3rd trimester':
        third_trimester += 1
        df_3rd_trimester_pregnancy = df_3rd_trimester_pregnancy.append(row)
  print('Number of covid infections during a pregnancy: ' + str(c))
  print('Number of covid infections during the 1st trimester of a completed pregnancy: ' + str(first_trimester))
  print('Number of covid infections during the 2nd trimester of a completed pregnancy: ' + str(second_trimester))
  print('Number of covid infections during the 3rd trimester of a completed pregnancy: ' + str(third_trimester))
  return(df_maternity_all.drop(columns=['covid_ordering_datetime']), df_1st_trimester_pregnancy.drop(columns=['covid_ordering_datetime']), \
         df_2nd_trimester_pregnancy.drop(columns=['covid_ordering_datetime']), df_3rd_trimester_pregnancy.drop(columns=['covid_ordering_datetime']))


  def drop_covid_columns(df):
  df = df.drop(columns=['death_date', 'sex', 'pat_enc_csn_id_collect_list', \
                        'ordering_datetime_collect_list', 'observation_datetime_collect_list', \
                        'result_short_collect_list', 'flagged_as_collect_list', \
                        'age_at_order_dt_collect_list', 'order_name_collect_list', \
                        'results_category', 'first_ordering_datetime'])
  return(df)


def determine_covid_maternity_cohorts(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity_all = pd.DataFrame(columns=cols)
  df_1st_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_2nd_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_3rd_trimester_pregnancy = pd.DataFrame(columns=cols)
  for index, row in df.iterrows():
    #if row['number_of_fetuses'] == 1 and row['gestational_days'] >= 140 and (row['insurance'] == 'Medicaid' or row['insurance'] == 'Commercial'):
    if row['number_of_fetuses'] == 1 and row['gestational_days'] >= 140:
      days_from_conception = determine_covid_days_from_conception(row)
      if days_from_conception is not None:
        df_maternity_all = df_maternity_all.append(row)
        trimester = determine_trimester(days_from_conception)
        if trimester == '1st trimester':
          df_1st_trimester_pregnancy = df_1st_trimester_pregnancy.append(row)
        elif trimester == '2nd trimester':
          df_2nd_trimester_pregnancy = df_2nd_trimester_pregnancy.append(row)
        elif trimester == '3rd trimester':
          df_3rd_trimester_pregnancy = df_3rd_trimester_pregnancy.append(row)
  print('Number of women with covid infections during a completed pregnancy: ' + str(len(df_maternity_all.pat_id.unique())))
  print('Number of women with covid infections during the 1st trimester of a completed pregnancy: ' + str(len(df_1st_trimester_pregnancy.pat_id.unique())))
  print('Number of women with covid infections during the 2nd trimester of a completed pregnancy: ' + str(len(df_2nd_trimester_pregnancy.pat_id.unique())))
  print('Number of women with covid infections during the 3rd trimester of a completed pregnancy: ' + str(len(df_3rd_trimester_pregnancy.pat_id.unique())))
  return(df_maternity_all, df_1st_trimester_pregnancy, df_2nd_trimester_pregnancy, df_3rd_trimester_pregnancy)


def determine_no_covid_maternity_cohort(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity = pd.DataFrame(columns=cols)
  for index, row in df.iterrows():
    #if row['number_of_fetuses'] == 1 and row['gestational_days'] >= 140 and (row['insurance'] == 'Medicaid' or row['insurance'] == 'Commercial'):
    if row['number_of_fetuses'] == 1 and row['gestational_days'] >= 140:
      if verify_covid_test_during_pregnancy(row):
        df_maternity = df_maternity.append(row)
  print('Number of women without a covid infection during a completed pregnancy: ' + str(len(df_maternity.pat_id.unique())))
  return(df_maternity)


def determine_covid_maternity_cohorts_all_pregnancies(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity_all = pd.DataFrame(columns=cols)
  df_1st_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_2nd_trimester_pregnancy = pd.DataFrame(columns=cols)
  df_3rd_trimester_pregnancy = pd.DataFrame(columns=cols)
  for index, row in df.iterrows():
    if row is not None:
      days_from_conception, c = determine_covid_days_from_conception_all_pregnancies(row)
      if days_from_conception:
        df_maternity_all = df_maternity_all.append(row)
        trimester = determine_trimester(days_from_conception)
        if trimester == '1st trimester':
          df_1st_trimester_pregnancy = df_1st_trimester_pregnancy.append(row)
        elif trimester == '2nd trimester':
          df_2nd_trimester_pregnancy = df_2nd_trimester_pregnancy.append(row)
        elif trimester == '3rd trimester':
          df_3rd_trimester_pregnancy = df_3rd_trimester_pregnancy.append(row)
  c = len(df_maternity_all.pat_id.unique())
  first_trimester = len(df_1st_trimester_pregnancy.pat_id.unique())
  second_trimester = len(df_2nd_trimester_pregnancy.pat_id.unique())
  third_trimester = len(df_3rd_trimester_pregnancy.pat_id.unique())
  print('Number of women with covid infections during a completed pregnancy: ' + str(c))
  print('Number of women with covid infections during the 1st trimester of a completed pregnancy: ' + str(first_trimester))
  print('Number of women with covid infections during the 2nd trimester of a completed pregnancy: ' + str(second_trimester))
  print('Number of women with covid infections during the 3rd trimester of a completed pregnancy: ' + str(third_trimester))
  return(df_maternity_all, df_1st_trimester_pregnancy, df_2nd_trimester_pregnancy, df_3rd_trimester_pregnancy)


def get_moms_with_covid_all():
  df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_maternity")  # load maternity and covid cohorts
  df_cohort_covid = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_covid")
  df_cohort_maternity.createOrReplaceTempView("mom")
  df_cohort_covid.createOrReplaceTempView("covid")
  df_covid_mom_all = spark.sql(
  """
  FROM covid AS c
  INNER JOIN mom AS m 
  ON c.covid_pat_id = m.pat_id AND c.covid_instance = m.instance
  WHERE c.results_category == '>= 1 positive'
  """)
  df_covid_mom_all = get_additional_info(df_covid_mom_all)
  write_data_frame_to_sandbox(df_covid_mom_all, 'cohort_covid_mom_all_pregnancies', sandbox_db='rdp_phi_sandbox', replace=True)


 def get_moms_without_covid():
  df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_maternity")  # load maternity and covid cohorts
  df_cohort_covid = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_covid")
  df_no_covid_mom = spark.sql(
  """
  SELECT *
  FROM covid AS c
  INNER JOIN mom AS m 
  ON c.covid_pat_id = m.pat_id AND c.covid_instance = m.instance
  WHERE m.ob_delivery_delivery_date > c.first_ordering_datetime AND c.results_category == '>= 1 negative (no positive)'
  """)
  df_no_covid_mom = get_additional_info(df_no_covid_mom)
  write_data_frame_to_sandbox(df_no_covid_mom, 'cohort_no_covid_mom', sandbox_db='rdp_phi_sandbox', replace=True)


  def get_2019_moms():
  import datetime
  df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.cohort_maternity") 
  df_cohort_maternity.createOrReplaceTempView("mom")
  df_2019_mom = spark.sql(
  """
  SELECT /*+ RANGE_JOIN(c, 50) */ *
  FROM mom
  WHERE ob_delivery_delivery_date BETWEEN '2019-04-01 00:00:00' AND '2019-12-15 00:00:00' 
  """)
  df_2019_mom = get_additional_info(df_2019_mom)
  write_data_frame_to_sandbox(df_2019_mom, 'cohort_2019_maternity', sandbox_db='rdp_phi_sandbox', replace=True)
