Author: Samantha Piekos
Date: 10/27/22

# load the environment
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statistics

from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
from pyspark.sql.functions import *, col, lit, unix_timestamp
from scipy import stats
from sklearn.impute import SimpleImputer

%matplotlib inline

dbutils.library.installPyPI("lifelines")
from lifelines import KaplanMeierFitter, CoxPHFitter
from lifelines.statistics import logrank_test


# define universal variables
DATE_CUTOFF = datetime.today()


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medication_orders
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.sars_cov_2_cohort_functions


# define functions
def determine_trimester(days):
  if days <= 90:
    return('1st trimester')
  elif days <= 181:
    return('2nd trimester')
  else:
    return('3rd trimester')

  
def determine_covid_days_from_conception(row, test_result='positive'):  # identify the number of days into pregnancy the first covid positive test was observed
  count = 0
  delivery_date = row['ob_delivery_delivery_date']
  gestational_days = row['gestational_days']
  for item in row['ordering_datetime_collect_list']:
    if row['result_short_collect_list'][count] == test_result:
      days_from_conception = row['gestational_days'] - (delivery_date - item)/np.timedelta64(1, 'D')
      days_from_term_delivery = 280 - days_from_conception
      if 0 <= days_from_conception <= row['gestational_days'] and (item + datetime.timedelta(days=days_from_term_delivery) < DATE_CUTOFF):
        return(days_from_conception, item)
    count += 1
  return(None, None)


def consolidate_race_responses(l):
  l_new = []
  for i in l:
    if i == 'White':
      l_new.append('White or Caucasian')
    elif i == 'Patient Refused' or i == 'Unable to Determine' or i == 'Declined' or i == 'Unknown':
      continue
    else:
      l_new.append(i)
  l_new = list(set(l_new))
  return(l_new)


def handle_multiracial_exceptions(l):
  l_new = consolidate_race_responses(l)
  if l_new is None:
    return('Unknown')
  if len(l_new) == 1:
    return(l_new[0])
  if 'Other' in l_new:
    l_new.remove('Other')
    if l_new is None:
      return('Other')
    if len(l_new) == 1:
      return(l_new[0])
  return('Multiracial')


def convert_race(l):
  new_race_col = []
  for i in l:
    race = 'Unknown'
    if i is None:
      new_race_col.append(race)
    elif len(i) > 1:
      new_race_col.append(handle_multiracial_exceptions(i))
    else:
      if i[0] == 'White':
        i[0] = 'White or Caucasian'
      elif i[0] == 'Unable to Determine' or i[0] == 'Patient Refused' or i[0] == 'Declined':
        i[0] = 'Unknown'
      new_race_col.append(i[0])
  return(new_race_col)


def evaluate_covid_induced_immunity(row, conception_date):  # determine if a women previously had a covid infection prior to becoming pregnant
  count = 0
  for item in row['ordering_datetime_collect_list']:
    test_result = row['result_short_collect_list'][count]
    if test_result == 'positive' and item < conception_date:
      return(1, item)
    count += 1
  return(0, None)


def determine_covid_maternity_cohort(df, test_result='positive'):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity_all = pd.DataFrame(columns=cols)
  df_additional_cols = pd.DataFrame(columns=['pat_id', 'instance', 'episode_id', 'child_episode_id',\
                                             'conception_date', 'covid_induced_immunity', 'covid_test_result',\
                                             'covid_test_date', 'covid_test_number_of_days_from_conception',\
                                             'trimester_of_covid_test'])
  df['race'] = convert_race(list(df['race']))
  for index, row in df.iterrows():
    conception_date = row['ob_delivery_delivery_date'] - timedelta(days=row['gestational_days'])
    covid_induced_immunity = 0
    if test_result == 'positive':
      covid_induced_immunity, covid_immunity_test_date = evaluate_covid_induced_immunity(row, conception_date)
    days_from_conception, order_date = determine_covid_days_from_conception(row, test_result=test_result)
    if days_from_conception is not None:
      df_maternity_all = df_maternity_all.append(row)
      trimester = determine_trimester(days_from_conception)
      df_additional_cols = df_additional_cols.append({'pat_id': row['pat_id'],\
                                                      'instance': int(row['instance']),\
                                                      'episode_id': row['episode_id'],\
                                                      'child_episode_id': row['child_episode_id'],\
                                                      'conception_date': conception_date,\
                                                      'covid_induced_immunity': covid_induced_immunity,\
                                                      'covid_test_result': test_result,\
                                                      'covid_test_date': order_date,\
                                                      'covid_test_number_of_days_from_conception': days_from_conception,\
                                                      'trimester_of_covid_test': trimester},\
                                                     ignore_index=True)
  df_maternity_all_final = pd.merge(df_maternity_all, df_additional_cols, on=['pat_id', 'instance', 'episode_id', 'child_episode_id']).drop_duplicates(subset=['pat_id', 'instance', 'episode_id', 'child_episode_id'])
  print('Number of women with covid infections during a completed pregnancy: ' + str(len(df_maternity_all_final)))
  return(df_maternity_all_final)


def determine_covid_induced_immunity_cohort(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity_all = pd.DataFrame(columns=cols)
  df_additional_cols = pd.DataFrame(columns=['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'covid_induced_immunity', 'first_covid_positive_test_date'])
  df['race'] = convert_race(list(df['race']))
  for index, row in df.iterrows():
    conception_date = row['ob_delivery_delivery_date'] - timedelta(days=row['gestational_days'])
    covid_induced_immunity, covid_test_date = evaluate_covid_induced_immunity(row, conception_date)
    if covid_test_date is not None:
      df_maternity_all = df_maternity_all.append(row)
      df_additional_cols = df_additional_cols.append({'pat_id': row['pat_id'],\
                                                      'instance': row['instance'],\
                                                      'episode_id': row['episode_id'],\
                                                      'child_episode_id': row['child_episode_id'],\
                                                      'conception_date': conception_date,\
                                                      'covid_induced_immunity': covid_induced_immunity,\
                                                      'first_covid_positive_test_date': covid_test_date},\
                                                     ignore_index=True)
  df_maternity_all_final = pd.merge(df_maternity_all, df_additional_cols, on=['pat_id', 'instance', 'episode_id', 'child_episode_id']).drop_duplicates(subset=['pat_id', 'instance', 'episode_id', 'child_episode_id'])
  print('Number of women with covid-induced immunity prior to a completed pregnancy: ' + str(len(df_maternity_all_final)))
  return(df_maternity_all_final)


def check_if_full_vaccination_at_delivery(vaccination_dates, delivery_date):
  for date in vaccination_dates:
    if date < delivery_date < (date + relativedelta(months=+6)):
      return True
  return False


def filter_rows(df):
  df_pd = df.toPandas()
  df_passed_filter = pd.DataFrame(columns = ['pat_id', 'episode_id', 'child_episode_id'])
  for index, row in df_pd.iterrows():
    vaccination_dates = row['all_immunization_dates']
    delivery_date = row['ob_delivery_delivery_date']
    if check_if_full_vaccination_at_delivery(vaccination_dates, delivery_date):
      df_passed_filter = df_passed_filter.append({'pat_id': row['pat_id'],\
                                                  'episode_id': row['episode_id'],\
                                                  'child_episode_id': row['child_episode_id']},\
                                                  ignore_index=True)
  return df_passed_filter


def verify_full_vaccination_status_maintained(df):
  df_filtered = spark.createDataFrame(filter_rows(df))
  df.createOrReplaceTempView("original")
  df_filtered.createOrReplaceTempView("filtered")
  df_final = spark.sql(
  """
  SELECT o.*
  FROM original AS o
  INNER JOIN filtered AS f
  ON o.pat_id = f.pat_id
    AND o.episode_id = f.episode_id
    AND o.child_episode_id = f.child_episode_id
  """).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id']).drop('all_immunization_dates')
  return df_final


# define COVID-positive mom cohorts
df_cohort_covid_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_6").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_cohort_covid_maternity = df_cohort_covid_maternity.na.drop(subset=["gestational_days"]).distinct()
df_cohort_covid_maternity = determine_covid_maternity_cohort(df_cohort_covid_maternity, test_result='positive').sort_index()


# create saved pyspark dataframe of singleton COVID-positive mom cohort
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_6").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_covid_mom.createOrReplaceTempView("covid_mom")
df_temp = spark.createDataFrame(df_cohort_covid_maternity[['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'covid_induced_immunity', 'covid_test_result', 'covid_test_date', 'covid_test_number_of_days_from_conception', 'trimester_of_covid_test']])
df_temp.createOrReplaceTempView("temp")
df_cohort_covid_maternity_final = spark.sql(
"""
  FROM covid_mom AS cm
  INNER JOIN temp AS t 
  ON cm.pat_id = t.pat_id
    AND cm.instance = t.instance
    AND cm.episode_id = t.episode_id
    AND cm.child_episode_id = t.child_episode_id
  SELECT cm.*, t.conception_date, t.covid_induced_immunity, t.covid_test_result, t.covid_test_date, t.covid_test_number_of_days_from_conception, t.trimester_of_covid_test
  """).dropDuplicates()
write_data_frame_to_sandbox(df_cohort_covid_maternity_final, 'snp2_cohort_covid_maternity_covid_test_data', sandbox_db='rdp_phi_sandbox', replace=True)


# define COVID-positive mom cohorts
df_cohort_maternity_covid_immunity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_6").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_cohort_maternity_covid_immunity = df_cohort_maternity_covid_immunity.na.drop(subset=["gestational_days"]).distinct()
df_cohort_maternity_covid_immunity = determine_covid_induced_immunity_cohort(df_cohort_maternity_covid_immunity).sort_index()


# create saved pyspark dataframe of singleton COVID-positive mom cohort
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_6").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_covid_mom.createOrReplaceTempView("covid_mom")
df_temp = spark.createDataFrame(df_cohort_maternity_covid_immunity[['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'covid_induced_immunity', 'first_covid_positive_test_date']])
df_temp.createOrReplaceTempView("temp")
df_cohort_maternity_covid_immunity_final = spark.sql(
"""
  FROM covid_mom AS cm
  INNER JOIN temp AS t 
  ON cm.pat_id = t.pat_id
    AND cm.instance = t.instance
    AND cm.episode_id = t.episode_id
    AND cm.child_episode_id = t.child_episode_id
  SELECT cm.*, t.conception_date, t.covid_induced_immunity, t.first_covid_positive_test_date
  """).dropDuplicates()
write_data_frame_to_sandbox(df_cohort_maternity_covid_immunity_final, 'snp2_cohort_maternity_covid_immunity', sandbox_db='rdp_phi_sandbox', replace=True)


# load immunization table
df_immunization = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_immunization")
df_immunization.createOrReplaceTempView("immunization")


# Identify COVID-19 breakthrough cases when fully vaccinated with J&J
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_test_data")
df_covid_mom.createOrReplaceTempView("covid_mom")

df_cohort_maternity_covid_breakthrough_jj_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_mom AS cm
ON i.pat_id = cm.pat_id
  AND i.first_immunization_date + interval '14' day < cm.covid_test_date
  AND i.first_immunization_name == 'COVID-19 (JANSSEN)'
  AND i.first_immunization_name == i.last_immunization_name
  AND cm.covid_induced_immunity == 0
SELECT cm.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, i.first_immunization_date + interval '14' day AS full_vaccination_date, i.first_immunization_name AS full_vaccination_name, i.all_immunization_dates
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_covid_breakthrough_jj_2 = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_mom AS cm
ON i.pat_id = cm.pat_id
  AND i.first_immunization_date + interval '14' day < cm.covid_test_date
  AND i.first_immunization_name == 'COVID-19 (JANSSEN)'
  AND element_at(array_distinct(i.all_immunization_dates), 2) > cm.ob_delivery_delivery_date
  AND cm.covid_induced_immunity == 0
SELECT cm.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, i.first_immunization_date + interval '14' day AS full_vaccination_date, i.first_immunization_name AS full_vaccination_name, i.all_immunization_dates
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_covid_breakthrough_jj = verify_full_vaccination_status_maintained(df_cohort_maternity_covid_breakthrough_jj)

# save dataframe
df_cohort_maternity_covid_breakthrough_jj = df_cohort_maternity_covid_breakthrough_jj_1.union(df_cohort_maternity_covid_breakthrough_jj_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_covid_breakthrough_jj, 'snp2_cohort_maternity_covid_breakthrough_jj', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify COVID-19 breakthrough cases when fully vaccinated with Moderna
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_test_data")
df_covid_mom.createOrReplaceTempView("covid_mom")
df_cohort_maternity_covid_breakthrough_moderna = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_mom AS cm
ON i.pat_id = cm.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < cm.covid_test_date
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
  AND i.last_immunization_date > i.first_immunization_date
  AND cm.covid_induced_immunity == 0
SELECT cm.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates
""").filter(col("first_immunization_name").like("%MODERNA%")).filter(col("full_vaccination_name").like("%MODERNA%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_covid_breakthrough_moderna = verify_full_vaccination_status_maintained(df_cohort_maternity_covid_breakthrough_moderna)

# save to dataframe
write_data_frame_to_sandbox(df_cohort_maternity_covid_breakthrough_moderna, 'snp2_cohort_maternity_covid_breakthrough_moderna', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify COVID-19 breakthrough cases when fully vaccinated with Pfizer
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_test_data")
df_covid_mom.createOrReplaceTempView("covid_mom")
df_cohort_maternity_covid_breakthrough_pfizer = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_mom AS cm
ON i.pat_id = cm.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < cm.covid_test_date
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
  AND i.last_immunization_date > i.first_immunization_date
  AND cm.covid_induced_immunity == 0
SELECT cm.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates
""").filter(col("first_immunization_name").like("%PFIZER%")).filter(col("full_vaccination_name").like("%PFIZER%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_covid_breakthrough_pfizer = verify_full_vaccination_status_maintained(df_cohort_maternity_covid_breakthrough_pfizer)

# save to dataframe
write_data_frame_to_sandbox(df_cohort_maternity_covid_breakthrough_pfizer, 'snp2_cohort_maternity_covid_breakthrough_pfizer', sandbox_db='rdp_phi_sandbox', replace=True)


# create single cohrot of fully vaccinated by mRNA breakthrough covid cases in pregnant women that have delivered
df_cohort_maternity_covid_breakthrough_mrna = df_cohort_maternity_covid_breakthrough_pfizer.union(df_cohort_maternity_covid_breakthrough_moderna).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_covid_breakthrough_mrna, 'snp2_cohort_maternity_covid_breakthrough_mrna', sandbox_db='rdp_phi_sandbox', replace=True)


# create single cohrot of fully vaccinated breakthrough covid cases in pregnant women that have delivered
df_cohort_maternity_covid_breakthrough = df_cohort_maternity_covid_breakthrough_jj.union(df_cohort_maternity_covid_breakthrough_pfizer).union(df_cohort_maternity_covid_breakthrough_moderna).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_covid_breakthrough, 'snp2_cohort_maternity_covid_breakthrough', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify SARS-CoV-2-positive pregnant women who were unvaccinated at time of infection and subsequently got vaccinated
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_test_data")
df_covid_mom.createOrReplaceTempView("covid_mom")
df_cohort_covid_maternity_covid_unvaccinated_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_mom AS cm
ON i.pat_id = cm.pat_id
  AND i.first_immunization_date < cm.covid_test_date
SELECT cm.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
# Identify SARS-CoV-2-positive pregnant women who never got vaccinated
df_cohort_covid_maternity_covid_unvaccinated_2 = spark.sql(
"""
FROM covid_mom AS cm
LEFT ANTI JOIN immunization AS i
ON i.pat_id = cm.pat_id
SELECT cm.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_covid_maternity_covid_unvaccinated_3 = df_cohort_covid_maternity_covid_unvaccinated_1.union(df_cohort_covid_maternity_covid_unvaccinated_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
df_cohort_covid_maternity_covid_unvaccinated = df_cohort_covid_maternity_covid_unvaccinated_3.filter(df_cohort_covid_maternity_covid_unvaccinated_3.covid_induced_immunity==0)
write_data_frame_to_sandbox(df_cohort_covid_maternity_covid_unvaccinated, 'snp2_cohort_covid_maternity_covid_unvaccinated', sandbox_db='rdp_phi_sandbox', replace=True)

# identify unvaccinated women that had covid-induced immunity prior to pregnancy
df_cohort_covid_maternity_covid_immunity = df_cohort_covid_maternity_covid_unvaccinated_3.filter(df_cohort_covid_maternity_covid_unvaccinated_3.covid_induced_immunity==1).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_covid_maternity_covid_immunity, 'snp2_cohort_covid_maternity_unvaccinated_covid_immunity', sandbox_db='rdp_phi_sandbox', replace=True)


# evaluate defined cohort sizes
df_cohort_maternity_covid_breakthrough = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_breakthrough")
print('# Number of women with breakthrough covid infection during pregnancy who has delivered: ' + str(df_cohort_maternity_covid_breakthrough.count()))

df_cohort_maternity_covid_breakthrough_jj = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_breakthrough_jj")
print('# Number of women with breakthrough covid infection (J&J) during pregnancy who has delivered: ' + str(df_cohort_maternity_covid_breakthrough_jj.count()))

df_cohort_maternity_covid_breakthrough_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_breakthrough_mrna")
print('# Number of women with breakthrough covid infection during pregnancy who has delivered: ' + str(df_cohort_maternity_covid_breakthrough_mrna.count()))

df_cohort_maternity_covid_breakthrough_moderna  = df_cohort_maternity_covid_breakthrough_mrna.filter(col("first_immunization_name").like("%MODERNA%"))
print('# Number of women with breakthrough covid infection (Moderna) during pregnancy who has delivered: ' + str(df_cohort_maternity_covid_breakthrough_moderna.count()))

df_cohort_maternity_covid_breakthrough_pfizer = df_cohort_maternity_covid_breakthrough_mrna.filter(col("first_immunization_name").like("%PFIZER%"))
print('# Number of women with breakthrough covid infection (Pfizer) during pregnancy who has delivered: ' + str(df_cohort_maternity_covid_breakthrough_pfizer.count()))

df_cohort_covid_maternity_covid_immunity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_unvaccinated_covid_immunity")
print('# Number of women with covid-induced immunity prior to pregnancy that got a subsequent COIVD-19 infection while unvaccinated during pregnancy who has delivered: ' + str(df_cohort_covid_maternity_covid_immunity.count()))

df_cohort_covid_maternity_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_unvaccinated")
print('# Number of women unvaccinated at time of COIVD-19 infection during pregnancy who has delivered: ' + str(df_cohort_covid_maternity_unvaccinated.count()))

df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_unvaccinated")
df_unvaccinated.createOrReplaceTempView("unvaccinated_mom")
df_unvaccinated = spark.sql("SELECT * FROM unvaccinated_mom AS um WHERE '2021-01-26' <= um.ob_delivery_delivery_date")
print('# Number of pregnant women unvaccinated at time of COIVD-19 infection during pregnancy who have delivered after 1-25-21: ' + str(df_unvaccinated.count()))


# filter maternity cohort for pregnant people that delivered >140 gestation, between the ages of 18-45, singleton pregnancy, and no recorded covid-19 infection recorded prior to pregnancy
df_cohort_maternity_covid_immunity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_immunity")
df_cohort_maternity_covid_immunity.createOrReplaceTempView("covid_immunity_mom")
df_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity AS m WHERE m.number_of_fetuses == 1 and m.age_at_start_dt >= 18 and m.age_at_start_dt < 45 and m.gestational_days >= 140 and m.ob_delivery_delivery_date >= '2020-03-05'").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_mom.createOrReplaceTempView("mom")
df_mom = spark.sql(
"""
FROM mom AS mom
LEFT ANTI JOIN covid_immunity_mom AS cim
ON cim.pat_id = mom.pat_id
  AND cim.instance = mom.instance
  AND cim.episode_id = mom.episode_id
  AND cim.child_episode_id = mom.child_episode_id
SELECT mom.*, date_sub(mom.ob_delivery_delivery_date, mom.gestational_days) AS conception_date
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_mom.createOrReplaceTempView("mom")


# Identify pregnant women fully vaccinated with J&J
df_cohort_maternity_jj_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date + interval '14' day < m.ob_delivery_delivery_date
  AND i.first_immunization_name == 'COVID-19 (JANSSEN)'
  AND i.first_immunization_name == i.last_immunization_name
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, i.first_immunization_date + interval '14' day AS full_vaccination_date, i.first_immunization_name AS full_vaccination_name, i.all_immunization_dates
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])


df_cohort_maternity_jj_2 = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date + interval '14' day < m.ob_delivery_delivery_date
  AND element_at(array_distinct(i.all_immunization_dates), 2) > m.ob_delivery_delivery_date
  AND i.first_immunization_name == 'COVID-19 (JANSSEN)'
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, i.first_immunization_date + interval '14' day AS full_vaccination_date, i.first_immunization_name AS full_vaccination_name, i.all_immunization_dates
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])


# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_jj = verify_full_vaccination_status_maintained(df_cohort_maternity_jj)
df_cohort_maternity_jj = df_cohort_maternity_jj_1.union(df_cohort_maternity_jj_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_jj, 'snp2_cohort_maternity_jj', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify pregnant women fully vaccinated with Moderna
df_cohort_maternity_moderna = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < m.ob_delivery_delivery_date
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates
""").filter(col("first_immunization_name").like("%MODERNA%")).filter(col("full_vaccination_name").like("%MODERNA%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_moderna = verify_full_vaccination_status_maintained(df_cohort_maternity_moderna)

write_data_frame_to_sandbox(df_cohort_maternity_moderna, 'snp2_cohort_maternity_moderna', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify pregnant women fully vaccinated with Pfizer
df_cohort_maternity_pfizer = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < m.ob_delivery_delivery_date
  AND i.last_immunization_date > i.first_immunization_date
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates
""").filter(col("first_immunization_name").like("%PFIZER%")).filter(col("full_vaccination_name").like("%PFIZER%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# filter for only those that were fully vaccinated (finished initial series or booster within the last six months) at delivery
#df_cohort_maternity_pfizer = verify_full_vaccination_status_maintained(df_cohort_maternity_pfizer)

write_data_frame_to_sandbox(df_cohort_maternity_pfizer, 'snp2_cohort_maternity_pfizer', sandbox_db='rdp_phi_sandbox', replace=True)


# create single cohrot of fully vaccinated pregnant women that have delivered
df_cohort_maternity_vaccinated_mrna = df_cohort_maternity_pfizer.union(df_cohort_maternity_moderna).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna', sandbox_db='rdp_phi_sandbox', replace=True)


# create single cohrot of fully vaccinated pregnant women that have delivered
df_cohort_maternity_vaccinated = df_cohort_maternity_jj.union(df_cohort_maternity_pfizer).union(df_cohort_maternity_moderna).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
write_data_frame_to_sandbox(df_cohort_maternity_vaccinated, 'snp2_cohort_maternity_vaccinated', sandbox_db='rdp_phi_sandbox', replace=True)


# Identify pregnant women who were unvaccinated at time of delivery
df_cohort_maternity_unvaccinated_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date > m.ob_delivery_delivery_date
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_2 = spark.sql(
"""
FROM mom AS m
LEFT ANTI JOIN immunization AS i
ON i.pat_id = m.pat_id
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_final = df_cohort_maternity_unvaccinated_1.union(df_cohort_maternity_unvaccinated_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

write_data_frame_to_sandbox(df_cohort_maternity_unvaccinated_final, 'snp2_cohort_maternity_unvaccinated', sandbox_db='rdp_phi_sandbox', replace=True)


df_cohort_maternity_unvaccinated_covid_immunity_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN covid_immunity_mom AS cim
ON i.pat_id = cim.pat_id
  AND i.first_immunization_date > cim.ob_delivery_delivery_date
SELECT cim.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_covid_immunity_2 = spark.sql(
"""
FROM covid_immunity_mom AS cim
LEFT ANTI JOIN immunization AS i
ON i.pat_id = cim.pat_id
SELECT cim.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_covid_immunity_final = df_cohort_maternity_unvaccinated_covid_immunity_1.union(df_cohort_maternity_unvaccinated_covid_immunity_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

write_data_frame_to_sandbox(df_cohort_maternity_unvaccinated_covid_immunity_final, 'snp2_cohort_maternity_unvaccinated_covid_immunity', sandbox_db='rdp_phi_sandbox', replace=True)


# evaluate defined cohort sizes
df_cohort_maternity_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated")
print('# Number of vaccinated pregnant women delivered: ' + str(df_cohort_maternity_vaccinated.count()))

df_cohort_maternity_jj = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_jj")
print('# Number of vaccinated (J&J) pregnant women who have delivered: ' + str(df_cohort_maternity_jj.count()))

df_cohort_maternity_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna")
print('# Number of vaccinated (mrna) pregnant women who have delivered: ' + str(df_cohort_maternity_vaccinated_mrna.count()))

df_cohort_maternity_moderna = df_cohort_maternity_vaccinated_mrna.filter(col("first_immunization_name").like("%MODERNA%"))
print('# Number of vaccinated (Moderna) pregnant women who have delivered: ' + str(df_cohort_maternity_moderna.count()))

df_cohort_maternity_pfizer  = df_cohort_maternity_vaccinated_mrna.filter(col("first_immunization_name").like("%PFIZER%")) 
print('# Number of vaccinated (Pfizer) pregnant women who have delivered: ' + str(df_cohort_maternity_pfizer.count()))

df_cohort_maternity_unvaccinated_covid_immunity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_covid_immunity")
print('# Number of women with covid-induced immunity prior to a completed singleton pregnancy: ' + str(df_cohort_maternity_unvaccinated_covid_immunity.count()))

df_cohort_maternity_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated")
print('# Number of unvaccinated women with a completed singleton pregnancy: ' + str(df_cohort_maternity_unvaccinated.count()))
df_cohort_maternity_unvaccinated.createOrReplaceTempView("unvaccinated_mom")
df_cohort_maternity_unvaccinated = spark.sql("SELECT * FROM unvaccinated_mom AS um WHERE '2021-01-26' <= um.ob_delivery_delivery_date")
print('# Number of unvaccinated pregnant women who have delivered after 1-25-21: ' + str(df_cohort_maternity_unvaccinated.count()))


# Check the number of unvaccinated patients that delivered pre-delta and during delta
df_cohort_maternity_covid_unvaccinated_pyspark = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_unvaccinated")
df_cohort_maternity_covid_unvaccinated_pyspark.createOrReplaceTempView("unvaccinated_covid_mom")
df_cohort_maternity_covid_unvaccinated_pre_delta = spark.sql("SELECT * FROM unvaccinated_covid_mom AS ucm WHERE '2021-05-29' >= ucm.covid_test_date")
print('# Number of women unvaccinated at time of COIVD-19 infection during pregnancy who has delivered before delta: ' + str(df_cohort_maternity_covid_unvaccinated_pre_delta.count()))

df_cohort_maternity_covid_unvaccinated_delta = spark.sql("SELECT * FROM unvaccinated_covid_mom AS ucm WHERE '2021-07-17' <= ucm.covid_test_date AND '2021-12-11' >= ucm.covid_test_date")
print('# Number of women unvaccinated at time of COIVD-19 infection during pregnancy who has delivered during delta: ' + str(df_cohort_maternity_covid_unvaccinated_delta.count()))

df_cohort_maternity_covid_unvaccinated_omicron = spark.sql("SELECT * FROM unvaccinated_covid_mom AS ucm WHERE '2022-01-01' <= ucm.covid_test_date")
print('# Number of women unvaccinated at time of COIVD-19 infection during pregnancy who has delivered during omicron: ' + str(df_cohort_maternity_covid_unvaccinated_omicron.count()))

df_cohort_maternity_covid_unvaccinated_pyspark = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_unvaccinated")
df_cohort_maternity_covid_unvaccinated_pyspark.agg({'covid_test_date': 'max'}).show()
df_cohort_maternity_covid_unvaccinated_pyspark.agg({'ob_delivery_delivery_date': 'max'}).show()


# Check Maternal Mortality of Unvaccinated Pre-Delta vs Delta Patients
# Identify SARS-CoV-2-positive unvaccinated patients that died pre-delta
df_patient = spark.sql("SELECT * FROM rdp_phi.patient")
df_patient.createOrReplaceTempView("patient")
df_cohort_maternity_unvaccinated_covid_death_pre_delta = spark.sql(
"""
FROM patient AS p
INNER JOIN unvaccinated_covid_mom AS ucm
ON p.pat_id = ucm.pat_id
  AND p.instance = ucm.instance
  AND '2021-05-29' >= ucm.covid_test_date
SELECT p.deathdate, ucm.*
""").distinct()
df_cohort_maternity_unvaccinated_covid_death_pre_delta_pd = df_cohort_maternity_unvaccinated_covid_death_pre_delta.toPandas()
print("# Number of unvaccinated COVID-19 pregnant women that have died pre_delta: " + str(len(df_cohort_maternity_unvaccinated_covid_death_pre_delta_pd)-df_cohort_maternity_unvaccinated_covid_death_pre_delta_pd.deathdate.isna().sum()))

# Identify SARS-CoV-2-positive unvaccinated patients that died during delta
df_cohort_maternity_unvaccinated_covid_death_delta = spark.sql(
"""
FROM patient AS p
INNER JOIN unvaccinated_covid_mom AS ucm
ON p.pat_id = ucm.pat_id
  AND p.instance = ucm.instance
  AND '2021-07-17' <= ucm.covid_test_date
  AND '2021-12-11' >= ucm.covid_test_date
SELECT p.deathdate, ucm.*
""").distinct()
df_cohort_maternity_unvaccinated_covid_death_delta_pd = df_cohort_maternity_unvaccinated_covid_death_delta.toPandas()
print("# Number of unvaccinated COVID-19 pregnant women that have died during delta: " + str(len(df_cohort_maternity_unvaccinated_covid_death_delta_pd)-df_cohort_maternity_unvaccinated_covid_death_delta_pd.deathdate.isna().sum()))

# Identify SARS-CoV-2-positive unvaccinated patients that died during omicron
df_cohort_maternity_unvaccinated_covid_death_omicron = spark.sql(
"""
FROM patient AS p
INNER JOIN unvaccinated_covid_mom AS ucm
ON p.pat_id = ucm.pat_id
  AND p.instance = ucm.instance
  AND '2022-01-01' <= ucm.covid_test_date
SELECT p.deathdate, ucm.*
""").distinct()
df_cohort_maternity_unvaccinated_covid_death_omicron_pd = df_cohort_maternity_unvaccinated_covid_death_omicron.toPandas()
print("# Number of unvaccinated COVID-19 pregnant women that have died during Omicron: " + str(len(df_cohort_maternity_unvaccinated_covid_death_omicron_pd)-df_cohort_maternity_unvaccinated_covid_death_omicron_pd.deathdate.isna().sum()))


# Check Maternal Mortality of Unvaccinated vs Vaccinated COVID-19 Pregnant Patients
# Identify SARS-CoV-2-positive unvaccinated patients that died
df_unvaccinated_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_covid_unvaccinated")
df_unvaccinated_covid_mom.createOrReplaceTempView("unvaccinated_covid_mom")
df_patient = spark.sql("SELECT * FROM rdp_phi.patient")
df_patient.createOrReplaceTempView("patient")
df_cohort_maternity_unvaccinated_covid_death = spark.sql(
"""
FROM patient AS p
INNER JOIN unvaccinated_covid_mom AS ucm
ON p.pat_id = ucm.pat_id
  AND p.instance = ucm.instance
SELECT p.deathdate, ucm.*
""").distinct()
df_cohort_maternity_unvaccinated_covid_death_pd = df_cohort_maternity_unvaccinated_covid_death.toPandas()
print("# Number of unvaccinated pregnant women that had COVID-19 and died: " + str(len(df_cohort_maternity_unvaccinated_covid_death_pd)-df_cohort_maternity_unvaccinated_covid_death_pd.deathdate.isna().sum()))

# Identify SARS-CoV-2 unvaccinated matched patients that died
df_unvaccinated_matched_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_matched_control_covid")
df_unvaccinated_matched_covid_mom.createOrReplaceTempView("unvaccinated_matched_covid_mom")
df_patient = spark.sql("SELECT * FROM rdp_phi.patient")
df_patient.createOrReplaceTempView("patient")
df_cohort_maternity_unvaccinated_matched_covid_death = spark.sql(
"""
FROM patient AS p
INNER JOIN unvaccinated_matched_covid_mom AS umcm
ON p.pat_id = umcm.pat_id
  AND p.instance = umcm.instance
SELECT p.deathdate, umcm.*
""").distinct()
df_cohort_maternity_unvaccinated_matched_covid_death_pd = df_cohort_maternity_unvaccinated_covid_death.toPandas()
print("# Number of unvaccinated matched pregnant women that had COVID-19 and died: " + str(len(df_cohort_maternity_unvaccinated_matched_covid_death_pd)-df_cohort_maternity_unvaccinated_matched_covid_death_pd.deathdate.isna().sum()))

# Identify SARS-CoV-2-positive vaccinated patients that died
df_covid_breakthrough_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_breakthrough")
df_covid_breakthrough_mom.createOrReplaceTempView("breakthrough_mom")
df_patient = spark.sql("SELECT * FROM rdp_phi.patient")
df_patient.createOrReplaceTempView("patient")
df_cohort_maternity_breakthrough_covid_death = spark.sql(
"""
FROM patient AS p
INNER JOIN breakthrough_mom AS bm
ON p.pat_id = bm.pat_id
  AND p.instance = bm.instance
SELECT p.deathdate, bm.*
""").distinct()
df_cohort_maternity_breakthrough_covid_death_pd = df_cohort_maternity_breakthrough_covid_death.toPandas()
print("# Number of vaccinated pregnant women that had COVID-19 and died: " + str(len(df_cohort_maternity_breakthrough_covid_death_pd)-df_cohort_maternity_breakthrough_covid_death_pd.deathdate.isna().sum()))


# Get Overview of Cohort Selection by Numbers
df_immunization = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_immunization")
df_immunization.createOrReplaceTempView("immunization")

print('# Number of unique patients with COVID-19 Immunization Records: ' + str(df_immunization.select('pat_id').dropDuplicates().count()))
df_mom= spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_mom.createOrReplaceTempView("mom")
df_mom = spark.sql("SELECT * FROM mom WHERE '2021-01-26' <= mom.ob_delivery_delivery_date").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of pregnant people that delivered from 1-26-21 through 10-26-22: ' + str(df_mom.count()))
df_mom.createOrReplaceTempView("mom")
df_mom_limited = spark.sql("SELECT * FROM mom WHERE mom.number_of_fetuses == 1 AND mom.age_at_start_dt >= 18 AND mom.age_at_start_dt < 45 AND mom.gestational_days >= 140")
print('# Number of pregnant people fitting the criteria that delivered from 1-26-21 through 10-26-22: ' + str(df_mom_limited.count()))

df_mom_limited.createOrReplaceTempView("mom")
df_mom_vaccinated = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of pregnant people with a vaccination record: ' + str(df_mom_vaccinated.count()))

df_mom_vaccinated_before_delivery = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date < m.ob_delivery_delivery_date
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of pregnant people with a vaccination record prior to delivery: ' + str(df_mom_vaccinated_before_delivery.count()))

# Identify pregnant women who were unvaccinated at time of delivery
df_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity AS m WHERE m.number_of_fetuses == 1 and m.age_at_start_dt >= 18 and m.age_at_start_dt < 45 and m.gestational_days >= 140 and m.ob_delivery_delivery_date >= '2021-01-26'").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_mom.createOrReplaceTempView("mom")
df_cohort_maternity_unvaccinated_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date > m.ob_delivery_delivery_date
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_2 = spark.sql(
"""
FROM mom AS m
LEFT ANTI JOIN immunization AS i
ON i.pat_id = m.pat_id
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_3 = df_cohort_maternity_unvaccinated_1.union(df_cohort_maternity_unvaccinated_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of unvaccinated pregnat people: ' + str(df_cohort_maternity_unvaccinated_3.count()))
df_cohort_maternity_unvaccinated_3.createOrReplaceTempView("unvaccinated_mom")

df_cohort_maternity_covid_immunity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_immunity")
df_cohort_maternity_covid_immunity.createOrReplaceTempView("covid_immunity_mom")
df_unvaccinated = spark.sql(
"""
FROM unvaccinated_mom AS um
LEFT ANTI JOIN covid_immunity_mom AS cim
ON cim.pat_id = um.pat_id
  AND cim.instance = um.instance
  AND cim.episode_id = um.episode_id
  AND cim.child_episode_id = um.child_episode_id
SELECT um.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of unvaccinated pregnant people with no prior covid exposure: ' + str(df_unvaccinated.count()))

# Identify pregnant women fully vaccinated with Moderna
df_cohort_mom_moderna = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < m.ob_delivery_delivery_date
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates, i.all_immunization_names
""").filter(col("first_immunization_name").like("%MODERNA%")).filter(col("full_vaccination_name").like("%MODERNA%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])


# identify pregnant people that were boosted (moderna) prior to delivery
df_cohort_mom_moderna.createOrReplaceTempView("moderna")
df_cohort_mom_moderna_boosted_1 = spark.sql("SELECT moderna.*, element_at(array_distinct(moderna.all_immunization_dates), 3) AS booster_date, element_at(array_distinct(moderna.all_immunization_names), 3) AS booster_name FROM moderna WHERE (full_vaccination_date - interval '14' day) != element_at(array_distinct(moderna.all_immunization_dates), 3) AND element_at(array_distinct(moderna.all_immunization_dates), 3) < ob_delivery_delivery_date")
df_cohort_mom_moderna_boosted_2 = spark.sql("SELECT moderna.*, last_immunization_date AS booster_date, last_immunization_name AS booster_name FROM moderna WHERE (full_vaccination_date - interval '14' day) != last_immunization_date AND last_immunization_date < ob_delivery_delivery_date")
df_cohort_mom_moderna_boosted = df_cohort_mom_moderna_boosted_1.union(df_cohort_mom_moderna_boosted_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# identify pregnant people that only received the full vaccination moderna series prior to delivery
df_cohort_mom_moderna_boosted.createOrReplaceTempView("moderna_boosted")
df_cohort_mom_moderna_2_shots_only = spark.sql(
"""
FROM moderna AS m
LEFT ANTI JOIN moderna_boosted AS mb
ON m.pat_id == mb.pat_id
  AND m.episode_id == mb.episode_id
  AND m.child_episode_id == mb.child_episode_id
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])


# Identify pregnant women fully vaccinated with Pfizer
df_cohort_mom_pfizer = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND element_at(array_distinct(i.all_immunization_dates), 1) != element_at(array_distinct(i.all_immunization_dates), 2)
  AND element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day < m.ob_delivery_delivery_date
  AND i.last_immunization_date > i.first_immunization_date
SELECT m.*, i.first_immunization_date, i.first_immunization_name, i.last_immunization_date, i.last_immunization_name, element_at(array_distinct(i.all_immunization_dates), 2) + interval '14' day AS full_vaccination_date, element_at(i.all_immunization_names, 2) AS full_vaccination_name, i.all_immunization_dates, i.all_immunization_names
""").filter(col("first_immunization_name").like("%PFIZER%")).filter(col("full_vaccination_name").like("%PFIZER%")).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# identify pregnant people that were boosted (pfizer) prior to delivery
df_cohort_mom_pfizer.createOrReplaceTempView("pfizer")
df_cohort_mom_pfizer_boosted_1 = spark.sql("SELECT pfizer.*, element_at(array_distinct(pfizer.all_immunization_dates), 3) AS booster_date, element_at(array_distinct(pfizer.all_immunization_names), 3) AS booster_name FROM pfizer WHERE (full_vaccination_date - interval '14' day) != element_at(array_distinct(pfizer.all_immunization_dates), 3) AND element_at(array_distinct(pfizer.all_immunization_dates), 3) < ob_delivery_delivery_date")
df_cohort_mom_pfizer_boosted_2 = spark.sql("SELECT pfizer.*, last_immunization_date AS booster_date, last_immunization_name AS booster_name FROM pfizer WHERE (full_vaccination_date - interval '14' day) != last_immunization_date AND last_immunization_date < ob_delivery_delivery_date")
df_cohort_mom_pfizer_boosted = df_cohort_mom_pfizer_boosted_1.union(df_cohort_mom_pfizer_boosted_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

# identify pregnant people that only received the full vaccination moderna series prior to delivery
df_cohort_mom_pfizer_boosted.createOrReplaceTempView("pfizer_boosted")
df_cohort_mom_pfizer_2_shots_only = spark.sql(
"""
FROM pfizer AS p
LEFT ANTI JOIN pfizer_boosted AS pb
ON p.pat_id == pb.pat_id
  AND p.episode_id == pb.episode_id
  AND p.child_episode_id == pb.child_episode_id
SELECT p.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])


print('# Number of pregnant people fully vaccinated (mRNA) at delivery: ' + str(df_cohort_mom_moderna.union(df_cohort_mom_pfizer).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id']).count()))
print('\n')

df_cohort_mom_mrna_2_shots_only = df_cohort_mom_pfizer_2_shots_only.union(df_cohort_mom_moderna_2_shots_only).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print('# Number of pregnant people that only received two shots prior to delivery: ' + str(df_cohort_mom_mrna_2_shots_only.count()))
df_cohort_mom_mrna_2_shots_only.createOrReplaceTempView("two_mrna_shots_only")
df_vaccinated_before_booster = spark.sql("SELECT tmso.* FROM two_mrna_shots_only AS tmso WHERE tmso.ob_delivery_delivery_date > '2021-09-22'")
print('# Number of pregnant people that were vaccinated, but not boosted and delivered after the booster became avaialable: ' + str(df_vaccinated_before_booster.count()))
df_vaccinated_but_not_boosted = spark.sql("SELECT tmso.* FROM two_mrna_shots_only AS tmso WHERE (tmso.full_vaccination_date + interval '168' day) < tmso.ob_delivery_delivery_date AND tmso.ob_delivery_delivery_date > '2021-09-22'")
print('# Number of pregnant people that were vaccinated, but not boosted where initial series ended >6 months prior: ' + str(df_vaccinated_but_not_boosted.count()))
print('\n')

df_cohort_mom_mrna_boosted = df_cohort_mom_pfizer_boosted.union(df_cohort_mom_moderna_boosted).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
df_cohort_mom_mrna_boosted.createOrReplaceTempView("boosted")
print('# Number of pregnant people that received at least three shots prior to delivery: ' + str(df_cohort_mom_mrna_boosted.count()))
df_cohort_mom_mrna_boosted_before_booster = spark.sql("SELECT b.* FROM boosted AS b WHERE b.ob_delivery_delivery_date > '2021-09-22'")
print('# Number of pregnant people that received at least three shots prior to delivery and boosted after booster release: ' + str(df_cohort_mom_mrna_boosted_before_booster.count()))
df_cohort_mom_mrna_boosted_less_than_6_months = spark.sql("SELECT b.* FROM boosted AS b WHERE b.ob_delivery_delivery_date > '2021-09-22' AND (b.full_vaccination_date + interval '168' day) < b.booster_date")
print('# Number of pregnant people that received at least three shots prior to delivery and boosted after booster release and at least 6 months after completing the initial series: ' + str(df_cohort_mom_mrna_boosted_less_than_6_months.count()))
print('# Number of pregnant people that received initial series and bivalent booster, but not 3rd booster shot: ' + str(df_cohort_mom_mrna_boosted.filter(col("booster_name").like("%BIVALENT%")).count()))
print('# Number of pregnant people that received initial series, third booster, and bivalent booster: ' + str(df_cohort_mom_mrna_boosted.filter(~col("booster_name").like("%BIVALENT%")).filter(col("last_immunization_name").like("%BIVALENT%")).count()))
