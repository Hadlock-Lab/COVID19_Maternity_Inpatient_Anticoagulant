# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medications_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medication_orders
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.social_history
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_registry
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.encounters_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.background_cohorts.define_covid_pregnancy_cohorts
import COVID19_Maternity_Inpatient_Anticoagulant.background_cohorts.define_covid_vaccination_cohorts

# load packages 
from datetime import date
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
import numpy as np
from matplotlib import pyplot as plt
from scipy import stats
import math
from statsmodels.graphics.gofplots import qqplot
from numpy.random import seed
import pyspark.sql.functions as F
import pyspark.sql.functions as f


def print_unique_column_list_items(lst):
  s = set()
  for l in lst:
    for item in l:
      s.add(item)
  for item in s:
    print(item)


def is_dose_not_administered(lst):
  list_remove = ['Canceled Entry', 'Held', 'Missed', 'Automatically Held', 'Held by Provider', 'MAR Hold', 'Stopped', 'Paused', 'Canceled Entry']
  lst_list = list(lst)
  if len(lst_list) == 0:
    return False
  for item in list_remove:
    if item in lst_list:
      lst_list.remove(item)
  if len(lst_list) == 0:
    return True
  return False


def is_non_covid_anticoagulant_use(med_admin_date, positive_test_date):
  positive_test_date = datetime.datetime.strptime(str(positive_test_date), '%Y-%m-%d %H:%M:%S').date()
  covid_start_date = positive_test_date + timedelta(days=-14)
  covid_end_date = positive_test_date + timedelta(days=28)
  if covid_start_date <= med_admin_date <= covid_end_date:
    return False
  return True


# def is_heparin_flush_only(med_admin):
#   list_remove = ['HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN', 'HEPARIN & NACL LOCK FLUSH 10-0.9 UNIT/ML-% IV KIT', 'HEPARIN LOCK FLUSH 1 UNIT/ML IV SOLN', 'HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN'] 
#   med_admin_list = med_admin.tolist()
#   for med in med_admin_list:
#     if med in list_remove:
#       med_admin_list.remove(med)
#   if len(med_admin_list) == 0:
#     return True
#   return False



def is_heparin_flush_only(med_admin):
  list_remove = ['HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN','HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN', 'HEPARIN & NACL LOCK FLUSH 10-0.9 UNIT/ML-% IV KIT', 'HEPARIN LOCK FLUSH 1 UNIT/ML IV SOLN', 'HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN LOCK FLUSH 10 UNIT/ML IV SOLN', 'HEPARIN SODIUM LOCK FLUSH 100 UNIT/ML IV SOLN'] 
  med_admin_list = list(med_admin)
  tf_med_admin_list = [med in list_remove for med in med_admin_list]
  if np.sum(tf_med_admin_list) == 0:
    return False
  elif np.sum(tf_med_admin_list) < len(med_admin_list):
    return False
  else:
    return True

def is_heparin_or_enoxaparin(med_admin):
  for med in med_admin:
    if 'HEPARIN' not in med and 'ENOXAPARIN' not in med and 'LOVENOX' not in med:
      return True
  return False


def filter_for_covid_anticoagulants(df):
  for index, row in df.iterrows():
    med_admin_date = row['med_administration_date']
    positive_test_date = row['covid_test_date']
    actions_taken = row['action_taken_collect_set']
    med_admin = row['name_collect_set']
    if is_dose_not_administered(actions_taken) or is_non_covid_anticoagulant_use(med_admin_date, positive_test_date) or is_heparin_flush_only(med_admin) or is_heparin_or_enoxaparin(med_admin):
      df = df.drop([index])
  return df


def filter_for_no_covid_anticoagulants(df):
  for index, row in df.iterrows():
    med_admin_date = row['med_administration_date']
    actions_taken = row['action_taken_collect_set']
    med_admin = row['name_collect_set']
    if is_dose_not_administered(actions_taken) or is_heparin_flush_only(med_admin) or is_heparin_or_enoxaparin(med_admin):
      df = df.drop([index])
  return df


def determine_term_or_preterm_status(df):
  d = {'term': 0, 'preterm': 0}
  for index, row in df.iterrows():
    gestational_days = row['gestational_days']
    if gestational_days >= 259:
      d['term'] += 1
    else:
      d['preterm'] += 1
  print(d)
  return(d)


def calc_fishers_exact_test_not_simulated(dict_obs, dict_exp):
  import numpy as np
  import rpy2.robjects.numpy2ri
  from rpy2.robjects.packages import importr
  rpy2.robjects.numpy2ri.activate()

  stats = importr('stats')
  list_obs, list_exp = [], []
  total_obs = sum(dict_obs.values())
  total_exp = sum(dict_exp.values())
  for key in dict_obs.keys():
    list_obs.append(dict_obs[key])
    list_exp.append(dict_exp[key])
  list_contingency = np.array([list_obs, list_exp])
  print(list_contingency)
  res = stats.fisher_test(x=list_contingency)
  print(res)
  

def determine_trimester(days):
  if days <= 90:
    return('1st trimester')
  elif days <= 181:
    return('2nd trimester')
  else:
    return('3rd trimester')


def is_preterm(gestational_days):
  if gestational_days >= 259:
      return False
  return True
    
  
def make_anticoagulant_type_dicts(df):
  d = {'therapeutic': 0, 'prophylactic': 0}
  d_therapeuctic = {'term': 0, 'preterm': 0}
  d_prophylactic = {'term': 0, 'preterm': 0}
  for index, row in df.iterrows():
    gestational_days = row['gestational_days']
    delivery_date = row['ob_delivery_delivery_date']
    med_date = row['med_administration_date']
    gestational_days_of_med = gestational_days - (delivery_date-med_date)/np.timedelta64(1, 'D')
    
    
def calc_fishers_exact_test_2x2(df_1, df_2, k):
  import numpy as np
  import rpy2.robjects.numpy2ri
  from rpy2.robjects.packages import importr
  rpy2.robjects.numpy2ri.activate()

  stats = importr('stats')
  list_1, list_2 = [], []
  df_1_n = len(df_1[k])
  df_2_n = len(df_2[k])
  df_1_sum = sum(df_1[k])
  list_1.append(sum(df_1[k]))
  list_1.append(len(df_1[k])-df_1_sum)
  df_2_sum = sum(df_2[k])
  list_2.append(sum(df_2[k]))
  list_2.append(len(df_2[k])-df_2_sum)
  list_contingency = np.array([list_1, list_2])
  print(list_contingency)
  res = stats.fisher_test(x=list_contingency)
  print(res) 
  
  
def run_mann_whitney_u_test(df_1, df_2, k):
  data_1, data_2 = [], []
  for index, row in df_1.iterrows():
    if row[k] is not None:
      data_1.append(float(row[k]))
  for index, row in df_2.iterrows():
    if row[k] is not None:
      data_2.append(float(row[k]))
  return(stats.mannwhitneyu(data_1, data_2))


def make_low_birth_weight_dict(df):
  d = {'low_birth_weight': 0, 'normal_birth_weight': 0}
  for index, row in df.dropna(subset=['delivery_infant_birth_weight_oz']).iterrows():
    weight = row['delivery_infant_birth_weight_oz']
    if weight < 88.1849:
      d['low_birth_weight'] += 1
    else:
      d['normal_birth_weight'] += 1
  print(d)
  return d


def get_fetal_growth_percentiles(df):
  data = []
  for index, row in df.dropna(subset=['delivery_infant_birth_weight_oz']).iterrows():
    weight = row['delivery_infant_birth_weight_oz']
    gestational_age = row['gestational_days']
    gender = row['ob_hx_infant_sex']
    if gender is None:
      gender = 'unknown'
    if weight is None or math.isnan(weight):
      continue
    if gestational_age is None or math.isnan(gestational_age):
      continue
    data.append(calc_birth_weight_percentile(weight, gestational_age, gender))
  return(data)


def count_small_for_gestational_age(l):
  d = {'SGA': 0, 'normal': 0}
  for item in l:
    if float(item) <= 10:
      d['SGA'] += 1
    else:
      d['normal'] += 1
  print(d)
  return d


def make_extremely_low_birth_weight_dict(df):
  d = {'extremely_low_birth_weight': 0, 'normal_birth_weight': 0}
  for index, row in df.dropna(subset=['delivery_infant_birth_weight_oz']).iterrows():
    weight = row['delivery_infant_birth_weight_oz']
    if weight < 52.91094:
      d['extremely_low_birth_weight'] += 1
    else:
      d['normal_birth_weight'] += 1
  print(d)
  return d
   

def make_stillbirth_dict(df):
  d = {'Living': 0, 'Fetal Demise': 0}
  for index, row in df.iterrows():
    if row['ob_history_last_known_living_status'] == 'Fetal Demise':
      d['Fetal Demise'] += 1
    else:
      d['Living'] += 1  # reported as 'Living' or missing field
  print(d)
  return d


  #load functions 
def determine_trimester(days):
  if days <= 90:
    return('1st trimester')
  elif days <= 181:
    return('2nd trimester')
  else:
    return('3rd trimester')
determine_trimester_udf = F.udf(lambda days: determine_trimester(days), StringType()) 

  
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

  #dosage category 
enox_p_dosage = ['ENOXAPARIN SODIUM 40 MG/0.4ML INJECTION SOSY', 'ENOXAPARIN SODIUM 30 MG/0.3ML INJECTION SOSY', 'ENOXAPARIN SODIUM 40 MG/0.4ML SC SOLN', 'LOVENOX 40 MG/0.4ML SC SOLN','ENOXAPARIN SODIUM 30 MG/0.3ML SC SOLN', 'LOVENOX 30 MG/0.3ML SC SOLN', 'ENOXAPARIN SODIUM 30 MG/0.3ML IJ SOSY', 'LOVENOX 40 MG/0.4ML IJ SOSY', 'ENOXAPARIN SODIUM 40 MG/0.4ML IJ SOSY', 'LOVENOX 30 MG/0.3ML IJ SOSY']
enox_t_dosage = ['ENOXAPARIN SODIUM 120 MG/0.8ML SC SOLN', 'ENOXAPARIN SODIUM 80 MG/0.8ML INJECTION SOSY', 'ENOXAPARIN SODIUM 80 MG/0.8ML SC SOLN', 'LOVENOX 80 MG/0.8ML SC SOLN', 'LOVENOX 120 MG/0.8ML SC SOLN', 'ENOXAPARIN SODIUM 80 MG/0.8ML IJ SOSY', 'ENOXAPARIN SODIUM 100 MG/ML IJ SOSY', 'ENOXAPARIN SODIUM 300 MG/3ML IJ SOLN', 'ENOXAPARIN SODIUM 100 MG/ML IJ SOSY', 'ENOXAPARIN SODIUM 150 MG/ML IJ SOSY', 'ENOXAPARIN SODIUM 120 MG/0.8ML IJ SOSY', 'ENOXAPARIN SODIUM 120 MG/0.8ML INJECTION SOSY', 'ENOXAPARIN SODIUM 100 MG/ML INJECTION SOSY']
enox_i_dosage = ['LOVENOX 60 MG/0.6ML SC SOLN', 'ENOXAPARIN SODIUM 60 MG/0.6ML SC SOLN', 'ENOXAPARIN SODIUM 60 MG/0.6ML IJ SOSY', 'LOVENOX 60 MG/0.6ML IJ SOSY','ENOXAPARIN SODIUM 60 MG/0.6ML INJECTION SOSY']
heparin_p_dosage = ['HEPARIN SODIUM (PORCINE) 1000 UNIT/ML IJ SOLN']
heparin_5000_dosage = ['HEPARIN SODIUM (PORCINE) 5000 UNIT/ML INJECTION SOLN', 'HEPARIN SODIUM (PORCINE) 5000 UNIT/ML IJ SOLN', 'HEPARIN SODIUM (PORCINE) PF 5000 UNIT/ML IJ SOLN', 'HEPARIN SODIUM (PORCINE) PF 5000 UNIT/0.5ML IJ SOLN', 'HEPARIN SODIUM (PORCINE) 5000 UNIT/0.5ML IJ SOSY']
heparin_10000_dosage = ['HEPARIN SODIUM (PORCINE) 10000 UNIT/ML IJ SOLN']
heparin_t_dosage = ['HEPARIN SODIUM (PORCINE) 20000 UNIT/ML IJ SOLN', 'HEPARIN (PORCINE) IN NACL 3000-0.9 UT/500ML-% IV SOLN', 'HEPARIN (PORCINE) IN NACL 25000-0.45 UT/250ML-% IV SOLN']





heparin_dosage = heparin_p_dosage + heparin_5000_dosage + heparin_10000_dosage + heparin_t_dosage
enox_dosage = enox_p_dosage + enox_t_dosage + enox_i_dosage 
any_dosage = heparin_dosage + enox_dosage 
#frequency category 
one = ['NIGHTLY', 'EVERY 24 HOURS INTERVAL', 'ONCE', 'DAILY', 'DAILY EVENING', 'EVERY 24 HOURS', 'DAILY AT NOON','DAILY WITH BREAKFAST']
two = ['2 TIMES DAILY', 'EVERY 12 HOURS', 'EVERY 12 HOURS INTERVAL', '2 TIMES DAILY WITH BREAKFAST & DINNER']
three = ['EVERY 8 HOURS', '3 TIMES DAILY']
prn = ['PRN', 'ONCE PRN', 'DAILY PRN', 'CONDITIONAL PRN', 'DIALYSIS-PRN']
therapeutic_freq = ['TITRATED','CONTINUOUS']
dialysis = ['DIALYSIS']
oncall = ['ON CALL']
prophylatic_freq = oncall + dialysis + prn
two_and_three = two + three
one_and_two = one + two

def p_or_t(name, converted_freq):
  if (name in any_dosage) and (converted_freq in therapeutic_freq):
    return 2 #threapeutic 
  elif (name in any_dosage) and (converted_freq in prophylatic_freq):
    return 1 #prophylatic
  elif (name in enox_t_dosage) or (name in heparin_t_dosage):
    return 2 #threapeutic
  elif (name in enox_p_dosage)  or (name in heparin_p_dosage):
    return 1 #prophylatic 
  elif (name in enox_i_dosage) and (converted_freq in one):
    return 1 #prophylactic
  elif (name in enox_i_dosage) and (converted_freq in two_and_three):
    return 2 #threapeutic
  elif (name in heparin_5000_dosage) and (converted_freq in three):
    return 2 #threapeutic
  elif (name in heparin_5000_dosage) and (converted_freq in one_and_two):
    return 1 #prophylactic
  elif (name in heparin_10000_dosage) and (converted_freq in one):
    return 1 #prophylactic
  elif (name in heparin_10000_dosage) and (converted_freq in two_and_three):
    return 2 #threapeutic 
  elif (name in heparin_5000_dosage):
    return 1 #prophylactic
  elif (name in heparin_10000_dosage):
    return 1 #prophylactic
  elif (name in enox_i_dosage):
    return 1 #prophylactic
  else:
    return 0
p_or_t_udf = F.udf(lambda name, freq: p_or_t(name, freq), IntegerType()) 

def get_joining_columns(df1, df2):
  df1_cols = df1.columns
  df2_cols = df2.columns
  join_cols = list(set(df1_cols) & set(df2_cols))
  return join_cols 


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


  def aggregate_data(df, partition_columns, aggregation_columns, order_by=None):
  """Aggregate data over specified partition columns
  
  Parameters:
  df (PySpark): Dataframe to aggregate
  partition_columns (str or list): Field(s) in df on which to partition. If partitioning on only one
                                   column, the column name can be provided as a str rather than a
                                   list
  aggregation_columns (dict): Must be a dict where the keys are fields in df to aggregate and values
                              are either a str or list, specifying the aggregation functions to use.
                              If using only one aggregation function for a given field, the name of
                              the aggregation function can be provided as a str rather than a list.
                              A separate column will be added for each aggregation function.
  order_by (str or list): Field(s) in df to use for ordering records in each partition. If None, do
                          not order. If ordering on only one column, the column name can be provided
                          as a str rather than a list
  
  Result:
  PySpark df: Dataframe containing the aggregated results
  
  """
  # First argument must be a PySpark dataframe
  assert(isinstance(df, DataFrame))
  
  # Input dataframe must contain specified partition columns
  partition_columns = partition_columns if isinstance(partition_columns, list) else [partition_columns]
  assert(all([s in df.columns for s in partition_columns]))
    
  # Perform validity checks on aggregation_columns
  assert(isinstance(aggregation_columns, dict))
  assert(all([s in df.columns for s in list(aggregation_columns.keys())]))
  valid_agg_functions = ['avg', 'collect_list', 'collect_set', 'concat_ws', 'count', 'first', 'last', 'max', 'mean', 'median', 'min', 'stddev', 'sum']
  for k in list(aggregation_columns.keys()):
    v = aggregation_columns[k]
    aggregation_columns[k] = v if isinstance(v, list) else [v]
    assert(all([s in valid_agg_functions for s in aggregation_columns[k]]))
  
  # order_by (if not None) must contain valid column names
  if(not(order_by is None)):
    order_by = order_by if isinstance(order_by, list) else [order_by]
    assert(all([s in df.columns for s in order_by]))
  
  # Define partition window
  w = Window.partitionBy(partition_columns)
  if(not(order_by is None)):
    w = w.orderBy(order_by).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  
  # Add aggregate columns
  results_df = df; new_columns = []
  for col, agg_func in aggregation_columns.items():
    for s in agg_func:
      # Check for boolean field (must be converted to 0/1 for some aggregation functions)
      bool_type = (dict(results_df.dtypes)[col] == 'boolean')
      
      # Apply aggregation function
      col_name = '_'.join([col, s])
      new_columns = new_columns + [col_name]
      print("Adding new column '{}'...".format(col_name))
      if(s in ['avg', 'mean']):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate avg/mean...".format(col))
          results_df = results_df.withColumn(col_name, F.avg(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.avg(col).over(w))
      elif(s == 'collect_list'):
        results_df = results_df.withColumn(col_name, F.collect_list(col).over(w))
      elif(s == 'collect_set'):
        results_df = results_df.withColumn(col_name, F.collect_set(col).over(w))
      elif(s == 'concat_ws'):
        results_df = results_df.withColumn(col_name, F.concat_ws(';', F.collect_list(col).over(w)))
      elif(s == 'count'):
        results_df = results_df.withColumn(col_name, F.count(col).over(w))
      elif(s == 'first'):
        results_df = results_df.withColumn(col_name, F.first(col).over(w))
      elif(s == 'last'):
        results_df = results_df.withColumn(col_name, F.last(col).over(w))
      elif(s == 'max'):
        results_df = results_df.withColumn(col_name, F.max(col).over(w))
      elif(s == 'min'):
        results_df = results_df.withColumn(col_name, F.min(col).over(w))
      elif(s == 'median'):
        results_df = results_df.withColumn(col_name, median_udf(F.collect_list(col).over(w)))
      elif(s == 'stddev'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate stddev...".format(col))
          results_df = results_df.withColumn(col_name,
                                             F.stddev(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.stddev(col).over(w))
      elif(s == 'sum'):
        if(bool_type):
          print("Casting boolean column '{}' to integer to calculate sum...".format(col))
          results_df = results_df.withColumn(col_name, F.sum(F.col(col).cast(IntegerType())).over(w))
        else:
          results_df = results_df.withColumn(col_name, F.sum(col).over(w))
  
  # Process the final dataframe for return
  final_columns = partition_columns + new_columns
  results_df = results_df.select(final_columns).dropDuplicates()
    
  return results_df

