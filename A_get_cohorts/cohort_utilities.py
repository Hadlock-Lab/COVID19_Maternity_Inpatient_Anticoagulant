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


def is_heparin_flush_only(med_admin):
  # hard-code heparin after the clinical review of all medication administration record
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


def filter_for_covid_anticoagulants(df): # filter non-flush, valid, heparin or enoxaprin only record. this code was generated because some of non-covid pregnant patients had non heparin/enoxaparin anticoagulant administration
  for index, row in df.iterrows():
    med_admin_date = row['med_administration_date']
    positive_test_date = row['covid_test_date']
    actions_taken = row['action_taken_collect_set']
    med_admin = row['name_collect_set']
    if is_dose_not_administered(actions_taken) or is_non_covid_anticoagulant_use(med_admin_date, positive_test_date) or is_heparin_flush_only(med_admin) or is_heparin_or_enoxaparin(med_admin):
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

  #dosage category. individual administration record was clinically reviewed and hard-coded 
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

  def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def add_previous_pregnancies_to_df(cohort, note_name):
  GPAL = add_GPP_columns(note_name)
  return_df = cohort.join(GPAL.drop('GPAL_max'), ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  return_df = return_df.fillna(value = 0,subset =['Gravidity', 'Parity', 'Preterm_history'])
  return return_df 


# get total number of problem diagnoses codes before covid infection 
def get_precovid_risk_factors (cohort_df, time_filter_string = None, rename_count = 'count_precovid_diagnoses', rename_list = 'list_precovid_diagnoses'):
  problem_list_df = get_problem_list(cohort_df = cohort_df,
                                     include_cohort_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'covid_test_date','conception_date']).filter(F.col('noted_date')<F.col('covid_test_date')).filter(((F.col('resolved_date')>F.col('covid_test_date'))|(F.col('resolved_date').isNull()))).filter((F.col('problem_status')!='Deleted'))
  if time_filter_string:
    problem_list_df = problem_list_df.filter(time_filter_string)
  get_count = F.udf(lambda s: len(s), IntegerType())
  problemlist_ag = aggregate_data(problem_list_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'dx_id':'collect_set', 'name':'collect_set'}).withColumn(rename_count, get_count(F.col('dx_id_collect_set'))).withColumnRenamed('name_collect_set', rename_list).drop('dx_id_collect_set') 
  
  return_df = cohort_df.join(problemlist_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = [rename_count])
  return return_df 

def get_covid_14days_med(cohort_df):
  # get non-anticoagulant medication order 
  cohort_df = cohort_df.withColumn('treatment_onset_index', F.when(F.col('covid_admissiondatetime_min')<F.date_sub(F.col('covid_test_date'),5), F.col('covid_test_date')).\
                                                                  when(F.col('covid_admissiondatetime_min')>=F.date_sub(F.col('covid_test_date'), 5), F.col('covid_admissiondatetime_min')).\
                                                                  otherwise(None))
  meds = get_medication_orders(cohort_df.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'treatment_onset_index'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant')).filter(F.col('ordering_datetime')>=F.col('treatment_onset_index')).filter(F.col('ordering_datetime')<=F.date_add(F.col('treatment_onset_index'),14))
  
  
  meds_time_index_df = meds.withColumn('time_index_days', F.datediff(F.col('ordering_datetime'),F.col('treatment_onset_index')))
  meds_time_index_df = meds_time_index_df.filter(F.col('time_index_days').isNotNull()).select('instance', 'pat_id', 'episode_id', 'child_episode_id','time_index_days','short_name').distinct()
  meds_time_index_df_grouped = meds_time_index_df.groupBy('instance', 'pat_id', 'episode_id', 'child_episode_id').pivot('time_index_days').agg(F.count('short_name'))
  meds_time_index_df_grouped = meds_time_index_df_grouped.fillna(0, subset = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13'])
  for i in range(0,14):
    meds_time_index_df_grouped = meds_time_index_df_grouped.withColumnRenamed(str(i), 'covidmeds_day'+str(i))

  
  return_df = cohort_df.join(meds_time_index_df_grouped, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  return return_df





def get_covid_2days_med(cohort_df):
  # get non-anticoagulant medication order 
  cohort_df = cohort_df.withColumn('treatment_onset_index', F.when(F.col('covid_admissiondatetime_min')<F.date_sub(F.col('covid_test_date'),5), F.col('covid_test_date')).\
                                                                  when(F.col('covid_admissiondatetime_min')>=F.date_sub(F.col('covid_test_date'), 5), F.col('covid_admissiondatetime_min')).\
                                                                  otherwise(None))
  meds = get_medication_orders(cohort_df.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'treatment_onset_index'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant')).filter(F.col('ordering_datetime')>=F.col('treatment_onset_index')).filter(F.col('ordering_datetime')<=F.date_add(F.col('treatment_onset_index'),2))
  
  
  meds_time_index_df = meds.withColumn('time_index_days', F.datediff(F.col('ordering_datetime'),F.col('treatment_onset_index')))
  meds_time_index_df = meds_time_index_df.filter(F.col('time_index_days').isNotNull()).select('instance', 'pat_id', 'episode_id', 'child_episode_id','time_index_days','short_name').distinct()
  meds_time_index_df_grouped = meds_time_index_df.groupBy('instance', 'pat_id', 'episode_id', 'child_episode_id').pivot('time_index_days').agg(F.count('short_name'))
  meds_time_index_df_grouped = meds_time_index_df_grouped.fillna(0, subset = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13'])
  for i in range(0,14):
    meds_time_index_df_grouped = meds_time_index_df_grouped.withColumnRenamed(str(i), 'covidmeds_day'+str(i))

  
  return_df = cohort_df.join(meds_time_index_df_grouped, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  return return_df



def get_postcovid_diagnosis_count(cohort_df, encounter_filter_string = None, problem_filter_string = None):
  # get post covid diagnosis count 
  df_encounter = get_encounters(cohort_df.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'lmp', 'covid_test_date', 'ob_delivery_delivery_date'), filter_string = "contact_date > covid_test_date AND contact_date < ob_delivery_delivery_date", join_table = 'encounterdiagnosis')
  df_problem_list = get_problem_list(cohort_df.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'lmp', 'covid_test_date', 'ob_delivery_delivery_date'), filter_string = "date_of_entry > covid_test_date AND date_of_entry < ob_delivery_delivery_date")
  
  if encounter_filter_string:
    df_encounter = df_encounter.filter(encounter_filter_string)
  if problem_filter_string:
    df_problem_list = df_problem_list.filter(problem_filter_string)
  
  df_encounter_concise = df_encounter.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'dx_id', 'name')
  df_problem_list_concise = df_problem_list.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'dx_id', 'name')
  
  df_diagnoses = df_encounter_concise.union(df_problem_list_concise) 
  
  get_count = F.udf(lambda s: len(s), IntegerType())
  return_df = aggregate_data(df_diagnoses, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'dx_id':'collect_set', 'name':'collect_set'}).withColumn('count_postcovid_diagnoses', get_count(F.col('dx_id_collect_set'))).withColumnRenamed('name_collect_set', 'list_postcovid_diagnoses').drop('dx_id_collect_set') 
  
  return return_df



def get_covid_48hours_med(cohort_df):
  # get non-anticoagulant medication order from COVID-19 treatment onset to two days after 
  list_administered = ['Anesthesia Volume Adjustment', 'Bolus', 'Bolus from Bag', 'Calc Rate', 'Continued by Anesthesia', 'Continued Bag', 'Continue bag from transfer', 'Dispense to Home', 'Given', 'Given by Other', 'Given During Downtime', 'Milk Verified', 'New Bag', 'New Syringe/Cartridge', 'Patch Applied', 'Push', 'Rate Verify', 'Rate Change', 'Rate/Dose Change-Dual Sign', 'Rate/Dose Verify-Dual Sign', 'Restarted', 'Started During Downtime', 'Unheld by Provider']
  meds = get_medication_orders(cohort_df.drop('name', 'start_date', 'end_date'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant'))
  total_meds = meds.filter(F.col('ordering_datetime')>=F.date_sub(F.col('treatment_onset_index'),2))

  total_meds = total_meds.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'short_name').distinct()
  total_meds_ag = aggregate_data(total_meds, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'short_name':'count'}).withColumnRenamed('short_name_count', 'covid_total_med_48hours_count')
  
  return_df = cohort_df.join(total_meds_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['covid_total_med_48hours_count'])
  return return_df 

def get_postcovid_48hours_med (cohort_df):
  # get non-anticoagulant medication order 
  list_administered = ['Anesthesia Volume Adjustment', 'Bolus', 'Bolus from Bag', 'Calc Rate', 'Continued by Anesthesia', 'Continued Bag', 'Continue bag from transfer', 'Dispense to Home', 'Given', 'Given by Other', 'Given During Downtime', 'Milk Verified', 'New Bag', 'New Syringe/Cartridge', 'Patch Applied', 'Push', 'Rate Verify', 'Rate Change', 'Rate/Dose Change-Dual Sign', 'Rate/Dose Verify-Dual Sign', 'Restarted', 'Started During Downtime', 'Unheld by Provider']
  meds = get_medication_orders(cohort_df.drop('name', 'start_date', 'end_date'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant'))
  inpatient_meds = meds.filter(F.col('ordering_datetime')>=F.date_sub(F.col('treatment_onset_index'),2)).filter(F.col('order_mode')=='Inpatient')
  outpatient_meds = meds.filter(F.col('ordering_datetime')>=F.date_sub(F.col('treatment_onset_index'),2)).filter(F.col('order_mode')=='Outpatient')
  total_meds = meds.filter(F.col('ordering_datetime')>=F.date_sub(F.col('treatment_onset_index'),2))
  
  inpatient_meds = inpatient_meds.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'short_name').distinct()
  outpatient_meds = outpatient_meds.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'short_name').distinct()
  total_meds = total_meds.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'short_name').distinct()
  ip_meds_ag = aggregate_data(inpatient_meds, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'short_name':'count'}).withColumnRenamed('short_name_count', 'covid_inpatient_med_post48hours_count')
  op_meds_ag = aggregate_data(outpatient_meds, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'short_name':'count'}).withColumnRenamed('short_name_count', 'covid_outpatient_med_post48hours_count')
  total_meds_ag = aggregate_data(total_meds, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'short_name':'count'}).withColumnRenamed('short_name_count', 'covid_total_med_post48hours_count')
  
  return_df = cohort_df.join(ip_meds_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['covid_inpatient_med_post48hours_count'])
  return_df = return_df.join(op_meds_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['covid_outpatient_med_post48hours_count'])
  return_df = return_df.join(total_meds_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['covid_total_med_post48hours_count'])
  return return_df 



def get_vasopressor_count(df, anti = False):
  who_admin = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_adss_study_who_med_admin_records")
  who_admin = who_admin.select('pat_id', 'instance', 'time_index_day', 'vasopressor_sum')
  if anti:
    admin_joined = df.join(who_admin, ['pat_id', 'instance'], 'inner').filter(F.col('time_index_day')>=F.col('start_date_min')).filter(F.col('time_index_day')<F.col('ob_delivery_delivery_date'))
  else:
    admin_joined = df.join(who_admin, ['pat_id', 'instance'], 'inner').filter(F.col('time_index_day')>=F.col('treatment_onset_index')).filter(F.col('time_index_day')<F.col('ob_delivery_delivery_date'))
  admin_ag = aggregate_data(admin_joined, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'vasopressor_sum':'sum'})
  return_df = df.join(admin_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset=['vasopressor_sum_sum'])
  return_df = return_df.withColumnRenamed('vasopressor_sum_sum', 'vasopressor_count')
  return return_df 


def fetch_coagulation_related_conditions(df, anti = False, encounter_filter_string = False, problem_filter_string = False):
  coagulatory_cc_list = list(conditions_cc_registry_coagulatory.keys())
  if anti:
    encounters_df = get_encounters(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id', 'start_date_min').distinct(), filter_string = encounter_filter_string, join_table = 'encounterdiagnosis', add_cc_columns = coagulatory_cc_list)
    problem_list_df = get_problem_list(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id', 'start_date_min').distinct(), filter_string = problem_filter_string, add_cc_columns = coagulatory_cc_list)
  else:
    encounters_df = get_encounters(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id', 'treatment_onset_index').distinct(), filter_string = encounter_filter_string, join_table = 'encounterdiagnosis', add_cc_columns = coagulatory_cc_list)
    problem_list_df = get_problem_list(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id', 'treatment_onset_index').distinct(), filter_string = problem_filter_string, add_cc_columns = coagulatory_cc_list)
    
    
  include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id']+coagulatory_cc_list
  include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id']+coagulatory_cc_list
  
  encounters_df = get_clinical_concept_records(encounters_df.select(*include_encounter_columns), coagulatory_cc_list)
  problem_list_df = get_clinical_concept_records(problem_list_df.select(*include_problem_columns), coagulatory_cc_list)
  encounters_df = encounters_df.withColumnRenamed('contact_date', 'diagnosis_date')
  problem_list_df = problem_list_df.withColumnRenamed('date_of_entry', 'diagnosis_date')
  conditions_df = encounters_df.union(problem_list_df)
  return conditions_df 

def add_coagulation_related_features(df, conditions_df):
  agg_cols = {}
  for cc in coagulatory_cc_list:
    agg_cols[cc] = 'max'
  conditions_df_ag = aggregate_data(conditions_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = agg_cols, order_by=None)
  return_df = df.join(conditions_df_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  for i in coagulatory_cc_list:
    return_df = return_df.withColumn(i + '_max', F.when(F.col(i+'_max')==True, F.lit(1)).otherwise(0))
  return return_df 

def check_inpatient_status(patientclass):
  patient_status_list = ['Inpatient', 'Inpatient Rehab Facility']
  if patientclass is None:
    return 0
  elif patientclass in patient_status_list:
    return 1
  else:
    return 0
check_inpatient_status_udf = F.udf(lambda patientclass: check_inpatient_status(patientclass), IntegerType())

def add_hospitalization(df):
  department = spark.sql("SELECT * FROM rdp_phi.department").select('DEPARTMENT_ID', 'ABBREVIATEDNAME', 'DEPARTMENTNAME', 'INSTANCE', 'SPECIALTY', 'ADTTYPE')
  encounter = spark.sql("SELECT * FROM rdp_phi.encounter").select('pat_id', 'instance', 'contact_date', 'patientclass','encountertype','department_id', 'pat_enc_csn_id').join(department, ['instance', 'DEPARTMENT_ID'], 'left')
  adtevent = spark.sql("SELECT * FROM rdp_phi.adtevent").select('instance', 'pat_id', 'pat_enc_csn_id','basepatientclass')
  encounter_adt = encounter.join(adtevent, ['instance', 'pat_id', 'pat_enc_csn_id'], 'left')
  df_concise = df.select('pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date', 'episode_id', 'child_episode_id')
  encounter_joined = df_concise.join(encounter_adt, ['pat_id', 'instance'], 'left').filter(F.col('contact_date')>F.col('lmp')).filter(F.col('contact_date')<F.date_sub(F.col('ob_delivery_delivery_date'), 3)).filter(~F.col('DEPARTMENTNAME').contains('LABOR AND DELIVERY'))
  encounter_joined = encounter_joined.withColumn('inpatient', check_inpatient_status_udf(F.col('basepatientclass')))
  encounter_ag = aggregate_data(encounter_joined, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], {'inpatient':'max'})
  return_df = df.join(encounter_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset=['inpatient_max']).withColumnRenamed('inpatient_max', 'inpatient_status')
  return return_df 

def determine_covid_days_from_conception(row):  # identify the number of days into pregnancy the first covid positive test was observed
  count = 0
  delivery_date = row['ob_delivery_delivery_date']
  gestational_days = row['gestational_days']
  check_date = datetime.datetime(2020, 2, 14, 0, 0, 0)
  for item in row['ordering_datetime_collect_list']:
    if row['result_short_collect_list'][count] == 'positive' and (check_date - item).total_seconds() > 0:
      days_from_conception = row['gestational_days'] - (delivery_date - item)/np.timedelta64(1, 'D')
      if 0 <= days_from_conception <= row['gestational_days']:
        return(days_from_conception, item)
    count += 1
  return(None, None)


def determine_covid_maternity_cohorts(df):
  import pandas as pd
  import numpy as np
  import datetime
  df = df.toPandas()
  cols = df.columns
  df_maternity_all = pd.DataFrame(columns=cols)
  df_temp = pd.DataFrame(columns=['pat_id', 'covid_test_date', 'covid_test_number_of_days_from_conception'])
  list_gestational_days_at_covid_infection = []
  list_positive_covid_test_order_date = []
  for index, row in df.iterrows():
    if row['number_of_fetuses'] == 1 and row['gestational_days'] >= 140:
      days_from_conception, order_date = determine_covid_days_from_conception(row)
      if days_from_conception is not None:
        df_maternity_all = df_maternity_all.append(row)
        df_temp = df_temp.append({'pat_id': row['pat_id'], 'covid_test_number_of_days_from_conception': days_from_conception, 'covid_test_date': order_date}, ignore_index=True)
  print('Number of women with covid infections during a completed pregnancy: ' + str(len(df_maternity_all.pat_id.unique())))
  return(df_maternity_all, df_temp.drop_duplicates())




def order_med_counts(med_df):
  """From the provided dataframe, return counts of inpatient and outpatient medication order  
  Parameters:
  cohort_df (PySpark df): A dataframe containing 'pat_id' 'instance' 'lmp' 'order_mode'
  
  Returns:
  PySpark df: count of inpatient and outpatient medication order 
  """
  ordermode_count = med_df.groupBy(['pat_id']).pivot("order_mode").count().withColumnRenamed('Inpatient', 'inpatient_meds_count').withColumnRenamed('Outpatient', 'outpatient_meds_count')
  ordermode_count = ordermode_count.na.fill(value=0)
  return ordermode_count


def unique_covid_ingredient_counts(med_df):
  """From the provided dataframe, return counts of inpatient and outpatient medication order  
  Parameters:
  cohort_df (PySpark df): A dataframe containing 'pat_id' 'instance' 'lmp' 'medication_description' 'term_type'
  
  Returns:
  PySpark df: count of unique ingredient 
  """

  ingredient_count = meds_ingredient.groupBy(['pat_id']).agg(f.countDistinct("short_name")).withColumnRenamed('count(short_name)', 'unique_covid_inpatient_medication_count')
  return ingredient_count



def make_class_encoder_dict(l):
  d = {}
  c = 0
  for i in l:
    d[i] = c
    c += 1
  return(d)





def count_encounters(df):
  d = {}
  for index, row in df.iterrows():
    k = row['pat_id']
    if k not in d:
      d[k] = 1
    else:
      d[k] += 1
  return(d)


def add_max_to_dict(d, k, v):
  if k not in d:
    d[k] = v
  elif v > d[k]:
    d[k] = v
  return(d)


def check_high_flow_o2_plus_vasopressor(oxygen_device, vasopressor_sum):
  if math.isnan(vasopressor_sum):
    return oxygen_device
  vasopressor_sum = float(vasopressor_sum)
  if oxygen_device == 2 and vasopressor_sum > 0:
    return 3
  return oxygen_device


def format_oxygen_device(df):
  '''
  0 = None
  1 = Low-Flow Oxygen Device
  2 = High-Flow Oxygen Device
  3 = High-Flow Oxygen Device + Vasopressors
  4 = Venhilator
  '''
  dict_conversion = {
 'T-piece': 4,
 'mechanical ventilation' : 4, 
 'heated': 2,
 'high-flow nasal cannula': 2,
 'manual bag ventilation': 2,
 'nonrebreather mask': 2,
 'partial rebreather mask': 2,
 'face tent': 2,
 'oxygen hood' : 2, 
 'CPAP' : 2,
 'non-invasive ventilation (i.e. bi-level)': 2, 
 'Venturi mask': 1,
 'aerosol mask' : 1, 
 'nasal cannula': 1,
 'nasal cannula with humidification': 1,
 'nasal cannula with reservoir (i.e. oximizer)': 1,
 'simple face mask': 1,
 'open oxygen mask': 1,
 'blow by': 1,
 'other (see comments)': 1,
 'room air': 0,
 'home unit' : 0,   
 'Room air (None)': 0}
  dict_oxygen_device_type = {}
  for index, v in df.iterrows():
    k = v.pat_id
    item = v.oxygen_device_concat_ws
    dict_oxygen_device_type = add_max_to_dict(dict_oxygen_device_type, k, 0)
    if item is None:
      continue
    for i in item.split(';'):
      if i is None:
        dict_oxygen_device_type = add_max_to_dict(dict_oxygen_device_type, k, 0)
        continue
      i = dict_conversion[i]
      dict_oxygen_device_type = add_max_to_dict(dict_oxygen_device_type, k, i)
    oxygen_device = dict_oxygen_device_type[k]
    vasopressor_sum = v.vasopressor_count
    dict_oxygen_device_type[k] = check_high_flow_o2_plus_vasopressor(oxygen_device, vasopressor_sum)
  return dict_oxygen_device_type
  
  
def format_patient_class(df):
  '''
  0 = None
  1 = Outpatient
  2 = Urgent Care / Emergency
  3 = Inpatient
  4 = ICU
  '''
  dict_conversion = {'Emergency': 2,
 'Extended Hospital Outpatient': 1,
 'Free Standing Outpatient': 1,
 'Hospital Ambulatory Surgery': 1,
 'Infusion Series': 1,
 'Inpatient': 3,
 'Inpatient Rehab Facility': 3,
 'MU Urgent Care': 2,
 'NM Series': 1,
 'Observation': 1,
 'Other Series': 1,
 'Outpatient': 1,
 'Outreach': 1,
 'Radiation/Oncology Series': 1,
 'Rural Health': 1,
 'Specimen': 1,
 'Therapies Series': 1, 
 'Anticoagulation Series' : 1, 
 'Provider Based Billing Outpatient':1}
  dict_patient_class = {}
  for index, v in df.iterrows():
    k = v.pat_id
    l = v.patient_class_collect_list
    dict_patient_class = add_max_to_dict(dict_patient_class, k, 0)
    if l is None:
      continue
    for item in l:
      if item is None:
        continue
      item = dict_conversion[item]
      add_max_to_dict(dict_patient_class, k, item)
  return(dict_patient_class)


def add_patient_deaths(df):
  # add patient deaths that occurred within 3 months after a covid infection
  df.createOrReplaceTempView("temp")
  df_patient = spark.sql("SELECT * FROM rdp_phi.patient")
  df_patient.createOrReplaceTempView("patient")
  df_plus_death = spark.sql(
    """
    FROM patient AS p
    RIGHT JOIN temp AS t
    ON p.pat_id = t.pat_id
      AND p.deathdate - interval '3' month < t.covid_test_date
    SELECT t.*, p.deathdate
    """)
  return df_plus_death


def add_column_to_df(df, dict_data, column_name):
  df[column_name] = df['pat_id'].map(dict_data)
  return df


def format_encounters(df_encounters, df):
  dict_n_encounters = count_encounters(df_encounters)
  df_encounters['who_score'] = df_encounters['who_score'].fillna(3)
  dict_oxygen_device = format_oxygen_device(df_encounters)
  dict_patient_class = format_patient_class(df_encounters)
  df_encounters = df_encounters.sort_values(['who_score',  'encounter_duration'], ascending=False).drop_duplicates('pat_id').sort_index()
  df_encounters = df_encounters.drop(columns=['oxygen_device_concat_ws', 'patient_class_collect_list'])
  df_encounters = add_column_to_df(df_encounters, dict_oxygen_device, 'max_oxygen_device')
  df_encounters = add_column_to_df(df_encounters, dict_patient_class, 'max_patient_class')
  df_encounters = add_column_to_df(df_encounters, dict_n_encounters, 'n_covid_encounters')
  df_final = df.merge(df_encounters, left_on='pat_id', right_on='pat_id', how='left')
  df_final['n_covid_encounters'] = df_final['n_covid_encounters'].fillna(0)
  df_final['max_oxygen_device'] = df_final['max_oxygen_device'].fillna(0)
  df_final['max_patient_class'] = df_final['max_patient_class'].fillna(0)
  df_final['who_score'] = df_final['who_score'].fillna(2)
  return df_final


def sum_list(l):
  l = list(l)
  s = 0
  for i in l:
    s += i
  return s




 
  
def make_covid_severity_dict(df):
  d = {'Mild': 0, 'Moderate': 0, 'Severe': 0, 'Dead': 0}
  for index, row in df.iterrows():
    if isinstance(row['deathdate'], datetime.datetime) and not is_nat(row['deathdate']):
      d['Dead'] += 1
    elif row['max_oxygen_device'] >= 2:
      d['Severe'] += 1
    elif row['max_patient_class'] >= 3 or row['max_oxygen_device'] == 1:
      d['Moderate'] += 1
    else:
      d['Mild'] += 1
  print(d)
  return d


def make_death_dict(df):
  d = {'None': 0, 'Dead': 0}
  for index, row in df.iterrows():
    if isinstance(row['deathdate'], datetime.datetime) and not is_nat(row['deathdate']):
      d['Dead'] += 1
    else:
      d['None'] += 1
  print(d)
  return d

def get_pph(df):
  cc_list = ['postpartum_hemorrhage']
  encounters_df = get_encounters(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), join_table = 'encounterdiagnosis', add_cc_columns = cc_list).filter(F.col('contact_date')>=F.col('ob_delivery_delivery_date')).filter(F.col('contact_date')<=F.date_add(F.col('ob_delivery_delivery_date'), 365))
  problem_list_df = get_problem_list(df.select('pat_id', 'instance', 'episode_id', 'lmp', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), add_cc_columns = cc_list).filter(F.col('date_of_entry')>=F.col('ob_delivery_delivery_date')).filter(F.col('date_of_entry')<=F.date_add(F.col('ob_delivery_delivery_date'), 365))
  include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'ob_delivery_delivery_date', 'contact_date']+cc_list
  include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'ob_delivery_delivery_date', 'date_of_entry']+cc_list
  encounters_df = get_clinical_concept_records(encounters_df.select(*include_encounter_columns), cc_list)
  problem_list_df = get_clinical_concept_records(problem_list_df.select(*include_problem_columns), cc_list)
  encounters_df = encounters_df.withColumnRenamed('contact_date', 'diagnosis_date')
  problem_list_df = problem_list_df.withColumnRenamed('date_of_entry', 'diagnosis_date')
  conditions_df = encounters_df.union(problem_list_df)
  return conditions_df

def add_pph(df, conditions_df):
  agg_cols = {}
  cc_list = ['postpartum_hemorrhage']
  for cc in cc_list:
    agg_cols[cc] = 'max'
  conditions_df_ag = aggregate_data(conditions_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = agg_cols, order_by=None)
  return_df = df.join(conditions_df_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  for i in cc_list:
    return_df = return_df.withColumn(i + '_max', F.when(F.col(i+'_max')==True, F.lit(1)).otherwise(0))
    return_df = return_df.withColumnRenamed(i + '_max', i)
  return return_df

  def get_prior_coagulopathy(df):
  cc_list = ['coagulopathy']
  encounters_df = get_encounters(df.select('pat_id', 'instance', 'episode_id', 'conception_date', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), filter_string = 'contact_date < conception_date ', join_table = 'encounterdiagnosis', add_cc_columns = cc_list)
  include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'contact_date']+cc_list
  include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'noted_date']+cc_list
  encounters_df = get_clinical_concept_records(encounters_df.select(*include_encounter_columns), cc_list)
  problem_list_df = get_problem_list(df.select('pat_id', 'instance', 'episode_id', 'conception_date', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), filter_string = 'noted_date < conception_date', add_cc_columns = cc_list)
  problem_list_df = get_clinical_concept_records(problem_list_df.select(*include_problem_columns), cc_list)
  encounters_df = encounters_df.withColumnRenamed('contact_date', 'diagnosis_date')
  problem_list_df = problem_list_df.withColumnRenamed('noted_date', 'diagnosis_date')
  conditions_df = encounters_df.union(problem_list_df)
  return conditions_df 

def add_prior_coagulopathy(df, conditions_df):
  agg_cols = {}
  cc_list = ['coagulopathy']
  for cc in cc_list:
    agg_cols[cc] = 'max'
  conditions_df_ag = aggregate_data(conditions_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = agg_cols, order_by=None)
  return_df = df.join(conditions_df_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  for i in cc_list:
    return_df = return_df.withColumn(i + '_max', F.when(F.col(i+'_max')==True, F.lit(1)).otherwise(0))
    return_df = return_df.withColumnRenamed(i + '_max', 'prior_'+i)
  return return_df

def get_secondary_coagulopathy(df, anticoagulant):
  cc_list = ['coagulopathy']
  
  if anticoagulant == True:
    encounter_filter_string = 'contact_date > start_date_min AND contact_date < ob_delivery_delivery_date'
    problem_filter_string = 'date_of_entry > start_date_min AND date_of_entry < ob_delivery_delivery_date'
    include_cohort_columns = ['pat_id', 'instance', 'episode_id', 'conception_date', 'ob_delivery_delivery_date', 'child_episode_id', 'covid_test_date', 'start_date_min']
    include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'contact_date', 'start_date_min', 'covid_test_date']+cc_list
    include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'date_of_entry', 'start_date_min', 'covid_test_date']+cc_list
  
  
  
  if anticoagulant == False:
    encounter_filter_string = 'contact_date > covid_test_date AND contact_date < ob_delivery_delivery_date'
    problem_filter_string = 'date_of_entry > covid_test_date AND date_of_entry < ob_delivery_delivery_date'
    include_cohort_columns = ['pat_id', 'instance', 'episode_id', 'conception_date', 'ob_delivery_delivery_date', 'child_episode_id', 'covid_test_date']
    include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'contact_date', 'covid_test_date']+cc_list
    include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'date_of_entry','covid_test_date']+cc_list
  cohort_df = df.select(*include_cohort_columns)
  encounters_df = get_encounters(cohort_df.distinct(), filter_string = encounter_filter_string, join_table = 'encounterdiagnosis', add_cc_columns = cc_list)
  problem_list_df = get_problem_list(cohort_df.distinct(), filter_string = problem_filter_string, add_cc_columns = cc_list)
  
  encounters_df = get_clinical_concept_records(encounters_df.select(*include_encounter_columns), cc_list)
  problem_list_df = get_clinical_concept_records(problem_list_df.select(*include_problem_columns), cc_list)
  encounters_df = encounters_df.withColumnRenamed('contact_date', 'diagnosis_date')
  problem_list_df = problem_list_df.withColumnRenamed('date_of_entry', 'diagnosis_date')
  conditions_df = encounters_df.union(problem_list_df)
  return conditions_df 

def add_secondary_coagulopathy(df, conditions_df):
  agg_cols = {}
  cc_list = ['coagulopathy']
  for cc in cc_list:
    agg_cols[cc] = 'max'
  conditions_df_ag = aggregate_data(conditions_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = agg_cols, order_by=None)
  return_df = df.join(conditions_df_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  for i in cc_list:
    return_df = return_df.withColumn(i + '_max', F.when(F.col(i+'_max')==True, F.lit(1)).otherwise(0))
    return_df = return_df.withColumnRenamed(i + '_max', 'secondary_'+i)
  return return_df 



  def get_contraindication(df):
  contraindication_cc_list = ['major_bleeding', 'peptic_ulcer', 'stage_2_hypertension', 'esophageal_varices', 'intracranial_mass', 'end_stage_liver_diseases', 'aneurysm', 'proliferative_retinopathy', 'risk_bleeding']
  encounters_df = get_encounters(df.select('pat_id', 'instance', 'episode_id', 'conception_date', 'covid_test_date', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), filter_string = 'contact_date < covid_test_date AND contact_date > date_sub(conception_date, 730)', join_table = 'encounterdiagnosis', add_cc_columns = contraindication_cc_list)
  include_encounter_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'contact_date']+contraindication_cc_list
  include_problem_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'conception_date', 'ob_delivery_delivery_date', 'noted_date']+contraindication_cc_list
  encounters_df = get_clinical_concept_records(encounters_df.select(*include_encounter_columns), contraindication_cc_list)
  
  
  problem_list_df = get_problem_list(df.select('pat_id', 'instance', 'episode_id', 'conception_date', 'covid_test_date', 'ob_delivery_delivery_date', 'child_episode_id').distinct(), filter_string = 'date_of_entry < covid_test_date AND date_of_entry > date_sub(conception_date, 730)', add_cc_columns = contraindication_cc_list).filter(((F.col('resolved_date')>F.col('covid_test_date'))|(F.col('resolved_date').isNull()))).filter((F.col('problem_status')!='Deleted'))
  problem_list_df = get_clinical_concept_records(problem_list_df.select(*include_problem_columns), contraindication_cc_list)
  encounters_df = encounters_df.withColumnRenamed('contact_date', 'diagnosis_date')
  problem_list_df = problem_list_df.withColumnRenamed('noted_date', 'diagnosis_date')
  conditions_df = encounters_df.union(problem_list_df)
  return conditions_df 

def add_contraindication(df, conditions_df):
  agg_cols = {}
  contraindication_cc_list = ['major_bleeding', 'peptic_ulcer', 'stage_2_hypertension', 'esophageal_varices', 'intracranial_mass', 'end_stage_liver_diseases', 'aneurysm', 'proliferative_retinopathy', 'risk_bleeding']
  for cc in contraindication_cc_list:
    agg_cols[cc] = 'max'
  conditions_df_ag = aggregate_data(conditions_df, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = agg_cols, order_by=None)
  return_df = df.join(conditions_df_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
  for i in contraindication_cc_list:
    return_df = return_df.withColumn(i + '_max', F.when(F.col(i+'_max')==True, F.lit(1)).otherwise(0))
    return_df = return_df.withColumnRenamed(i + '_max', 'contraindication_'+i)
  return return_df


def get_covid_initial_med(cohort_df):
  # get non-anticoagulant medication order 
  list_administered = ['Anesthesia Volume Adjustment', 'Bolus', 'Bolus from Bag', 'Calc Rate', 'Continued by Anesthesia', 'Continued Bag', 'Continue bag from transfer', 'Dispense to Home', 'Given', 'Given by Other', 'Given During Downtime', 'Milk Verified', 'New Bag', 'New Syringe/Cartridge', 'Patch Applied', 'Push', 'Rate Verify', 'Rate Change', 'Rate/Dose Change-Dual Sign', 'Rate/Dose Verify-Dual Sign', 'Restarted', 'Started During Downtime', 'Unheld by Provider']
  meds = get_medication_orders(cohort_df.drop('name', 'start_date', 'end_date'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant'))
  total_meds = meds.filter(F.col('ordering_datetime')>=F.date_sub(F.col('treatment_onset_index'),3)).filter(F.col('ordering_datetime')<=F.date_add(F.col('treatment_onset_index'),3))

  total_meds = total_meds.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'short_name').distinct()
  total_meds_ag = aggregate_data(total_meds, partition_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id'], aggregation_columns = {'short_name':'count'}).withColumnRenamed('short_name_count', 'covid_med_initial_count')
  
  return_df = cohort_df.join(total_meds_ag, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['covid_med_initial_count'])
  return return_df 


def rename_columns(df, rename_string = ""):
  return_df = df.toDF(*(c.replace('_sum', rename_string) for c in df.columns))
  return return_df 


def save_problem_diagnosis(cohort_df=None,
                         filter_string = "date_of_entry >= lmp AND date_of_entry < ob_delivery_delivery_date",
                         add_cc_columns = coagulatory_cc_list, 
                         save_table_name = 'yh_covid_coagulatory_problem_temp'):
    """Save problem diagnoses based on specified filter criteria
  
    Parameters:
    cohort_df (PySpark df): Optional cohort dataframe for which to get problem list records (
                          get all patients in the RDP). This dataframe *must* include pat_id, instance, ob_delivery_delivery_date columns for joining
    filter_string (string): conditions. use *date_of_entry*  
    add_cc_columns (list): List of condition columns to add (e.g. 'asthma',
                         'cardiac_arrhythmia', 'chronic_lung_disease')
    save_table_name (string): name of the temporary problem list diagnoses dataframe to be saved 
    """
    conditions = add_cc_columns
    spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_covid_dx_id_cc_label_mapping")
    cc_mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_dx_id_cc_label_mapping")
    if 'name' in cohort_df.columns:
      cohort_df = cohort_df.drop('name')
    # get problem 
    from pyspark.sql.functions import when
    
    problem_table_name = 'hadlock_problem_list'
    problem_records_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{table}".format(table=problem_table_name))
    
    problem_records_df = problem_records_df.join(cc_mapping_df, ['instance', 'diagnosis_id'], how='inner')
    
    patient_problem_df = problem_records_df.join(cohort_df, ['instance', 'pat_id'], how = 'right') \
      .filter(filter_string)\
      .select(['instance', 'pat_id','ob_delivery_delivery_date'] + conditions)
    # get active problem list 
    active_patient_problem_df = patient_problem_df.where(F.col('problem_status')=='Active')
    problem_table_name = save_table_name
    write_data_frame_to_sandbox(active_patient_problem_df, problem_table_name, sandbox_db='rdp_phi_sandbox', replace=True)
    

    
def save_encounter_diagnosis(cohort_df=None,
                         filter_string = "contact_date >= lmp AND contact_date < ob_delivery_delivery_date",
                         add_cc_columns = coagulatory_cc_list,
                         save_table_name = 'yh_covid_coagulatory_encounter_temp'):
    """Save encounter diagnoses based on specified filter criteria
  
    Parameters:
    cohort_df (PySpark df): Optional cohort dataframe for which to get problem list records (
                          get all patients in the RDP). This dataframe *must* include pat_id, instance, ob_delivery_delivery_date columns for joining
    filter_string (string): condition statement. use *contact_date* 
    add_cc_columns (list): List of condition cc columns to add (e.g. 'asthma',
                         'cardiac_arrhythmia', 'chronic_lung_disease')
    save_table_name (string): name of the temporary encounter diagnoses dataframe to be saved 
    """
    conditions = add_cc_columns
    spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_covid_dx_id_cc_label_mapping")
    cc_mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_dx_id_cc_label_mapping")
    
    if 'name' in cohort_df.columns:
      cohort_df = cohort_df.drop('name')
    # get problem 

    encounter_table_name = 'hadlock_encounters'    #has to be manually updated 
    encounters_records_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{table}".format(table=encounter_table_name))
    encounters_records_df = encounters_records_df.withColumn('diagnosis_id',F.explode(F.col('diagnosis_id')))    
    encounters_records_df = encounters_records_df.join(cc_mapping_df, ['instance', 'diagnosis_id'], how='inner')

    patient_encounter_df = encounters_records_df.join(cohort_df, ['instance','pat_id'], how = 'right') \
      .filter(filter_string)\
      .select(['instance', 'pat_id','ob_delivery_delivery_date'] + conditions)
    encounter_table_name = save_table_name 
    write_data_frame_to_sandbox(patient_encounter_df, encounter_table_name, sandbox_db='rdp_phi_sandbox', replace=True)
    

def add_problems_encounters_conditions(cohort_df=None,
                                       add_cc_columns=coagulatory_cc_list,
                                       problem_table_name = 'yh_covid_coagulatory_problem_temp',
                                       encounter_table_name = 'yh_covid_coagulatory_encounter_temp', 
                                       rename_string = ""):
    """Get conditions status based on specified filter criteria
  
    Parameters:
    cohort_df (PySpark df): Optional cohort dataframe for which to get encounter records (
                          get all patients in the RDP). This dataframe *must* include pat_id, instance, ob_delivery_delivery_date columns for joining
    add_cc_columns (list): List of medication cc columns to add (e.g. 'asthma',
                         'cardiac_arrhythmia', 'chronic_lung_disease')
    problem_table_name (string): name of the temporary problem list diagnoses dataframe saved in save_problem_diagnosis function 
    encounter_table_name (string): name of the temporary encounter diagnoses dataframe saved in save_encounter_diagnosis function 
    rename_string (string): string that will be added to each condition column name 
  
    Returns:
    PySpark df: Dataframe containing condition status satisfying specified filter criteria. Dataframe will contain pat_id, instance, ob_delivery_delivery_date and individual condition columns 
    """
    from pyspark.sql.functions import when
    conditions = add_cc_columns
    
    problems_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{0}".format(problem_table_name))
    encounters_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{0}".format(encounter_table_name))
    
    union_df = problems_df.union(encounters_df).select(['instance', 'pat_id', 'ob_delivery_delivery_date']+conditions) 
    #write aggregate column dict and na fill dict 
    agg_function_dict = {}
    na_fill_dict = {} #for the result df 
    for condition in conditions:
      agg_function_dict[condition] = 'sum'
      na_fill_dict[condition+'_sum'] = 0
    
    union_agg = aggregate_data(df = union_df, 
                                 partition_columns = ['instance', 'pat_id', 'ob_delivery_delivery_date'], 
                                 aggregation_columns = agg_function_dict)
    
    #remove this for loop if you are interested in getting the count of diagnosis 
    for condition in conditions:
      union_agg = union_agg.withColumn(condition+'_sum', \
              when(union_agg[condition+'_sum'] > 0, 1).otherwise(0))
      print (condition+'_sum to status')
    result_df = cohort_df.select(['instance', 'pat_id', 'ob_delivery_delivery_date']).distinct()
    result_df = result_df.join(union_agg, ['instance', 'pat_id', 'ob_delivery_delivery_date'], 'left').fillna(na_fill_dict)
    result_df = rename_columns(result_df, rename_string)
    return result_df


def add_med(cohort_df = None,
            cc_columns = coagulation_cc_med_list, 
            filter_string =  "start_date >= date_sub(lmp,180) AND start_date < lmp", 
            rename_string = ""):
  """Get medication count based on specified filter criteria
  
    Parameters:
    cohort_df (PySpark df): Optional cohort dataframe for which to get encounter records (
                          get all patients in the RDP). This dataframe *must* include pat_id, instance, ob_delivery_delivery_date columns for joining
    add_cc_columns (list): List of medication cc columns to add (e.g. 'coagulation_modifier',
                         'antidepressant')
    filter_string (string): condition statement 
    rename_string (string): string that will be added to each medication column name 
  
    Returns:
    PySpark df: Dataframe containing condition status satisfying specified filter criteria. Dataframe will contain pat_id, instance, ob_delivery_delivery_date and individual medication columns 
    """
  
  
  
  med_df = get_medication_orders(cohort_df= cohort_df,
                              filter_string = filter_string, 
                              add_cc_columns = cc_columns).filter(F.col('coagulation_modifier'))
  agg_function_dict = {}
  na_fill_dict = {}
  for med in cc_columns:
    agg_function_dict[med] = 'sum'
    na_fill_dict[med+'_sum'] = 0
  med_agg = aggregate_data(df = med_df, 
                           partition_columns = ['pat_id', 'instance','ob_delivery_delivery_date'], 
                           aggregation_columns = agg_function_dict)
  result_df = cohort_df.select(['pat_id', 'instance', 'ob_delivery_delivery_date']).distinct()
  result_df = result_df.join(med_agg, ['pat_id', 'instance', 'ob_delivery_delivery_date'], 'left').fillna(na_fill_dict)
  result_df = rename_columns(result_df, rename_string)
  return result_df 

