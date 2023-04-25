# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities

from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.utils import resample
from sklearn.metrics import accuracy_score
import imblearn as imb
from datetime import date
from pyspark.sql.functions import unix_timestamp
import pandas as pd
import numpy as np
import scipy
import sklearn 
import matplotlib.pyplot as plt
%matplotlib inline
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn import datasets, linear_model
from sklearn import metrics
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.datasets import make_regression
from sklearn.utils import shuffle
from sklearn.metrics import average_precision_score
import math
plt.rcParams.update({'font.size': 12})
plt.rcParams['pdf.fonttype'] = 42
spark.conf.set('spark.sql.execution.arrow.enabled', False)

# Set a seed value
seed_value= 457
# 1. Set `PYTHONHASHSEED` environment variable at a fixed value
import os
os.environ['PYTHONHASHSEED']=str(seed_value)
# 2. Set `python` built-in pseudo-random generator at a fixed value
import random
random.seed(seed_value)
# 3. Set `numpy` pseudo-random generator at a fixed value
import numpy as np
np.random.seed(seed_value)


import math
!pip install psmpy
from psmpy import PsmPy
from psmpy.functions import cohenD
from psmpy.plotting import *


def impute_missing_data(df):
  df.fillna(value=pd.np.nan, inplace=True)
  imputer = SimpleImputer(missing_values=np.nan, strategy='median')
  imputer.fit_transform(df)
  return(imputer.fit_transform(df))


def get_matching_pairs(df_experimental, df_control, scaler=True):
  if scaler:
      scaler = StandardScaler()
      scaler.fit(df_experimental.append(df_control))
      df_experimental_scaler = scaler.transform(df_experimental)
      df_control_scaler = scaler.transform(df_control)
      nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree', metric='euclidean').fit(df_control_scaler)
      distances, indices = nbrs.kneighbors(df_experimental_scaler)
  
  else:
    nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree', metric='euclidean').fit(df_control)
    distances, indices = nbrs.kneighbors(df_experimental)
  indices = indices.reshape(indices.shape[0])
  matched = df_control.iloc[indices, :]
  return matched

def select_psm_columns(df, columns):
  return_df = df.select(*columns)
  return return_df


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


def format_race(i):
  if i is None:
    return('Unknown')
  if len(i) > 1:
    return('Multiracial')
  if i[0] == 'White':
    return('White or Caucasian')
  if i[0] == 'Declined' or i[0] == 'Patient Refused':
    return('Unknown')
  if i[0] == 'Unable to Determine':
    return('Unknown')
  return(i[0])


def format_ethnic_group(i):
  if i is None:
    return 'Unknown'
  if i == 'American' or i == 'Samoan':
    return 'Not Hispanic or Latino'
  elif i == 'Filipino' or i == 'Hmong':
    return 'Not Hispanic or Latino'
  elif i == 'Sudanese':
    return 'Not Hispanic or Latino'
  if i == 'Patient Refused' or i == 'None':
    return 'Unknown'
  return i


def format_parity(i):
  if i is None:
    return 0
  i = int(i)
  if i == 0 or i == 1:
    return 0
  if i > 1 and i < 5:
    return 1
  if i >= 5:
    return 2
  return 0


def format_gravidity(gravidity):
  if gravidity is None:
    return 0
  gravidity = int(gravidity)
  if gravidity == 0 or gravidity == 1:
    return 0
  elif gravidity > 1 and gravidity < 6:
    return 1
  elif gravidity >= 6:
    return 2
  return 0
    
  
def format_preterm_history(preterm_history, gestational_days):

  if preterm_history is None:
    return 0
  else:
    preterm_history = int(preterm_history)
    if preterm_history == 0 or (preterm_history == 1 and gestational_days < 259):
      return 0
    else:
      return 1
  return 0


def encode_delivery_method(i):
  '''
  0 = Vaginal
  1 = C-Section
  -1 = Unknown
  '''
  list_vaginal = ['Vaginal, Spontaneous',
       'Vaginal, Vacuum (Extractor)',
       'Vaginal, Forceps', 'Vaginal < 20 weeks',
       'Vaginal, Breech', 'VBAC, Spontaneous',
       'Vaginal Birth after Cesarean Section',
       'Spontaneous Abortion']
  list_c_section = ['C-Section, Low Transverse',
       'C-Section, Low Vertical',
       'C-Section, Classical',
       'C-Section, Unspecified']
  if i in list_vaginal:
    return(0)
  if i in list_c_section:
    return(1)
  return(-1)

def encode_ruca(ruca):
  if ruca is None:
    return -1
  if ruca == 'Rural':
    return 0
  if ruca == 'SmallTown':
    return 1
  if ruca == 'Micropolitan':
    return 2
  if ruca == 'Metropolitan':
    return 3
  return -1


def encode_bmi(bmi):
  if bmi is None or math.isnan(bmi):
    return -1
  bmi = int(bmi)
  if bmi >= 15 and bmi < 18.5:
    return 0
  if bmi < 25:
    return 1
  if bmi < 30:
    return 2
  if bmi < 35:
    return 3
  if bmi < 40:
    return 4
  return -1


def encode_age(age):
  if age < 25:
    return 0
  if age < 30:
    return 1
  if age < 35:
    return 2
  if age < 40:
    return 3
  if age < 45:
    return 4
  return -1


def encode_oxygen_assistance(oxygen):
  if oxygen is None:
    return 0
  elif oxygen == -1:
    return 0
  elif oxygen == 0:
    return 0
  elif oxygen > 0:
    return 1


def handle_missing_bmi(df):
  print('# Percent of patients with pregravid BMI:', str(round(100*(len(df) - df['pregravid_bmi'].isna().sum())/len(df), 1)), '%')
  print('Imputing median pregravid BMI of', str(round(df['pregravid_bmi'].median(), 2)), '...')
  df['pregravid_bmi'].fillna(df['pregravid_bmi'].median(), inplace = True)
  print('\n')
  return df

def handle_missing_svi(df, col):
  print('# Percent of patients with svi:', str(round(100*(len(df) - df[col].isna().sum())/len(df), 1)), '%')
  print('Imputing median svi of', str(round(df[col].median(), 2)), '...')
  df[col].fillna(df[col].median(), inplace = True)
  print('\n')
  return df



from sklearn.preprocessing import MinMaxScaler
import numpy as np
  
  



def format_dataframe_for_psm(df, select_columns):
  dict_white = {'White or Caucasian': 1, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_asian = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 1, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_multiracial = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 1, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_other = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 1, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_black = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 1, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_pacific_islander = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 1, 'American Indian or Alaska Native': 0}
  dict_native_american = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 1}
  dict_ethnic_groups = {'Unknown': -1, 'Hispanic or Latino': 1, 'Not Hispanic or Latino': 0}
  dict_fetal_sex = {None: -1, 'Male': 1, 'Female': 0, 'Other': -1, 'Unknown': -1}
  dict_commercial_insurance = {'Medicaid': 0, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 1}
  dict_governmental_insurance = {'Medicaid': 1, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 0}
  dict_wildtype = {'wild_type' : 1, 'alpha' : 0, 'delta' : 0, 'omicron1' : 0, 'omicron2': 0, 'omicron': 0}
  dict_alpha = {'wild_type' : 0, 'alpha' : 1, 'delta' : 0, 'omicron1' : 0, 'omicron2': 0, 'omicron': 0}
  dict_delta = {'wild_type' : 0, 'alpha' : 0, 'delta' : 1, 'omicron1' : 0, 'omicron2': 0, 'omicron': 0}
  dict_omicron = {'wild_type' : 0, 'alpha' : 0, 'delta' : 0, 'omicron1' : 1, 'omicron2': 1, 'omicron': 1}
  dict_covid_1st = {'1st trimester' : 1, '2nd trimester' : 0, '3rd trimester' : 0}
  dict_covid_2nd = {'1st trimester' : 0, '2nd trimester' : 1, '3rd trimester' : 0}
  dict_covid_3rd = {'1st trimester' : 0, '2nd trimester' : 0, '3rd trimester' : 1}
  dict_covid_guideline0 = {'guideline0' : 1, 'guideline1' : 0, 'guideline2' : 0}
  dict_covid_guideline1 = {'guideline0' : 0, 'guideline1' : 1, 'guideline2' : 0}
  dict_covid_guideline2 = {'guideline0' : 0, 'guideline1' : 0, 'guideline2' : 1}
  df = select_psm_columns(df, select_columns).toPandas()
  for index, row in df.iterrows():
    df.at[index, 'race'] = format_race(row['race'])
    df.at[index, 'ethnic_group'] = format_ethnic_group(row['ethnic_group'])
    df.at[index, 'Preterm_history'] = format_preterm_history(row['Preterm_history'], row['gestational_days'])
  for index, row in df.iterrows():
    df.at[index, 'race_white'] = dict_white[row['race']]
    df.at[index, 'race_asian'] = dict_asian[row['race']]
    df.at[index, 'race_black'] = dict_black[row['race']]
    df.at[index, 'race_other'] = dict_other[row['race']]
    df.at[index, 'variant_wt'] = dict_wildtype[row['covid_variant']]
    df.at[index, 'variant_alpha'] = dict_alpha[row['covid_variant']]
    df.at[index, 'variant_delta'] = dict_delta[row['covid_variant']]
    df.at[index, 'variant_omicron'] = dict_omicron[row['covid_variant']]
    df.at[index, 'timing_1st'] = dict_covid_1st[row['trimester_of_covid_test']]
    df.at[index, 'timing_2nd'] = dict_covid_2nd[row['trimester_of_covid_test']]
    df.at[index, 'timing_3rd'] = dict_covid_3rd[row['trimester_of_covid_test']]
    df.at[index, 'guideline0'] = dict_covid_guideline0[row['covid_guideline']]
    df.at[index, 'guideline1'] = dict_covid_guideline1[row['covid_guideline']]
    df.at[index, 'guideline2'] = dict_covid_guideline2[row['covid_guideline']]
    df.at[index, 'ethnic_group'] = dict_ethnic_groups[row['ethnic_group']]
    df.at[index, 'ob_hx_infant_sex'] = dict_fetal_sex[row['ob_hx_infant_sex']]
    df.at[index, 'commercial_insurance'] = dict_commercial_insurance[row['insurance']]
    df.at[index, 'Parity'] = format_parity(row['Parity'])
    df.at[index, 'Gravidity'] = format_gravidity(row['Gravidity'])
    df.at[index, 'pregravid_bmi'] = encode_bmi(row['pregravid_bmi'])
    df.at[index, 'age_at_start_dt'] = encode_age(row['age_at_start_dt'])
    df.at[index, 'ruca_categorization'] = encode_ruca(row['ruca_categorization'])
    df.at[index, 'oxygen_assistance'] = encode_oxygen_assistance(row['max_oxygen_device'])
  df = df.drop(columns=['gestational_days', 'insurance', 'race', 'lmp', 'trimester_of_covid_test', 'covid_variant', 'covid_guideline', 'max_oxygen_device'])
  df = handle_missing_svi(df, 'RPL_THEME1')
  df = handle_missing_svi(df, 'RPL_THEME2')
  df = handle_missing_svi(df, 'RPL_THEME3')
  df = handle_missing_svi(df, 'RPL_THEME4' )
  columns_to_be_normalized = [ 'covid_med_initial_count', 'covid_48hours_med_count', 'count_precovid2yrs_diagnoses', 'covid_total_med_post48hours_count']
  columns_to_be_normalized2 = [c for c in columns_to_be_normalized if c in df.columns]
  for c in columns_to_be_normalized2:
    df[c] = MinMaxScaler().fit_transform(np.array(df[c]).reshape(-1,1))
  
  print('Columns used for matching:')
  for col in df.columns:
    print(col)
  print('\n')
  print('\n')
  return(df)


covid_administered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = covid_notadministered.withColumn('covid_test_date',F.to_timestamp("covid_test_date"))

# format dataframes for propensity score matching
df_experimental_psm = format_dataframe_for_psm(covid_administered, select_columns)
print('# Number of women administered anticoagulant at time of delivery: ' + str(len(df_experimental_psm)))
df_experimental_psm = df_experimental_psm.rename(columns=column_dict)
df_control_psm = format_dataframe_for_psm(covid_notadministered, select_columns)
df_control_psm = df_control_psm.rename(columns=column_dict)
print('# Number of women who did not administer anticoagulant at time of delivery: ' + str(len(df_control_psm)))

precovid_diagnoses = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'pre-covid diagnoses count']
precovid_features = ['pat_id', 'instance', 'episode_id', 'child_episode_id',
       'covid_total_med_post48hours_count', 'vaccination status',
       'pregravid BMI', 'maternal age', 'ethnicity', 'fetal sex', 'parity',
       'gravidity', 'preterm history',
       'pre-covid diagnoses count', 'illegal drug use', 'smoking',
       'socioeconomic', 'household composition and disability',
       'minority status and language', 'housing type and transportation',
       'rural-urban classification', 'race_white', 'race_asian', 'race_black',
       'race_other', 'variant_wt', 'variant_alpha', 'variant_delta',
       'variant_omicron', '1st trimester infection', '2nd trimester infection',
       '3rd trimester infection', 'guideline_0', 'guideline_1', 'guideline_2',
       'commercial insurance']
precovid_topfeatures = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'variant_omicron', 'socioeconomic', 'household composition and disability',
       'minority status and language', 'housing type and transportation', '3rd trimester infection', 'pre-covid diagnoses count']
precovid_top_sa1_features = precovid_topfeatures  + ['covid_48hours_med_count']
precovid_top_sa1_features2 = precovid_topfeatures  + ['covid inital med count']
precovid_top_sa2_features = precovid_topfeatures  + ['oxygen assistance']


df_experimental_psm_precovid_diagnoses = shuffle(df_experimental_psm[precovid_diagnoses], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_diagnoses)))
df_control_psm_precovid_diagnoses = shuffle(df_control_psm[precovid_diagnoses], random_state=seed_value)


df_experimental_psm_precovid = shuffle(df_experimental_psm[precovid_features], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid)))
df_control_psm_precovid = shuffle(df_control_psm[precovid_features], random_state=seed_value)


df_experimental_psm_precovid_top = shuffle(df_experimental_psm[precovid_topfeatures], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top)))
df_control_psm_precovid_top = shuffle(df_control_psm[precovid_topfeatures], random_state=seed_value)


df_experimental_psm_precovid_top_sa1 = shuffle(df_experimental_psm[precovid_top_sa1_features], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top_sa1)))
df_control_psm_precovid_top_sa1 = shuffle(df_control_psm[precovid_top_sa1_features], random_state=seed_value)


df_experimental_psm_precovid_top_sa1_2 = shuffle(df_experimental_psm[precovid_top_sa1_features2], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top_sa1_2)))
df_control_psm_precovid_top_sa1_2 = shuffle(df_control_psm[precovid_top_sa1_features2], random_state=seed_value)


df_experimental_psm_precovid_top_sa2 = shuffle(df_experimental_psm[precovid_top_sa2_features], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top_sa2)))
df_control_psm_precovid_top_sa2 = shuffle(df_control_psm[precovid_top_sa2_features], random_state=seed_value)



# identify top features predicting who gets vaccinated using machine learning
# format for machine learning
df_experimental_psm['anticoagulant_status'] = 1
df_control_psm['anticoagulant_status'] = 0
df_final = shuffle(df_experimental_psm.append(df_control_psm, ignore_index=True), random_state=seed_value)
cols = df_final.columns
df_final[cols] = df_final[cols].apply(pd.to_numeric, errors='coerce')
df_final2 = df_final.dropna()
df_final2['id'] = df_final2['pat_id'].astype(str) + df_final2['episode_id'].astype('int').astype('str') + df_final2['child_episode_id'].astype('int').astype('str')
df_final2[cols] = df_final2[cols].apply(pd.to_numeric, errors='coerce')
df_final2.head()


df_experimental_psm_precovid_diagnoses['anticoagulant_status'] = 1
df_control_psm_precovid_diagnoses['anticoagulant_status'] = 0
df_final_precovid_diagnoses = shuffle(df_experimental_psm_precovid_diagnoses.append(df_control_psm_precovid_diagnoses, ignore_index=True), random_state=seed_value)
cols = df_final_precovid_diagnoses.columns
df_final_precovid_diagnoses[cols] = df_final_precovid_diagnoses[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid_diagnoses = df_final_precovid_diagnoses.dropna()
df_final2_precovid_diagnoses['id'] = df_final2_precovid_diagnoses['pat_id'].astype(str) + df_final2_precovid_diagnoses['episode_id'].astype('int').astype('str') + df_final2_precovid_diagnoses['child_episode_id'].astype('int').astype('str')
df_final2_precovid_diagnoses[cols] = df_final2_precovid_diagnoses[cols].apply(pd.to_numeric, errors='coerce')


df_experimental_psm_precovid['anticoagulant_status'] = 1
df_control_psm_precovid['anticoagulant_status'] = 0
df_final_precovid = shuffle(df_experimental_psm_precovid.append(df_control_psm_precovid, ignore_index=True), random_state=seed_value)
cols = df_final_precovid.columns
df_final_precovid[cols] = df_final_precovid[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid = df_final_precovid.dropna()
df_final2_precovid['id'] = df_final2_precovid['pat_id'].astype(str) + df_final2_precovid['episode_id'].astype('int').astype('str') + df_final2_precovid['child_episode_id'].astype('int').astype('str')
df_final2_precovid[cols] = df_final2_precovid[cols].apply(pd.to_numeric, errors='coerce')


df_experimental_psm_precovid_top['anticoagulant_status'] = 1
df_control_psm_precovid_top['anticoagulant_status'] = 0
df_final_precovid_top = shuffle(df_experimental_psm_precovid_top.append(df_control_psm_precovid_top, ignore_index=True), random_state=seed_value)
cols = df_final_precovid_top.columns
df_final_precovid_top[cols] = df_final_precovid_top[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid_top = df_final_precovid_top.dropna()
df_final2_precovid_top['id'] = df_final2_precovid_top['pat_id'].astype(str) + df_final2_precovid_top['episode_id'].astype('int').astype('str') + df_final2_precovid_top['child_episode_id'].astype('int').astype('str')
df_final2_precovid_top[cols] = df_final2_precovid_top[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid_top.head()


df_experimental_psm_precovid_top_sa1['anticoagulant_status'] = 1
df_control_psm_precovid_top_sa1['anticoagulant_status'] = 0
df_final_precovid_top_sa1 = shuffle(df_experimental_psm_precovid_top_sa1.append(df_control_psm_precovid_top_sa1, ignore_index=True), random_state=seed_value)
cols = df_final_precovid_top_sa1.columns
df_final_precovid_top_sa1[cols] = df_final_precovid_top_sa1[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid_top_sa1 = df_final_precovid_top_sa1.dropna()
df_final2_precovid_top_sa1['id'] = df_final2_precovid_top_sa1['pat_id'].astype(str) + df_final2_precovid_top_sa1['episode_id'].astype('int').astype('str') + df_final2_precovid_top_sa1['child_episode_id'].astype('int').astype('str')
df_final2_precovid_top_sa1[cols] = df_final2_precovid_top_sa1[cols].apply(pd.to_numeric, errors='coerce')
df_final2_precovid_top_sa1.head()