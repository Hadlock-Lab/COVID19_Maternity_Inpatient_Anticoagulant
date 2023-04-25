# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities

# load packages 
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
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
from sklearn.model_selection import LeaveOneOut
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from numpy import mean
from numpy import absolute
from numpy import sqrt
import pandas as pd
from sklearn.datasets import make_regression
from sklearn.utils import shuffle
from sklearn.metrics import average_precision_score
import math
import imblearn
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline
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

# feature engineering 
# function for geocoding 


def add_geo_features(cohort_df, geo_df_name, join_cols = ['pat_id', 'instance']):
  geodf_list = ['ruca2010revised', 'countytypologycodes2015', 'farcodeszip2010', 'ruralurbancontinuumcodes2013', 'urbaninfluencecodes2013', 'svi2018_us', 'svi2018_us_county']
  master_patient = spark.sql("SELECT * FROM rdp_phi.dim_patient_master").select('pat_id','instance', 'PATIENT_STATE_CD', 'PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED', 'ZIP')
  
  if geo_df_name not in geodf_list:
    print ('incorrect geo df name')
  else:
    geo_df = spark.sql("SELECT * from rdp_phi_sandbox.{0}".format(geo_df_name))
    if geo_df_name == 'ruca2010revised':
      geo_df = geo_df.withColumn('FIPS', F.col('State_County_Tract_FIPS_Code').cast(StringType())).drop('State_County_Tract_FIPS_Code')
      master_patient = master_patient.withColumn("FIPS", F.expr("CASE WHEN PATIENT_STATE_CD = 'CA' THEN substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 2, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-2) ELSE substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-1) END"))
      joined_df = master_patient.join(geo_df, 'FIPS', 'inner')
    elif geo_df_name == 'svi2018_us':
      master_patient = master_patient.withColumn("FIPS", F.expr("substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-1)"))
      joined_df = master_patient.join(geo_df, 'FIPS', 'inner') 
    elif ((geo_df_name == 'countytypologycodes2015')|(geo_df_name == 'urbaninfluencecodes2013')):
      geo_df = geo_df.withColumn('FIPS4', F.col('FIPStxt').cast(StringType())).drop('FIPStxt')
      master_patient = master_patient.withColumn("FIPS4", F.expr("CASE WHEN PATIENT_STATE_CD = 'CA' THEN substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 2, 4) ELSE substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, 5) END"))
      joined_df = master_patient.join(geo_df, 'FIPS4', 'inner')
    elif ((geo_df_name == 'svi2018_us_county')|(geo_df_name == 'ruralurbancontinuumcodes2013')):
      geo_df = geo_df.withColumn('FIPS5', F.col('FIPS').cast(StringType()))
      master_patient = master_patient.withColumn("FIPS5", F.expr("substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, 5)"))
      joined_df = master_patient.join(geo_df, 'FIPS5', 'inner')    
    elif geo_df_name == 'farcodeszip2010':
      geo_df = geo_df.withColumn('ZIP5', F.col('ZIP').cast(StringType())).drop('ZIP')
      master_patient = master_patient.withColumn("ZIP5", F.expr("substring(ZIP, 0, 5)")).drop('ZIP')
      joined_df = master_patient.join(geo_df, 'ZIP5', 'inner')
    return_df = cohort_df.join(joined_df, join_cols, 'left')
  return return_df 




import numpy as np
import scipy.stats
from scipy import stats

# AUC comparison adapted from
# https://github.com/Netflix/vmaf/
def compute_midrank(x):
    """Computes midranks.
    Args:
       x - a 1D numpy array
    Returns:
       array of midranks
    """
    J = np.argsort(x)
    Z = x[J]
    N = len(x)
    T = np.zeros(N, dtype=np.float)
    i = 0
    while i < N:
        j = i
        while j < N and Z[j] == Z[i]:
            j += 1
        T[i:j] = 0.5*(i + j - 1)
        i = j
    T2 = np.empty(N, dtype=np.float)
    # Note(kazeevn) +1 is due to Python using 0-based indexing
    # instead of 1-based in the AUC formula in the paper
    T2[J] = T + 1
    return T2


def compute_midrank_weight(x, sample_weight):
    """Computes midranks.
    Args:
       x - a 1D numpy array
    Returns:
       array of midranks
    """
    J = np.argsort(x)
    Z = x[J]
    cumulative_weight = np.cumsum(sample_weight[J])
    N = len(x)
    T = np.zeros(N, dtype=np.float)
    i = 0
    while i < N:
        j = i
        while j < N and Z[j] == Z[i]:
            j += 1
        T[i:j] = cumulative_weight[i:j].mean()
        i = j
    T2 = np.empty(N, dtype=np.float)
    T2[J] = T
    return T2


def fastDeLong(predictions_sorted_transposed, label_1_count, sample_weight):
    if sample_weight is None:
        return fastDeLong_no_weights(predictions_sorted_transposed, label_1_count)
    else:
        return fastDeLong_weights(predictions_sorted_transposed, label_1_count, sample_weight)


def fastDeLong_weights(predictions_sorted_transposed, label_1_count, sample_weight):
    """
    The fast version of DeLong's method for computing the covariance of
    unadjusted AUC.
    Args:
       predictions_sorted_transposed: a 2D numpy.array[n_classifiers, n_examples]
          sorted such as the examples with label "1" are first
    Returns:
       (AUC value, DeLong covariance)
    Reference:
     @article{sun2014fast,
       title={Fast Implementation of DeLong's Algorithm for
              Comparing the Areas Under Correlated Receiver Oerating Characteristic Curves},
       author={Xu Sun and Weichao Xu},
       journal={IEEE Signal Processing Letters},
       volume={21},
       number={11},
       pages={1389--1393},
       year={2014},
       publisher={IEEE}
     }
    """
    # Short variables are named as they are in the paper
    m = label_1_count
    n = predictions_sorted_transposed.shape[1] - m
    positive_examples = predictions_sorted_transposed[:, :m]
    negative_examples = predictions_sorted_transposed[:, m:]
    k = predictions_sorted_transposed.shape[0]

    tx = np.empty([k, m], dtype=np.float)
    ty = np.empty([k, n], dtype=np.float)
    tz = np.empty([k, m + n], dtype=np.float)
    for r in range(k):
        tx[r, :] = compute_midrank_weight(positive_examples[r, :], sample_weight[:m])
        ty[r, :] = compute_midrank_weight(negative_examples[r, :], sample_weight[m:])
        tz[r, :] = compute_midrank_weight(predictions_sorted_transposed[r, :], sample_weight)
    total_positive_weights = sample_weight[:m].sum()
    total_negative_weights = sample_weight[m:].sum()
    pair_weights = np.dot(sample_weight[:m, np.newaxis], sample_weight[np.newaxis, m:])
    total_pair_weights = pair_weights.sum()
    aucs = (sample_weight[:m]*(tz[:, :m] - tx)).sum(axis=1) / total_pair_weights
    v01 = (tz[:, :m] - tx[:, :]) / total_negative_weights
    v10 = 1. - (tz[:, m:] - ty[:, :]) / total_positive_weights
    sx = np.cov(v01)
    sy = np.cov(v10)
    delongcov = sx / m + sy / n
    return aucs, delongcov


def fastDeLong_no_weights(predictions_sorted_transposed, label_1_count):
    """
    The fast version of DeLong's method for computing the covariance of
    unadjusted AUC.
    Args:
       predictions_sorted_transposed: a 2D numpy.array[n_classifiers, n_examples]
          sorted such as the examples with label "1" are first
    Returns:
       (AUC value, DeLong covariance)
    Reference:
     @article{sun2014fast,
       title={Fast Implementation of DeLong's Algorithm for
              Comparing the Areas Under Correlated Receiver Oerating
              Characteristic Curves},
       author={Xu Sun and Weichao Xu},
       journal={IEEE Signal Processing Letters},
       volume={21},
       number={11},
       pages={1389--1393},
       year={2014},
       publisher={IEEE}
     }
    """
    # Short variables are named as they are in the paper
    m = label_1_count
    n = predictions_sorted_transposed.shape[1] - m
    positive_examples = predictions_sorted_transposed[:, :m]
    negative_examples = predictions_sorted_transposed[:, m:]
    k = predictions_sorted_transposed.shape[0]

    tx = np.empty([k, m], dtype=np.float)
    ty = np.empty([k, n], dtype=np.float)
    tz = np.empty([k, m + n], dtype=np.float)
    for r in range(k):
        tx[r, :] = compute_midrank(positive_examples[r, :])
        ty[r, :] = compute_midrank(negative_examples[r, :])
        tz[r, :] = compute_midrank(predictions_sorted_transposed[r, :])
    aucs = tz[:, :m].sum(axis=1) / m / n - float(m + 1.0) / 2.0 / n
    v01 = (tz[:, :m] - tx[:, :]) / n
    v10 = 1.0 - (tz[:, m:] - ty[:, :]) / m
    sx = np.cov(v01)
    sy = np.cov(v10)
    delongcov = sx / m + sy / n
    return aucs, delongcov


def calc_pvalue(aucs, sigma):
    """Computes log(10) of p-values.
    Args:
       aucs: 1D array of AUCs
       sigma: AUC DeLong covariances
    Returns:
       log10(pvalue)
    """
    l = np.array([[1, -1]])
    z = np.abs(np.diff(aucs)) / np.sqrt(np.dot(np.dot(l, sigma), l.T))
    return np.log10(2) + scipy.stats.norm.logsf(z, loc=0, scale=1) / np.log(10)


def compute_ground_truth_statistics(ground_truth, sample_weight):
    assert np.array_equal(np.unique(ground_truth), [0, 1])
    order = (-ground_truth).argsort()
    label_1_count = int(ground_truth.sum())
    if sample_weight is None:
        ordered_sample_weight = None
    else:
        ordered_sample_weight = sample_weight[order]

    return order, label_1_count, ordered_sample_weight


def delong_roc_variance(ground_truth, predictions, sample_weight=None):
    """
    Computes ROC AUC variance for a single set of predictions
    Args:
       ground_truth: np.array of 0 and 1
       predictions: np.array of floats of the probability of being class 1
    """
    order, label_1_count, ordered_sample_weight = compute_ground_truth_statistics(
        ground_truth, sample_weight)
    predictions_sorted_transposed = predictions[np.newaxis, order]
    aucs, delongcov = fastDeLong(predictions_sorted_transposed, label_1_count, ordered_sample_weight)
    assert len(aucs) == 1, "There is a bug in the code, please forward this to the developers"
    return aucs[0], delongcov


# function for covid information 

def determine_covid_variant(covid_datetime):  # identify the number of days into pregnancy the first covid positive test was observed
  from datetime import date
  covid_date = datetime.date(covid_datetime)
  wt_start = date.fromisoformat('2020-03-05')
  a_start = date.fromisoformat('2021-04-24')
  d_start = date.fromisoformat('2021-07-03')
  o1_start = date.fromisoformat('2021-12-25')
  o1_end = date.fromisoformat('2022-03-25')
  o2_start = date.fromisoformat('2022-03-06')
  o2_end = date.fromisoformat('2022-05-05')
  if covid_date >= wt_start and covid_date < a_start:
    return 'wild_type'
  elif covid_date < d_start:
    return 'alpha'
  elif covid_date < o1_start:
    return 'delta'
  elif covid_date >= o1_start:
    return 'omicron'
  else:
    return None
determine_covid_variant_udf = F.udf(lambda covid_datetime: determine_covid_variant(covid_datetime), StringType())
# load dataframe 

covid_administered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_15_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_15_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = covid_notadministered.withColumn('covid_test_date',F.to_timestamp("covid_test_date"))


# Cohort Selection 


# covid hospitalized patients
# no prior coagulopathy 
# anticoagulant administered -2 days before the treatment + 14 days 
covid_administered = covid_administered.filter(F.col('covid_admissiondatetime_min').isNotNull()).filter(F.col('end_date_max')>=F.date_sub(F.col('treatment_onset_index'),2)).filter(F.col('start_date_min')<=F.date_add(F.col('treatment_onset_index'),14)).filter(F.col('prior_coagulopathy')==0)
covid_notadministered = covid_notadministered.filter(F.col('covid_admissiondatetime_min').isNotNull()).filter(F.col('prior_coagulopathy')==0)
contraindication_columns = ['contraindication_major_bleeding',
 'contraindication_peptic_ulcer',
 'contraindication_stage_2_hypertension',
 'contraindication_esophageal_varices',
 'contraindication_intracranial_mass',
 'contraindication_end_stage_liver_diseases',
 'contraindication_aneurysm',
 'contraindication_proliferative_retinopathy',
 'contraindication_risk_bleeding']
for column in contraindication_columns:
  covid_administered = covid_administered.filter(F.col(column)==0)
  covid_notadministered = covid_notadministered.filter(F.col(column)==0)
print (covid_administered.count())
print (covid_notadministered.count())


covid_administered = covid_administered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))


def categorize_ruca(code):
  if code is None:
    return None
  elif code < 4:
    return 'Metropolitan'
  elif code < 7:
    return 'Micropolitan'
  elif code < 10:
    return 'SmallTown'
  elif code < 99:
    return 'Rural'
  elif code == 99:
    return 'NotCoded'
  else:
    return None 
categorize_ruca_udf = F.udf(lambda code: categorize_ruca(code), StringType())


def determine_covid_guideline(covid_datetime):  # identify the number of days into pregnancy the first covid positive test was observed
  from datetime import date
  covid_date = datetime.date(covid_datetime)
  gl_change1 = date.fromisoformat('2020-12-17')
  gl_change2 = date.fromisoformat('2022-02-24')
  if covid_date < gl_change1 :
    return 'guideline0'
  elif covid_date < gl_change2:
    return 'guideline1'
  elif covid_date >= gl_change2:
    return 'guideline2'
  else:
    return None
determine_covid_guideline_udf = F.udf(lambda covid_datetime: determine_covid_guideline(covid_datetime), StringType())
covid_administered = covid_administered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))



# function for models 

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
  df = df.drop(columns=['gestational_days', 'insurance', 'race', 'lmp', 'trimester_of_covid_test', 'covid_variant', 'covid_guideline'])
  columns_to_be_normalized = [ 'covid_med_48hours_count', 'count_precovid2yrs_diagnoses', 'covid_total_med_post48hours_count']
  columns_to_be_normalized2 = [c for c in columns_to_be_normalized if c in df.columns]
  for c in columns_to_be_normalized2:
    df[c] = MinMaxScaler().fit_transform(np.array(df[c]).reshape(-1,1))
  
  print('Columns used for matching:')
  for col in df.columns:
    print(col)
  print('\n')
  print('\n')
  return(df)


def run_logistic_regression(df):
  i = 0
  acc_dec = 0
  y_test_dec=[] #Store y_test for every split
  y_pred_prob_dec=[] #Store probablity for positive label for every split
  y_pred_dec = [] # Store y_predict for every split 
  df_model = df
  y = df_model.anticoagulant_status
  X = df_model.drop(columns=['anticoagulant_status'])
  loo = LeaveOneOut() 
  features = X.columns 
  features_dict = {}
  features_dict['features'] = features 
  
  for train, test in loo.split(X):    #Leave One Out Cross Validation
    #Create training and test sets for split indices
    X_train = X.loc[train]  
    y_train = y.loc[train]
    X_train = handle_missing_svi(X_train, 'socioeconomic')
    X_train = handle_missing_svi(X_train, 'household composition and disability')
    X_train = handle_missing_svi(X_train, 'minority status and language')
    X_train = handle_missing_svi(X_train, 'housing type and transportation')
    X_test = X.loc[test]
    y_test = y.loc[test]
    undersample = RandomUnderSampler(random_state = seed_value, replacement = True)
    X_resampled, y_resampled = undersample.fit_resample(X_train, y_train)
    regr = LogisticRegression(max_iter=1000,  random_state = seed_value)
    regr = regr.fit(X_resampled, y_resampled)
    y_pred = regr.predict(X_test)
    acc_dec = acc_dec +  metrics.accuracy_score(y_test, y_pred)
    y_test_dec.append(y_test.to_numpy()[0])
    y_pred_prob_dec.append(regr.predict_proba(X_test)[:,1][0])
    y_pred_dec.append(y_pred[0])
    i+=1
    features_dict[i] = regr.coef_[0]
  return y_test_dec, y_pred_prob_dec, y_pred_dec, features_dict 


def get_auc(y_test_dec, y_pred_dec):
  alpha = .95
  auc, auc_cov = delong_roc_variance(np.array(y_test_dec),np.array(y_pred_dec))
  auc_std = np.sqrt(auc_cov)
  lower_upper_q = np.abs(np.array([0, 1]) - (1 - alpha) / 2)
  ci = stats.norm.ppf(
    lower_upper_q,
    loc=auc,
    scale=auc_std)
  ci[ci > 1] = 1
  print('AUC:', auc)
  print('AUC COV:', auc_cov)
  print('95% AUC CI:', ci)
  return auc, ci
def get_roc_curve_figure(fpr, tpr, threshold, auc, figure_label, figure_title):
  print(fpr, tpr, threshold) #For validation
  plt.figure(1, figsize=(5,5))
  plt.plot(fpr, tpr, lw=2, alpha=0.5, label='{0} LOOCV ROC (AUC = {1})'.format(figure_label, str(round(auc,2))))
  plt.plot([0, 1], [0, 1], linestyle='--', lw=2, color='k', label='Chance level', alpha=.8)
  plt.xlim([-0.05, 1.05])
  plt.ylim([-0.05, 1.05])
  plt.xlabel('False Positive Rate')
  plt.ylabel('True Positive Rate')
  plt.title('{0}'.format(figure_title))
  plt.legend(loc="lower right")
  plt.grid()
  plt.show()
def model_evaluation(y_test_dec, y_pred_dec):
  # Evaluate the performance of the model
  mean_absolute_error = metrics.mean_absolute_error(y_test_dec, y_pred_dec)
  mean_squared_error = metrics.mean_squared_error(y_test_dec, y_pred_dec)
  root_mean_squared_error = np.sqrt(metrics.mean_squared_error(y_test_dec, y_pred_dec))
  auc, auc_cov = delong_roc_variance(np.array(y_test_dec),np.array(y_pred_dec))
  r2 = metrics.r2_score(y_test_dec, y_pred_dec)
  model_dict = {'Mean Absolute Error':[mean_absolute_error],
             'Mean Squared Error': [mean_squared_error], 
             'Root Mean Squared Error': [root_mean_squared_error], 
             'Area Under the Curve': [auc], 
             'Coefficient of determination': [r2]}
  print (model_dict)
  return model_dict
def get_feature_importance(feature_dict, color, title):
  features_df = pd.DataFrame.from_dict(feature_dict)
  features_df['Importance average']= features_df.iloc[:, 1:len(feature_dict)].sum(axis=1)/len(feature_dict)
  features_df = features_df[['features','Importance average']].sort_values(by=['Importance average'], ascending = False)
  # plot feature importance
  labels = features_df['features']
  importance = features_df['Importance average']
  plt.bar([x for x in range(len(importance))], importance, color=color)
  plt.xticks(np.arange(0, len(importance), 1), labels=labels, rotation = 90)
  plt.ylabel('Contribution')
  plt.title (title)
  display(plt.show())
  return (features_df)
def get_precision_recall_AUC(y_test_dec, y_pred_dec, figure_title):
  import matplotlib.pyplot as plt
  from sklearn.datasets import make_classification
  from sklearn.metrics import (precision_recall_curve, PrecisionRecallDisplay)
  from sklearn.metrics import f1_score
  from sklearn.metrics import auc
  plt.figure(1, figsize=(8,6))
  precision, recall, _ = precision_recall_curve(y_test_dec, y_pred_dec)
  disp = PrecisionRecallDisplay(precision=precision, recall=recall)
  auc = metrics.auc(recall, precision)
  print(' auc=%.3f' % ( auc))
  disp.plot(name="Precision-recall AUC =%.2f" % (auc))
  plt.title(figure_title)
  plt.show()
  # calculate F1 score
  #f1 = f1_score(y_test_dec, y_pred_dec)
  # calculate precision-recall AUC





select_columns = ['vaccination_status', 'pregravid_bmi', 'age_at_start_dt', 'insurance', 'ethnic_group', 'ob_hx_infant_sex', 'covid_variant', 'trimester_of_covid_test', 'race', 'Parity', 'Gravidity', 'Preterm_history',  'count_precovid2yrs_diagnoses', 'illegal_drug_user', 'smoker', 'RPL_THEME1', 'RPL_THEME2','RPL_THEME3','RPL_THEME4', 'lmp','gestational_days', 'prior_covid_infection', 'ruca_categorization', 'covid_guideline']

column_dict = {
  'vaccination_status' : 'vaccination status',
  'pregravid_bmi' : 'pregravid BMI', 
  'age_at_start_dt' : 'maternal age',
  'ethnic_group' : 'ethnicity', 
  'ob_hx_infant_sex' : 'fetal sex', 
  'Parity' : 'parity', 
  'Gravidity' : 'gravidity', 
  'Preterm_history' : 'preterm history',
  'count_precovid2yrs_diagnoses' : 'pre-covid diagnoses count',
  'illegal_drug_user' : 'illegal drug use',
  'smoker' : 'smoking', 
  'RPL_THEME1' : 'socioeconomic', 
  'RPL_THEME2' : 'household composition and disability', 
  'RPL_THEME3' : 'minority status and language', 
  'RPL_THEME4' : 'housing type and transportation', 
  'prior_covid_infection' : 'prior covid infection', 
  'ruca_categorization' : 'rural-urban classification', 
  'race_white' : 'race_white', 
  'race_asian' : 'race_asian', 
  'race_black' : 'race_black', 
  'variant_wt' : 'variant_wt', 
  'variant_alpha' : 'variant_alpha', 
  'variant_delta' : 'variant_delta', 
  'variant_omicron' : 'variant_omicron', 
  'timing_1st' : '1st trimester infection', 
  'timing_2nd' : '2nd trimester infection', 
  'timing_3rd' : '3rd trimester infection', 
  'guideline0' : 'guideline_0',
  'guideline1' : 'guideline_1', 
  'guideline2' : 'guideline_2', 
  'commercial_insurance' : 'commercial insurance'
 }


 # format dataframes for propensity score matching
df_experimental_psm = format_dataframe_for_psm(covid_administered, select_columns)
print('# Number of women administered anticoagulant at time of delivery: ' + str(len(df_experimental_psm)))
df_experimental_psm = df_experimental_psm.rename(columns=column_dict)
df_control_psm = format_dataframe_for_psm(covid_notadministered, select_columns)
df_control_psm = df_control_psm.rename(columns=column_dict)
print('# Number of women who did not administer anticoagulant at time of delivery: ' + str(len(df_control_psm)))

# identify top features predicting who gets vaccinated using machine learning
# format for machine learning
df_experimental_psm['anticoagulant_status'] = 1
df_control_psm['anticoagulant_status'] = 0
df_final = shuffle(df_experimental_psm.append(df_control_psm, ignore_index=True), random_state=seed_value).fillna(0)


from collections import Counter
df_model = df_final
y = df_model.anticoagulant_status
X = df_model.drop(columns=['anticoagulant_status'])
counter = Counter(y)
print(counter)




ruca_col = ['SecondaryRUCACode2010']



SVI_score = ['RPL_THEMES', #overall tract summary ranking variable 
             'RPL_THEME1', #socioeconomic ranking variable 
             'RPL_THEME2', #household composition and disability 
             'RPL_THEME3', #minority status and language 
             'RPL_THEME4']  #housing type and transportation 


covid_administered = add_geo_features(covid_administered, 'svi2018_us', join_cols = ['pat_id', 'instance']).select(*(covid_administered.columns + SVI_score))
covid_notadministered = add_geo_features(covid_notadministered, 'svi2018_us', join_cols = ['pat_id', 'instance']).select(*(covid_notadministered.columns + SVI_score))

covid_administered = add_geo_features(covid_administered, 'ruca2010revised', join_cols = ['pat_id', 'instance']).select(*(covid_administered.columns + ruca_col))
covid_notadministered = add_geo_features(covid_notadministered, 'ruca2010revised', join_cols = ['pat_id', 'instance']).select(*(covid_notadministered.columns + ruca_col))
covid_administered = covid_administered.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))
covid_notadministered = covid_notadministered.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))



for svi in SVI_score:
  covid_administered = covid_administered.withColumn(svi, F.col(svi).cast(FloatType())).withColumn(svi, F.when(F.col(svi)<0, None).otherwise(F.col(svi)))
  covid_notadministered = covid_notadministered.withColumn(svi, F.col(svi).cast(FloatType())).withColumn(svi, F.when(F.col(svi)<0, None).otherwise(F.col(svi)))
  

covid_administered = covid_administered.withColumn('covid_48hours_med_count', F.col('covidmeds_day0') + F.col('covidmeds_day1'))
covid_notadministered = covid_notadministered.withColumn('covid_48hours_med_count', F.col('covidmeds_day0') + F.col('covidmeds_day1'))


lr_list = run_logistic_regression(df_final)
y_test_dec, y_pred_prob_dec, y_pred_dec, features_dict = lr_list 


auc, ci = get_auc(y_test_dec, y_pred_dec)
fpr,tpr,threshold = metrics.roc_curve(y_test_dec,y_pred_prob_dec, pos_label = 1)
print (ci)

get_roc_curve_figure(fpr, tpr, threshold, auc, "LR", figure_title = 'LR AUC-ROC Curve')

model_dict = model_evaluation(y_test_dec, y_pred_dec)
pd.DataFrame.from_dict(model_dict)


get_precision_recall_AUC(y_test_dec, y_pred_prob_dec, figure_title = 'LR Precision-Recall Curve')

def run_random_forest(df):
  i=0
  acc_dec = 0
  y_test_dec=[] #Store y_test for every split
  y_pred_dec=[] #Store probablity for positive label for every split
  df_model = df
  y = df_model.anticoagulant_status
  X = df_model.drop(columns=['anticoagulant_status'])
  loo = LeaveOneOut()
  features = X.columns
  features_dict = {}
  features_dict['features'] = features
  for train, test in loo.split(X):    #Leave One Out Cross Validation
    #Create training and test sets for split indices
    X_train = X.loc[train]  
    y_train = y.loc[train]
    X_train = handle_missing_svi(X_train, 'socioeconomic')
    X_train = handle_missing_svi(X_train, 'household composition and disability')
    X_train = handle_missing_svi(X_train, 'minority status and language')
    X_train = handle_missing_svi(X_train, 'housing type and transportation')
    X_test = X.loc[test]
    y_test = y.loc[test]
    undersample = RandomUnderSampler(random_state = seed_value, replacement = True)
    X_resampled, y_resampled = undersample.fit_resample(X_train, y_train)
    rf = RandomForestRegressor(n_estimators = 100,  random_state = seed_value)
    rf = rf.fit(X_resampled, y_resampled) 
    y_pred = rf.predict(X_test)
    y_test_dec.append(y_test.to_numpy()[0])
    y_pred_dec.append(y_pred[0])
    i+=1
    features_dict[i] = rf.feature_importances_
    if i%10==0:
      print (i)
  return y_test_dec, y_pred_dec, features_dict


rf_list = run_random_forest(df_final)

y_test_dec, y_pred_dec, features_dict = rf_list 
auc, ci = get_auc(y_test_dec, y_pred_dec)
fpr,tpr,threshold = metrics.roc_curve(y_test_dec,y_pred_dec)
print (ci)


get_roc_curve_figure(fpr, tpr, threshold, auc, "RF", figure_title = 'RF AUC-ROC Curve')


get_feature_importance(features_dict, color = 'goldenrod', title = 'Logistic Regression Feature Importance')

model_dict = model_evaluation(y_test_dec, y_pred_dec)
pd.DataFrame.from_dict(model_dict)


get_precision_recall_AUC(y_test_dec, y_pred_dec, figure_title = 'RF Precision-Recall Curve')

features_df = get_feature_importance(features_dict, color = 'c', title = 'Random Forest Feature Importance')

top_features = list(features_df['features'][0:7])
df_final_top = df_final[top_features + ['anticoagulant_status']]

top_rf_list = run_random_forest(df_final_top)


top_y_test_dec, top_y_pred_dec, top_features_dict = top_rf_list 
top_auc, top_ci = get_auc(top_y_test_dec, top_y_pred_dec)
top_fpr,top_tpr,top_threshold = metrics.roc_curve(top_y_test_dec,top_y_pred_dec)
print (top_ci)

get_roc_curve_figure(top_fpr,top_tpr,top_threshold , top_auc, "Random Forest", figure_title = 'RF Limited AUC-ROC Curve')

model_dict = model_evaluation(top_y_test_dec, top_y_pred_dec)
pd.DataFrame.from_dict(model_dict)


get_precision_recall_AUC(top_y_test_dec, top_y_pred_dec, figure_title = 'RF Limited Precision-Recall Curve')

top_features_df = get_feature_importance(top_features_dict, color = 'c', title = 'Random Forest Top Feature Importance')


def run_GBM(df):
  i=0
  acc_dec = 0
  y_test_dec=[] #Store y_test for every split
  y_pred_dec=[] #Store probablity for positive label for every split
  df_model = df
  y = df_model.anticoagulant_status
  X = df_model.drop(columns=['anticoagulant_status'])
  loo = LeaveOneOut()
  features = X.columns
  features_dict = {}
  features_dict['features'] = features
  for train, test in loo.split(X):    #Leave One Out Cross Validation
    #Create training and test sets for split indices
    X_train = X.loc[train]  
    y_train = y.loc[train]
    X_train = handle_missing_svi(X_train, 'socioeconomic')
    X_train = handle_missing_svi(X_train, 'household composition and disability')
    X_train = handle_missing_svi(X_train, 'minority status and language')
    X_train = handle_missing_svi(X_train, 'housing type and transportation')
    X_test = X.loc[test]
    y_test = y.loc[test]
    undersample = RandomUnderSampler(random_state = seed_value, replacement = True)
    X_resampled, y_resampled = undersample.fit_resample(X_train, y_train)
    
    
    gbm = GradientBoostingRegressor(random_state = seed_value)
    gbm.fit(X_resampled, y_resampled)
    y_pred = gbm.predict(X_test)
    y_test_dec.append(y_test.to_numpy()[0])
    y_pred_dec.append(y_pred[0])
    i+=1
    features_dict[i] = gbm.feature_importances_

    if i%10==0:
      print (i)
  return y_test_dec, y_pred_dec, features_dict


gbm_list = run_GBM(df_final)

y_test_dec, y_pred_dec, features_dict = gbm_list 
auc, ci = get_auc(y_test_dec, y_pred_dec)
fpr,tpr,threshold = metrics.roc_curve(y_test_dec,y_pred_dec)
print (ci)


get_roc_curve_figure(fpr, tpr, threshold, auc, "GBM", figure_title = 'GBM AUC-ROC Curve')

model_dict = model_evaluation(y_test_dec, y_pred_dec)
pd.DataFrame.from_dict(model_dict)

get_precision_recall_AUC(y_test_dec, y_pred_dec, figure_title = 'GBM Precision-Recall Curve')

features_df = get_feature_importance(features_dict, color = 'royalblue', title = 'GBM Feature Importance')

top_features = list(features_df['features'][0:7])
df_final_top = df_final[top_features + ['anticoagulant_status']]

top_gbm_list = run_GBM(df_final_top)

top_y_test_dec, top_y_pred_dec, top_features_dict = top_gbm_list 
top_auc, top_ci = get_auc(top_y_test_dec, top_y_pred_dec)
top_fpr,top_tpr,top_threshold = metrics.roc_curve(top_y_test_dec,top_y_pred_dec)
print (top_ci)


get_roc_curve_figure(top_fpr,top_tpr,top_threshold , top_auc, "GBM", figure_title = 'GBM Limited AUC-ROC Curve')


model_dict = model_evaluation(top_y_test_dec, top_y_pred_dec)
pd.DataFrame.from_dict(model_dict)


get_precision_recall_AUC(top_y_test_dec, top_y_pred_dec, figure_title = 'GBM Limited Precision-Recall Curve')


top_features_df = get_feature_importance(top_features_dict, color = 'c', title = 'GBM Top Feature Importance')

# comparison between original and limited model 

plt.figure(figsize=(5, 7))
plt.title('Anticoagulant Administration Status')
plt.plot(fpr, tpr, 'b', label = 'Original Model AUC = %0.2f' % auc, color='royalblue')
plt.plot(top_fpr, top_tpr, 'b', label = 'Limited Model AUC = %0.2f' % top_auc, color='mediumorchid')
plt.legend(loc = 'lower right')
plt.plot([0, 1], [0, 1],'r--', color='dimgray')
plt.xlim([0, 1])
plt.ylim([0, 1])
plt.ylabel('True Positive Rate')
plt.xlabel('False Positive Rate')
plt.show()

#shapley 

top_features = list(features_df['features'][0:7])
df_final_top = df_final[top_features + ['anticoagulant_status']]
y = df_final_top['anticoagulant_status']
X = df_final_top.drop(columns=['anticoagulant_status'])
X = handle_missing_svi(X, 'socioeconomic')
X = handle_missing_svi(X, 'household composition and disability')
X = handle_missing_svi(X, 'minority status and language')
X = handle_missing_svi(X, 'housing type and transportation')
undersample = RandomUnderSampler(random_state = seed_value, replacement = True)
X_resampled, y_resampled = undersample.fit_resample(X, y)
gbm = GradientBoostingRegressor(random_state = seed_value)
gbm.fit(X_resampled, y_resampled)


import warnings
import shap
with warnings.catch_warnings():
  warnings.filterwarnings('ignore')
  reg_explainer = shap.KernelExplainer(gbm.predict, X_resampled)
  reg_shap_values = reg_explainer.shap_values(X_resampled)
  shap.summary_plot(reg_shap_values, X_resampled)

  