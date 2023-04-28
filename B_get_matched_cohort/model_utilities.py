# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks


# load packages 
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *

import pyspark.sql.functions as F
from pyspark.sql.types import *

import math
import pandas as pd
import numpy as np
import scipy
import sklearn 
import matplotlib.pyplot as plt
from scipy import stats

from collections import Counter

from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.neighbors import NearestNeighbors
from sklearn import datasets, linear_model
from sklearn import metrics
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import train_test_split, KFold, cross_val_score, LeaveOneOut
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.utils import resample, shuffle 
from sklearn.metrics import accuracy_score, average_precision_score

from numpy import mean, absolute, sqrt

import imblearn as imb
from imb.over_sampling import SMOTE
from imb.under_sampling import RandomUnderSampler
from imb.pipeline import Pipeline

from psmpy import PsmPy
from psmpy.functions import cohenD
from psmpy.plotting import *



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


def retrieve_matched_id_info(df_final, df_psm):
  df_matched = pd.DataFrame(columns=list(df_final.columns))
  for item in list(df_psm['matched_ID']):
    row = df_final.loc[df_final['id'] == item]
    df_matched = df_matched.append(row, ignore_index=True)
  print('# Number of Matched Anticoagulant Unadministered Patients: ' + str(len(df_matched)))
  return df_matched