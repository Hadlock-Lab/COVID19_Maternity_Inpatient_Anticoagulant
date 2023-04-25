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


spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_16_102422")
covid_administered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_16_102422")
covid_notadministered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = covid_notadministered.withColumn('covid_test_date',F.to_timestamp("covid_test_date"))



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



covid_administered = add_geo_features(covid_administered, 'svi2018_us', join_cols = ['pat_id', 'instance']).select(*(covid_administered.columns + SVI_score))
covid_notadministered = add_geo_features(covid_notadministered, 'svi2018_us', join_cols = ['pat_id', 'instance']).select(*(covid_notadministered.columns + SVI_score))

covid_administered = add_geo_features(covid_administered, 'ruca2010revised', join_cols = ['pat_id', 'instance']).select(*(covid_administered.columns + ruca_col))
covid_notadministered = add_geo_features(covid_notadministered, 'ruca2010revised', join_cols = ['pat_id', 'instance']).select(*(covid_notadministered.columns + ruca_col))
covid_administered = covid_administered.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))
covid_notadministered = covid_notadministered.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))



for svi in SVI_score:
  covid_administered = covid_administered.withColumn(svi, F.col(svi).cast(FloatType())).withColumn(svi, F.when(F.col(svi)<0, None).otherwise(F.col(svi)))
  covid_notadministered = covid_notadministered.withColumn(svi, F.col(svi).cast(FloatType())).withColumn(svi, F.when(F.col(svi)<0, None).otherwise(F.col(svi)))


covid_administered = covid_administered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))



# sensitivity anlaysis columns 


select_columns = ['vaccination_status', 'pregravid_bmi', 'age_at_start_dt', 'insurance', 'ethnic_group', 'ob_hx_infant_sex', 'covid_variant', 'trimester_of_covid_test', 'race', 'Parity', 'Gravidity', 'Preterm_history',  'count_precovid2yrs_diagnoses', 'illegal_drug_user', 'smoker', 'RPL_THEME1', 'RPL_THEME2','RPL_THEME3','RPL_THEME4', 'lmp','gestational_days', 'prior_covid_infection', 'ruca_categorization', 'covid_guideline', 'covid_med_initial_count']


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
  'commercial_insurance' : 'commercial insurance',
  'covid_med_initial_count' : 'covid initial med count'
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
df_final.head()



top_features = ['pre-covid diagnoses count', '3rd trimester infection', 'socioeconomic', 'household composition and disability', 'minority status and language', 'housing type and transportation', 'covid initial med count', 'variant_omicron', 'anticoagulant_status']



# summarize class distribution
from collections import Counter
df_model = df_final[top_features]
y = df_model.anticoagulant_status
X = df_model.drop(columns=['anticoagulant_status'])
counter = Counter(y)
print(counter)


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

gbm_list = run_GBM(df_model)  
y_test_dec, y_pred_dec, features_dict = gbm_list 
auc, ci = get_auc(y_test_dec, y_pred_dec)
fpr,tpr,threshold = metrics.roc_curve(y_test_dec,y_pred_dec)
print (ci)


get_roc_curve_figure(fpr, tpr, threshold, auc, "GBM", figure_title = 'GBM AUC-ROC Curve')

model_dict = model_evaluation(y_test_dec, y_pred_dec)
pd.DataFrame.from_dict(model_dict)



features_df = get_feature_importance(features_dict, color = 'royalblue', title = 'GBM Feature Importance')



y = df_model['anticoagulant_status']
X = df_model.drop(columns=['anticoagulant_status'])
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