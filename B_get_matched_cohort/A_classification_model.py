# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.B_get_matched_cohorts.model_utilities


# Workflow of A_classification_model.py 
# 1. Set random seed
# 2. load dataframe and filter inclusion and exclusion criteria  
# 3. Add features 
# 4. format dataframe to run ML Model 
# 5. Run Classification Models
# 5.1 compare with top features ML performance  
# 6. Feature importance 




# 1. Set a seed value
seed_value= 457
# a. Set `PYTHONHASHSEED` environment variable at a fixed value
import os
os.environ['PYTHONHASHSEED']=str(seed_value)
# b. Set `python` built-in pseudo-random generator at a fixed value
import random
random.seed(seed_value)
# c. Set `numpy` pseudo-random generator at a fixed value
import numpy as np
np.random.seed(seed_value)

# feature engineering 
# function for geocoding 


# 2.load dataframe and filter inclusion and exclusion criteria  
covid_administered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_15_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_15_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = covid_notadministered.withColumn('covid_test_date',F.to_timestamp("covid_test_date"))



# covid hospitalized patients
# no prior coagulopathy 
# no contraindication
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

# 3. Add features 
# 3.1 COVID-19 variant
covid_administered = covid_administered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))



# 3.2 NIH guideline 
covid_administered = covid_administered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))

 
# 3.3 Add geocodes (SVI & RUCA code )

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
  



# 4. format dataframe to run ML Model 

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





# 5. Run classification models 



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

  