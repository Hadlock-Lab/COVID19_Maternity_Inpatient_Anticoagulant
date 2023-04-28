# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.B_get_matched_cohorts.model_utilities


# Workflow of B_classification_model.py 
# 1. Set random seed
# 2. format dataframe to psm
# 5. Run PSM
# 5.1 Run PSM on all features before COVID-19 treatment onset began 
# 5.2 Run PSM on top features before COVID-19 treatment onset began 
# 5.3 Run PSM on sensitivity analysis accounting for medication count at the time of COVID-19 treatment onset
# 6. visaulization of covariate balance
# 7. Retrieve information on matched cohort 


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




# format dataframes for propensity score matching
df_experimental_psm = format_dataframe_for_psm(covid_administered, select_columns)
print('# Number of women administered anticoagulant at time of delivery: ' + str(len(df_experimental_psm)))
df_experimental_psm = df_experimental_psm.rename(columns=column_dict)
df_control_psm = format_dataframe_for_psm(covid_notadministered, select_columns)
df_control_psm = df_control_psm.rename(columns=column_dict)
print('# Number of women who did not administer anticoagulant at time of delivery: ' + str(len(df_control_psm)))


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
precovid_top_sa1_features = precovid_topfeatures  + ['covid inital med count']




df_experimental_psm_precovid = shuffle(df_experimental_psm[precovid_features], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid)))
df_control_psm_precovid = shuffle(df_control_psm[precovid_features], random_state=seed_value)


df_experimental_psm_precovid_top = shuffle(df_experimental_psm[precovid_topfeatures], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top)))
df_control_psm_precovid_top = shuffle(df_control_psm[precovid_topfeatures], random_state=seed_value)


df_experimental_psm_precovid_top_sa1 = shuffle(df_experimental_psm[precovid_top_sa1_features], random_state=seed_value)
print('# Number of women vaccinated women selected as a random subset for propensity score matching: ' + str(len(df_experimental_psm_precovid_top_sa1)))
df_control_psm_precovid_top_sa1 = shuffle(df_control_psm[precovid_top_sa1_features], random_state=seed_value)



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




# propensity score match with replacement - all features before COVID-19 treatment onset began
psm = PsmPy(df_final2_precovid, treatment='anticoagulant_status', indx='id', exclude = ['pat_id', 'instance', 'episode_id', 'child_episode_id'])
psm.logistic_ps(balance=True)
psm.knn_matched(matcher='propensity_logit', replacement=True, caliper=0.2)


# propensity score match with replacement - top features before COVID-19 treatment onset began 
psm_top = PsmPy(df_final2_precovid_top, treatment='anticoagulant_status', indx='id', exclude = ['pat_id', 'instance', 'episode_id', 'child_episode_id'])
psm_top.logistic_ps(balance=True)
psm_top.knn_matched(matcher='propensity_logit', replacement=True, caliper=0.2)

# propensity score match with replacement - sensitivity analysis including medication count at the time of COVID-19 treatment onset 
psm_top_sa1 = PsmPy(df_final2_precovid_top_sa1, treatment='anticoagulant_status', indx='id', exclude = ['pat_id', 'instance', 'episode_id', 'child_episode_id'])
psm_top_sa1.logistic_ps(balance=True)
psm_top_sa1.knn_matched(matcher='propensity_logit', replacement=True, caliper=0.2)


# visualization of covariate balance between control and treatment group  
psm.plot_match(Title='Side by side matched controls', Ylabel='Number of patients', Xlabel= 'Propensity logit', names = ['Administrated', 'Not administrated'], save=True)
psm.effect_size_plot(save=False)

psm_top.plot_match(Title='Side by side matched controls', Ylabel='Number of patients', Xlabel= 'Propensity logit', names = ['Administrated', 'Not administrated'], save=True)
psm_top.effect_size_plot(save=False)

psm_top_sa1.plot_match(Title='Side by side matched controls', Ylabel='Number of patients', Xlabel= 'Propensity logit', names = ['Administrated', 'Not administrated'], save=True)
psm_top_sa1.effect_size_plot(save=False)



# retreive full information on those matched patients 

df_matched = retrieve_matched_id_info(df_final2_precovid, psm.matched_ids)
df_temp = spark.createDataFrame(df_matched[['pat_id', 'instance', 'episode_id', 'child_episode_id']])
df_control = covid_notadministered
df_control.createOrReplaceTempView("control")
df_temp.createOrReplaceTempView("temp")
df_matched_final = spark.sql(
"""
SELECT c.*
FROM control AS c
INNER JOIN temp AS t 
ON c.pat_id = t.pat_id
  AND c.instance = t.instance
  AND c.episode_id = t.episode_id
  AND c.child_episode_id = t.child_episode_id
  """)


df_matched2 = retrieve_matched_id_info(df_final2_precovid_top, psm_top.matched_ids)
df_temp2 = spark.createDataFrame(df_matched2[['pat_id', 'instance', 'episode_id', 'child_episode_id']])
df_control = covid_notadministered
df_control.createOrReplaceTempView("control")
df_temp2.createOrReplaceTempView("temp")
df_matched_final_top = spark.sql(
"""
SELECT c.*
FROM control AS c
INNER JOIN temp AS t 
ON c.pat_id = t.pat_id
  AND c.instance = t.instance
  AND c.episode_id = t.episode_id
  AND c.child_episode_id = t.child_episode_id
  """)


df_matched3 = retrieve_matched_id_info(df_final2_precovid_top_sa1, psm_top_sa1.matched_ids)
df_temp3 = spark.createDataFrame(df_matched3[['pat_id', 'instance', 'episode_id', 'child_episode_id']])
df_control = covid_notadministered
df_control.createOrReplaceTempView("control")
df_temp3.createOrReplaceTempView("temp")
df_matched_final_top_sa1 = spark.sql(
"""
SELECT c.*
FROM control AS c
INNER JOIN temp AS t 
ON c.pat_id = t.pat_id
  AND c.instance = t.instance
  AND c.episode_id = t.episode_id
  AND c.child_episode_id = t.child_episode_id
  """)


