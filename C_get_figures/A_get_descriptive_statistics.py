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



# table of contents

# 1. trimester of covid infection 
# 2. timeline version of anticoagulant administration 
# 3. based on guideline change 
# 4. based on variant version 
# 5. age distribution, pearson correlation, pre-covid diagnosis count, initial medication count 


# load cohorts 
covid_administered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = spark.sql('SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_16_102422').dropDuplicates(['pat_id','episode_id','child_episode_id'])
covid_notadministered = covid_notadministered.withColumn('covid_test_date',F.to_timestamp("covid_test_date"))

# feature engineering 



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
covid_administered = covid_administered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_guideline', determine_covid_guideline_udf(F.col('covid_test_date')))
covid_administered = covid_administered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))
covid_notadministered = covid_notadministered.withColumn('covid_variant', determine_covid_variant_udf(F.col('covid_test_date')))


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
  

covid_administered = covid_administered.withColumn('days_from_treatment_onset', F.datediff(F.col('med_administration_date'), F.col('treatment_onset_index')))




select_columns = ['vaccination_status', 'pregravid_bmi', 'age_at_start_dt', 'insurance', 'ethnic_group', 'ob_hx_infant_sex', 'covid_variant', 'trimester_of_covid_test', 'race', 'Parity', 'Gravidity', 'Preterm_history',  'count_precovid2yrs_diagnoses', 'illegal_drug_user', 'smoker', 'RPL_THEME1', 'RPL_THEME2','RPL_THEME3','RPL_THEME4', 'lmp','gestational_days', 'prior_covid_infection', 'ruca_categorization', 'covid_guideline']


# format dataframes for propensity score matching
df_experimental_psm = format_dataframe_for_psm(covid_administered, select_columns)
print('# Number of women administered anticoagulant at time of delivery: ' + str(len(df_experimental_psm)))
df_control_psm = format_dataframe_for_psm(covid_notadministered, select_columns)
print('# Number of women who did not administer anticoagulant at time of delivery: ' + str(len(df_control_psm)))


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


df_experimental_psm = df_experimental_psm.rename(columns=column_dict)
df_control_psm = df_control_psm.rename(columns=column_dict)
df_experimental_psm['anticoagulant use'] = 1
df_control_psm['anticoagulant use'] = 0


final_pd = df_experimental_psm.append(df_control_psm)


# get correlation matrix 

cormat = final_pd.corr()
round(cormat,2)

# get correlation to anticoagulant administration status figure 

au_corr = cormat[['anticoagulant use']].sort_values(ascending=False, by = 'anticoagulant use')
au_corr.drop(labels = 'anticoagulant use', axis = 0, inplace = True)

import matplotlib.colors as mcolors
from matplotlib.colors import TwoSlopeNorm
fig, ax = plt.subplots(figsize =(7,10))
norm = TwoSlopeNorm(vmin=-1, vcenter =0, vmax=1)
colors = [plt.cm.RdYlGn(norm(c[0])) for c in au_corr.values]
plt.barh(au_corr.index, au_corr['anticoagulant use'], color = colors)
plt.title(label = 'Correlation to Anticoagulant Administration Status')
plt.show()



# get correlation heatmap 


# Increase the size of the heatmap.
plt.figure(figsize=(22, 15))
# Store heatmap object in a variable to easily access it when you want to include more features (such as title).
# Set the range of values to be displayed on the colormap from -1 to 1, and set the annotation to True to display the correlation values on the heatmap.
heatmap = sns.heatmap(cormat.corr(), vmin=-1, vmax=1, annot=True)
# Give a title to the heatmap. Pad defines the distance of the title from the top of the heatmap.
heatmap.set_title('Correlation Heatmap', fontdict={'fontsize':30}, pad=12);
for label in heatmap.get_yticklabels():
  label.set_size(13)
for label in heatmap.get_xticklabels():
  label.set_size(13)


# get distribution plot of 'date difference between covid treatment onset and anticoagulant administration date'

anti_pd = covid_administered.toPandas()
noanti_pd = covid_notadministered.toPandas()


anti_pd.days_from_treatment_onset.describe()

plt.figure(figsize=(10, 6))
p = sns.histplot(data=anti_pd, x='days_from_treatment_onset', discrete = True)
p.set_xlabel("days from COVID-19 treatment onset", fontsize = 15)
p.set_ylabel("number of patients", fontsize = 15)
p.set_title('Date Difference Between Anticoagulant Administration and COVID-19 Treatment Onset', fontdict={'fontsize':15}, pad=12)




# prevalence rate of anticoagulant administration based on covid variant 
anti_variant_dict = anti_pd.covid_variant.value_counts().to_dict()
noanti_variant_dict = noanti_pd.covid_variant.value_counts().to_dict()
total_variant_dict = {}
prevalence_rate_dict = {}
for key in anti_variant_dict.keys():
  total_variant_dict[key] = anti_variant_dict[key] + noanti_variant_dict[key]
  prevalence_rate_dict[key] = anti_variant_dict[key]/total_variant_dict[key]
total_variant_pd = pd.DataFrame.from_dict(total_variant_dict, orient='index', columns=['covid_variant'])
prevalence_rate_pd = pd.DataFrame.from_dict(prevalence_rate_dict, orient='index', columns=['covid_variant'])
prevalence_rate_pd = prevalence_rate_pd.reset_index().rename(columns = {'index':'covid_variant', 'covid_variant':'prevalence rate'})

prevalence_rate_pd['prevalence rate'] = round(prevalence_rate_pd['prevalence rate'],2)


plt.figure(figsize=(8, 6))
plt.ylim(0,1)
p = sns.barplot(x="covid_variant", y="prevalence rate", data=prevalence_rate_pd, order=["wild_type", "alpha", "delta", "omicron"],  palette=['red','blue','orange','purple'])
p.set_xlabel("COVID-19 variant", fontsize = 15)
p.set_ylabel("anticoagulant administration rate", fontsize = 15)
p.set_title('Anticoagulant Administration Rate Based on COVID-19 Variant', fontdict={'fontsize':15}, pad=12)
p.set_xticklabels(["WT", "Alpha", "Delta", "Omicron"], fontsize = 14)
p.bar_label(p.containers[0])


# prevalence rate of anticoagulant administration based on NIH guideline 
guideline_administration_dict = {}
guideline_administration_dict['guideline0'] = anti_pd.covid_guideline.value_counts()['guideline0']/(noanti_pd.covid_guideline.value_counts()['guideline0']+anti_pd.covid_guideline.value_counts()['guideline0'])
guideline_administration_dict['guideline1'] = anti_pd.covid_guideline.value_counts()['guideline1']/(noanti_pd.covid_guideline.value_counts()['guideline1']+anti_pd.covid_guideline.value_counts()['guideline1'])
guideline_administration_dict['guideline2'] = anti_pd.covid_guideline.value_counts()['guideline2']/(noanti_pd.covid_guideline.value_counts()['guideline2']+anti_pd.covid_guideline.value_counts()['guideline2'])

guideline_df = pd.DataFrame.from_dict(guideline_administration_dict, orient='index').reset_index()

guideline_df  = guideline_df.rename(columns={"index": "guideline", 0: "administration rate"})
guideline_df['administration rate'] = round(guideline_df['administration rate'],2)

plt.figure(figsize=(10, 6))
plt.ylim(0, 1)

p = sns.barplot(data=guideline_df, x="guideline", y="administration rate")
plt.xticks([0,1,2], labels=['no guideline', 'first update\n(2020-12-17)', 'second update\n(2022-02-24)'], fontsize = 15)
p.set_ylabel("anticoagulant administration rate", fontsize = 15)
p.set_xlabel("COVID-19 Antithrombotic Guideline", fontsize = 15)
p.set_title('Anticoagulant Administration Rate Based on COVID-19 Antithrombotic Guideline Update', fontdict={'fontsize':15
}, pad=12)
p.bar_label(p.containers[0])


# timeline figure - anticoagulant administration and number of hospitalization 

select_columns = ['covid_variant', 'covid_test_date']
anti_pd_covidtest = anti_pd[select_columns]
anti_pd_covidtest['anticoagulant_status'] = 1
noanti_pd_covidtest = noanti_pd[select_columns]
noanti_pd_covidtest['anticoagulant_status'] = 0 
total_covidtest = anti_pd_covidtest.append(noanti_pd_covidtest)
total_ag = total_covidtest.groupby(['covid_test_date', 'covid_variant']).agg({'anticoagulant_status':['count', 'sum']})
total_ag.columns = ['hospitalized COVID-19 patients', 'administration count']
total_ag['anticoagulant administration rate'] = total_ag['administration count']/total_ag['hospitalized COVID-19 patients']
total_ag = total_ag.reset_index()
total_ag["covid test date"] = pd.to_datetime(total_ag["covid_test_date"])


plt.figure(figsize=(15, 8))
ax1 = plt.subplot()
ax2 = ax1.twinx()
sns.scatterplot(data=total_ag, x='covid test date', y='anticoagulant administration rate', hue = "covid_variant",  ax=ax1, hue_order = ["wild_type", "alpha", "delta", "omicron"], 
palette = {'wild_type': 'red', 'alpha': 'blue', 'delta':'orange', 'omicron':'purple'})
sns.lineplot(data=total_ag, x='covid test date', y='hospitalized COVID-19 patients', color = 'black', ax=ax2, alpha = 0.2, legend = 'full', label = 'hospitalized\nCOVID-19 patients' )
ax2.legend(loc='center right', bbox_to_anchor=(1, 0.7))
ax1.legend(title='COVID-19 variant', loc='upper right', labels=['WT', 'Alpha', 'Delta', 'Omicron'])
#plt.legend([ax2], ['count of covid positive patients'])
xposition = [pd.to_datetime('2020-12-17'), pd.to_datetime('2022-02-24')]
ax1.set_xlabel("COVID-19 positive test date", fontsize = 15)
ax1.set_ylabel("anticoagulant administration rate", fontsize = 15)
ax2.set_ylabel("hospitalized COVID-19 patients count", fontsize = 15)
ax1.set_title('Anticoagulant Administration Rate Timeline', fontdict={'fontsize':20}, pad=12)
for xc in xposition:
    plt.axvline(x=xc, color='black', linestyle='-', linewidth = 3)


 # fisher exact result based on covid variant 

 from scipy.stats import fisher_exact
 def get_variant(variant):
  return [total_variant_dict[variant]-anti_variant_dict[variant], anti_variant_dict[variant]]

print (fisher_exact([get_variant('wild_type'), get_variant('alpha')]))
print (fisher_exact([get_variant('wild_type'), get_variant('delta')]))
print (fisher_exact([get_variant('wild_type'), get_variant('omicron')]))
print (fisher_exact([get_variant('alpha'), get_variant('delta')]))
print (fisher_exact([get_variant('alpha'), get_variant('omicron')]))
print (fisher_exact([get_variant('delta'), get_variant('omicron')]))



# get figures for continuous variables 


def make_n_violin_box_plot(dict_outliers):
  dict_filtered = {}
  for k, v in dict_outliers.items():
      if len(v) > 9:
          dict_filtered[k] = v
  fig, ax = plt.subplots()

  # Create a plot
  ax.violinplot(list(dict_filtered.values()))
  ax.boxplot(list(dict_filtered.values()))

  # add x-tick labels
  xticklabels = dict_filtered.keys()
  ax.set_ylabel('Number of Unique Medications')
  ax.set_xticks([1,2])
  ax.set_xticklabels(xticklabels, rotation = 15)
  ax.set_ylim([0, 70])
  plt.show()
def convert_dict_to_df(d, y_label):
  df = pd.DataFrame(columns = ['Group', y_label])
  for k, v in d.items():
    for days in v:
      df = df.append({'Group': k, y_label: days}, ignore_index = True)
  df[y_label] = df[y_label].astype(int)
  return df

def create_violin_plot(d, y_label, list_colors):
  df = convert_dict_to_df(d, y_label)
  ax = sns.violinplot(x='Group', y=y_label, data=df, palette=list_colors)
  ax.set(xlabel=None)
  ax.set_ylim([0, 80])
  plt.show()
  
def run_mann_whitney_u_test(df_1, df_2, k):
  data_1, data_2 = [], []
  for index, row in df_1.iterrows():
    if row[k] is not None:
      data_1.append(float(row[k]))
  for index, row in df_2.iterrows():
    if row[k] is not None:
      data_2.append(float(row[k]))
  return(stats.mannwhitneyu(data_1, data_2))


def make_n_violin_box_plot(dict_outliers):
  dict_filtered = {}
  for k, v in dict_outliers.items():
      if len(v) > 9:
          dict_filtered[k] = v
  fig, ax = plt.subplots()

  # Create a plot
  ax.violinplot(list(dict_filtered.values()))
  ax.boxplot(list(dict_filtered.values()))

  # add x-tick labels
  xticklabels = dict_filtered.keys()
  ax.set_ylabel('Number of Pre-covid Unique Diagnoses')
  ax.set_xticks([1,2])
  ax.set_xticklabels(xticklabels, rotation = 15)
  ax.set_ylim([0, 100])
  plt.show()