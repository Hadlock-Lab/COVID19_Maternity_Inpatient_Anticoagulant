Author: Samantha Piekos
Date: 10/27/22


# load environment
from datetime import date
import numpy as np
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import *


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.maternity_cohort_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.sars_cov_2_cohort_functions
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.summary_block


# get basic cohorts
df_cohort_maternity = get_maternity_cohort_custom()
df_cohort_covid = get_sars_cov_2_cohort()
# this function allows you to work pyspark df in sql format 
df_cohort_maternity.createOrReplaceTempView("mom")
df_cohort_covid.createOrReplaceTempView("covid")


# code from Cheng Dai
# get insurance status of pregnant women
def clean_payors(x):
  # Reclassify insurance by looking at the payor 
  if pd.isna(x['payor']):
    if x['financialclass'] == 'OTHER GOVERNMENT':
      return 'Other'
    elif x['financialclass'] == 'UNIFORM MEDICAL':
      return 'Other'
    else:
      return x['financialclass']
  if 'medicare' in x['payor'].lower():
    return 'Medicare'
  elif 'medicaid' in x['payor'].lower() or 'medi-cal' in x['payor'].lower() :
    return 'Medicaid'
  elif 'uninsured' in x['payor'].lower() or 'preexisting condition' in x['payor'].lower():
    return 'Self-pay'
  elif 'workers compensation' in x['payor'].lower() or 'victims' in x['payor'].lower():
    return 'Other'
  elif 'patient responsibility' in x['payor'].lower():
    return 'Self-pay'
  else:
    return 'Commercial'


insurnance_map = {'AETNA':'3_Commercial',
'BASIC HEALTH PLAN':'3_Commercial',
'CIGNA':'3_Commercial',
'CIGNA FIRST CHOICE':'3_Commercial',
'Capitation':'3_Commercial',
'Commercial':'3_Commercial',
'FIRST CHOICE':'3_Commercial',
'GREAT WEST':'3_Commercial',
'HEALTHY OPTIONS':'3_Commercial',
'LABOR AND INDUSTRIES':'3_Commercial',
'MEDICAID OTHER':'1_Medicaid',
'MEDICARE HMO':'2_Medicare',
'MEDICARE HMO MEDICAL GROUP':'2_Medicare',
'Managed Care':'3_Commercial',
'Medicaid':'1_Medicaid',
'Medicaid HMO':'1_Medicaid',
'Medicare':'2_Medicare',
'Medicare HMO':'2_Medicare',
'OTHER GOVERNMENT':'5_Other-Government',
'OTHER MANAGED CARE':'3_Commercial',
'Other':'6_Other',
'PACIFICARE':'3_Commercial',
'PACMED':'3_Commercial',
'PREMERA':'3_Commercial',
'REGENCE':'3_Commercial',
'Self-pay':'4_Uninsured-Self-Pay',
'UNIFORM MEDICAL':'5_Other-Government',
'UNITED':'3_Commercial',
"Worker's Comp":"6_Other",
"Uninsured":"7_Uninsured"}


guarantor_query = \
'''
select 
  a.pat_id,
  a.instance,
  b.guarantorpatientrelationship,
  c.type,
  c.financialclass,
  e.payor
from 
  mom as a 
    join rdp_phi.guarantorpatient as b on a.pat_id == b.pat_id and a.instance == b.instance
    join rdp_phi.guarantor as c on b.account_id == c.account_id and b.instance == c.instance
    left join rdp_phi.guarantorcoverage as d on b.account_id == d.account_id and b.instance == d.instance
    left join rdp_phi.coverage as e on d.coverage_id == e.coverage_id and d.instance == e.instance
where 
  guarantorpatientrelationship IS NOT NULL 
  and c.type IS NOT NULL 
  and financialclass IS NOT NULL
  and c.type == 'Personal/Family'
'''
guarantor_df = spark.sql(guarantor_query)
guarantor_df = guarantor_df.toPandas()

guarantor_filtered = guarantor_df.sort_values('pat_id').drop_duplicates()
print(f'Number of individuals with coverage: {len(guarantor_filtered["pat_id"].unique())}')

guarantor_filtered['financialclass'] = guarantor_filtered.apply(lambda pat: clean_payors(pat), axis = 1)
guarantor_filtered['insurance'] = guarantor_filtered['financialclass'].apply(insurnance_map.get)

guarantor_filtered_renamed = guarantor_filtered[['pat_id','instance','insurance']].sort_values(['pat_id','insurance'])
guarantor_filtered_renamed = guarantor_filtered_renamed.groupby('pat_id').first()
guarantor_filtered_renamed['insurance'] = guarantor_filtered_renamed['insurance'].str.split('_').str[1]
guarantor_filtered_renamed = guarantor_filtered_renamed.reset_index()
guarantor_filtered_renamed = spark.createDataFrame(guarantor_filtered_renamed)
guarantor_filtered_renamed = guarantor_filtered_renamed.withColumnRenamed('pat_id', 'insurance_pat_id')
guarantor_filtered_renamed = guarantor_filtered_renamed.withColumnRenamed('instance', 'insurance_instance')
guarantor_filtered_renamed.createOrReplaceTempView("maternity_insurance")

write_data_frame_to_sandbox(guarantor_filtered_renamed, 'snp2_maternity_insurance', sandbox_db='rdp_phi_sandbox', replace=True)
df_maternity_insurance = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_maternity_insurance")
df_maternity_insurance.createOrReplaceTempView("maternity_insurance")


# add insuarance to maternity cohort
df_cohort_maternity = spark.sql(
"""
SELECT /*+ RANGE_JOIN(c, 50) */ m.*, mi.*
FROM mom AS m
INNER JOIN maternity_insurance AS mi
ON m.pat_id = mi.insurance_pat_id
  AND m.instance = mi.insurance_instance
""")
df_cohort_maternity = df_cohort_maternity \
  .drop("insurance_pat_id") \
  .drop("insurance_instance")


# save basic maternity and covid cohorts
# save cohort dataframes
write_data_frame_to_sandbox(df_cohort_maternity, 'snp2_cohort_maternity', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_cohort_covid, 'snp2_cohort_covid', sandbox_db='rdp_phi_sandbox', replace=True)


# reformat dataframes
# Rename duplicate column names in the df_cohort_covid dataframe
#df_cohort_covid = df_cohort_covid.select(col("").alias("name"), col("askdaosdka").alias("age")
df_cohort_covid = df_cohort_covid.withColumnRenamed('pat_id', 'covid_pat_id')
df_cohort_covid = df_cohort_covid.withColumnRenamed('instance', 'covid_instance')
df_cohort_covid = df_cohort_covid.withColumnRenamed('race', 'covid_race')
df_cohort_covid = df_cohort_covid.withColumnRenamed('ethnic_group', 'covid_ethnic_group')
df_cohort_covid.createOrReplaceTempView("covid")


# define cohort of people that have had COVID-19 and were pregnant during the pandemic
df_covid_mom = spark.sql(
"""
SELECT /*+ RANGE_JOIN(c, 50) */ m.*, c.*
FROM covid AS c
INNER JOIN mom AS m 
ON c.covid_pat_id = m.pat_id
  AND c.covid_instance = m.instance
WHERE c.results_category == '>= 1 positive'
  AND m.ob_delivery_delivery_date >= date'2020-03-05'
""")
df_covid_mom = df_covid_mom \
  .drop("covid_pat_id") \
  .drop("covid_instance") \
  .drop("covid_race") \
  .drop("covid_ethnic_group") 


# define cohort of people that have not had COVID-19 and have been pregnant duirng the pandemic
df_no_covid_mom = spark.sql(
"""
SELECT /*+ RANGE_JOIN(c, 50) */ m.*, c.*
FROM covid AS c
INNER JOIN mom AS m 
ON c.covid_pat_id = m.pat_id 
  AND c.covid_instance = m.instance
WHERE c.results_category == '>= 1 negative (no positive)'
  AND m.ob_delivery_delivery_date > c.first_ordering_datetime
  AND m.ob_delivery_delivery_date >= date'2020-03-05'
""")
df_no_covid_mom = df_no_covid_mom \
  .drop("covid_pat_id") \
  .drop("covid_instance") \
  .drop("covid_race") \
  .drop("covid_ethnic_group") 
#  AND m.age_at_start_dt >= 18
#  AND m.age_at_start_dt < 45



# restrict cohorts based on study restrictions
df_covid_mom = spark.sql(
"""
SELECT /*+ RANGE_JOIN(c, 50) */ m.*, c.*
FROM covid AS c
INNER JOIN mom AS m 
ON c.covid_pat_id = m.pat_id
  AND c.covid_instance = m.instance
WHERE c.results_category == '>= 1 positive'
  AND m.ob_delivery_delivery_date >= date'2020-03-05'
  AND m.age_at_start_dt >= 18
  AND m.age_at_start_dt < 45
  AND m.gestational_days >= 140
  AND m.number_of_fetuses == 1
""")
df_covid_mom = df_covid_mom \
  .drop("covid_pat_id") \
  .drop("covid_instance") \
  .drop("covid_race") \
  .drop("covid_ethnic_group").distinct()


df_no_covid_mom = spark.sql(
"""
SELECT /*+ RANGE_JOIN(c, 50) */ m.*, c.*
FROM covid AS c
INNER JOIN mom AS m 
ON c.covid_pat_id = m.pat_id 
  AND c.covid_instance = m.instance
WHERE c.results_category == '>= 1 negative (no positive)'
  AND m.ob_delivery_delivery_date > c.first_ordering_datetime
  AND m.ob_delivery_delivery_date >= date'2020-03-05'
  AND m.age_at_start_dt >= 18
  AND m.age_at_start_dt < 45
  AND m.gestational_days >= 140
  AND m.number_of_fetuses == 1
""")
df_no_covid_mom = df_no_covid_mom \
  .drop("covid_pat_id") \
  .drop("covid_instance") \
  .drop("covid_race") \
  .drop("covid_ethnic_group").distinct()


# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity', sandbox_db='rdp_phi_sandbox', replace=True)
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity")


# evaluate delivery date ranges for cohorts
df_covid_mom.agg({'ob_delivery_delivery_date': 'min'}).show()
df_covid_mom.agg({'ob_delivery_delivery_date': 'max'}).show()
df_no_covid_mom.agg({'ob_delivery_delivery_date': 'min'}).show()
df_no_covid_mom.agg({'ob_delivery_delivery_date': 'max'}).show()
df_cohort_maternity.agg({'ob_delivery_delivery_date': 'min'}).show()
df_cohort_maternity.agg({'ob_delivery_delivery_date': 'max'}).show()


# evaluate most recent first covid test date within cohorts
df_covid_mom.agg({'first_ordering_datetime': 'max'}).show()
df_cohort_covid.agg({'first_ordering_datetime': 'max'}).show()