# Author: Samantha Piekos
# Date: 10/20/22


# load environment
from datetime import date
import numpy as np
import pandas as pd
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import *

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
sql("SET spark.databricks.delta.formatCheck.enabled=false")


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.cohort_covid_pregnancy_functions
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.geo_features
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.sars_cov_2_cohort_functions
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.social_history


# load cohorts
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])


# create patient condition tables
# save patient condition dataframes
write_all_pat_conditions_df(df_covid_mom, 'snp2_covid_maternity_conditions')
write_all_pat_conditions_df(df_no_covid_mom, 'snp2_no_covid_maternity_conditions')

# load patient condition dataframes
df_pat_conditions_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_covid_maternity_conditions")
df_pat_conditions_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_no_covid_maternity_conditions")


# add chronic diabetes and chronic hypertension to cohort tables
df_covid_mom = add_chronic_conditions_during_pregnancy_to_df(df_covid_mom, df_pat_conditions_covid_mom)
print('\n')
df_no_covid_mom = add_chronic_conditions_during_pregnancy_to_df(df_no_covid_mom, df_pat_conditions_no_covid_mom)

# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_expanded")


# add gestational hypertension and gestational diabetes to cohorts
df_covid_mom = add_pregnancy_gestational_conditions_to_df(df_covid_mom, df_pat_conditions_covid_mom)
print('\n')
df_no_covid_mom = add_pregnancy_gestational_conditions_to_df(df_no_covid_mom, df_pat_conditions_no_covid_mom)

# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded_2', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded_2', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_2")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_expanded_2")


# add preeclampsia related conditions to cohorts
df_covid_mom = add_preeclampsia_conditions_to_df(df_covid_mom, df_pat_conditions_covid_mom)
print('\n')
df_no_covid_mom = add_preeclampsia_conditions_to_df(df_no_covid_mom, df_pat_conditions_no_covid_mom)

# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded_3', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded_3', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_3")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_expanded_3")


# add smoking and illegal drug use status to cohorts
# obtain patient drug use status
df_covid_mom  = add_drug_use_to_df(df_covid_mom)
print('\n')
df_no_covid_mom  = add_drug_use_to_df(df_no_covid_mom)

# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded_4', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded_4', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_4")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_expanded_4")


# add geographical features to cohorts
# add social vulnerability index
df_covid_mom = add_geo_features(df_covid_mom, 'svi2018_us').select(*(df_covid_mom.columns + SVI_score))
df_no_covid_mom = add_geo_features(df_no_covid_mom, 'svi2018_us').select(*(df_no_covid_mom.columns + SVI_score))

# add urban vs rural
ruca_col = ['SecondaryRUCACode2010']
df_covid_mom = add_geo_features(df_covid_mom, 'ruca2010revised').select(*(df_covid_mom.columns + ruca_col))
df_covid_mom = df_covid_mom.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))
df_no_covid_mom = add_geo_features(df_no_covid_mom, 'ruca2010revised').select(*(df_no_covid_mom.columns + ruca_col))
df_no_covid_mom = df_no_covid_mom.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))

# save cohort dataframes
write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded_5', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded_5', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_5")
df_no_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_expanded_5")


# add GPAL features to cohorts
# obtain parity, gravidity, and previous preterm births info
df_covid_mom  = add_previous_pregnancies_to_df(df_covid_mom, "snp2_covid_mom_notes")
print('\n')
df_no_covid_mom  = add_previous_pregnancies_to_df(df_no_covid_mom, "snp2_no_covid_mom_notes")

# remove duplicates
df_covid_mom = df_covid_mom.distinct()
df_no_covid_mom = df_no_covid_mom.distinct()
# save cohort dataframes

write_data_frame_to_sandbox(df_covid_mom, 'snp2_cohort_covid_maternity_expanded_6', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_no_covid_mom, 'snp2_cohort_no_covid_maternity_expanded_6', sandbox_db='rdp_phi_sandbox', replace=True)
