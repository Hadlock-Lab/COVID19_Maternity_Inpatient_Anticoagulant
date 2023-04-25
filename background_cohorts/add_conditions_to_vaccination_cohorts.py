# Author: Samantha Piekos
# Date: 7/12/22

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
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])


# write patient conditions tables
# save patient condition dataframes
write_all_pat_conditions_df(df_vaccinated, 'snp2_covid_vaccinated_maternity_conditions')
write_all_pat_conditions_df(df_vaccinated_mrna, 'snp2_covid_vaccinated_mrna_maternity_conditions')
write_all_pat_conditions_df(df_unvaccinated, 'snp2_covid_unvaccinated_maternity_conditions')

# load patient condition dataframes
df_pat_conditions_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_covid_vaccinated_maternity_conditions")
df_pat_conditions_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_covid_vaccinated_mrna_maternity_conditions")
df_pat_conditions_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_covid_unvaccinated_maternity_conditions")


# add chronic diabetes and chronic hypertension status to tables
df_vaccinated = add_chronic_conditions_during_pregnancy_to_df(df_vaccinated, df_pat_conditions_vaccinated)
print('\n')
df_vaccinated_mrna = add_chronic_conditions_during_pregnancy_to_df(df_vaccinated_mrna, df_pat_conditions_vaccinated_mrna)
print('\n')
df_unvaccinated = add_chronic_conditions_during_pregnancy_to_df(df_unvaccinated, df_pat_conditions_unvaccinated)

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp2_cohort_maternity_vaccinated_expanded', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna_expanded', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_unvaccinated, 'snp2_cohort_maternity_unvaccinated_expanded', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_expanded")
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna_expanded")
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_expanded")


# add gestational diabetes and gestational hypertension status to tables
df_vaccinated = add_pregnancy_gestational_conditions_to_df(df_vaccinated, df_pat_conditions_vaccinated)
print('\n')
df_vaccinated_mrna = add_pregnancy_gestational_conditions_to_df(df_vaccinated_mrna, df_pat_conditions_vaccinated_mrna)
print('\n')
df_unvaccinated = add_pregnancy_gestational_conditions_to_df(df_unvaccinated, df_pat_conditions_unvaccinated)

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp2_cohort_maternity_vaccinated_expanded_2', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna_expanded_2', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_unvaccinated, 'snp2_cohort_maternity_unvaccinated_expanded_2', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_expanded_2")
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna_expanded_2")
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_expanded_2")


# add preeclampsia related conditions statuses to tables
df_vaccinated = add_preeclampsia_conditions_to_df(df_vaccinated, df_pat_conditions_vaccinated)
print('\n')
df_vaccinated_mrna = add_preeclampsia_conditions_to_df(df_vaccinated_mrna, df_pat_conditions_vaccinated_mrna)
print('\n')
df_unvaccinated = add_preeclampsia_conditions_to_df(df_unvaccinated, df_pat_conditions_unvaccinated)

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp2_cohort_maternity_vaccinated_expanded_3', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna_expanded_3', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_unvaccinated, 'snp2_cohort_maternity_unvaccinated_expanded_3', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_expanded_3")
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna_expanded_3")
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_expanded_3")


# add smoker status and illegal drug use to tables
# obtain patient drug use status
df_vaccinated  = add_drug_use_to_df(df_vaccinated)
print('\n')
df_vaccinated_mrna  = add_drug_use_to_df(df_vaccinated_mrna)
print('\n')
df_unvaccinated  = add_drug_use_to_df(df_unvaccinated)

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp2_cohort_maternity_vaccinated_expanded_4', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna_expanded_4', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_unvaccinated, 'snp2_cohort_maternity_unvaccinated_expanded_4', sandbox_db='rdp_phi_sandbox', replace=True)


# add geographical features to tables
# load saved dataframes
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_expanded_4")
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna_expanded_4")

# add social vulnerability index
df_vaccinated = add_geo_features(df_vaccinated, 'svi2018_us').select(*(df_vaccinated.columns + SVI_score))
df_vaccinated_mrna = add_geo_features(df_vaccinated_mrna, 'svi2018_us').select(*(df_vaccinated_mrna.columns + SVI_score))

# add urban vs rural
ruca_col = ['SecondaryRUCACode2010']
df_vaccinated = add_geo_features(df_vaccinated, 'ruca2010revised').select(*(df_vaccinated.columns + ruca_col))
df_vaccinated = df_vaccinated.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))
df_vaccinated_mrna = add_geo_features(df_vaccinated_mrna, 'ruca2010revised').select(*(df_vaccinated_mrna.columns + ruca_col))
df_vaccinated_mrna = df_vaccinated_mrna.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp2_cohort_maternity_vaccinated_expanded_5', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp2_cohort_maternity_vaccinated_mrna_expanded_5', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_expanded_4")

# add social vulnerability index
df_unvaccinated = add_geo_features(df_unvaccinated, 'svi2018_us').select(*(df_unvaccinated.columns + SVI_score))

# add urban vs rural
ruca_col = ['SecondaryRUCACode2010']
df_unvaccinated = add_geo_features(df_unvaccinated, 'ruca2010revised').select(*(df_unvaccinated.columns + ruca_col))
df_unvaccinated = df_unvaccinated.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))

# save cohort dataframes
write_data_frame_to_sandbox(df_unvaccinated, 'snp2_cohort_maternity_unvaccinated_expanded_5', sandbox_db='rdp_phi_sandbox', replace=True)


# add GPAL data to tables
# load saved dataframes
df_vaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_expanded_5")
df_vaccinated_mrna = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_vaccinated_mrna_expanded_5")

# obtain parity, gravidity, and previous preterm births info
df_vaccinated  = add_previous_pregnancies_to_df(df_vaccinated, "snp2_vaccinated_mom_notes")
print('\n')
df_vaccinated_mrna  = add_previous_pregnancies_to_df(df_vaccinated_mrna, "snp2_vaccinated_mrna_mom_notes")
print('\n')

# remove duplicates
df_vaccinated = df_vaccinated.distinct()
df_vaccinated_mrna = df_vaccinated_mrna.distinct()

# save cohort dataframes
write_data_frame_to_sandbox(df_vaccinated, 'snp3_cohort_maternity_vaccinated_expanded_6', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(df_vaccinated_mrna, 'snp3_cohort_maternity_vaccinated_mrna_expanded_6', sandbox_db='rdp_phi_sandbox', replace=True)

# load saved dataframes
df_unvaccinated = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_unvaccinated_expanded_5")

# obtain parity, gravidity, and previous preterm births info
df_unvaccinated  = add_previous_pregnancies_to_df(df_unvaccinated, "snp2_unvaccinated_mom_notes")

# remove duplicates
df_unvaccinated = df_unvaccinated.distinct()

# save cohort dataframes
write_data_frame_to_sandbox(df_unvaccinated, 'snp3_cohort_maternity_unvaccinated_expanded_6', sandbox_db='rdp_phi_sandbox', replace=True)
