# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities



# Workflow of A_finalize_cohort1.py 
# 1. Get basic maternal/pregnancy characteristics 
# 2. Get patient condition characteristics 
# 3. Get alcohol/illegal drug use/smoking status 
# 4. Get GPAL information 



# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_final_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_final_102422")



# 1. Get basic maternal/pregnancy characteristics 
filter_start_date = '2020-03-05'
filter_end_date = '2022-10-20'
query_string = \
  "ob_delivery_delivery_date >= '{0}' AND ob_delivery_delivery_date < '{1}'".format(filter_start_date, filter_end_date)
df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity").filter(query_string).na.drop(subset=['pat_id', 'episode_id', 'child_episode_id']).filter(F.col('instance')==1000) #source maternity cohort 
print ('source population (delivery records between {0} and {1}):'.format(filter_start_date, filter_end_date), df_cohort_maternity.count())

df_cohort_maternity_filtered = df_cohort_maternity.filter(F.col('number_of_fetuses') == 1).filter((F.col('age_at_start_dt') >= 18)&(F.col('age_at_start_dt')<45)).filter(F.col('gestational_days') >= 140)
print ('source population after exclusion criteria', df_cohort_maternity_filtered.count())                                  



join_cols = intersection(df_cohort_maternity_filtered.columns, covid_administered.columns)
covid_administered_expanded = covid_administered.join(df_cohort_maternity_filtered, join_cols, 'inner')

join_cols = intersection(df_cohort_maternity_filtered.columns, covid_notadministered.columns)
covid_notadministered_expanded = covid_notadministered.join(df_cohort_maternity_filtered, join_cols, 'inner') 


write_data_frame_to_sandbox(covid_administered_expanded, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_1_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_expanded, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_1_102422', sandbox_db='rdp_phi_sandbox', replace=True)



# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_1_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_1_102422")
# no covid anticoagulant prophylactic dose administered cohort 

# 2. Get patient condition characteristics 
# save patient condition dataframes
write_all_pat_conditions_df(covid_administered, 'yh_covid_maternity_covid_anticoagulant_conditions_102422')
write_all_pat_conditions_df(covid_notadministered, 'yh_covid_maternity_covid_noanticoagulant_conditions_102422')


# load patient condition dataframes
df_pat_conditions_covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_maternity_covid_anticoagulant_conditions_102422")
df_pat_conditions_covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_maternity_covid_noanticoagulant_conditions_102422")



df_pat_conditions_covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_maternity_covid_anticoagulant_conditions_102422")
df_pat_conditions_covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_covid_maternity_covid_noanticoagulant_conditions_102422")


covid_administered = add_chronic_conditions_during_pregnancy_to_df(covid_administered, df_pat_conditions_covid_administered)
print('\n')
covid_notadministered = add_chronic_conditions_during_pregnancy_to_df(covid_notadministered, df_pat_conditions_covid_notadministered)
print('\n')


# save cohort dataframes
write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_2_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_2_102422', sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_2_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_2_102422")


covid_administered = add_pregnancy_gestational_conditions_to_df(covid_administered, df_pat_conditions_covid_administered)
print('\n')
covid_notadministered = add_pregnancy_gestational_conditions_to_df(covid_notadministered, df_pat_conditions_covid_notadministered)
print('\n')

# save cohort dataframes
write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_3_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_3_102422', sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_3_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_3_102422")


covid_administered = add_preeclampsia_conditions_to_df(covid_administered, df_pat_conditions_covid_administered)
print('\n')
covid_notadministered = add_preeclampsia_conditions_to_df(covid_notadministered, df_pat_conditions_covid_notadministered)
print('\n')


# save cohort dataframes
write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_4_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_4_102422', sandbox_db='rdp_phi_sandbox', replace=True)

# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_4_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_4_102422")





# 3. Get alcohol/illegal drug use/smoking status 
covid_administered = add_drug_use_to_df(covid_administered)
print('\n')
covid_notadministered = add_drug_use_to_df(covid_notadministered)
print('\n')


# save cohort dataframes
write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_5_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_5_102422', sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_5_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_5_102422")


filter_start_date = '2020-03-05'
filter_end_date = '2022-10-20'
query_string = \
  "ob_delivery_delivery_date >= '{0}' AND ob_delivery_delivery_date < '{1}'".format(filter_start_date, filter_end_date)
df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity").filter(query_string).na.drop(subset=['pat_id', 'episode_id', 'child_episode_id']).filter(F.col('instance')==1000) #source maternity cohort 
print ('source population (delivery records between {0} and {1}):'.format(filter_start_date, filter_end_date), df_cohort_maternity.count())

df_cohort_maternity_filtered = df_cohort_maternity.filter(F.col('number_of_fetuses') == 1).filter((F.col('age_at_start_dt') >= 18)&(F.col('age_at_start_dt')<45)).filter(F.col('gestational_days') >= 140)
print ('source population after exclusion criteria', df_cohort_maternity_filtered.count())       
spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.yh_cohort_maternity_GPAL_102422")
load_notes_join_save(df_cohort_maternity_filtered, "yh_cohort_maternity_GPAL_102422")



# obtain GPAL 
covid_administered  = add_previous_pregnancies_to_df(covid_administered, "yh_cohort_maternity_GPAL_102422")
print('\n')
covid_notadministered  = add_previous_pregnancies_to_df(covid_notadministered, "yh_cohort_maternity_GPAL_102422")
print('\n')

# remove duplicates
covid_administered  = covid_administered.distinct()
print('\n')
covid_notadministered  = covid_notadministered.distinct()
print('\n')

# save cohort dataframes
write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_6_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_6_102422', sandbox_db='rdp_phi_sandbox', replace=True)