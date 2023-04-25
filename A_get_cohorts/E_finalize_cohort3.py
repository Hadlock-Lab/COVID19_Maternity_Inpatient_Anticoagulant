# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities


# covid anticoagulant prophylactic dose administered cohort 
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_7_102422")
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_7_102422")
# covid anticoagulant not administered cohort 
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_7_102422")
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_7_102422")



meds = get_medication_orders(covid_administered.drop('name', 'start_date', 'end_date'), add_cc_columns=['sam_anticoagulant']).filter(~F.col('sam_anticoagulant'))

covid_administered_postcovid_diagnoses = get_postcovid_diagnosis_count(covid_administered)
covid_administered = covid_administered.join(covid_administered_postcovid_diagnoses, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['count_postcovid_diagnoses'])

covid_notadministered_postcovid_diagnoses = get_postcovid_diagnosis_count(covid_notadministered)
covid_notadministered = covid_notadministered.join(covid_notadministered_postcovid_diagnoses, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left').fillna(0, subset = ['count_postcovid_diagnoses'])

covid_administered = get_postcovid_48hours_med(covid_administered)
covid_notadministered = get_postcovid_48hours_med(covid_notadministered)

write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_8_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_8_102422', sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_8_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_8_102422")



covid_administered_expanded1 = get_vasopressor_count(covid_administered, anti = True)
covid_notadministered_expanded1 = get_vasopressor_count(covid_notadministered, anti = False)

covid_administered_expanded2 = add_hospitalization(covid_administered_expanded1)
covid_notadministered_expanded2 =  add_hospitalization(covid_notadministered_expanded1)

write_data_frame_to_sandbox(covid_administered_expanded2, "yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_9_102422", sandbox_db='rdp_phi_sandbox', replace=True)


write_data_frame_to_sandbox(covid_notadministered_expanded2, "yh_cohort_covid_maternity_no_anticoagulant_expanded_9_102422", sandbox_db='rdp_phi_sandbox', replace=True)


covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_9_102422")
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_9_102422")



covid_administered_expanded_conditions = fetch_coagulation_related_conditions(covid_administered,
                                                                              anti = True, 
                                                                             encounter_filter_string = "contact_date >= start_date_min AND contact_date < ob_delivery_delivery_date",
                                                                              problem_filter_string = "date_of_entry >= start_date_min AND date_of_entry < ob_delivery_delivery_date")
covid_notadministered_expanded_conditions = fetch_coagulation_related_conditions(covid_notadministered,
                                                                                 anti = False, 
                                                                                 encounter_filter_string = "contact_date >= treatment_onset_index AND contact_date < ob_delivery_delivery_date",
                                                                                 problem_filter_string = "date_of_entry >= treatment_onset_index AND date_of_entry < ob_delivery_delivery_date")


covid_administered_expanded = add_coagulation_related_features(covid_administered, covid_administered_expanded_conditions)
covid_notadministered_expanded = add_coagulation_related_features(covid_notadministered, covid_notadministered_expanded_conditions)


write_data_frame_to_sandbox(covid_administered_expanded, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_10_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_expanded, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_10_102422', sandbox_db='rdp_phi_sandbox', replace=True)
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_10_102422")
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_10_102422")

#load unvaccinated maternity cohort
unvax_women = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_maternity_unvaccinated_102422").select(['pat_id', 'instance', 'episode_id', 'child_episode_id']).distinct()
joining_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id']


covid_administered_vax_status = covid_administered.join(unvax_women,joining_columns,'left_anti').withColumn("vaccination_status",F.lit(1)).\
unionAll(covid_administered.join(unvax_women, joining_columns,'left_semi').withColumn("vaccination_status",F.lit(0)))

covid_notadministered_vax_status = covid_notadministered.join(unvax_women,joining_columns,'left_anti').withColumn("vaccination_status",F.lit(1)).\
unionAll(covid_notadministered.join(unvax_women, joining_columns,'left_semi').withColumn("vaccination_status",F.lit(0)))


write_data_frame_to_sandbox(covid_administered_vax_status, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_11_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_vax_status, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_11_102422', sandbox_db='rdp_phi_sandbox', replace=True)


covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_11_102422")
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_11_102422")


covid_administered_with_deaths = add_patient_deaths(covid_administered)
covid_notadministered_with_deaths = add_patient_deaths(covid_notadministered)


# get covid encounter info
df_covid_pyspark = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_adss_study_who_scores")
df_covid_pyspark.createOrReplaceTempView("covid")
covid_administered.createOrReplaceTempView("covid_mom")
covid_administered_covid_records = spark.sql(
"""
FROM covid AS c
INNER JOIN covid_mom AS cm
ON c.pat_id = cm.pat_id
  AND c.discharge_datetime_last >= cm.covid_test_date 
SELECT c.pat_id, c.who_score, c.oxygen_device_concat_ws, c.encounter_duration, c.bp_systolic_first, 
c.bp_diastolic_first, c.temperature_first, c.pulse_first, c.respirations_first, c.patient_class_collect_list,
c.vasopressor_sum, cm.vasopressor_count
""").toPandas()



covid_notadministered.createOrReplaceTempView("covid_mom")

covid_notadministered_covid_records = spark.sql(
"""
FROM covid AS c
INNER JOIN covid_mom AS cm
ON c.pat_id = cm.pat_id
  AND c.discharge_datetime_last >= cm.covid_test_date 
SELECT c.pat_id, c.who_score, c.oxygen_device_concat_ws, c.encounter_duration, c.bp_systolic_first, 
c.bp_diastolic_first, c.temperature_first, c.pulse_first, c.respirations_first, c.patient_class_collect_list,
c.vasopressor_sum, cm.vasopressor_count
""").toPandas()


covid_administered_pd = covid_administered_with_deaths.toPandas()
covid_notadministered_pd = covid_notadministered_with_deaths.toPandas()

covid_administered_final = format_encounters(covid_administered_covid_records , covid_administered_pd)
covid_notadministered_final = format_encounters(covid_notadministered_covid_records, covid_notadministered_pd)


drop_columns = ['ob_sticky_note_text','ob_delivery_episode_type','ob_delivery_delivery_csn_mom','ob_delivery_labor_onset_date','ob_delivery_total_delivery_blood_loss_ml','child_type','child_ob_delivery_episode_type','ob_delivery_birth_csn_baby','ob_delivery_record_baby_id','child_ob_delivery_labor_onset_date','delivery_birth_comments','child_ob_sticky_note_text','child_ob_delivery_total_delivery_blood_loss_ml','delivery_cord_vessels']


covid_administered = spark.createDataFrame(covid_administered_final.drop(columns=drop_columns))
covid_notadministered = spark.createDataFrame(covid_notadministered_final.drop(columns=drop_columns))


write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_12_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_12_102422', sandbox_db='rdp_phi_sandbox', replace=True)



# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_12_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_12_102422")



covid_administered = covid_administered.drop('coagulopathy_max') 
covid_notadministered = covid_notadministered.drop('coagulopathy_max') 


covid_administered_conditions_primary = get_prior_coagulopathy(covid_administered)
covid_administered_primary = add_prior_coagulopathy(covid_administered, covid_administered_conditions_primary)
covid_notadministered_conditions_primary = get_prior_coagulopathy(covid_notadministered)
covid_notadministered_primary = add_prior_coagulopathy(covid_notadministered, covid_notadministered_conditions_primary)


covid_administered_conditions_secondary = get_secondary_coagulopathy(covid_administered_primary, anticoagulant = True)
covid_administered_coagulopathy = add_secondary_coagulopathy(covid_administered_primary, covid_administered_conditions_secondary)

covid_notadministered_conditions_secondary = get_secondary_coagulopathy(covid_notadministered_primary, anticoagulant = False)
covid_notadministered_coagulopathy = add_secondary_coagulopathy(covid_notadministered_primary, covid_notadministered_conditions_secondary)


covid_administered_conditions_pph = get_pph(covid_administered_coagulopathy)
covid_administered_pph = add_pph(covid_administered_coagulopathy, covid_administered_conditions_pph)
covid_notadministered_conditions_pph = get_pph(covid_notadministered_coagulopathy)
covid_notadministered_pph = add_pph(covid_notadministered_coagulopathy, covid_notadministered_conditions_pph)


write_data_frame_to_sandbox(covid_administered_pph, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_13_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_pph, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_13_102422' , sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_13_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_13_102422")


covid_administered_conditions_contraindication = get_contraindication(covid_administered)
covid_administered_contraindication = add_contraindication(covid_administered, covid_administered_conditions_contraindication)
covid_notadministered_conditions_contraindication = get_contraindication(covid_notadministered)
covid_notadministered_contraindication = add_contraindication(covid_notadministered, covid_notadministered_conditions_contraindication)


write_data_frame_to_sandbox(covid_administered_contraindication, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_14_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_contraindication, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_14_102422' , sandbox_db='rdp_phi_sandbox', replace=True)


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_14_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_14_102422")


covid_administered_concise = covid_administered.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'covid_test_date', 'conception_date')
covid_notadministered_concise = covid_notadministered.select('pat_id', 'instance', 'episode_id', 'child_episode_id', 'covid_test_date', 'conception_date')
covid_union_df = covid_administered_concise.union(covid_notadministered_concise)


anticoagulant_cc_list = list(medications_cc_registry_coagulation.keys())
query_string = \
  "ordering_datetime >= date_sub(conception_date, 730) AND ordering_datetime < covid_test_date"

# Specify medication clinical concept list
cc_list = anticoagulant_cc_list


# Columns on which to partition records for aggregation
partition_columns = covid_union_df.columns

# Get medication order and admin records
medication_admin_df = get_medication_orders(
  cohort_df=covid_union_df,
  include_cohort_columns=partition_columns,
  join_table='medicationadministration',
  filter_string=query_string,
  add_cc_columns=cc_list)

# Only keep records for specified clinical concept list
medication_admin_df = get_clinical_concept_records(medication_admin_df, cc_list).filter(F.col('sam_anticoagulant'))
medication_admin_df = medication_admin_df.withColumn('dosage_strength', p_or_t_udf(F.col('name'), F.col('freq_name')))
medication_admin_df = medication_admin_df \
  .withColumn('med_admin_or_order_dt',
              F.when(F.col('administration_datetime').isNotNull(),
                     F.col('administration_datetime')).otherwise(F.col('ordering_datetime')))


# Specify how to aggregate medication_order columns
med_agg_columns = {
  'start_date' : 'min',
  'end_date' : 'max',
  'medication_id': 'collect_set',
  'name': 'collect_set',
  'order_mode': 'collect_set',
  'action_taken': 'collect_set',
  'pat_enc_csn_id': 'collect_set',
  'freq_name' : 'collect_set',
  'dosage_strength' : 'max'
}

# Specify aggregation functions for clinical concept columns
for cc in cc_list:
  med_agg_columns[cc] = 'sum'

# Aggregate by patient and day
medication_admin_aggregated_df = aggregate_by_regular_time_windows(
  df=medication_admin_df,
  partition_columns=partition_columns,
  aggregation_columns=med_agg_columns,
  datetime_column='med_admin_or_order_dt',
  type='day')


prior_therapeutic = medication_admin_aggregated_df.filter(F.col('dosage_strength_max')==2).select('pat_id', 'instance', 'episode_id', 'child_episode_id').distinct()
prior_therapeutic.count()


covid_administered = covid_administered.join(prior_therapeutic, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left_anti')

covid_notadministered = covid_notadministered.join(prior_therapeutic, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left_anti')


write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_15_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_15_102422' , sandbox_db='rdp_phi_sandbox', replace=True)


covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_15_102422")
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_15_102422")


covid_administered = get_covid_initial_med(covid_administered)
covid_notadministered = get_covid_initial_med(covid_notadministered)


write_data_frame_to_sandbox(covid_administered, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_16_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_16_102422' , sandbox_db='rdp_phi_sandbox', replace=True)


