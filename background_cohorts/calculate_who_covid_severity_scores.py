# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities


# declare universal variables
# Table to which to write records
table_name = 'snp2_adss_study_who_scores'


# specify conditions cc list
# Clinical concepts to use
cc_list = ['acute_lymphoid_leukemia', 'acute_myeloid_leukemia', 'acute_pulmonary_edema', 'acute_renal_failure', 'ards', 'acute_respiratory_failure', 'arthritis', 'arteriosclerotic_vascular_disease', 'asthma', 'atrial_fibrillation', 'blood_loss', 'cardiac_arrhythmia', 'cerebrovascular_accident', 'chronic_kidney_disease', 'chronic_lung_disease', 'chronic_lymphoid_leukemia', 'chronic_myeloid_leukemia', 'chronic_renal_failure', 'congestive_heart_failure', 'copd', 'coronary_artery_disease', 'covid_19', 'crohns_disease', 'deep_vein_thrombosis', 'dementia', 'depression', 'diabetes', 'diabetes_type_1', 'diabetes_type_2', 'encephalopathy', 'gerd', 'heart_failure', 'hypertension', 'influenza', 'leukemia', 'malignant_neoplastic_disease', 'multiple_sclerosis', 'mycosis', 'myocardial_infarction', 'neutropenia', 'obesity', 'osteoarthritis', 'osteoporosis', 'peripheral_arterial_disease', 'pneumonia', 'protein_calorie_malnutrition', 'renal_insufficiency', 'respiratory_distress', 'rheumatoid_arthritis', 'seizure', 'sepsis', 'stroke', 'sirs', 'tachycardia', 'traumatic_brain_injury', 'urinary_tract_infection']


# get tables to use for the calculation of WHO scores for COVID-19 severity
# Devices table
devices_table = "snp2_adss_study_who_device_records"
devices_df = spark.sql("SElECT * FROM rdp_phi_sandbox.{}".format(devices_table)) \
  .withColumn('time_index_date', F.to_date('time_index_day')) \
  .withColumnRenamed('pat_enc_csn_id_collect_set', 'devices_pat_enc_csn_id_list')

# Medication administration table
med_admin_table = "snp2_adss_study_who_med_admin_records"
med_admin_df = spark.sql("SElECT * FROM rdp_phi_sandbox.{}".format(med_admin_table)) \
  .withColumn('time_index_date', F.to_date('time_index_day')) \
  .withColumnRenamed('name_collect_set', 'med_name_collect_set') \
  .withColumnRenamed('pat_enc_csn_id_collect_set', 'meds_pat_enc_csn_id_list')

# Encounters table with admission dates
encounters_table = "snp2_adss_study_who_encounter_records"
encounters_admit_df = spark.sql("SElECT * FROM rdp_phi_sandbox.{}".format(encounters_table)) \
  .withColumn('time_index_date', F.to_date('admission_datetime_first')) \
  .withColumn('encounter_duration',
              F.round((F.unix_timestamp('discharge_datetime_last') -
                       F.unix_timestamp('admission_datetime_first'))/86400, 2)) \
  .withColumnRenamed('pat_enc_csn_id_collect_list', 'admit_pat_enc_csn_id_list')

# Encounters table with discharge dates
encounters_table = "snp2_adss_study_who_encounter_records"
encounters_disch_df = spark.sql("SElECT * FROM rdp_phi_sandbox.{}".format(encounters_table)) \
  .withColumn('time_index_date', F.to_date('discharge_datetime_last')) \
  .withColumnRenamed('pat_enc_csn_id_collect_list', 'disch_pat_enc_csn_id_list')


# Get encounter diagnoses
# Encounter diagnosis table
encounter_diagnosis_table = "snp2_adss_study_who_encounter_diagnoses"
encounter_diagnoses_df = spark.sql("SElECT * FROM rdp_phi_sandbox.{}".format(encounter_diagnosis_table)) \
  .withColumn('time_index_date', F.to_date('time_index_day'))


# Get union of patient IDs and time indexes from tables
# Patient IDs and time indexes
pat_id_time_index_labels = ['pat_id', 'instance', 'time_index_date']
pat_id_time_index_df = devices_df.select(pat_id_time_index_labels) \
  .union(med_admin_df.select(pat_id_time_index_labels)) \
  .union(encounters_admit_df.select(pat_id_time_index_labels)) \
  .union(encounters_disch_df.select(pat_id_time_index_labels)) \
  .union(encounter_diagnoses_df.select(pat_id_time_index_labels)) \
  .dropDuplicates()


# Join data and calculate final WHO scores
# Device columns to keep
device_columns = pat_id_time_index_labels + \
  ['birth_date', 'death_date', 'sex', 'ethnic_group', 'race', 'flo_meas_id_collect_set',
   'name_collect_set', 'crrt_concat_ws', 'ecmo_concat_ws', 'oxygen_device_concat_ws',
   'nominal_who_score', 'devices_pat_enc_csn_id_list']

# Medication administration columns to keep
med_admin_columns = pat_id_time_index_labels + \
  ['medication_id_collect_set', 'med_name_collect_set', 'order_mode_collect_set',
   'action_taken_collect_set', 'vasopressor_sum', 'meds_pat_enc_csn_id_list']

# Encounter admission columns to keep
encounter_admit_columns = pat_id_time_index_labels + \
  ['admit_pat_enc_csn_id_list', 'pat_enc_csn_id_last', 'encounter_duration', 'contact_date_collect_list',
   'admission_datetime_first', 'encounter_type_collect_list', 'base_patient_class_collect_list',
   'patient_class_collect_list', 'patient_status_collect_list', 'bp_systolic_first', 'bp_diastolic_first',
   'temperature_first', 'pulse_first', 'respirations_first']

# Encounter discharge columns to keep
encounter_disch_columns = pat_id_time_index_labels + \
  ['disch_pat_enc_csn_id_list', 'discharge_datetime_last', 'discharge_disposition_last']

# Encounter diagnosis table columns to keep
#encounter_diagnosis_columns = pat_id_time_index_labels + get_condition_cc_list()
encounter_diagnosis_columns = pat_id_time_index_labels + cc_list

# Join tables
all_data_df = pat_id_time_index_df \
  .join(devices_df.select(device_columns), pat_id_time_index_labels, how='left') \
  .join(med_admin_df.select(med_admin_columns), pat_id_time_index_labels, how='left') \
  .join(encounters_admit_df.select(encounter_admit_columns), pat_id_time_index_labels, how='left') \
  .join(encounters_disch_df.select(encounter_disch_columns), pat_id_time_index_labels, how='left') \
  .withColumn('pat_enc_csn_id_list',
              F.when(F.col('devices_pat_enc_csn_id_list').isNotNull(),
                     F.col('devices_pat_enc_csn_id_list')) \
               .when(F.col('meds_pat_enc_csn_id_list').isNotNull(),
                     F.col('meds_pat_enc_csn_id_list')) \
               .when(F.col('admit_pat_enc_csn_id_list').isNotNull(),
                     F.col('admit_pat_enc_csn_id_list')) \
               .otherwise(F.col('disch_pat_enc_csn_id_list')))

# Calculate final WHO score
all_data_df = get_adjusted_who_score(
  df=all_data_df,
  who_nominal_column='nominal_who_score',
  crrt_column='crrt_concat_ws',
  ecmo_column='ecmo_concat_ws',
  pressor_column='vasopressor_sum',
  discharge_disposition_column='discharge_disposition_last',
  new_column_name='who_score')

# Join encounter diagnosis columns
all_data_df = all_data_df \
  .join(encounter_diagnoses_df.select(encounter_diagnosis_columns), pat_id_time_index_labels, how='left')


# save table
write_data_frame_to_sandbox(all_data_df, table_name, sandbox_db='rdp_phi_sandbox', replace=True)
