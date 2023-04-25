spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.encounters_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities


# declare universal variables
# Table to which to write records
table_name = 'snp2_adss_study_who_encounter_diagnoses'

# Specify bounds for encounter records
later_than_date = '2020-03-05'
before_date = datetime.date.today()
query_string = \
  "admission_datetime >= '{0}' AND admission_datetime < '{1}'".format(later_than_date, before_date)


# Get encounter records
# Clinical concepts to use
cc_list = ['acute_lymphoid_leukemia', 'acute_myeloid_leukemia', 'acute_pulmonary_edema', 'acute_renal_failure', 'ards', 'acute_respiratory_failure', 'arthritis', 'arteriosclerotic_vascular_disease', 'asthma', 'atrial_fibrillation', 'blood_loss', 'cardiac_arrhythmia', 'cerebrovascular_accident', 'chronic_kidney_disease', 'chronic_lung_disease', 'chronic_lymphoid_leukemia', 'chronic_myeloid_leukemia', 'chronic_renal_failure', 'congestive_heart_failure', 'copd', 'coronary_artery_disease', 'covid_19', 'crohns_disease', 'deep_vein_thrombosis', 'dementia', 'depression', 'diabetes', 'diabetes_type_1', 'diabetes_type_2', 'encephalopathy', 'gerd', 'heart_failure', 'hypertension', 'influenza', 'leukemia', 'malignant_neoplastic_disease', 'multiple_sclerosis', 'mycosis', 'myocardial_infarction', 'neutropenia', 'obesity', 'osteoarthritis', 'osteoporosis', 'peripheral_arterial_disease', 'pneumonia', 'protein_calorie_malnutrition', 'renal_insufficiency', 'respiratory_distress', 'rheumatoid_arthritis', 'seizure', 'sepsis', 'stroke', 'sirs', 'tachycardia', 'traumatic_brain_injury', 'urinary_tract_infection']

# Get patient cohort
basic_cohort_df = get_basic_cohort()

# Columns on which to partition records for aggregation
partition_columns = basic_cohort_df.columns

# Get encounter records
encounters_df = get_encounters(
  cohort_df=basic_cohort_df,
  include_cohort_columns=partition_columns,
  join_table='encounterdiagnosis',
  filter_string=query_string,
  add_cc_columns=cc_list)

# Specify how to aggregate encounter columns
agg_columns = {
  'pat_enc_csn_id': 'collect_list',
  'contact_date': 'collect_list',
  'admission_datetime': 'first',
  'discharge_datetime': 'last',
  'encounter_type': 'collect_list',
  'patient_class': 'collect_list',
  'patient_status': 'collect_list',
  'discharge_disposition': 'last',
  'diagnosis_name': 'collect_set'
}

# Specify aggregation functions for clinical concept columns
for cc in cc_list:
  agg_columns[cc] = 'sum'

# Aggregate by patient and day
encounters_aggregated_df = aggregate_by_regular_time_windows(
  df=encounters_df,
  partition_columns=partition_columns,
  aggregation_columns=agg_columns,
  datetime_column='admission_datetime',
  type='day')

# Replace sums in cc columns with 0/1 indicating False/True
for cc_label in cc_list:
  current_col_name = '{}_sum'.format(cc_label)
  encounters_aggregated_df = encounters_aggregated_df \
    .withColumn(cc_label, F.when(F.col(current_col_name) > 0, 1).otherwise(0)) \
    .drop(current_col_name)


# save table
# Table to which to write records
write_data_frame_to_sandbox(encounters_aggregated_df, table_name, sandbox_db='rdp_phi_sandbox', replace=True)
