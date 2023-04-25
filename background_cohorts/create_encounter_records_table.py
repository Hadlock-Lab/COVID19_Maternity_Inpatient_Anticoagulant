spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.encounters_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities


# declare universal variables
# Table to which to write records
table_name = 'snp2_adss_study_who_encounter_records'

# Specify bounds for encounter records
later_than_date = '2020-03-05'
before_date = datetime.date.today()
query_string = \
  "admission_datetime >= '{0}' AND admission_datetime < '{1}'".format(later_than_date, before_date)


# get encounter records
# Get patient cohort
basic_cohort_df = get_basic_cohort()

# Columns on which to partition records for aggregation
partition_columns = basic_cohort_df.columns

# Get encounter records
encounters_df = get_encounters(
  cohort_df=basic_cohort_df,
  include_cohort_columns=partition_columns,
  filter_string=query_string,
  join_table = 'adtevent')

# Specify how to aggregate encounter columns
agg_columns = {
  'contact_date': 'collect_list',
  'pat_enc_csn_id': ['collect_list', 'first', 'last'],
  'admission_datetime': 'first',
  'discharge_datetime': 'last',
  'base_patient_class': 'collect_list',
  'encounter_type': 'collect_list',
  'patient_class': 'collect_list',
  'patient_status': 'collect_list',
  'discharge_disposition': 'last',
  'bp_systolic': 'first',
  'bp_diastolic': 'first',
  'temperature': 'first',
  'pulse': 'first',
  'respirations': 'first'
}

# Aggregate by patient and day
encounters_aggregated_df = aggregate_by_regular_time_windows(
  df=encounters_df,
  partition_columns=partition_columns,
  aggregation_columns=agg_columns,
  datetime_column='admission_datetime',
  type='day')


# save table
write_data_frame_to_sandbox(encounters_aggregated_df, table_name, sandbox_db='rdp_phi_sandbox', replace=True)
