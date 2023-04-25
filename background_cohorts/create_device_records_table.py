spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.flowsheet_utilities


# declare universal variables
# Table to which to write records
table_name = 'snp2_adss_study_who_device_records'

# Specify bounds for flowsheet records
later_than_date = '2020-03-05'
before_date = datetime.date.today()
query_string = \
  "recorded_time >= '{0}' AND recorded_time < '{1}'".format(later_than_date, before_date)


# get device records
# Specify device clinical concept list
cc_list = ['crrt', 'ecmo', 'oxygen_device']

# Get patient cohort
basic_cohort_df = get_basic_cohort()

# Columns on which to partition records for aggregation
partition_columns = basic_cohort_df.columns

# Get flowsheet records
flowsheets_df = get_flowsheets(
  cohort_df=basic_cohort_df,
  include_cohort_columns=partition_columns,
  join_table='inpatientdata',
  filter_string=query_string,
  add_cc_columns=cc_list)

# Only keep device records
flowsheets_df = get_clinical_concept_records(flowsheets_df, cc_list)

# Specify how to aggregate flowsheet columns
agg_columns = {
  'flo_meas_id': 'collect_set',
  'name': 'collect_set',
  'pat_enc_csn_id': 'collect_set'
}

# Specify aggregation functions for clinical concept columns
for cc in cc_list:
  agg_columns[cc] = 'concat_ws'

# Aggregate by patient and day
flowsheets_aggregated_df = aggregate_by_regular_time_windows(
  df=flowsheets_df,
  partition_columns=partition_columns,
  aggregation_columns=agg_columns,
  datetime_column='recorded_time',
  type='day')

# Add column with nominal WHO score based only on oxygen support
flowsheets_aggregated_df = add_device_who_score_column(
  df=flowsheets_aggregated_df,
  value_column='oxygen_device_concat_ws',
  new_column_name='nominal_who_score')


# save table
write_data_frame_to_sandbox(flowsheets_aggregated_df, table_name, sandbox_db='rdp_phi_sandbox', replace=True)
