spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.medication_orders


# declare universal variables
# Table to which to write records
table_name = 'snp2_adss_study_who_med_admin_records'

# Specify bounds for medication order/admin records
later_than_date = '2020-03-05'
before_date = datetime.date.today()
query_string = \
  "ordering_datetime >= '{0}' AND ordering_datetime < '{1}'".format(later_than_date, before_date)


# get medication order/admin records
# Specify medication clinical concept list
cc_list = ['vasopressor']

# Get patient cohort
basic_cohort_df = get_basic_cohort()

# Columns on which to partition records for aggregation
partition_columns = basic_cohort_df.columns

# Get medication order and admin records
medication_admin_df = get_medication_orders(
  cohort_df=basic_cohort_df,
  include_cohort_columns=partition_columns,
  join_table='medicationadministration',
  filter_string=query_string,
  add_cc_columns=cc_list)

# Only keep records for specified clinical concept list
medication_admin_df = get_clinical_concept_records(medication_admin_df, cc_list)

# Add column with administration datetime, if available, otherwise ordering datetime
medication_admin_df = medication_admin_df \
  .withColumn('med_admin_or_order_dt',
              F.when(F.col('administration_datetime').isNotNull(),
                     F.col('administration_datetime')).otherwise(F.col('ordering_datetime')))

# Specify how to aggregate medication_order columns
med_agg_columns = {
  'medication_id': 'collect_set',
  'name': 'collect_set',
  'order_mode': 'collect_set',
  'action_taken': 'collect_set',
  'pat_enc_csn_id': 'collect_set'
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


# save table
df_to_save = medication_admin_aggregated_df
write_data_frame_to_sandbox(df_to_save, table_name, sandbox_db='rdp_phi_sandbox', replace=True)
