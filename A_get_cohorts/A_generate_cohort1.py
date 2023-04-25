# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities


#load anticoagulant medication record 
# Specify bounds for medication order/admin records
DATE_START = '2020-03-05'
DATE_END = '2022-10-20'

anticoagulant_cc_list = list(medications_cc_registry_coagulation.keys())
query_string = \
  "ordering_datetime >= '{0}' AND ordering_datetime < '{1}'".format(DATE_START, DATE_END)

# Specify medication clinical concept list
cc_list = anticoagulant_cc_list

# Get patient cohort
basic_cohort_df = basic_cohort_df = get_basic_cohort()

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


# Cohort Selection 
filter_start_date = '2020-03-05'
filter_end_date = '2022-10-20'
query_string = \
  "ob_delivery_delivery_date >= '{0}' AND ob_delivery_delivery_date < '{1}'".format(filter_start_date, filter_end_date)
df_cohort_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity").filter(query_string).na.drop(subset=['pat_id', 'episode_id', 'child_episode_id']).filter(F.col('instance')==1000) #source maternity cohort 
print ('source population (delivery records between {0} and {1}):'.format(filter_start_date, filter_end_date), df_cohort_maternity.count())

df_cohort_maternity_filtered = df_cohort_maternity.filter(F.col('number_of_fetuses') == 1).filter((F.col('age_at_start_dt') >= 18)&(F.col('age_at_start_dt')<45)).filter(F.col('gestational_days') >= 140)
   
# load COVID-19 vaccination record 

table_name = 'immunization'
w1 = Window.partitionBy('pat_id').orderBy('immunization_date') \
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_immunization = spark.sql("SELECT * FROM rdp_phi.{}".format(table_name)) \
  .where(F.col('immunizationname').like('%COVID-19%')) \
  .select('pat_id', F.col('immunizationdate').alias('immunization_date'), F.col('immunizationname').alias('immunization_name')) \
  .select('pat_id', F.first('immunization_date').over(w1).alias('first_immunization_date'),
          F.first('immunization_name').over(w1).alias('first_immunization_name'),
          F.last('immunization_date').over(w1).alias('last_immunization_date'),
          F.last('immunization_name').over(w1).alias('last_immunization_name'),
          F.collect_list('immunization_date').over(w1).alias('all_immunization_dates'),
          F.collect_list('immunization_name').over(w1).alias('all_immunizations')).dropDuplicates()
write_data_frame_to_sandbox(df_immunization, 'yh_immunization_102422', sandbox_db='rdp_phi_sandbox', replace=True)
print('# Number of unique patients with COVID-19 Vaccination Records: ' + str(df_immunization.select('pat_id').distinct().count()))

# load patients who were covid infected before pregnancy 
df_immunization.createOrReplaceTempView("immunization")
df_cohort_maternity_filtered.createOrReplaceTempView("mom")
df_cohort_maternity_covid_immunity_lab = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_maternity_covid_immunity")

# get patients who were covid patients before pregnancy 
cc_list = ['covid_19']
problemlist_covid = get_problem_list(df_cohort_maternity_filtered.drop('name'), add_cc_columns = cc_list)
encounter_covid = get_encounters(df_cohort_maternity_filtered.drop('name'), add_cc_columns = cc_list, join_table= 'encounterdiagnosis')
covid_19_problemlist_before_pregnancy = problemlist_covid.select('pat_id','instance','episode_id','child_episode_id','noted_date','covid_19').filter(F.col('covid_19')==True).filter(F.col('noted_date')<F.col('lmp'))
covid_19_encounter_before_pregnancy = encounter_covid.select('pat_id','instance','episode_id','child_episode_id','contact_date','covid_19').filter(F.col('covid_19')==True).filter(F.col('contact_date')<F.col('lmp'))
c19problem = covid_19_problemlist_before_pregnancy.select('pat_id','instance','episode_id','child_episode_id').distinct()
c19encounter = covid_19_encounter_before_pregnancy.select('pat_id','instance','episode_id','child_episode_id').distinct()
c19_diagnosis_before_pregnancy = c19problem.union(c19encounter).distinct()

# get patients who were covid infected before pregnancy 

c19_before_pregnancy = df_cohort_maternity_covid_immunity_lab.select('pat_id','instance','episode_id','child_episode_id').union(c19_diagnosis_before_pregnancy).distinct()

write_data_frame_to_sandbox(c19_before_pregnancy, 'yh_cohort_maternity_covid_immunity_102422', sandbox_db='rdp_phi_sandbox', replace=True)

# get unvaccinated cohort 
# Identify pregnant women who were unvaccinated at time of delivery
df_immunization.createOrReplaceTempView("immunization")
df_cohort_maternity_filtered.createOrReplaceTempView("mom")
c19_before_pregnancy.createOrReplaceTempView("covid_immunity_mom")
# remove mothers with covid-induced immunity from mothers  
df_mom = spark.sql(
"""
FROM mom AS mom
LEFT ANTI JOIN covid_immunity_mom AS cim
ON cim.pat_id = mom.pat_id
  AND cim.instance = mom.instance
  AND cim.episode_id = mom.episode_id
  AND cim.child_episode_id = mom.child_episode_id
SELECT mom.*, date_sub(mom.ob_delivery_delivery_date, mom.gestational_days) AS conception_date
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_mom.createOrReplaceTempView("mom")

df_cohort_maternity_unvaccinated_1 = spark.sql(
"""
FROM immunization AS i
INNER JOIN mom AS m
ON i.pat_id = m.pat_id
  AND i.first_immunization_date > m.ob_delivery_delivery_date
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_2 = spark.sql(
"""
FROM mom AS m
LEFT ANTI JOIN immunization AS i
ON i.pat_id = m.pat_id
SELECT m.*
""").dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])

df_cohort_maternity_unvaccinated_final = df_cohort_maternity_unvaccinated_1.union(df_cohort_maternity_unvaccinated_2).dropDuplicates(['pat_id', 'episode_id', 'child_episode_id'])
print ('number of unvaccinated patient at the time of pregnancy', df_cohort_maternity_unvaccinated_final.count())
write_data_frame_to_sandbox(df_cohort_maternity_unvaccinated_final, 'yh_cohort_maternity_unvaccinated_102422', sandbox_db='rdp_phi_sandbox', replace=True)


unvax = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_maternity_unvaccinated_102422").select('instance', 'pat_id', 'instance', 'child_episode_id')
print ('number of unvaccinated singleton pregnancy delivery records between {0} and {1}: '.format(filter_start_date, filter_end_date), unvax.count())
print ('number of singleton pregnancy delivery records between {0} and {1} without covid immunity: '.format(filter_start_date, filter_end_date), df_mom.count())


# load patients who had covid infection during the pregnancy 

import datetime
DATE_CUTOFF = datetime.datetime(2022, 10, 20, 5, 45, 33, 753611)


df_cohort_covid_maternity = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_covid_maternity_expanded_5").na.drop(subset=['pat_id', 'episode_id', 'child_episode_id'])
df_cohort_covid_maternity = df_cohort_covid_maternity.na.drop(subset=["gestational_days"]).distinct()
df_cohort_covid_maternity = determine_covid_maternity_cohort(df_cohort_covid_maternity, test_result='positive').sort_index()


cc_list = ['covid_19']
problemlist_covid = get_problem_list(df_cohort_maternity_filtered.drop('name'), add_cc_columns = cc_list)
encounter_covid = get_encounters(df_cohort_maternity_filtered.drop('name'), add_cc_columns = cc_list, join_table= 'encounterdiagnosis')
covid_19_problemlist_during = problemlist_covid.select('pat_id','instance','episode_id','child_episode_id','noted_date','lmp','gestational_days','ob_delivery_delivery_date','covid_19').filter(F.col('covid_19')==True).filter(F.col('noted_date')>F.col('lmp')).filter(F.col('noted_date')<F.col('ob_delivery_delivery_date'))
covid_19_encounter_during = encounter_covid.select('pat_id','instance','episode_id','child_episode_id','contact_date','lmp','gestational_days','ob_delivery_delivery_date','covid_19').filter(F.col('covid_19')==True).filter(F.col('contact_date')>F.col('lmp')).filter(F.col('contact_date')<F.col('ob_delivery_delivery_date'))

#get the first date of covid-19 encounter/problem 
c19problem = aggregate_data(covid_19_problemlist_during, ['pat_id','instance','episode_id','child_episode_id','lmp','gestational_days','ob_delivery_delivery_date'], aggregation_columns = {'noted_date' : 'min'}).withColumnRenamed('noted_date_min', 'covid_test_date')
c19encounter = aggregate_data(covid_19_encounter_during, ['pat_id','instance','episode_id','child_episode_id','lmp','gestational_days','ob_delivery_delivery_date'], aggregation_columns = {'contact_date' : 'min'}).withColumnRenamed('contact_date_min', 'covid_test_date')
c19diagnosis = c19problem.union(c19encounter).distinct()
c19diagnosis_ag = aggregate_data(c19diagnosis, ['pat_id','instance','episode_id','child_episode_id','lmp','gestational_days','ob_delivery_delivery_date'], aggregation_columns = {'covid_test_date' : 'min'}).withColumnRenamed('covid_test_date_min', 'covid_test_date')

#load patients who had covid infection before pregnancy 
c19_before_pregnancy = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_maternity_covid_immunity_102422")



c19_during_pregnancy = c19diagnosis
#exclude patients who had covid infection before pregnancy -- uncomment_Sam 
#c19_during_pregnancy = c19diagnosis.join(c19_before_pregnancy, ['pat_id', 'instance', 'episode_id', 'child_episode_id'],'left_anti') 
c19_during_pregnancy = c19_during_pregnancy.withColumnRenamed('lmp','conception_date').withColumn('covid_test_date', F.to_date(F.col('covid_test_date')))



covid_maternity = spark.createDataFrame(df_cohort_covid_maternity[['pat_id','instance','episode_id','child_episode_id','conception_date','gestational_days','ob_delivery_delivery_date','covid_test_date']])
covid_maternity = covid_maternity.withColumn('conception_date', F.to_date(F.col('conception_date')))
covid_maternity = covid_maternity.withColumn('gestational_days', F.col('gestational_days').cast(IntegerType())).withColumn('covid_test_date', F.to_date(F.col('covid_test_date')))

covid19_pregnancy = c19_during_pregnancy.union(covid_maternity).distinct()
covid19_pregnancy_ag = aggregate_data(covid19_pregnancy, ['pat_id','instance','episode_id','child_episode_id','conception_date', 'gestational_days','ob_delivery_delivery_date'], {'covid_test_date':'min'}).withColumnRenamed('covid_test_date_min', 'covid_test_date').withColumn('covid_test_number_of_days_from_conception', F.datediff(F.col('covid_test_date'),F.col('conception_date'))).withColumn('trimester_of_covid_test', determine_trimester_udf(F.col('covid_test_number_of_days_from_conception')))


c19_before_pregnancy2 = c19_before_pregnancy
c19_before_pregnancy2 = c19_before_pregnancy2.withColumn('prior_covid_infection', F.lit(1))
covid19_pregnancy_ag2 = covid19_pregnancy_ag.join(c19_before_pregnancy2, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], 'left')
covid19_pregnancy_ag2 = covid19_pregnancy_ag2.fillna(0, subset = ['prior_covid_infection'])



write_data_frame_to_sandbox(covid19_pregnancy_ag2, 'yh_cohort_covid_maternity_102422', sandbox_db='rdp_phi_sandbox', replace=True)
print('# Number of unique patients with COVID-19 infection during pregnancy: ' + str(covid19_pregnancy_ag.count()))


spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_102422")
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_102422")
#df_covid_mom = df_covid_mom.withColumn('ob_delivery_delivery_date', F.date_add(F.col('conception_date'), F.col('gestational_days')))
df_covid_mom.createOrReplaceTempView("covid_mom")
medication_admin_aggregated_df = medication_admin_aggregated_df.withColumn("med_administration_date", to_date(col("time_index_day"),"yyyy-MM-dd"))
medication_admin_aggregated_df.createOrReplaceTempView("meds")
df_cohort_covid_maternity_covid_anticoagulants = spark.sql(
"""
FROM meds AS m
INNER JOIN covid_mom AS cm
ON m.pat_id = cm.pat_id
  AND m.instance = cm.instance
  AND m.med_administration_date BETWEEN cm.conception_date AND cm.ob_delivery_delivery_date
  AND m.med_administration_date BETWEEN cm.covid_test_date - interval '14' day AND cm.covid_test_date + interval '60' day
SELECT cm.*, m.med_administration_date, m.medication_id_collect_set, m.name_collect_set,
 m.order_mode_collect_set, m.action_taken_collect_set, m.pat_enc_csn_id_collect_set, m.freq_name_collect_set, m.dosage_strength_max, m.sam_anticoagulant_sum, m.start_date_min, m.end_date_max
""").dropDuplicates()

print (df_cohort_covid_maternity_covid_anticoagulants.count())

df_to_save = administration_trimester(df_cohort_covid_maternity_covid_anticoagulants)
table_name = "yh_cohort_covid_maternity_anticoagulant_102422"
write_data_frame_to_sandbox(df_to_save, table_name, sandbox_db='rdp_phi_sandbox', replace=True)



df_cohort_covid_maternity_covid_no_anticoagulants = spark.sql(
"""
FROM covid_mom AS cm
LEFT ANTI JOIN meds AS m
ON m.pat_id = cm.pat_id
  AND m.instance = cm.instance
  AND m.med_administration_date BETWEEN cm.conception_date AND cm.ob_delivery_delivery_date
  AND m.med_administration_date BETWEEN cm.covid_test_date - interval '14' day AND cm.covid_test_date + interval '60' day
SELECT cm.*
""").dropDuplicates()
df_cohort_covid_maternity_covid_no_anticoagulants.createOrReplaceTempView("covid_mom_no_anticoagulant")
df_cohort_covid_maternity_covid_anticoagulants.createOrReplaceTempView("covid_mom_anticoagulant")
df_cohort_covid_maternity_covid_no_anticoagulants_filtered = spark.sql(
"""
FROM covid_mom_no_anticoagulant AS cmna
LEFT ANTI JOIN covid_mom_anticoagulant AS cma
ON cmna.pat_id = cma.pat_id
  AND cmna.instance = cma.instance
SELECT cmna.*
""").dropDuplicates()
print (df_cohort_covid_maternity_covid_no_anticoagulants_filtered.count())
df_to_save = df_cohort_covid_maternity_covid_no_anticoagulants_filtered
table_name = "yh_cohort_covid_maternity_no_anticoagulant_102422"
write_data_frame_to_sandbox(df_to_save, table_name, sandbox_db='rdp_phi_sandbox', replace=True)



spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_anticoagulant_102422")
df_covid_mom_anticoagulants_all = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_anticoagulant_102422").drop('ordering_datetime_collect_list', 'observation_datetime_collect_list').toPandas().sort_index()

spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_102422")
df_covid_mom_no_anticoagulants_all = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_102422").drop('ordering_datetime_collect_list', 'observation_datetime_collect_list').toPandas().sort_index()


#define no covid cohort 
nocovid = spark.sql("SELECT * FROM rdp_phi_sandbox.snp2_cohort_no_covid_maternity_covid_test_data")
df_covid_mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity")
nocovid_filtered = nocovid.join(df_covid_mom, ['pat_id', 'instance', 'episode_id', 'child_episode_id'], "left_anti")


nocovid_filtered.createOrReplaceTempView("no_covid")
medication_admin_aggregated_df = medication_admin_aggregated_df.withColumn("med_administration_date", to_date(col("time_index_day"),"yyyy-MM-dd"))
medication_admin_aggregated_df.createOrReplaceTempView("meds")
df_cohort_no_covid_maternity_anticoagulants = spark.sql(
"""
FROM meds AS m
INNER JOIN no_covid AS nc
ON m.pat_id = nc.pat_id
  AND m.instance = nc.instance
  AND m.med_administration_date BETWEEN nc.conception_date AND nc.ob_delivery_delivery_date
SELECT nc.*, m.med_administration_date, m.medication_id_collect_set, m.name_collect_set,
 m.order_mode_collect_set, m.action_taken_collect_set, m.pat_enc_csn_id_collect_set,m.freq_name_collect_set, m.dosage_strength_max,  m.sam_anticoagulant_sum, m.start_date_min, m.end_date_max
""").dropDuplicates()
df_cohort_no_covid_maternity_no_anticoagulants = spark.sql(
"""
FROM no_covid AS nc
LEFT ANTI JOIN meds as m 
ON m.pat_id = nc.pat_id
  AND m.instance = nc.instance
  AND m.med_administration_date BETWEEN nc.conception_date AND nc.ob_delivery_delivery_date
SELECT nc.*
""").dropDuplicates()

df_to_save = administration_trimester(df_cohort_no_covid_maternity_anticoagulants)
print (df_cohort_no_covid_maternity_anticoagulants.count())
table_name = "yh_cohort_nocovid_maternity_anticoagulant_102422"
write_data_frame_to_sandbox(df_to_save, table_name, sandbox_db='rdp_phi_sandbox', replace=True)


df_to_save = df_cohort_no_covid_maternity_no_anticoagulants
print (df_cohort_no_covid_maternity_no_anticoagulants.count())
table_name = "yh_cohort_nocovid_maternity_no_anticoagulant_102422"
write_data_frame_to_sandbox(df_to_save, table_name, sandbox_db='rdp_phi_sandbox', replace=True)


df_no_covid_mom_anticoagulants_all = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_nocovid_maternity_anticoagulant_102422").drop('ordering_datetime_collect_list', 'observation_datetime_collect_list').toPandas().sort_index()
df_no_covid_mom_no_anticoagulants_all = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_nocovid_maternity_no_anticoagulant_102422").drop('ordering_datetime_collect_list', 'observation_datetime_collect_list').toPandas().sort_index()


df_covid_mom_anticoagulants_all['med_administration_date'] = pd.to_datetime(df_covid_mom_anticoagulants_all['med_administration_date'], infer_datetime_format=True)
df_covid_mom_anticoagulants_all['covid_test_date'] = pd.to_datetime(df_covid_mom_anticoagulants_all['covid_test_date'], infer_datetime_format=True)
df_no_covid_mom_anticoagulants_all['med_administration_date'] = pd.to_datetime(df_no_covid_mom_anticoagulants_all['med_administration_date'], infer_datetime_format=True)


df_cohort_covid_maternity_anticoagulants = filter_for_covid_anticoagulants(df_covid_mom_anticoagulants_all).drop_duplicates(subset=['pat_id', 'instance', 'episode_id', 'child_episode_id'])

df_cohort_no_covid_maternity_anticoagulants = filter_for_no_covid_anticoagulants(df_no_covid_mom_anticoagulants_all).drop_duplicates(subset=['pat_id', 'instance', 'episode_id', 'child_episode_id'])
print('# Number of pregnant patients without covid administered anticoagulants during pregnancy: ' + str(df_cohort_no_covid_maternity_anticoagulants.shape[0]))


covid_prophylactic_anticoagulant_pd = df_cohort_covid_maternity_anticoagulants[df_cohort_covid_maternity_anticoagulants.dosage_strength_max==1]
covid_prophylactic_anticoagulant_pd.shape[0]

covid_prophylactic_anticoagulant_df = spark.createDataFrame(covid_prophylactic_anticoagulant_pd) 


nocovid_prophylactic_anticoagulant_pd = df_cohort_no_covid_maternity_anticoagulants[df_cohort_no_covid_maternity_anticoagulants.dosage_strength_max==1].drop(['child_ob_sticky_note_text', 'flagged_as_collect_list'], axis = 1)
nocovid_prophylactic_anticoagulant_pd.shape[0]

nocovid_prophylactic_anticoagulant_df = spark.createDataFrame(nocovid_prophylactic_anticoagulant_pd) 