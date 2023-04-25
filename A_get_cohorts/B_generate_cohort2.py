# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities

 # get inpatient information 

 inpatientadt = adtevent.filter(F.col('patientclass')=='Inpatient')
inpatientadt = inpatientadt.withColumn('admissiondatetime', F.when(F.col('EVENTTYPE')=='Admission', F.col('EVENTTIMESTAMP')). \
                                                            when(F.col('EVENTTYPE')=='Transfer In', F.col('EVENTTIMESTAMP')). \
                                                            otherwise(None))
                                                            
inpatientadt = inpatientadt.withColumn('dischargedatetime', F.when(F.col('EVENTTYPE')=='Discharge', F.col('EVENTTIMESTAMP')). \
                                                            otherwise(None))
inpatientadt_ag = aggregate_data(inpatientadt, partition_columns = ['PAT_ID', 'INSTANCE', 'PAT_ENC_CSN_ID'], aggregation_columns = {'admissiondatetime':'min', 'dischargedatetime':'max'}).withColumnRenamed('admissiondatetime_min', 'admissiondatetime'). withColumnRenamed('dischargedatetime_max', 'dischargedatetime').select('PAT_ID', 'INSTANCE', 'PAT_ENC_CSN_ID', 'ADMISSIONDATETIME', 'DISCHARGEDATETIME')             
inpatient = encounter.union(inpatientadt_ag)

inpatient_final_ag = aggregate_data(inpatient, partition_columns = ['PAT_ID', 'INSTANCE', 'PAT_ENC_CSN_ID'], aggregation_columns = {'ADMISSIONDATETIME':'min', 'DISCHARGEDATETIME':'max'}).withColumnRenamed('ADMISSIONDATETIME_min', 'ADMISSIONDATETIME').withColumnRenamed('DISCHARGEDATETIME_max', 'DISCHARGEDATETIME')


# get admission and discharge date with covid 
anti_encounter1 = anti.join(inpatient_final_ag, ['pat_id', 'instance'], 'left')\
                    .filter(((F.col('dischargedatetime')>F.col('covid_test_date')) & (F.col('covid_test_date') >= F.col('admissiondatetime'))) | ((F.col('admissiondatetime')>F.date_sub(F.col('covid_test_date'),7))&(F.col('admissiondatetime')<F.date_add(F.col('covid_test_date'), 14)))).withColumn('admission_withcovid', F.datediff(F.col('admissiondatetime'), F.col('covid_test_date'))).withColumn('discharge_withcovid', F.datediff(F.col('dischargedatetime'), F.col('covid_test_date'))).withColumnRenamed('admissiondatetime', 'covid_admissiondatetime').withColumnRenamed('dischargedatetime', 'covid_dischargedatetime')
select_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'admission_withcovid', 'discharge_withcovid', 'covid_admissiondatetime', 'covid_dischargedatetime']
anti_encounter1 = anti_encounter1.select(*select_columns)


anti_encounter2 = anti.join(inpatient_final_ag, ['pat_id', 'instance'], 'left').filter(F.col('dischargedatetime')>=F.col('covid_test_date')).filter(F.col('admissiondatetime')<F.col('ob_delivery_delivery_date')).withColumn('admissiondatetime', F.when(F.col('admissiondatetime')<=F.col('covid_test_date'), F.col('covid_test_date')).otherwise(F.col('admissiondatetime'))).withColumn('inpatient_duration', F.datediff(F.col('dischargedatetime'), F.col('admissiondatetime'))).withColumn('inpatient_duration', F.col('inpatient_duration')+1).withColumnRenamed('admissiondatetime', 'inpatient_admissiondatetime').withColumnRenamed('dischargedatetime', 'inpatient_dischargedatetime')

select_columns = ['pat_id', 'instance','episode_id', 'child_episode_id', 'inpatient_duration', 'inpatient_admissiondatetime', 'inpatient_dischargedatetime']

anti_encounter2 = anti_encounter2.select(*select_columns)



# get admission and discharge date with covid 
noanti_encounter1 = noanti.join(inpatient_final_ag, ['pat_id', 'instance'], 'left')\
                    .filter(((F.col('dischargedatetime')>F.col('covid_test_date')) & (F.col('covid_test_date') >= F.col('admissiondatetime'))) | ((F.col('admissiondatetime')>F.date_sub(F.col('covid_test_date'),7))&(F.col('admissiondatetime')<F.date_add(F.col('covid_test_date'), 14)))).withColumn('admission_withcovid', F.datediff(F.col('admissiondatetime'), F.col('covid_test_date'))).withColumn('discharge_withcovid', F.datediff(F.col('dischargedatetime'), F.col('covid_test_date'))).withColumnRenamed('admissiondatetime', 'covid_admissiondatetime').withColumnRenamed('dischargedatetime', 'covid_dischargedatetime')
select_columns = ['pat_id', 'instance', 'episode_id', 'child_episode_id', 'admission_withcovid', 'discharge_withcovid', 'covid_admissiondatetime', 'covid_dischargedatetime']
noanti_encounter1 = noanti_encounter1.select(*select_columns)


noanti_encounter2 = noanti.join(inpatient_final_ag, ['pat_id', 'instance'], 'left').filter(F.col('dischargedatetime')>=F.col('covid_test_date')).filter(F.col('admissiondatetime')<F.col('ob_delivery_delivery_date')).withColumn('admissiondatetime', F.when(F.col('admissiondatetime')<=F.col('covid_test_date'), F.col('covid_test_date')).otherwise(F.col('admissiondatetime'))).withColumn('inpatient_duration', F.datediff(F.col('dischargedatetime'), F.col('admissiondatetime'))).withColumn('inpatient_duration', F.col('inpatient_duration')+1).withColumnRenamed('admissiondatetime', 'inpatient_admissiondatetime').withColumnRenamed('dischargedatetime', 'inpatient_dischargedatetime')

select_columns = ['pat_id', 'instance','episode_id', 'child_episode_id', 'inpatient_duration', 'inpatient_admissiondatetime', 'inpatient_dischargedatetime']

noanti_encounter2 = noanti_encounter2.select(*select_columns)


anti1_agg_data = aggregate_data(anti_encounter1, partition_columns = ['pat_id', 'instance','episode_id','child_episode_id'], aggregation_columns = {'admission_withcovid':'min', 'discharge_withcovid':'max', 'covid_admissiondatetime': 'min', 'covid_dischargedatetime' : 'max'})
anti2_agg_data = aggregate_data(anti_encounter2, partition_columns = ['pat_id', 'instance','episode_id','child_episode_id'], aggregation_columns = {'inpatient_duration':'sum', 'inpatient_admissiondatetime': 'min', 'inpatient_dischargedatetime':'max'})


noanti1_agg_data = aggregate_data(noanti_encounter1, partition_columns = ['pat_id', 'instance','episode_id','child_episode_id'], aggregation_columns = {'admission_withcovid':'min', 'discharge_withcovid':'max', 'covid_admissiondatetime': 'min', 'covid_dischargedatetime' : 'max'})
noanti2_agg_data = aggregate_data(noanti_encounter2, partition_columns = ['pat_id', 'instance','episode_id','child_episode_id'], aggregation_columns = {'inpatient_duration':'sum', 'inpatient_admissiondatetime': 'min', 'inpatient_dischargedatetime':'max'})


anti_expanded1 = anti.join(anti1_agg_data,  ['pat_id', 'instance','episode_id','child_episode_id'], 'left')
anti_expanded2 = anti_expanded1.join(anti2_agg_data,  ['pat_id', 'instance','episode_id','child_episode_id'], 'left')

noanti_expanded1 = noanti.join(noanti1_agg_data,  ['pat_id', 'instance','episode_id','child_episode_id'], 'left')
noanti_expanded2 = noanti_expanded1.join(noanti2_agg_data,  ['pat_id', 'instance','episode_id','child_episode_id'], 'left')

write_data_frame_to_sandbox(anti_expanded2,table_name='yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_final_102422', sandbox_db='rdp_phi_sandbox', replace = True )


write_data_frame_to_sandbox(noanti_expanded2,table_name='yh_cohort_covid_maternity_no_anticoagulant_final_102422', sandbox_db='rdp_phi_sandbox', replace = True )