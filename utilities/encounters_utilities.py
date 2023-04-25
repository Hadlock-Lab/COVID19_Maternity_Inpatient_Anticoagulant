# load environment
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.basic_cohort
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.conditions_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.general_utilities


# Utility functions to get default columns to omit
def encounters_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the encounter table and other tables that
  are optionally joined
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing encounter-related table fields to omit by default
  
  """
  # Encounter table columns to omit
  encounter_columns_to_omit = ['obstetric_status', 'mode_of_arrival', 'appointment_datetime', 'appointment_status', 'arrival_datetime', 'confirmation_status', 'cancel_reason', 'expected_admission_date', 'expected_discharge_date', 'discharge_destination', 'discharge_transportation', 'accomodation_code', 'acuity', 'facility_state', 'facility_zip', 'hospital_area_id', 'hsp_account_id', 'admission_prov_id', 'discharge_prov_id', 'visit_prov_id', 'bed_id', 'bill_attend_prov_id', 'ed_episode_id', 'ip_episode_id', 'pri_problem_id', 'room_id']
  
  # Optionally-joined table columns to omit
  adtevent_columns_to_omit = ['adt_room_id', 'adt_bed_id']
  encounteradmissionreason_columns_to_omit = ['pat_enc_date_real']
  encounterchiefcomplaint_columns_to_omit = ['pat_enc_date_real']
  encounterdiagnosis_columns_to_omit = ['pat_enc_date_real']
  medicalhistory_columns_to_omit = []
  notes_columns_to_omit = ['author_provider_type', 'author_user_id', 'entry_user_id']
  
  columns_to_omit_table_map = {
    'encounter': encounter_columns_to_omit,
    'adtevent': adtevent_columns_to_omit,
    'encounteradmissionreason': encounteradmissionreason_columns_to_omit,
    'encounterchiefcomplaint': encounterchiefcomplaint_columns_to_omit,
    'encounterdiagnosis': encounterdiagnosis_columns_to_omit,
    'medicalhistory': medicalhistory_columns_to_omit,
    'notes': notes_columns_to_omit
  }
  
  # Ensure that table is a valid list
  if(table_list is None):
    table_list = list(columns_to_omit_table_map.keys())
  else:
    table_list = table_list if isinstance(table_list, list) else [table_list]
    assert(all([s in list(columns_to_omit_table_map.keys()) for s in table_list]))
  
  # Get columns to omit
  columns_to_omit = []
  for t in table_list:
    columns_to_omit = columns_to_omit + columns_to_omit_table_map[t]
  
  return columns_to_omit


  # Get encounter records based on specified filter criteria
def get_encounters(cohort_df=None, include_cohort_columns=None, join_table=None, filter_string=None, omit_columns=encounters_default_columns_to_omit(), add_cc_columns=[]):
  """Get encounter records based on specified filter criteria
  
  Parameters:
  cohort_df (PySpark df): Optional cohort dataframe for which to get encounter records (if None,
                          get all patients in the RDP). This dataframe *must* include pat_id and
                          instance columns for joining
  include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None, in which case all columns from cohort dataframe are kept.
  join_table (str): Optionally specify table to join to encounters (this can only be one of the
                    following: 'adtevent', 'encounteradmissionreason', 'encounterchiefcomplaint',
                    'encounterdiagnosis', 'medicalhistory', 'notes'). Default is None (i.e. do not
                    join any additional table)
  filter_string (str): String to use as a WHERE clause filter/condition.
  omit_columns (list): List of columns/fields to exclude from the final dataframe.
  add_cc_columns (list): List of medication cc columns to add (e.g. 'asthma',
                         'cardiac_arrhythmia', 'chronic_lung_disease')
  cache_table_name (str): Name of cache table to use to force evaluation when adding many clinical
                          concept columns
  
  Returns:
  PySpark df: Dataframe containing encounters satisfying specified filter criteria
  
  """
  # If not None, filter_string must be a string
  if(filter_string is not None):
    assert(isinstance(filter_string, str))
  
  # 'Omit' columns input must be a list
  omit_columns_from_final = [] if (omit_columns is None) else omit_columns
  assert(isinstance(omit_columns_from_final, list))
  
  # add_cc_columns must be a list of valid condition cc labels
  cc_columns_list = add_cc_columns if isinstance(add_cc_columns, list) else [add_cc_columns]
  assert(all([s in get_condition_cc_list() for s in cc_columns_list]))
  
  # If no cohort input is provided, use all patients in the RDP
  results_df = cohort_df if (cohort_df is not None) else get_basic_cohort(include_race=True)
  assert(isinstance(results_df, DataFrame))
  
  # 'Include cohort' columns input must be a list
  keep_cohort_columns = results_df.columns if (include_cohort_columns is None) else include_cohort_columns
  assert(isinstance(keep_cohort_columns, list))
  
  # Patient cohort dataframe must also include 'pat_id' and 'instance'
  for s in ['instance', 'pat_id']:
    if(s not in keep_cohort_columns):
      keep_cohort_columns = [s] + keep_cohort_columns
  
  # Make sure cohort columns to keep are all present
  assert(all([s in results_df.columns for s in keep_cohort_columns]))
  
#   # Make sure cache table name, if not None, is a string
#   if(not(cache_table_name is None)):
#     assert(isinstance(cache_table_name, str))
  
  # Get initial columns from patient cohort dataframe
  print('Getting initial columns from patient cohort dataframe...')
  results_df = results_df.select(keep_cohort_columns)
  
  # Join patient cohort to encounters table
  print('Joining encounter table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        pat_id,
        instance,
        pat_enc_csn_id,
        contact_date,
        encountertype as encounter_type,
        visittype as visit_type,
        patientclass as patient_class,
        patientstatus as patient_status,
        obstetricstatus as obstetric_status,
        modeofarrival as mode_of_arrival,
        appointmentdatetime as appointment_datetime,
        appointmentstatus as appointment_status,
        arrivaldatetime as arrival_datetime,
        confirmationstatus as confirmation_status,
        cancelreason as cancel_reason,
        expectedadmissiondate as expected_admission_date,
        admissiondatetime as admission_datetime,
        expecteddischargedate as expected_discharge_date,
        dischargedatetime as discharge_datetime,
        dischargedestination as discharge_destination,
        dischargedisposition as discharge_disposition,
        dischargetransportation as discharge_transportation,
        eddisposition as ed_disposition,
        pulse,
        respirations,
        temperature,
        bp_diastolic,
        bp_systolic,
        height,
        weight,
        bmi,
        accomodationcode as accomodation_code,
        acuity,
        facilitystate as facility_state,
        facilityzip as facility_zip,
        hospital_area_id,
        hsp_account_id,
        admission_prov_id,
        discharge_prov_id,
        visit_prov_id,
        bed_id,
        department_id,
        bill_attend_prov_id,
        ed_episode_id,
        ip_episode_id,
        pri_problem_id,
        room_id
      FROM rdp_phi.encounter"""), ['pat_id', 'instance'], how='left')
  
  # If birth_date is present in the cohort table, calculate age at encounter contact_date
  if('birth_date' in results_df.columns):
    print('Adding age (at encounter contact date) to results...')
    results_df = results_df.withColumn(
      'age_at_contact_dt',
      F.round(F.datediff(F.col('contact_date'), F.col('birth_date'))/365.25, 1))
  
  # Optionally join an additional table
  if(join_table == 'adtevent'):
    print('Joining adtevent table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          basepatientclass as base_patient_class,
          patientclass as adt_patient_class,
          effectivedatetime as effective_datetime,
          eventtype as event_type,
          eventsubtype as event_subtype,
          eventtimestamp as event_timestamp,
          firstinpatient as first_inpatient,
          event_id,
          room_id as adt_room_id,
          xfer_in_event_id,
          bed_id as adt_bed_id,
          department_id as adt_department_id
        FROM rdp_phi.adtevent
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
    print('Joining encounteradmissionreason table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          dx_id,
          codeddx as coded_dx,
          freetextreason as free_text_reason,
          pat_enc_date_real
        FROM rdp_phi.encounteradmissionreason
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
  elif(join_table == 'encounterchiefcomplaint'):
    print('Joining encounterchiefcomplaint table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          dx_id,
          chiefcomplaint as chief_complaint,
          pat_enc_date_real
        FROM rdp_phi.encounterchiefcomplaint
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
  elif(join_table == 'encounterdiagnosis'):
    print('Joining encounterdiagnosis table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          dx_id,
          diagnosisname as diagnosis_name,
          primarydiagnosis as primary_diagnosis,
          eddiagnosis as ed_diagnosis,
          problem_list_id,
          pat_enc_date_real
        FROM rdp_phi.encounterdiagnosis
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
  elif(join_table == 'medicalhistory'):
    print('Joining medicalhistory table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          dx_id,
          medhxstartdate as medhx_start_date,
          medhxenddate as medhx_end_date,
          medhxdatefreetxt as medhx_date_free_text
        FROM rdp_phi.medicalhistory
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
  elif(join_table == 'notes'):
    print('Joining notes table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          pat_id,
          instance,
          pat_enc_csn_id,
          note_id,
          note_csn_id,
          contact_date as notes_contact_date,
          completionstatus as completion_status,
          documentsource as document_source,
          full_note_text,
          lettertype as letter_type,
          notefiledatetime as note_file_datetime,
          notetype as note_type,
          notestatus as note_status,
          recordtype as record_type,
          ucnnotetype as ucn_note_type,
          authorprovidertype as author_provider_type,
          author_user_id,
          entry_user_id
        FROM rdp_phi.notes
        """), ['pat_id', 'instance', 'pat_enc_csn_id'], how='left')
  
  if(join_table in ['encounteradmissionreason', 'encounterchiefcomplaint', 'encounterdiagnosis', 'medicalhistory']):
    # Join to diagnosis table
    print('Joining diagnosis table to get descriptions...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          dx_id,
          instance,
          name
        FROM rdp_phi.diagnosis"""), ['dx_id', 'instance'], how='left')
    
    # Join diagnosis SNOMED codes
    snomed_codes_df = get_snomed_from_dx_ids(results_df, use_column='dx_id')
    print('Joining SNOMED codes...')
    results_df = results_df.join(F.broadcast(snomed_codes_df), ['instance', 'dx_id'], how='left')
    
    # Add condition cc columns
    for label in cc_columns_list:
      print("Adding column for '{}' condition clinical concept...".format(label))
      results_df = add_condition_cc_column(results_df, label, search_column='dx_id')
  
  # Apply filter to get subset of records
  if(filter_string is not None):
    print('Applying specified filter to get subset of records...')
    filter_string_for_print = filter_string if (len(filter_string) <= 1000) else '{}...'.format(filter_string[:1000])
    print(filter_string_for_print)
    results_df = results_df.where(filter_string)
  
  # Exclude specified columns
  if(len(omit_columns_from_final) > 0):
    print('Omitting the following columns:')
    print(omit_columns_from_final)
    results_df = results_df.select(
      [s for s in results_df.columns if s not in omit_columns_from_final])
  
  return results_df
  