# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities


# covid anticoagulant prophylactic dose administered cohort 
covid_administered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_6_102422")
# covid anticoagulant not administered cohort 
covid_notadministered = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_cohort_covid_maternity_no_anticoagulant_expanded_6_102422")



covid_administered_expanded1 = get_precovid_risk_factors(covid_administered)
covid_administered_expanded2 = get_precovid_risk_factors(covid_administered_expanded1, time_filter_string = 'date_of_entry > date_sub(conception_date, 730) AND date_of_entry < covid_test_date', rename_count = 'count_precovid2yrs_diagnoses', rename_list = 'list_precovid2yrs_diagnoses')
covid_notadministered_expanded1 = get_precovid_risk_factors(covid_notadministered)
covid_notadministered_expanded2 = get_precovid_risk_factors(covid_notadministered_expanded1, time_filter_string = 'date_of_entry > date_sub(conception_date, 730) AND date_of_entry < covid_test_date', rename_count = 'count_precovid2yrs_diagnoses', rename_list = 'list_precovid2yrs_diagnoses')


covid_administered_expanded3 = get_covid_14days_med(covid_administered_expanded2)
covid_notadministered_expanded3 = get_covid_14days_med(covid_notadministered_expanded2)

write_data_frame_to_sandbox(covid_administered_expanded3, 'yh_cohort_covid_maternity_covid_anticoagulant_prophylactic_expanded_7_102422', sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(covid_notadministered_expanded3, 'yh_cohort_covid_maternity_no_anticoagulant_expanded_7_102422', sandbox_db='rdp_phi_sandbox', replace=True)


