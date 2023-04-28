# COVID19_Maternity_Inpatient_Anticoagulant

This contains the code used to conduct the retrospective cohort study for Hwang et al. 2023 manuscript on Inpatient prophylactic anticoagulant use in pregnant patients with COVID-19. 

All clinical logic is shared. Results were aggregated and reported within the paper to the extent possible while maintaining privacy from personal health information (PHI) as required by law. All data is archived within PHS systems in a HIPAA-secure audited compute environment to facilitate verification of study conclusions. Due to the sensitive nature of the data we are unable to share the data used in these analyses, although, all scripts used to conduct the analyses of the paper are shared herein. 

This study was conducted on a PHSA COVID-19 maternity cohort, investigated under the study by Piekos et al. 2022. 
* For codes of background cohort and generation, please refer to https://github.com/Hadlock-Lab/COVID19_vaccination_in_pregnancy 
* For codes of ETL tools, please refer to https://github.com/Hadlock-Lab/CEDA_tools_ver20221220

## Installation
We used Python version 3.8.10. 

## Workflow 
Our workflow is described using alphabets. 

- [utilities](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/utilities) contains functions written by Hadlock Lab and required to be loaded for analysis   

- [background cohort](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/background_cohorts) is used to generate PHSA COVID-19 Maternity Cohort. These codes were from [Piekos et al. 2022](https://github.com/Hadlock-Lab/COVID19_vaccination_in_pregnancy) 

- [A_get_cohorts](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/A_get_cohorts) prepares cohort for analysis. Includes cohort selection process and feature engineering. Functions that are repeatedly used are in cohort_utilities.py

- [B_get_matched_cohort](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/B_get_matched_cohort) runs classification model, propensity score matching, and feature importance. Functions that are repeatedly used are in model_utilities.py

- [C_get_figures](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/C_get_figures) contains codes used for figure or table generation 

- [D_sensitivity_analysis](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/blob/main/D_sensitivity_analysis.py) is code for sensitivity analysis 
