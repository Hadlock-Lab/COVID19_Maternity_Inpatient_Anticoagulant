# Author: Yeon Mi Hwang
# Date: 4/20/23

# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.A_get_cohorts.cohort_utilities

def append_outcome_df(event_name, treatment_dict, control_dict, matched_control_dict, SA1_matched_control_dict, yes = 'Yes', no = 'No'):
  event.append(event_name)
  treatment_yes.append(treatment_dict[yes])
  treatment_no.append(treatment_dict[no])
  control_yes.append(control_dict[yes])
  control_no.append(control_dict[no])
  matched_control_yes.append(matched_control_dict[yes])
  matched_control_no.append(matched_control_dict[no])
  SA1_matched_control_yes.append(SA1_matched_control_dict[yes])
  SA1_matched_control_no.append(SA1_matched_control_dict[no])
  treatment_prevalence_rate.append(get_prevalence(treatment_dict, 'treatment_group', no_string=no, yes_string =yes)[0])
  treatment_CI_low.append(get_prevalence(treatment_dict, 'treatment_group', no_string=no, yes_string =yes)[1])
  treatment_CI_up.append(get_prevalence(treatment_dict, 'treatment_group', no_string=no, yes_string =yes)[2])
  control_prevalence_rate.append(get_prevalence(control_dict, 'control_group', no_string=no, yes_string =yes)[0])
  control_CI_low.append(get_prevalence(control_dict, 'control_group', no_string=no, yes_string =yes)[1])
  control_CI_up.append(get_prevalence(control_dict, 'control_group', no_string=no, yes_string =yes)[2])
  matched_control_prevalence_rate.append(get_prevalence(matched_control_dict, 'matched_control_group', no_string=no, yes_string =yes)[0])
  matched_control_CI_low.append(get_prevalence(matched_control_dict, 'matched_control_group', no_string=no, yes_string =yes)[1])
  matched_control_CI_up.append(get_prevalence(matched_control_dict, 'matched_control_group', no_string=no, yes_string =yes)[2])
  SA1_matched_control_prevalence_rate.append(get_prevalence(SA1_matched_control_dict, 'SA1_matched_control_group', no_string=no, yes_string =yes)[0])
  SA1_matched_control_CI_low.append(get_prevalence(SA1_matched_control_dict, 'SA1_matched_control_group', no_string=no, yes_string =yes)[1])
  SA1_matched_control_CI_up.append(get_prevalence(SA1_matched_control_dict, 'SA1_matched_control_group', no_string=no, yes_string =yes)[2])


event = []
treatment_yes = []
treatment_no = []
control_yes = []
control_no = []
matched_control_yes = []
matched_control_no = []
SA1_matched_control_yes = []
SA1_matched_control_no = []
treatment_prevalence_rate = []
control_prevalence_rate = []
matched_control_prevalence_rate = []
SA1_matched_control_prevalence_rate = []
treatment_CI_low = []
treatment_CI_up = []
control_CI_low = []
control_CI_up = []
matched_control_CI_low = []
matched_control_CI_up = []
SA1_matched_control_CI_low = []
SA1_matched_control_CI_up = [] 


dict_anti_sc = make_condition_dict(anti_pd, 'secondary_coagulopathy')
dict_noanti_sc = make_condition_dict(noanti_pd, 'secondary_coagulopathy')
dict_noanti_m_sc = make_condition_dict(noanti_m_pd, 'secondary_coagulopathy')
dict_noanti_m_sa1_sc = make_condition_dict(noanti_m_sa1_pd, 'secondary_coagulopathy')
append_outcome_df('secondary_coagulopathy', dict_anti_sc, dict_noanti_sc, dict_noanti_m_sc, dict_noanti_m_sa1_sc)


dict_anti_bleeding = make_condition_dict(anti_pd, 'bleeding_max')
dict_noanti_bleeding = make_condition_dict(noanti_pd, 'bleeding_max')
dict_noanti_m_bleeding = make_condition_dict(noanti_m_pd, 'bleeding_max')
dict_noanti_m_sa1_bleeding = make_condition_dict(noanti_m_sa1_pd, 'bleeding_max')
append_outcome_df('bleeding_max', dict_anti_bleeding, dict_noanti_bleeding, dict_noanti_m_bleeding, dict_noanti_m_sa1_bleeding)

dict_anti_pph = make_condition_dict(anti_pd, 'postpartum_hemorrhage')
dict_noanti_pph = make_condition_dict(noanti_pd, 'postpartum_hemorrhage')
dict_noanti_m_pph = make_condition_dict(noanti_m_pd, 'postpartum_hemorrhage')
dict_noanti_m_sa1_pph = make_condition_dict(noanti_m_sa1_pd, 'postpartum_hemorrhage')
append_outcome_df('postpartum_hemorrhage', dict_anti_pph, dict_noanti_pph, dict_noanti_m_pph, dict_noanti_m_sa1_pph)


dict_anti_pe = make_condition_dict(anti_pd, 'preeclampsia_max')
dict_noanti_pe = make_condition_dict(noanti_pd, 'preeclampsia_max')
dict_noanti_m_pe = make_condition_dict(noanti_m_pd, 'preeclampsia_max')
dict_noanti_m_sa1_pe = make_condition_dict(noanti_m_sa1_pd, 'preeclampsia_max')
append_outcome_df('preeclampsia_max', dict_anti_pe, dict_noanti_pe, dict_noanti_m_pe, dict_noanti_m_sa1_pe)



dict_anti_lbw = make_low_birth_weight_dict(anti_pd)
dict_noanti_lbw = make_low_birth_weight_dict(noanti_pd)
dict_noanti_m_lbw = make_low_birth_weight_dict(noanti_m_pd)
dict_noanti_m_sa1_lbw = make_low_birth_weight_dict(noanti_m_sa1_pd)
append_outcome_df('low birth weight', dict_anti_lbw, dict_noanti_lbw, dict_noanti_m_lbw, dict_noanti_m_sa1_lbw, yes = 'low_birth_weight', no = 'normal_birth_weight')


dict_anti_sb = make_stillbirth_dict(anti_pd)
dict_noanti_sb = make_stillbirth_dict(noanti_pd)
dict_noanti_m_sb = make_stillbirth_dict(noanti_m_pd)
dict_noanti_m_sa1_sb = make_stillbirth_dict(noanti_m_sa1_pd)
append_outcome_df('stillbirth', dict_anti_sb, dict_noanti_sb, dict_noanti_m_sb, dict_noanti_m_sa1_sb, yes = 'Fetal Demise', no = 'Living')


def determine_term_or_preterm_status(df):
  d = {'term': 0, 'preterm': 0}
  for index, row in df.iterrows():
    gestational_days = row['gestational_days']
    if gestational_days >= 259:
      d['term'] += 1
    else:
      d['preterm'] += 1
  print(d)
  return(d)





dict_anti_ptb = determine_term_or_preterm_status(anti_pd)
dict_noanti_ptb = determine_term_or_preterm_status(noanti_pd)
dict_noanti_m_ptb = determine_term_or_preterm_status(noanti_m_pd)
dict_noanti_m_sa1_ptb = determine_term_or_preterm_status(noanti_m_sa1_pd)
append_outcome_df('preterm birth', dict_anti_ptb, dict_noanti_ptb, dict_noanti_m_ptb, dict_noanti_m_sa1_ptb, yes = 'preterm', no = 'term')


def determine_epb_status(df):
  d = {'nonEPB': 0, 'EPB': 0}
  for index, row in df.iterrows():
    gestational_days = row['gestational_days']
    if gestational_days >= 28*7:
      d['nonEPB'] += 1
    else:
      d['EPB'] += 1
  print(d)
  return(d)


dict_anti_epb = determine_epb_status(anti_pd)
dict_noanti_epb = determine_epb_status(noanti_pd)
dict_noanti_m_epb = determine_epb_status(noanti_m_pd)
dict_noanti_m_sa1_epb = determine_epb_status(noanti_m_sa1_pd)
append_outcome_df('extremely preterm birth', dict_anti_epb, dict_noanti_epb, dict_noanti_m_epb, dict_noanti_m_sa1_epb, yes = 'EPB', no = 'nonEPB')



anti_pd = get_outcomes(anti_pd)
noanti_pd = get_outcomes(noanti_pd)
noanti_m_pd = get_outcomes(noanti_m_pd)
noanti_m_sa1_pd = get_outcomes(noanti_m_sa1_pd)



def make_SGA_dict(df):
  d = {'nonSGA': 0, 'SGA': 0}
  for index, row in df.iterrows():
    if row['SGA'] == 'SGA':
      d['SGA'] += 1
    else:
      d['nonSGA'] += 1  # reported as 'Living' or missing field
  print(d)
  return d

dict_anti_SGA = make_SGA_dict(anti_pd)
dict_noanti_SGA = make_SGA_dict(noanti_pd)
dict_noanti_m_SGA = make_SGA_dict(noanti_m_pd)
dict_noanti_m_sa1_SGA = make_SGA_dict(noanti_m_sa1_pd)
append_outcome_df('small for gestational age', dict_anti_SGA, dict_noanti_SGA, dict_noanti_m_SGA, dict_noanti_m_sa1_SGA, yes = 'SGA', no = 'nonSGA')



def make_oxygen_device_dict(l):
  d = {'None': 0, 'Low-Flow': 0, 'High-Flow': 0, 'High-Flow + Vasopressors': 0, 'Venhilator': 0}
  for i in l:
    if i is None:
      d['None'] += 1
    elif i == -1:
      d['None'] += 1
    elif i == 0:
      d['None'] += 1
    elif i == 1:
      d['Low-Flow'] += 1
    elif i == 2:
      d['High-Flow'] += 1
    elif i == 3:
      d['High-Flow + Vasopressors'] += 1
    elif i == 4:
      d['Venhilator'] += 1
    else:
      print(i)
  print(d)
  return d


def make_vasopressor_dict(l):
  d = {'Yes': 0, 'No': 0}
  for i in l:
    if isinstance(i, float):
      i = float(i)
      if i > 0:
        d['Yes'] += 1
      else:
        d['No'] += 1
    else:
      d['No'] += 1
  print(d)
  return d


def make_oxygen_assistance_dict(d):
  new_d = {}
  #{'None': 0, 'Low-Flow': 0, 'High-Flow': 0, 'Venhilator': 0}
  new_d['None'] =  d['None']
  new_d['Oxygen Assistance'] = d['Low-Flow'] + d['High-Flow'] + d['High-Flow + Vasopressors'] + d['Venhilator']
  print(new_d)
  return new_d


def make_patient_class_dict(df):
  d = {'None': 0, 'Outpatient': 0, 'Emergency Care': 0, 'Inpatient': 0}
  for index, row in df.iterrows():
    patient_class = row['max_patient_class']
    if patient_class < 1:
      d['None'] += 1
    elif patient_class == 1:
      d['Outpatient'] += 1
    elif patient_class == 2:
      d['Emergency Care'] += 1
    else:
      d['Inpatient'] += 1
  print(d)
  return d




dict_anti_o2 = make_oxygen_device_dict(anti_pd['max_oxygen_device'])
dict_noanti_o2 = make_oxygen_device_dict(noanti_pd['max_oxygen_device'])
dict_noanti_m_o2 = make_oxygen_device_dict(noanti_m_pd['max_oxygen_device'])
dict_noanti_m_sa1_o2 = make_oxygen_device_dict(noanti_m_sa1_pd['max_oxygen_device'])


dict_anti_o2_a = make_oxygen_assistance_dict(dict_anti_o2)
dict_noanti_o2_a = make_oxygen_assistance_dict(dict_noanti_o2)
dict_noanti_m_o2_a = make_oxygen_assistance_dict(dict_noanti_m_o2)
dict_noanti_m_sa1_o2_a = make_oxygen_assistance_dict(dict_noanti_m_sa1_o2)

append_outcome_df('O2 assistance', dict_anti_o2_a, dict_noanti_o2_a, dict_noanti_m_o2_a, dict_noanti_m_sa1_o2_a, yes = 'Oxygen Assistance', no = 'None')



dict_anti_v = make_vasopressor_dict(anti_pd['vasopressor_sum'])
dict_noanti_v = make_vasopressor_dict(noanti_pd['vasopressor_sum'])
dict_noanti_m_v = make_vasopressor_dict(noanti_m_pd['vasopressor_sum'])
dict_noanti_m_sa1_v = make_vasopressor_dict(noanti_m_sa1_pd['vasopressor_sum'])
append_outcome_df('vasopressor use', dict_anti_v, dict_noanti_v, dict_noanti_m_v, dict_noanti_m_sa1_v)




def make_death_dict(df):
  d = {'None': 0, 'Dead': 0}
  for index, row in df.iterrows():
    if pd.isnull(row['deathdate']):
      d['None'] += 1
    else:
      d['Dead'] += 1
  print(d)
  return d

import datetime
import numpy as np
def make_covid_severity_dict(df):
  d1 = {'Mild': 0, 'Moderate': 0, 'Severe': 0, 'Dead': 0}
  d2 = {'Mild': 0, 'Moderate,Severe,Dead': 0}
  for index, row in df.iterrows():
    if isinstance(row['deathdate'], datetime.datetime) and not np.isnat(np.datetime64(row['deathdate'])):
      d1['Dead'] += 1
      d2['Moderate,Severe,Dead'] += 1
    elif row['max_oxygen_device'] >= 2:
      d1['Severe'] += 1
      d2['Moderate,Severe,Dead'] += 1
    elif row['max_patient_class'] >= 3 or row['max_oxygen_device'] == 1:
      d1['Moderate'] += 1
      d2['Moderate,Severe,Dead'] += 1
    else:
      d1['Mild'] += 1
      d2['Mild'] += 1
  print(d1)
  print (d2)
  return d1, d2 



dict_anti_death = make_death_dict(anti_pd)
dict_noanti_death = make_death_dict(noanti_pd)
dict_noanti_m_death = make_death_dict(noanti_m_pd)
dict_noanti_m_sa1_death = make_death_dict(noanti_m_sa1_pd)
append_outcome_df('death', dict_anti_death, dict_noanti_death, dict_noanti_m_death, dict_noanti_m_sa1_death, yes = 'Dead', no = 'None')


data = {
  'outcome': event,
  'treatment_yes': treatment_yes, 
  'treatment_no' : treatment_no, 
  'control_yes' : control_yes, 
  'control_no' : control_no, 
  'matched_control_yes' : matched_control_yes, 
  'matched_control_no' : matched_control_no, 
  'SA1_matched_control_yes' : SA1_matched_control_yes, 
  'SA1_matched_control_no' : SA1_matched_control_no, 
  'treatment_prevalence_rate' : treatment_prevalence_rate, 
  'treatment_CI_low' : treatment_CI_low, 
  'treatment_CI_up' : treatment_CI_up, 
  'control_prevalence_rate' : control_prevalence_rate, 
  'control_CI_low' : control_CI_low, 
  'control_CI_up' : control_CI_up, 
  'matched_control_prevalence_rate' : matched_control_prevalence_rate, 
  'matched_control_CI_low' : matched_control_CI_low, 
  'matched_control_CI_up' : matched_control_CI_up, 
  'SA1_matched_control_prevalence_rate' : SA1_matched_control_prevalence_rate, 
  'SA1_matched_control_CI_low' : SA1_matched_control_CI_low, 
  'SA1_matched_control_CI_up' : SA1_matched_control_CI_up 
}
outcome = pd.DataFrame.from_dict(data)



# get figures 


df = outcome
def get_2list(pd_df, outcome):
  outcome_info = pd_df.loc[pd_df["outcome"]==outcome].reset_index()
  treatment_list = list(outcome_info.loc[0,['treatment_prevalence_rate', 'treatment_CI_low', 'treatment_CI_up']])
  #control_list = list(outcome_info.loc[0,['control_prevalence_rate', 'control_CI_low', 'control_CI_up']])
  matched_control_list = list(outcome_info.loc[0,['matched_control_prevalence_rate', 'matched_control_CI_low', 'matched_control_CI_up']])
  #SA1_matched_control_list = list(outcome_info.loc[0,['SA1_matched_control_prevalence_rate', 'SA1_matched_control_CI_low', 'SA1_matched_control_CI_up']])
  return treatment_list,  matched_control_list


def get_CI_2figure(anti_list, noanti_m_list, figure_title = None, figure_path = None, save = False):
  plt.rcParams.update({'font.size': 12})
  plt.rcParams['pdf.fonttype'] = 42
  plt.figure(figsize=(3, 5))
  x_ticks = ("treatment group\n(n=191)", "matched\ncontrol group\n(n=188)")
  plt.title(figure_title)
  x_1, y_1 = 1.8, anti_list[0]
  x_2, y_2 = 3.2, noanti_m_list[0]
  #x_3, y_3 = 2.4, noanti_m_sa1_list[0]
             
  y_1_li, y_1_ui = anti_list[1], anti_list[2]
  y_2_li, y_2_ui = noanti_m_list[1], noanti_m_list[2]
  #y_3_li, y_3_ui = noanti_m_sa1_list[1], noanti_m_sa1_list[2]

  err_1 = np.array([[(y_1 - y_1_li)], [(y_1_ui - y_1)]])
  err_2 = np.array([[(y_2 - y_2_li)], [(y_2_ui - y_2)]])
  #err_3 = np.array([[(y_3 - y_3_li)], [(y_3_ui - y_3)]])
  
  plt.errorbar(x=x_1, y=y_1, yerr=err_1, color="red", capsize=3,
             linestyle="None",
             marker="s", markersize=10, mfc="red", mec="black")
  plt.errorbar(x=x_2, y=y_2, yerr=err_2, color="green", capsize=3,
             linestyle="None",
             marker="s", markersize=10, mfc="green", mec="black")
  # plt.errorbar(x=x_3, y=y_3, yerr=err_3, color="blue", capsize=3,
  #            linestyle="None",
  #            marker="s", markersize=10, mfc="blue", mec="black")

  
  
  plt.ylabel("{0}\n".format(figure_title))
  plt.xticks([1.8, 3.2], x_ticks, rotation = 90)
  plt.xlim(1,4)
  plt.ylim(0, 0.5)

  plt.tight_layout()
  if save == True:
    plt.savefig(figure_path, format = 'pdf')
    plt.show()
    from google.colab import files
    files.download(figure_path) 



 def get_CI_3figure(anti_list, noanti_m_list, noanti_m_sa1_list, figure_title = None, figure_path = None, save = False):
  plt.rcParams.update({'font.size': 12})
  plt.rcParams['pdf.fonttype'] = 42
  plt.figure(figsize=(4, 5))
  x_ticks = ("treatment group\n(n=191)", "matched\ncontrol group\n(n=186)", "SA matched\ncontrol group\n(n=189)")
  plt.title(figure_title)
  x_1, y_1 = 1.2, anti_list[0]
  x_2, y_2 = 1.8, noanti_m_list[0]
  x_3, y_3 = 2.4, noanti_m_sa1_list[0]
             
  y_1_li, y_1_ui = anti_list[1], anti_list[2]
  y_2_li, y_2_ui = noanti_m_list[1], noanti_m_list[2]
  y_3_li, y_3_ui = noanti_m_sa1_list[1], noanti_m_sa1_list[2]

  err_1 = np.array([[(y_1 - y_1_li)], [(y_1_ui - y_1)]])
  err_2 = np.array([[(y_2 - y_2_li)], [(y_2_ui - y_2)]])
  err_3 = np.array([[(y_3 - y_3_li)], [(y_3_ui - y_3)]])
  
  plt.errorbar(x=x_1, y=y_1, yerr=err_1, color="red", capsize=3,
             linestyle="None",
             marker="s", markersize=10, mfc="red", mec="black")
  plt.errorbar(x=x_2, y=y_2, yerr=err_2, color="green", capsize=3,
             linestyle="None",
             marker="s", markersize=10, mfc="green", mec="black")
  plt.errorbar(x=x_3, y=y_3, yerr=err_3, color="blue", capsize=3,
             linestyle="None",
             marker="s", markersize=10, mfc="blue", mec="black")

  
  
  plt.ylabel("{0}\n".format(figure_title))
  plt.xticks([1.2, 1.8, 2.4], x_ticks, rotation = 90)
  plt.xlim(1,2.6)
  plt.ylim(0, 0.5)

  plt.tight_layout()
  if save == True:
    plt.savefig(figure_path, format = 'pdf')
    plt.show()
    from google.colab import files
    files.download(figure_path) 



 # figure output 


anti_list, noanti_m_list = get_2list(df, 'secondary_coagulopathy')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Secondary Coagulopathy", figure_path = "secondary_coaglopathy.pdf", save = True)


anti_list,  noanti_m_list = get_2list(df, 'bleeding_max')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Bleeding", figure_path = "bleeding.pdf", save = True)


anti_list, noanti_m_list = get_2list(df, 'postpartum_hemorrhage')
get_CI_2figure(anti_list, noanti_m_list,  figure_title = "Postpartum Hemorrhage", figure_path = "pph.pdf", save = True)


anti_list, noanti_m_list= get_2list(df, 'stillbirth')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Fetal Demise", figure_path = "iufd.pdf", save = True)


anti_list, noanti_m_list= get_2list(df, 'preeclampsia_max')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Preeclampsia", figure_path = "pe.pdf", save = True)


anti_list, noanti_m_list = get_2list(df, 'low birth weight')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Low Birth Weight", figure_path = "lbw.pdf", save = True)



anti_list,  noanti_m_list = get_2list(df, 'preterm birth')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Preterm Birth", figure_path = "ptb.pdf", save = True)


anti_list, noanti_m_list,  = get_2list(df, 'extremely preterm birth')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Extremely Preterm Birth", figure_path = "epb.pdf")



anti_list, noanti_m_list = get_2list(df, 'small for gestational age')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Small for Gestational Age", figure_path = "sga.pdf", save = True)


anti_list, noanti_m_list = get_2list(df, 'O2 assistance')
get_CI_2figure(anti_list, noanti_m_list,  figure_title = "O2 assistance", figure_path = "o2.pdf", save = True)



anti_list, noanti_m_list = get_2list(df, 'vasopressor use')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "Vasopressor Use", figure_path = "vasopressor.pdf", save = True)

anti_list,  noanti_m_list = get_2list(df, 'death')
get_CI_2figure(anti_list, noanti_m_list, figure_title = "death", figure_path = "death.pdf", save = True)



anti_df = pd.read_csv('anti.csv')
#noanti_df = pd.read_csv('noanti.csv')
noanti_m_df = pd.read_csv('noanti_m.csv')
#noanti_m_sa1_df = pd.read_csv('noanti_m_sa1.csv')
anti_df['inpatient_duration_sum'] = anti_df['inpatient_duration_sum'].fillna(0)
noanti_m_df['inpatient_duration_sum'] = noanti_m_df['inpatient_duration_sum'].fillna(0)


dict_number_of_covid_encounters = {'treatment group\n(n=191)': list(anti_df.n_covid_encounters),
                                   'matched\ncontrol group\n(n=188)': list(noanti_m_df.n_covid_encounters)}

dict_number_of_postcovid_diagnoses = {'treatment group\n(n=191)': list(anti_df.count_postcovid_diagnoses),
                                   'matched\ncontrol group\n(n=188)': list(noanti_m_df.count_postcovid_diagnoses)}

dict_inpatient_stay = {'treatment group\n(n=191)': list(anti_df.inpatient_duration_sum),
                                   'matched\ncontrol group\n(n=188)': list(noanti_m_df.inpatient_duration_sum)}


                                   
