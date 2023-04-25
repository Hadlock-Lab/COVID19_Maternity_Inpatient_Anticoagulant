# Define lab field search strings map
cc_lab_search_strings = {
  'sars_cov_2_pcr_naat': {
    'common_name': [
      'SARS CORONAVIRUS 2 RNA ORD', 'POC SARS CORONAVIRUS 2 RNA ORD', 'SARS-COV-2 (COVID-19) QUAL PCR RESULT', 'COV19EX'],
    'order_name': ['NAAT', 'PCR']
  }
}

# SARS-CoV-2 PCR or NAAT
sars_cov_2_codes = [
  '94306-8',  # SARS-CoV-2 (COVID-19) RNA panel - Unspecified specimen by NAA with probe detection
  '94309-2',  # SARS-CoV-2 (COVID-19) RNA [Presence] in Unspecified specimen by NAA with probe detection
  '94500-6',  # SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection
  '94531-1',  # SARS-CoV-2 (COVID-19) RNA panel - Respiratory specimen by NAA with probe detection
  '94533-7',  # SARS-CoV-2 (COVID-19) N gene [Presence] in Respiratory specimen by NAA with probe detection
  '94565-9'   # SARS-CoV-2 (COVID-19) RNA [Presence] in Nasopharynx by NAA with non-probe detection
]

# create registry
labs_cc_registry = {
  'sars_cov_2': sars_cov_2_codes
}
