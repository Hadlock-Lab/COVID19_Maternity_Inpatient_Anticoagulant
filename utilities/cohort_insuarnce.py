def clean_payors(x):
  # Reclassify insurance by looking at the payor 
  if pandas.isna(x['payor']):
    if x['financialclass'] == 'OTHER GOVERNMENT':
      return 'Other'
    elif x['financialclass'] == 'UNIFORM MEDICAL':
      return 'Other'
    else:
      return x['financialclass']
  if 'medicare' in x['payor'].lower():
    return 'Medicare'
  elif 'medicaid' in x['payor'].lower() or 'medi-cal' in x['payor'].lower() :
    return 'Medicaid'
  elif 'uninsured' in x['payor'].lower() or 'preexisting condition' in x['payor'].lower():
    return 'Self-pay'
  elif 'workers compensation' in x['payor'].lower() or 'victims' in x['payor'].lower():
    return 'Other'
  elif 'patient responsibility' in x['payor'].lower():
    return 'Self-pay'
  else:
    return 'Commercial'


insurnance_map = {'AETNA':'3_Commercial',
'BASIC HEALTH PLAN':'3_Commercial',
'CIGNA':'3_Commercial',
'CIGNA FIRST CHOICE':'3_Commercial',
'Capitation':'3_Commercial',
'Commercial':'3_Commercial',
'FIRST CHOICE':'3_Commercial',
'GREAT WEST':'3_Commercial',
'HEALTHY OPTIONS':'3_Commercial',
'LABOR AND INDUSTRIES':'3_Commercial',
'MEDICAID OTHER':'1_Medicaid',
'MEDICARE HMO':'2_Medicare',
'MEDICARE HMO MEDICAL GROUP':'2_Medicare',
'Managed Care':'3_Commercial',
'Medicaid':'1_Medicaid',
'Medicaid HMO':'1_Medicaid',
'Medicare':'2_Medicare',
'Medicare HMO':'2_Medicare',
'OTHER GOVERNMENT':'5_Other-Government',
'OTHER MANAGED CARE':'3_Commercial',
'Other':'6_Other',
'PACIFICARE':'3_Commercial',
'PACMED':'3_Commercial',
'PREMERA':'3_Commercial',
'REGENCE':'3_Commercial',
'Self-pay':'4_Uninsured-Self-Pay',
'UNIFORM MEDICAL':'5_Other-Government',
'UNITED':'3_Commercial',
"Worker's Comp":"6_Other",
"Uninsured":"7_Uninsured"}


guarantor_query = \
'''
select 
  a.pat_id,
  a.instance,
  b.guarantorpatientrelationship,
  c.type,
  c.financialclass,
  e.payor
from 
  covid_patients as a 
    join guarantorpatient as b on a.pat_id == b.pat_id and a.instance == b.instance
    join guarantor as c on b.account_id == c.account_id and b.instance == c.instance
    left join rdp_phi.guarantorcoverage as d on b.account_id == d.account_id and b.instance == d.instance
    left join coverage as e on d.coverage_id == e.coverage_id and d.instance == e.instance
where 
  guarantorpatientrelationship IS NOT NULL 
  and c.type IS NOT NULL 
  and financialclass IS NOT NULL
  and c.type == 'Personal/Family'
'''

guarantor_df = spark.sql(guarantor_query)
guarantor_df = guarantor_df.toPandas()

guarantor_filtered = guarantor_df.sort_values('pat_id').drop_duplicates()
print(f'Number of individuals with coverage: {len(guarantor_filtered["pat_id"].unique())}')

guarantor_filtered['financialclass'] = guarantor_filtered.apply(lambda pat: clean_payors(pat), axis = 1)
guarantor_filtered['insurance'] = guarantor_filtered['financialclass'].apply(insurnance_map.get)

guarantor_filtered_renamed = guarantor_filtered[['pat_id','instance','insurance']].sort_values(['pat_id','insurance'])
guarantor_filtered_renamed = guarantor_filtered_renamed.groupby('pat_id').first()
guarantor_filtered_renamed['insurance'] = guarantor_filtered_renamed['insurance'].str.split('_').str[1]
guarantor_filtered_renamed = guarantor_filtered_renamed.reset_index()
