# Load environment
import pandas as pd
from io import StringIO
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# import functions from other notebooks
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.basic_cohort
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.devices_cc_utilities
import COVID19_Maternity_Inpatient_Anticoagulant.utilities.vitals_cc_utilities


# Load flowsheet clinical concepts
flowsheet_id_concept_map = {
  'oxygen_device': {
    '1000': ['12225787', '1209418', '12225537', '10020158', '12221949'],
    '2000': ['3567', '12995', '9943', '600603', '2273', '13997', '12926'],
    '4000': ['301030', '3043000', '7075522', '1120009200', '2100000006', '507012', '7086000', '1140100030']
  },
  'crrt': {
    '1000': ['12236972', '12236981', '12236980', '12236983', '12236982', '12236984', '12236985', '13022991', '12236969', '12236971', '12236970', '12236973', '13022980', '12266944', '12236989'],
    '2000': ['10623', '6277', '11289', '16468', '16469', '16470', '16472', '16471', '16473', '21363'],
    '4000': []
  },
  'ecmo': {
    '1000': ['12019586', '12019597', '12019598', '12019599', '12246460', '12246532', '12246529', '12246530', '12019585', '12267866', '12019596', '12246461', '12246533'],
    '2000': ['21354', '21355'],
    '4000': []
  }
}


# Utility function to get default columns to omit
def flowsheets_default_columns_to_omit(table_list=None):
  """Get list of columns that are omitted, by default, from the flowsheet tables and other tables that
  are optionally joined
  
  Parameters:
  table_list (str or list): Table or list of tables for which to get default columns to omit
  
  Returns:
  list: A list containing flowsheet-related table fields to omit by default
  
  """
  # Flowsheet entry table columns to omit
  flowsheet_entry_columns_to_omit = ['current_date', 'entry_user_id', 'instant_recorded_local', 'taken_user_id']
  
  # Flowsheet definition table columns to omit
  flowsheet_definition_columns_to_omit = ['can_be_graphed', 'cross_encounter', 'multiselect', 'stores_calculated_data']
  
  # Flowsheet table columns to omit
  flowsheet_columns_to_omit = []
  
  # Optionally-joined table columns to omit
  inpatient_data_columns_to_omit = []
  inpatient_data_flowsheet_columns_to_omit = []
  
  columns_to_omit_table_map = {
    'flowsheetentry': flowsheet_entry_columns_to_omit,
    'flowsheetdefinition': flowsheet_definition_columns_to_omit,
    'flowsheetentry': flowsheet_entry_columns_to_omit,
    'inpatientdata': inpatient_data_columns_to_omit,
    'inpatientdataflowsheet': inpatient_data_flowsheet_columns_to_omit
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


# Define flo_meas_id to SNOMED code mapping
headers_flo_meas_id_to_snomed_mapping = \
"""
flo_meas_id	instance	code	code_system	description
"""

device_flo_meas_id_to_snomed_mapping = \
"""
10020158	1000	336589003	SNOMED	Oxygen equipment (physical object)
1209418	1000	336589003	SNOMED	Oxygen equipment (physical object)
12221949	1000	336589003	SNOMED	Oxygen equipment (physical object)
12225537	1000	336589003	SNOMED	Oxygen equipment (physical object)
12225787	1000	336589003	SNOMED	Oxygen equipment (physical object)
12236384	1000	336589003	SNOMED	Oxygen equipment (physical object)
12250869	1000	336589003	SNOMED	Oxygen equipment (physical object)
10338	2000	336589003	SNOMED	Oxygen equipment (physical object)
12926	2000	336589003	SNOMED	Oxygen equipment (physical object)
12995	2000	336589003	SNOMED	Oxygen equipment (physical object)
13997	2000	336589003	SNOMED	Oxygen equipment (physical object)
16208	2000	336589003	SNOMED	Oxygen equipment (physical object)
2273	2000	336589003	SNOMED	Oxygen equipment (physical object)
3567	2000	336589003	SNOMED	Oxygen equipment (physical object)
600603	2000	336589003	SNOMED	Oxygen equipment (physical object)
1120009200	4000	336589003	SNOMED	Oxygen equipment (physical object)
1140100030	4000	336589003	SNOMED	Oxygen equipment (physical object)
2100000006	4000	336589003	SNOMED	Oxygen equipment (physical object)
301030	4000	336589003	SNOMED	Oxygen equipment (physical object)
3043000	4000	336589003	SNOMED	Oxygen equipment (physical object)
507012	4000	336589003	SNOMED	Oxygen equipment (physical object)
7086000	4000	336589003	SNOMED	Oxygen equipment (physical object)
12236969	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236970	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236971	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236972	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236973	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236980	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236981	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236982	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236983	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236984	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12236985	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12247729	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12247730	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12247731	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12247732	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12266944	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
13022991	1000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
10626	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12458	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12459	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12460	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12461	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12462	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12463	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16468	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16469	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16470	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16471	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16472	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16590	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16592	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16595	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16597	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16599	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
16600	2000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200004	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200005	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200006	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200007	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200009	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200011	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200012	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200013	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200014	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200015	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200016	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
304200017	4000	714749008	SNOMED	Continuous renal replacement therapy (procedure)
12019586	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019590	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019591	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019596	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019597	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019598	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12019599	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246460	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246461	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246529	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246530	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246532	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
12246533	1000	233573008	SNOMED	Extracorporeal membrane oxygenation (procedure)
"""

vitals_flo_meas_id_to_snomed_mapping = \
"""
10020276	1000	75367002	SNOMED	Blood pressure (observable entity)
10020960	1000	75367002	SNOMED	Blood pressure (observable entity)
10020961	1000	75367002	SNOMED	Blood pressure (observable entity)
10023224	1000	75367002	SNOMED	Blood pressure (observable entity)
10023225	1000	75367002	SNOMED	Blood pressure (observable entity)
10025820	1000	75367002	SNOMED	Blood pressure (observable entity)
10030004	1000	75367002	SNOMED	Blood pressure (observable entity)
10030005	1000	75367002	SNOMED	Blood pressure (observable entity)
10031312	1000	75367002	SNOMED	Blood pressure (observable entity)
10031699	1000	75367002	SNOMED	Blood pressure (observable entity)
10031700	1000	75367002	SNOMED	Blood pressure (observable entity)
10031701	1000	75367002	SNOMED	Blood pressure (observable entity)
10031702	1000	75367002	SNOMED	Blood pressure (observable entity)
12012303	1000	75367002	SNOMED	Blood pressure (observable entity)
12012316	1000	75367002	SNOMED	Blood pressure (observable entity)
12013354	1000	75367002	SNOMED	Blood pressure (observable entity)
1209441	1000	75367002	SNOMED	Blood pressure (observable entity)
1209443	1000	75367002	SNOMED	Blood pressure (observable entity)
12220031	1000	75367002	SNOMED	Blood pressure (observable entity)
12232293	1000	75367002	SNOMED	Blood pressure (observable entity)
12232294	1000	75367002	SNOMED	Blood pressure (observable entity)
12232295	1000	75367002	SNOMED	Blood pressure (observable entity)
12240286	1000	75367002	SNOMED	Blood pressure (observable entity)
12240288	1000	75367002	SNOMED	Blood pressure (observable entity)
12268464	1000	75367002	SNOMED	Blood pressure (observable entity)
12268520	1000	75367002	SNOMED	Blood pressure (observable entity)
12268554	1000	75367002	SNOMED	Blood pressure (observable entity)
12271157	1000	75367002	SNOMED	Blood pressure (observable entity)
12273951	1000	75367002	SNOMED	Blood pressure (observable entity)
12274722	1000	75367002	SNOMED	Blood pressure (observable entity)
12274727	1000	75367002	SNOMED	Blood pressure (observable entity)
12275875	1000	75367002	SNOMED	Blood pressure (observable entity)
12278579	1000	75367002	SNOMED	Blood pressure (observable entity)
9005	1000	75367002	SNOMED	Blood pressure (observable entity)
900890	1000	75367002	SNOMED	Blood pressure (observable entity)
1120000015	2000	75367002	SNOMED	Blood pressure (observable entity)
1120000017	2000	75367002	SNOMED	Blood pressure (observable entity)
1120000019	2000	75367002	SNOMED	Blood pressure (observable entity)
1120000024	2000	75367002	SNOMED	Blood pressure (observable entity)
13734	2000	75367002	SNOMED	Blood pressure (observable entity)
13735	2000	75367002	SNOMED	Blood pressure (observable entity)
13738	2000	75367002	SNOMED	Blood pressure (observable entity)
21465	2000	75367002	SNOMED	Blood pressure (observable entity)
21470	2000	75367002	SNOMED	Blood pressure (observable entity)
23078	2000	75367002	SNOMED	Blood pressure (observable entity)
4234	2000	75367002	SNOMED	Blood pressure (observable entity)
4237	2000	75367002	SNOMED	Blood pressure (observable entity)
5	2000	75367002	SNOMED	Blood pressure (observable entity)
9512751820	2000	75367002	SNOMED	Blood pressure (observable entity)
9512751821	2000	75367002	SNOMED	Blood pressure (observable entity)
9512751822	2000	75367002	SNOMED	Blood pressure (observable entity)
9512751826	2000	75367002	SNOMED	Blood pressure (observable entity)
9512751828	2000	75367002	SNOMED	Blood pressure (observable entity)
10070	4000	75367002	SNOMED	Blood pressure (observable entity)
10071	4000	75367002	SNOMED	Blood pressure (observable entity)
1120100019	4000	75367002	SNOMED	Blood pressure (observable entity)
1120100050	4000	75367002	SNOMED	Blood pressure (observable entity)
1492	4000	75367002	SNOMED	Blood pressure (observable entity)
210501	4000	75367002	SNOMED	Blood pressure (observable entity)
301260	4000	75367002	SNOMED	Blood pressure (observable entity)
301280	4000	75367002	SNOMED	Blood pressure (observable entity)
5	4000	75367002	SNOMED	Blood pressure (observable entity)
890	4000	75367002	SNOMED	Blood pressure (observable entity)
10024524	1000	386725007	SNOMED	Body temperature (observable entity)
10024525	1000	386725007	SNOMED	Body temperature (observable entity)
10024526	1000	386725007	SNOMED	Body temperature (observable entity)
10024527	1000	386725007	SNOMED	Body temperature (observable entity)
10024528	1000	386725007	SNOMED	Body temperature (observable entity)
10024529	1000	386725007	SNOMED	Body temperature (observable entity)
10024530	1000	386725007	SNOMED	Body temperature (observable entity)
10024531	1000	386725007	SNOMED	Body temperature (observable entity)
10024532	1000	386725007	SNOMED	Body temperature (observable entity)
12010569	1000	386725007	SNOMED	Body temperature (observable entity)
12013357	1000	386725007	SNOMED	Body temperature (observable entity)
12014469	1000	386725007	SNOMED	Body temperature (observable entity)
12236704	1000	386725007	SNOMED	Body temperature (observable entity)
9006	1000	386725007	SNOMED	Body temperature (observable entity)
1120000001	2000	386725007	SNOMED	Body temperature (observable entity)
1120000002	2000	386725007	SNOMED	Body temperature (observable entity)
1120000003	2000	386725007	SNOMED	Body temperature (observable entity)
1120000004	2000	386725007	SNOMED	Body temperature (observable entity)
1120000005	2000	386725007	SNOMED	Body temperature (observable entity)
1120100081	2000	386725007	SNOMED	Body temperature (observable entity)
1120100082	2000	386725007	SNOMED	Body temperature (observable entity)
1120100113	2000	386725007	SNOMED	Body temperature (observable entity)
1120100114	2000	386725007	SNOMED	Body temperature (observable entity)
6	2000	386725007	SNOMED	Body temperature (observable entity)
3040064000	4000	386725007	SNOMED	Body temperature (observable entity)
3040100959	4000	386725007	SNOMED	Body temperature (observable entity)
6	4000	386725007	SNOMED	Body temperature (observable entity)
7085940	4000	386725007	SNOMED	Body temperature (observable entity)
891	4000	386725007	SNOMED	Body temperature (observable entity)
10020274	1000	364075005	SNOMED	Heart rate (observable entity)
10024600	1000	364075005	SNOMED	Heart rate (observable entity)
10029996	1000	364075005	SNOMED	Heart rate (observable entity)
10029997	1000	364075005	SNOMED	Heart rate (observable entity)
10031311	1000	364075005	SNOMED	Heart rate (observable entity)
12012302	1000	364075005	SNOMED	Heart rate (observable entity)
12012315	1000	364075005	SNOMED	Heart rate (observable entity)
12013355	1000	364075005	SNOMED	Heart rate (observable entity)
12015981	1000	364075005	SNOMED	Heart rate (observable entity)
12220030	1000	364075005	SNOMED	Heart rate (observable entity)
12232296	1000	364075005	SNOMED	Heart rate (observable entity)
12232297	1000	364075005	SNOMED	Heart rate (observable entity)
12232298	1000	364075005	SNOMED	Heart rate (observable entity)
12236736	1000	364075005	SNOMED	Heart rate (observable entity)
12240289	1000	364075005	SNOMED	Heart rate (observable entity)
12262025	1000	364075005	SNOMED	Heart rate (observable entity)
12262118	1000	364075005	SNOMED	Heart rate (observable entity)
12264213	1000	364075005	SNOMED	Heart rate (observable entity)
12271158	1000	364075005	SNOMED	Heart rate (observable entity)
12275876	1000	364075005	SNOMED	Heart rate (observable entity)
9008	1000	364075005	SNOMED	Heart rate (observable entity)
1120000007	2000	364075005	SNOMED	Heart rate (observable entity)
1120000008	2000	364075005	SNOMED	Heart rate (observable entity)
118000001	2000	364075005	SNOMED	Heart rate (observable entity)
118000289	2000	364075005	SNOMED	Heart rate (observable entity)
13736	2000	364075005	SNOMED	Heart rate (observable entity)
13737	2000	364075005	SNOMED	Heart rate (observable entity)
13739	2000	364075005	SNOMED	Heart rate (observable entity)
21464	2000	364075005	SNOMED	Heart rate (observable entity)
21469	2000	364075005	SNOMED	Heart rate (observable entity)
2204	2000	364075005	SNOMED	Heart rate (observable entity)
2207	2000	364075005	SNOMED	Heart rate (observable entity)
4221	2000	364075005	SNOMED	Heart rate (observable entity)
8	2000	364075005	SNOMED	Heart rate (observable entity)
11089	4000	364075005	SNOMED	Heart rate (observable entity)
3040084000	4000	364075005	SNOMED	Heart rate (observable entity)
3043432	4000	364075005	SNOMED	Heart rate (observable entity)
3044000	4000	364075005	SNOMED	Heart rate (observable entity)
315880	4000	364075005	SNOMED	Heart rate (observable entity)
7085920	4000	364075005	SNOMED	Heart rate (observable entity)
8	4000	364075005	SNOMED	Heart rate (observable entity)
892	4000	364075005	SNOMED	Heart rate (observable entity)
12249473	1000	86290005	SNOMED	Respiratory rate (observable entity)
900893	1000	86290005	SNOMED	Respiratory rate (observable entity)
9009	1000	86290005	SNOMED	Respiratory rate (observable entity)
9	2000	86290005	SNOMED	Respiratory rate (observable entity)
893	4000	86290005	SNOMED	Respiratory rate (observable entity)
9	4000	86290005	SNOMED	Respiratory rate (observable entity)
12230957	1000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
90010	1000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
10	2000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
10	4000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
11793	4000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
30445608	4000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
894	4000	250546000	SNOMED	Measurement of partial pressure of oxygen in blood (procedure)
"""

#maybe
# """
# 12238041	1000
# 12249470	1000

# """

flo_meas_id_to_snomed_mapping = \
  headers_flo_meas_id_to_snomed_mapping + \
  device_flo_meas_id_to_snomed_mapping + \
  vitals_flo_meas_id_to_snomed_mapping


# Generate flo_meas_id to SNOMED code mapping dataframe
# Load flo_meas_id to SNOMED code mapping into Pandas dataframe
concept_mapping_str = StringIO(flo_meas_id_to_snomed_mapping)
concept_mapping_pd_df = pd.read_csv(concept_mapping_str, sep='\t')

# Convert to PySpark dataframe
schema = [
  StructField('flo_meas_id', StringType(), True),
  StructField('instance', IntegerType(), True),
  StructField('code', StringType(), True),
  StructField('code_system', StringType(), True),
  StructField('description', StringType(), True)]
concept_mapping_df = spark.createDataFrame(concept_mapping_pd_df, schema=StructType(schema))


# Get flowsheet records based on specified filter criteria
def get_flowsheets(cohort_df=None, include_cohort_columns=None, join_table=None, filter_string=None, omit_columns=flowsheets_default_columns_to_omit(), add_cc_columns=[]):
  """Get flowsheet records based on specified filter criteria
  
  Parameters:
  cohort_df (PySpark df): Optional cohort dataframe for which to get flowsheet records (if None,
                          get all patients in the RDP). This dataframe *must* include pat_id and
                          instance columns for joining
  include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None, in which case all columns from cohort dataframe are kept.
  join_table (string): Optionally specify table to join to flowsheets (this can only be one of the
                       following: 'inpatientdata', 'inpatientdataflowsheet'). Default is None (i.e.
                       do not join any additional table)
  filter_string (string): String to use as a WHERE clause filter/condition. Default is None.
  omit_columns (list): List of columns/fields to exclude from the final dataframe.
  add_cc_columns (list): List of device cc columns to add (e.g. 'crrt', 'ecmo_ecls',
                         'oxygen_delivery')
  
  Returns:
  PySpark df: Dataframe containing flowsheet records satisfying specified filter criteria
  
  """
  # If not None, filter_string must be a string
  if(filter_string is not None):
    assert(isinstance(filter_string, str))
  
  # 'Omit' columns input must be a list
  omit_columns_from_final = [] if omit_columns is None else omit_columns
  assert(isinstance(omit_columns_from_final, list))
  
  # add_cc_columns must be a list of valid device or vitals cc labels
  cc_columns_list = add_cc_columns if isinstance(add_cc_columns, list) else [add_cc_columns]
  assert(all([s in (get_device_cc_list() + get_vitals_cc_list()) for s in cc_columns_list]))
  
  # If no cohort input is provided, use all patients in the RDP
  results_df = cohort_df if cohort_df is not None else get_basic_cohort(include_race=True)
  assert(isinstance(results_df, DataFrame))
  
  # 'Include cohort' columns input must be a list
  keep_cohort_columns = results_df.columns if include_cohort_columns is None else include_cohort_columns
  assert(isinstance(keep_cohort_columns, list))
  
  # Patient cohort dataframe must also include 'pat_id' and 'instance'
  for s in ['instance', 'pat_id']:
    if(s not in keep_cohort_columns):
      keep_cohort_columns = [s] + keep_cohort_columns
  
  # Make sure cohort columns to keep are all present
  assert(all([s in results_df.columns for s in keep_cohort_columns]))
  
  # Get initial columns from patient cohort dataframe
  print('Getting initial columns from patient cohort dataframe...')
  results_df = results_df.select(keep_cohort_columns)
  
  # Join patient cohort to flowsheet table
  print('Joining flowsheet table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        pat_id,
        instance,
        fsd_id,
        inpatient_data_id
      FROM rdp_phi.flowsheet"""), ['pat_id', 'instance'], how='left')
  
  # Join to flowsheetentry table
  print('Joining flowsheetentry table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        fsd_id,
        instance,
        flo_meas_id,
        inpatient_data_id,
        recorded_time,
        instantrecordedlocal as instant_recorded_local,
        value,
        entry_user_id,
        taken_user_id
      FROM rdp_phi.flowsheetentry"""), ['fsd_id', 'instance', 'inpatient_data_id'], how='left')
  
  # Join to flowsheetdefinition table
  print('Joining flowsheetdefinition table...')
  results_df = results_df.join(
    spark.sql(
      """
      SELECT
        flo_meas_id,
        instance,
        name,
        displayname as display_name,
        type,
        valuetype as value_type,
        multiselect,
        crossencounter as cross_encounter,
        storescalculateddata as stores_calculated_data,
        canbegraphed as can_be_graphed
      FROM rdp_phi.flowsheetdefinition"""), ['flo_meas_id', 'instance'], how='left')
  
  # Join concept mapping
  print('Joining SNOMED concept mapping...')
  results_df = results_df \
    .join(F.broadcast(concept_mapping_df), ['flo_meas_id', 'instance'], how='left')
  
  # If birth_date is present in the cohort table, calculate age at flowsheet entry recorded_time
  if('birth_date' in results_df.columns):
    print('Adding age (at flowsheet entry recorded_time) to results...')
    results_df = results_df.withColumn(
      'age_at_recorded_dt',
      F.round(F.datediff(F.col('recorded_time'), F.col('birth_date'))/365.25, 1))
  
  # Optionally join an additional table
  if(join_table == 'inpatientdata'):
    print('Joining inpatientdata table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          inpatient_data_id,
          instance,
          pat_enc_csn_id,
          status
        FROM rdp_phi.inpatientdata
        """), ['inpatient_data_id', 'instance'], how='left')
  elif(join_table == 'inpatientdataflowsheet'):
    print('Joining inpatientdataflowsheet table...')
    results_df = results_df.join(
      spark.sql(
        """
        SELECT
          flo_meas_id,
          inpatient_data_id,
          instance,
          ip_lda_id,
          line as ipd_line,
          pat_enc_csn_id
        FROM rdp_phi.inpatientdataflowsheet
        """), ['flo_meas_id', 'inpatient_data_id', 'instance'], how='left')
  
  # Apply filter to get subset of records
  if(filter_string is not None):
    print('Applying specified filter to get subset of records...')
    filter_string_for_print = filter_string if (len(filter_string) <= 1000) else '{}...'.format(filter_string[:1000])
    print(filter_string_for_print)
    results_df = results_df.where(filter_string)
  
  # Add device cc columns
  for cc_label in cc_columns_list:
    if(cc_label in get_device_cc_list()):
      print("Adding column for '{}' device clinical concept...".format(cc_label))
      results_df = add_device_cc_column(results_df, cc_label)
    else:
      print("Adding column for '{}' vitals clinical concept...".format(cc_label))
      results_df = add_vitals_cc_column(results_df, cc_label)
  
  # Exclude specified columns
  if(len(omit_columns_from_final) > 0):
    print('Omitting the following columns:')
    print(omit_columns_from_final)
    results_df = results_df.select(
      [s for s in results_df.columns if s not in omit_columns_from_final])
  
  return results_df
