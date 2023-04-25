# mychem.info fields
# MyChem search fields
CHEBI_SEARCH_FIELDS = ['chebi.name', 'chebi.synonyms']
CHEMBL_SEARCH_FIELDS = ['chembl.molecule_synonyms.synonyms', 'chembl.pref_name']
DRUGBANK_SEARCH_FIELDS = ['drugbank.international_brands.name', 'drugbank.name', 'drugbank.synonyms', 'drugbank.products.name']
DRUGCENTRAL_SEARCH_FIELDS = ['chembl.xrefs.drugcentral.name', 'drugcentral.synonyms']
GINAS_SEARCH_FIELDS = ['ginas.names_list', 'ginas.preferred_name']
PHARMKGB_SEARCH_FIELDS = ['pharmgkb.name']
UNII_SEARCH_FIELDS = ['unii.preferred_term']
OTHER_SEARCH_FIELDS = ['chebi.xrefs.hmdb']

# Specify all search fields
MYCHEM_SEARCH_FIELDS = CHEBI_SEARCH_FIELDS + CHEMBL_SEARCH_FIELDS + DRUGBANK_SEARCH_FIELDS + DRUGCENTRAL_SEARCH_FIELDS + \
  GINAS_SEARCH_FIELDS + PHARMKGB_SEARCH_FIELDS + UNII_SEARCH_FIELDS + OTHER_SEARCH_FIELDS
# MYCHEM_SEARCH_FIELDS = CHEBI_SEARCH_FIELDS

# MyChem name fields to return
MYCHEM_NAME_FIELDS = ['chebi.name', 'chembl.pref_name', 'drugbank.name', 'chembl.xrefs.drugcentral.name', 'pharmgkb.name']

# MyChem ID fields to return
CAS_RETURN_FIELDS = ['chebi.xrefs.cas', 'ginas.xrefs.CAS', 'pharmgkb.xrefs.cas']
CHEBI_RETURN_FIELDS = ['chebi.id', 'drugbank.xrefs.chebi', 'drugcentral.xrefs.chebi']
CHEMBL_RETURN_FIELDS = ['drugbank.xrefs.chembl', 'drugcentral.xrefs.chembl_id']
DRUGBANK_RETURN_FIELDS = ['drugbank.id', 'chebi.xrefs.drugbank']
DRUGCENTRAL_RETURN_FIELDS = ['chembl.xrefs.drugcentral.id']
HMDB_RETURN_FIELDS = ['chebi.xrefs.hmdb']
KEGGC_RETURN_FIELDS = ['drugbank.xrefs.kegg.cid']
KEGGD_RETURN_FIELDS = ['chebi.xrefs.kegg_drug', 'drugcentral.xrefs.kegg_drug', 'pharmgkb.xrefs.kegg_drug']
MESH_RETURN_FIELDS = ['ginas.xrefs.MESH', 'pharmgkb.xrefs.mesh']
PHARMKGB_RETURN_FIELDS = ['pharmgkb.id', 'drugbank.xrefs.pharmgkb']
PUBCHEM_RETURN_FIELDS = ['drugcentral.xrefs.pubchem_cid', 'drugbank.xrefs.pubchem.cid']
RXNORM_RETURN_FIELDS = ['pharmgkb.xrefs.rxnorm', 'drugcentral.xrefs.rxnorm', 'ginas.xrefs.RXCUI', 'unii.rxcui']
UNII_RETURN_FIELDS = ['drugcentral.xrefs.unii', 'unii.unii']

# All MyChem ID fields
ALL_MYCHEM_ID_FIELDS = {
  'cas_id': CAS_RETURN_FIELDS,
  'chebi_id': CHEBI_RETURN_FIELDS,
  'chembl_id': CHEMBL_RETURN_FIELDS,
  'drugbank_id': DRUGBANK_RETURN_FIELDS,
  'drugcentral_id': DRUGCENTRAL_RETURN_FIELDS,
  'hmdb_id': HMDB_RETURN_FIELDS,
  'kegg_cid': KEGGC_RETURN_FIELDS,
  'kegg_did': KEGGD_RETURN_FIELDS,
  'mesh_id': MESH_RETURN_FIELDS,
  'pharmkgb_id': PHARMKGB_RETURN_FIELDS,
  'pubchem_cid': PUBCHEM_RETURN_FIELDS,
  'rxnorm_id': RXNORM_RETURN_FIELDS,
  'unii_id': UNII_RETURN_FIELDS
}

MYCHEM_ADD_COLUMNS_IN_COALESCED = []

# All MyChem return fields
MYCHEM_RETURN_FIELDS = MYCHEM_NAME_FIELDS + [f for k, v in ALL_MYCHEM_ID_FIELDS.items() for f in v]


# mydisease.info fields
# MyDisease search fields
#DOID_SEARCH_FIELDS = ['disease_ontology.name', 'disease_ontology.synonyms', 'disease_ontology.synonyms.exact']
DOID_SEARCH_FIELDS = ['disease_ontology.name']
DISGENET_SEARCH_FIELDS = ['disgenet.xrefs.disease_name']
HPO_SEARCH_FIELDS = ['hpo.disease_name']
#MONDO_SEARCH_FIELDS = ['mondo.definition', 'mondo.label', 'mondo.synonym', 'mondo.synonym.exact']
MONDO_SEARCH_FIELDS = ['mondo.label', 'mondo.synonym']

# Specify all search fields
#MYDISEASE_SEARCH_FIELDS = DOID_SEARCH_FIELDS + DISGENET_SEARCH_FIELDS + HPO_SEARCH_FIELDS + MONDO_SEARCH_FIELDS
MYDISEASE_SEARCH_FIELDS = DOID_SEARCH_FIELDS + HPO_SEARCH_FIELDS + MONDO_SEARCH_FIELDS

# MyDisease name fields to return
#MYDISEASE_NAME_FIELDS = ['disease_ontology.name', 'disgenet.xrefs.disease_name', 'hpo.disease_name', 'mondo.synonym', 'mondo.synonym.exact']
MYDISEASE_NAME_FIELDS = MYDISEASE_SEARCH_FIELDS

# MyDisease ID fields to return
DOID_RETURN_FIELDS = ['disease_ontology.doid', 'disgenet.xrefs.doid', 'mondo.xrefs.doid']
HPO_RETURN_FIELDS = ['disgenet.xrefs.hp', 'mondo.xrefs.hp']
ICD10_RETURN_FIELDS = ['disease_ontology.xrefs.icd10', 'disgenet.xrefs.icd10', 'mondo.xrefs.icd10', 'umls.icd10']
KEGG_RETURN_FIELDS = ['disease_ontology.xrefs.kegg', 'mondo.xrefs.kegg']
MESH_RETURN_FIELDS = ['disease_ontology.xrefs.mesh', 'disgenet.xrefs.mesh', 'mondo.xrefs.mesh', 'umls.mesh']
MONDO_RETURN_FIELDS = ['disgenet.xrefs.mondo', 'mondo.mondo']
NCIT_RETURN_FIELDS = ['disease_ontology.xrefs.ncit', 'mondo.xrefs.ncit']
OMIM_RETURN_FIELDS = ['disease_ontology.xrefs.omim', 'disgenet.xrefs.omim', 'hpo.omim', 'mondo.xrefs.omim']
SNOMED_RETURN_FIELDS = ['mondo.xrefs.sctid', 'umls.snomed']
UMLS_RETURN_FIELDS = ['disease_ontology.xrefs.umls_cui', 'disgenet.xrefs.umls', 'mondo.xrefs.umls' 'mondo.xrefs.umls_cui', 'umls.umls']

# All MyDisease ID fields
ALL_MYDISEASE_ID_FIELDS = {
  'doid_id': DOID_RETURN_FIELDS,
  'hpo_id': HPO_RETURN_FIELDS,
  'icd10_id': ICD10_RETURN_FIELDS,
  'kegg_id': KEGG_RETURN_FIELDS,
  'mesh_id': MESH_RETURN_FIELDS,
  'mondo_id': MONDO_RETURN_FIELDS,
  'ncit_cid': NCIT_RETURN_FIELDS,
  'omim_did': OMIM_RETURN_FIELDS,
  'snomed_id': SNOMED_RETURN_FIELDS,
  'umls_id': UMLS_RETURN_FIELDS,
}

MYDISEASE_ADD_COLUMNS_IN_COALESCED = ['disease_ontology.name', 'hpo.disease_name', 'mondo.label']

# All MyDisease return fields
MYDISEASE_RETURN_FIELDS = MYDISEASE_NAME_FIELDS + [f for k, v in ALL_MYDISEASE_ID_FIELDS.items() for f in v]


# Functions for clinical concept name resolution
# Get results from the specified API:
# concept_cateogory='DISEASE': http://mydisease.info/
# concept_cateogory='GENE': http://mygene.info/
# concept_cateogory='MEDICATION': http://mychem.info/
def get_name_resolution_results(cc_label_list, concept_category='DISEASE'):
  # Get base URL and specify search and return fields
  if (concept_category == 'DISEASE'):
    base_url = 'http://mydisease.info/v1/query?q='
    search_fields = MYDISEASE_SEARCH_FIELDS
    return_fields = MYDISEASE_RETURN_FIELDS
  elif (concept_category == 'GENE'):
    base_url = 'http://mygene.info/v1/query?q='
    search_fields = MYGENE_SEARCH_FIELDS
    return_fields = MYGENE_RETURN_FIELDS
  elif (concept_category == 'MEDICATION'):
    base_url = 'http://mychem.info/v1/query?q='
    search_fields = MYCHEM_SEARCH_FIELDS
    return_fields = MYCHEM_RETURN_FIELDS
  else:
    print("Invalid concept category specified: {}!".format(concept_category))
    print("Must be one of the following: DISEASE, GENE, MEDICATION")
    return None
  
  all_results = {}
  for label in cc_label_list:    
    # Construct query
    print("Searching for '{}'...".format(label))
    query_term = ' OR '.join(['{0}:{1}'.format(f, label.replace('_', ' ')) for f in search_fields])
    return_fields_term = ','.join(return_fields)
    query_str = base_url + query_term + '&fields=' + return_fields_term + '&dotfield=true&size=10'
    
    # Get results, if available
    try:
      all_hits = requests.get(query_str).json().get('hits')
    except:
      print("No valid results returned for {}!".format(label))
      all_hits = []
    
    # Skip if None was returned
    if (all_hits is None):
      print("'None' returned from ID resolution API for '{}'!".format(label))
      all_results.update({label: {}})
      continue
    
    # Get highest-scoring result, if results are available
    if (len(all_hits) > 0):
      best_hit = all_hits[0]
      for hit in all_hits[1:]:
        if (hit.get('_score') > best_hit.get('_score')):
          best_hit = hit
    else:
      best_hit = {}
    
    # Add to results dictionary
    all_results.update({label: best_hit})
  
  return all_results

# Convert name resolution results to a dataframe
def name_resolution_results_to_dataframe(name_resolution_results, cc_label_list, concept_category='DISEASE'):
  # Specify return fields
  if (concept_category == 'DISEASE'):
    all_id_fields = ALL_MYDISEASE_ID_FIELDS
    return_fields = MYDISEASE_RETURN_FIELDS
    add_cols_in_coalesced = MYDISEASE_ADD_COLUMNS_IN_COALESCED
  elif (concept_category == 'GENE'):
    all_id_fields = ALL_MYGENE_ID_FIELDS
    return_fields = MYGENE_RETURN_FIELDS
    add_cols_in_coalesced = MYGENE_ADD_COLUMNS_IN_COALESCED
  elif (concept_category == 'MEDICATION'):
    all_id_fields = ALL_MYCHEM_ID_FIELDS
    return_fields = MYCHEM_RETURN_FIELDS
    add_cols_in_coalesced = MYCHEM_ADD_COLUMNS_IN_COALESCED
  else:
    print("Invalid concept category specified: {}!".format(concept_category))
    print("Must be one of the following: DISEASE, GENE, MEDICATION")
    return None
  
  # Convert to list of tuples
  all_data = []
  for label in cc_label_list:
    data = [label]
    for c in return_fields:
      current_data = name_resolution_results[label].get(c)
      if isinstance(current_data, list):
        current_data = '|'.join(current_data)
      data.append(current_data)
    all_data.append(tuple(data))
  
  # Convert to dataframe
  column_names = [c.replace('.', '_') for c in return_fields]
  schema = StructType([StructField(c, StringType(), nullable = True) for c in ['cc_label'] + column_names])
  all_data_df = spark.createDataFrame(all_data, schema)
  
  # Coalesced ID column names
  coalesced_id_col_names = [k.replace('.', '_') for k, v in all_id_fields.items()]
  
  # Specify label columns
  final_label_columns = ['cc_label'] + [c.replace('.', '_') for c in add_cols_in_coalesced]
  
  # Coalesce columns
  coalesce_id_cols = [F.coalesce(*[c.replace('.', '_') for c in v]).alias(k) for k, v in all_id_fields.items()]
  coalesced_ids_df = all_data_df.select(*final_label_columns, *coalesce_id_cols)
  
  # Clean columns that have multiple IDs
  clean_id_cols = [F.when(F.col(c).contains('|'), F.split(c, '\|').getItem(0)).otherwise(F.col(c)).alias(c) for c in coalesced_id_col_names]
  coalesced_ids_df = coalesced_ids_df.select(*final_label_columns, *clean_id_cols)
  
  return all_data_df, coalesced_ids_df