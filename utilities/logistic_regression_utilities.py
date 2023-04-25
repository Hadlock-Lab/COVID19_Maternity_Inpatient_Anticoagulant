# Run logistic regression model on ML table
# Define function for running logistic regression
def run_logistic_regression(ml_encounter_table, outcome_column):
  # Load ML encounter table and select patients >= 18 years of age
  ml_encounters_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{}".format(ml_encounter_table))
  # Specify non-clinical-concept and clinical concept columns
  non_cc_col_names = ['patient_id', 'age', 'sex', 'ethnic_group', 'encounter_id', 'first_record', 'last_record', 'race']
  feature_col_names = [c for c in ml_encounters_df.columns if not(c in non_cc_col_names)]
  # Add outcome column and demographics feature columns
  outcome_label = outcome_column
  w1 = Window.partitionBy('patient_id')
  ml_encounters_df = ml_encounters_df.withColumn('outcome', F.max(outcome_label).over(w1))
  # Get positive encounters and select only the last (chronologically) for each patient
  w2 = Window.partitionBy('patient_id').orderBy('first_record', 'last_record') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  ml_enc_pos_df = ml_encounters_df \
    .where(F.col(outcome_label) == 1) \
    .withColumn('latest_encounter_id', F.max('encounter_id').over(w2)) \
    .where(F.col('latest_encounter_id') == F.col('encounter_id'))
  # Get negative encounters and select only the last (chronologically) for each patient
  ml_enc_neg_df = ml_encounters_df \
    .where(F.col('outcome') == 0) \
    .withColumn('latest_encounter_id', F.max('encounter_id').over(w2)) \
    .where(F.col('latest_encounter_id') == F.col('encounter_id'))
  # Get positive and negative patient counts
  pos_patient_count = ml_enc_pos_df.count()
  neg_patient_count = ml_enc_neg_df.count()
  # Take union of positive and negative records and select feature and outcome columns
  demographic_features = ['age_gte_65', 'male_sex', 'hispanic']
  final_feature_col_names = demographic_features + [c for c in feature_col_names if not(c in [outcome_label])]
  ml_data_df = ml_enc_pos_df.union(ml_enc_neg_df) \
    .select('outcome', *final_feature_col_names)
#   print(ml_data_df)
  # Assemble feature values and add to stages list
  assembler = VectorAssembler(inputCols=final_feature_col_names, outputCol='all_features')
  stages = [assembler]
  
  # Create pipeline model
  pipeline = Pipeline(stages = stages)
  pipelineModel = pipeline.fit(ml_data_df)
  model_df = pipelineModel.transform(ml_data_df)
  model_df = model_df.select('outcome', 'all_features')
  
  # Add weight column
  model_df = model_df \
    .withColumn('class_weight', F.when(F.col('outcome') == 1, 0.99).otherwise(0.01))
  # Run logistic regression model
  lr = LogisticRegression(
    featuresCol='all_features',
    labelCol='outcome',
    weightCol='class_weight',
    maxIter=50)
  lr_model = lr.fit(model_df)
#   print(lr_model.summary)
  # Get the area under the ROC
  area_under_roc = lr_model.summary.areaUnderROC
#   trainingSummary = lr_Model.summary
#   roc = trainingSummary.roc.toPandas()
#   plt.plot(roc['FPR'],roc['TPR'])
#   plt.ylabel('False Positive Rate')
#   plt.xlabel('True Positive Rate')
#   plt.title('ROC Curve')
#   plt.show()
#   print('Training set areaUnderROC: ' + str(trainingSummary.areaUnderROC)

#   p_value = summary.pValues
  result = ChiSquareTest.test(model_df, 'all_features', 'outcome').head()
  pvalue = result.pValues
  # Store model information in a dictionary
  model_info = {
    'pos_count': pos_patient_count,
    'neg_count': neg_patient_count,
    'feature_labels': final_feature_col_names,
    'auc_roc': area_under_roc}
  print(model_info)
  return lr_model, model_info


# Save model and information to output folder
def save_lr_model_and_info(lr_model, model_info,   outcome_label, folder_path):
  # Save logistic regression model
  model_name = '{}_lr_model'.format(outcome_label)
  lr_model.write().overwrite().save(folder_path + model_name)
  print("model df:", model_info)
  # Write model information to json file
  info_file_name = '{}_lr_model_info.json'.format(outcome_label)
  full_file_path = folder_path + info_file_name
  print("folder path:", full_file_path)
  try:
    dbutils.fs.ls(full_file_path)
    print("File exists! Deleting existing file...")
    dbutils.fs.rm(full_file_path)
  except:
    pass
  print("Writing model information: '{}'...".format(full_file_path))
  dbutils.fs.put( full_file_path, json.dumps(model_info))
#   dbutils.fs.put(full_file_path, json.dumps(model_info, indent = 4))
#   model_info_json = model_info.to_json()
#   model_info_json = json.loads(model_info)
#   dbutils.fs.put(full_file_path, model_info_json)


# Load model and information
def load_lr_model_and_info(outcome_label, folder_path):
  # Load logistic regression model
  model_name = '{}_lr_model'.format(outcome_label)
  lr_model = LogisticRegressionModel.load(folder_path + model_name)
  # Load additional model information
  info_file_name = '{}_lr_model_info.json'.format(outcome_label)
  model_info_df = spark.read.json(folder_path + info_file_name)
  model_info = model_info_df.rdd.collect()[0].asDict()
  
  # Get model information
  pos_count = model_info['pos_count']
  neg_count = model_info['neg_count']
  feature_list = model_info['feature_labels']
#   print("coef len", len(feature_list))
  pvalue = model_info['pvalue']
#   print("pvalue", pvalue)
  auc_roc = model_info['auc_roc']
#   # Generate table with model coefficients
  coefs_list = [
    (feature, float(coef), pvalue, outcome_label, pos_count, neg_count,  auc_roc) for feature, coef, pvalue in zip(feature_list, lr_model.coefficients, pvalue)]
  headers = [
    'feature', 'model_coefficient', 'pvalue', 'outcome', 'positive_patient_count', 'negative_patient_count', 'auc_roc']
  coefs_df = spark.createDataFrame(coefs_list, headers).orderBy(F.col('model_coefficient').desc())
  return lr_model, model_info, coefs_df