from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
import pandas as pd


spark = (SparkSession.builder
  .config("spark.tasks.cpus", "2")
  .appName("ClassifierTest")
  .getOrCreate()
)

# TODO: https://github.com/catboost/catboost/issues/1932
# https://catboost.ai/en/docs/installation/spark-installation-build-from-source-maven
# https://github.com/catboost/catboost/tree/f253741f6e15487a623e1e8aba846420770f3b25/catboost/spark/catboost4j-spark

import catboost_spark

srcDataSchema = [
    StructField("features", VectorUDT()),
    StructField("label", StringType())
]


#### HELPER FUNCTIES, SKIP NAAR BENEDEN
def load_df():
    df = pd.read_csv("/data/users/Public/driesseb/catboost/final_features.csv",index_col=0).drop_duplicates()
    # df = df[~df["B06_p50"].isnull()]
    df = df.dropna()
    df["groupID"] = df["groupID"].astype(int)
    df["zoneID"] = df["zoneID"].astype(int) ## floats kunnen niet als categorical feats
    return df, [i for i in df.columns if i not in ["id"]]

def winter_spring_grouped(x):
    ## Winter wheat: 2746 samples (1110)
    ## Spring wheat: 91 samples (1120)
    ## Winter barley: 2423 samples (1510)
    ## Spring barley: 1119 samples (1520)
    ## Winter cereal: 1078 samples (1910)
    ## Spring cereal: 299 samples (1920)
    ## Maize: 2937 samples (1200)
    ## Winter rapeseed: 633 (4351)
    ## Potatoes: 1942 samples (5100)
    ## Sugar beet: 1223 samples (8100)
    ## Grass 1206, 1163, 776 samples (9100, 9110, 9120)
    if x == 1100 or x == 1110 or x == 1120: #WHEAT
        return "1100"
    elif x == 1500 or x == 1510 or x == 1520: #BARLEY
        return "1500"
    elif x == 1900 or x == 1910 or x == 1920: #CEREAL
        return "1900"
    elif x == 1200: #MAIZE
        return "1200"
    elif x == 4350 or x == 4351 or x == 4352: #RAPESEED
        return "4350"
    elif x == 5100: #POTATOES
        return "5100"
    elif x == 8100: #SUGARBEET
        return "8100"
    elif x == 9100 or x == 9110 or x == 9120: #GRASS
        return "9100"
    else: #ALHETANDERE
        return "0"

### THIS IS WHAT IS PROPOSED IN THE PRODUCT SPECIFICATION DOCUMENT https://teams.microsoft.com/_?tenantId=f2ae2753-a73a-421e-8bb1-01533753b791#/docx/viewer/teams/https:~2F~2Feodcgmbheu.sharepoint.com~2Fsites~2FopenEOplattformall~2FShared%20Documents~2FGeneral~2F02_deliverables~2FCCN1_European_Continental_Crop_Map~2FCCN-del-02-ProductSpecification-v2.0.docx?threadId=19:d12d493dacd646088ee822ee24b98e28@thread.tacv2&baseUrl=https:~2F~2Feodcgmbheu.sharepoint.com~2Fsites~2FopenEOplattformall&fileId=517578d6-a93b-42f2-9ad1-d04a2247854e&ctx=files&rootContext=items_view&viewerAction=view
def cereals_grouped(x):
    ## Winter wheat: 2746 samples (1110)
    ## Spring wheat: 91 samples (1120)
    ## Winter barley: 2423 samples (1510)
    ## Spring barley: 1119 samples (1520)
    ## Winter cereal: 1078 samples (1910)
    ## Spring cereal: 299 samples (1920)
    ## Maize: 2937 samples (1200)
    ## Winter rapeseed: 633 (4351)
    ## Potatoes: 1942 samples (5100)
    ## Sugar beet: 1223 samples (8100)
    ## Grass 1206, 1163, 776 samples (9100, 9110, 9120)
    if x == 1110 or x == 1510 or x == 1910: #WINTER
        return "1910"
    elif x == 1120 or x == 1520 or x == 1920: #SPRING
        return "1920"
    elif x == 1200: #MAIZE
        return "1200"
    elif x == 4350 or x == 4351 or x == 4352: #RAPESEED
        return "4350"
    elif x == 5100: #POTATO
        return "5100"
    elif x == 8100: #SUGARBEET
        return "8100"
    elif x == 9100 or x == 9110 or x == 9120: #GRASS
        return "9100"
    else:
        return "0"


def train_spark_model(pd_df, fp, bands):
  old_trainDf = spark.createDataFrame(pd_df, schema=bands+["label"])

  assembler = VectorAssembler(
      inputCols=[x for x in old_trainDf.columns if x in bands],
      outputCol='features')

  nieuw_df = assembler.transform(old_trainDf)
  print(nieuw_df.select("features", "label").toPandas().head(5))

  (training_data, test_data) = nieuw_df.select("features", "label").randomSplit([0.7, 0.3], seed=24)

  train_pool = catboost_spark.Pool(training_data)
  test_pool = catboost_spark.Pool(test_data)
  print("test")


  ### EERSTE RUN
#   classifier = catboost_spark.CatBoostClassifier(
#     iterations=10000,
#     # # learningRate=None,
#     # # randomSeed=99,
#     # # l2LeafReg=0,
#     depth=7,
#     borderCount=200,
#     # # growPolicy missing, min_data_in_leaf missing
#     # baggingTemperature=1, #maar is al default + gooit float error, mss moet ik 1.0 doen als ik 'm wil zetten ? enfin is al default 1
#     randomStrength=0.2
#   )

  classifier = catboost_spark.CatBoostClassifier(
    iterations=100,
    # # learningRate=None,
    # # randomSeed=99,
#     l2LeafReg=3,
#     depth=7,
#     borderCount=200,
    # # growPolicy missing, min_data_in_leaf missing
    # baggingTemperature=1, #maar is al default + gooit float error, mss moet ik 1.0 doen als ik 'm wil zetten ? enfin is al default 1
#     randomStrength=1
  )


  # train a model
  model = classifier.fit(train_pool, evalDatasets=[test_pool])

  # apply the model
#   predictions = model.transform(test_pool.data)

  # evaluator = MulticlassClassificationEvaluator(
  #   labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
  # accuracy = evaluator.evaluate(predictions)
  # print("Test Error = %g " % (1.0 - accuracy))

  ## save test set:
  predictions = model.transform(test_pool.data)
  predictions.toPandas().to_csv("/data/users/Public/driesseb/catboost/"+fp+".csv")
    
  # # save the model
  savedModelPath = "file:/data/users/Public/driesseb/catboost/"+fp
  model.write().overwrite().save(savedModelPath)

  # save the model as a local file in CatBoost native format
  savedNativeModelPath = '/data/users/Public/driesseb/catboost/'+fp+'.cbm'
  model.saveNativeModel(savedNativeModelPath)


### DATA LOADING
df, model_band_names = load_df()

### DATA PREP AND TRAINING

### EEN MET ALLE FEATURES EN DE TARGETS VOORGESTELD DOOR KRISTOF
df_ws_grouped = df.copy()
df_ws_grouped = df_ws_grouped[~df_ws_grouped["id"].isin([0, 1000, 991, 9998])]
df_ws_grouped["label"] = df_ws_grouped["id"].apply(winter_spring_grouped)

final_df_ws_grouped = df_ws_grouped[model_band_names+["label"]]
train_spark_model(final_df_ws_grouped, 'ws_grouped_groot',model_band_names)

### EEN MET MINDER FEATURES
most_important_features = [
  "B06_sd","B06_p25","B06_p50","B06_p75","B06_t4","B06_t7","B06_t10","B06_t13","B06_t16","B06_t19",
  "B12_sd","B12_p25","B12_p50","B12_p75","B12_t4","B12_t7","B12_t10","B12_t13","B12_t16","B12_t19",
  "ANIR_sd","ANIR_p25","ANIR_p50","ANIR_p75","ANIR_t4","ANIR_t7","ANIR_t10","ANIR_t13","ANIR_t16","ANIR_t19",
  "NDGI_sd","NDGI_p25","NDGI_p50","NDGI_p75","NDGI_t4", "NDGI_t7","NDGI_t10", "NDGI_t13","NDGI_t16","NDGI_t19",
  "VV_sd","VV_p25","VV_p50","VV_p75","VV_t2","VV_t5","VV_t8","VV_t11","VV_t14","VV_t17",
  "VH_sd","VH_p25","VH_p50","VH_p75","VH_t2","VH_t5","VH_t8","VH_t11","VH_t14","VH_t17", 
  "ratio_sd","ratio_p25","ratio_p50","ratio_p75",
  "NDVI_sd", "NDVI_p25","NDVI_p50","NDVI_p75",
  "NDRE1_sd","NDRE1_p25","NDRE1_p50","NDRE1_p75",
  "NDRE2_sd","NDRE2_p25","NDRE2_p50","NDRE2_p75",
  "NDRE5_sd","NDRE5_p25","NDRE5_p50","NDRE5_p75",
]

final_df_ws_grouped_small = df_ws_grouped[most_important_features+["label"]]
train_spark_model(final_df_ws_grouped_small, 'ws_grouped_klein',most_important_features)

### EEN MET FEATURES PROPOSED IN PROPOSAL
df_c_grouped = df.copy()
df_c_grouped = df_c_grouped[~df_c_grouped["id"].isin([0, 1000, 991, 9998, 1100, 1500, 1900])]
df_c_grouped["label"] = df_c_grouped["id"].apply(cereals_grouped)

final_df_c_grouped = df_c_grouped[model_band_names+["label"]]
train_spark_model(final_df_c_grouped, 'c_grouped_groot',model_band_names)


### EEN MET LOSSE STRATA
## 46000: frankrijk, engeland, etc
## 22000: oostenrijk, hongarije, rusland, czechie, noorwegen
## 43000: griekenland spanje
## 6000: oostkust bulgarije roemenie moldavie ukraine

#### 46000
df_ws_grouped_midwest = df.copy()
df_ws_grouped_midwest = df_ws_grouped_midwest[df_ws_grouped_midwest["groupID"] == 46000]
df_ws_grouped_midwest = df_ws_grouped_midwest[~df_ws_grouped_midwest["id"].isin([0, 1000, 991, 9998])]
df_ws_grouped_midwest["label"] = df_ws_grouped_midwest["id"].apply(winter_spring_grouped)

final_df_ws_grouped_midwest = df_ws_grouped_midwest[model_band_names+["label"]]
train_spark_model(final_df_ws_grouped_midwest, 'ws_groot_strat_46000',model_band_names)

#### 22000
df_ws_grouped_northeast = df.copy()
df_ws_grouped_northeast = df_ws_grouped_northeast[df_ws_grouped_northeast["groupID"] == 22000]
df_ws_grouped_northeast = df_ws_grouped_northeast[~df_ws_grouped_northeast["id"].isin([0, 1000, 991, 9998])]
df_ws_grouped_northeast["label"] = df_ws_grouped_northeast["id"].apply(winter_spring_grouped)

final_df_ws_grouped_northeast = df_ws_grouped_northeast[model_band_names+["label"]]
train_spark_model(final_df_ws_grouped_northeast, 'ws_groot_strat_22000',model_band_names)

#### 43000 6000
df_ws_grouped_south = df.copy()
df_ws_grouped_south = df_ws_grouped_south[df_ws_grouped_south["groupID"].isin([43000, 6000])]
df_ws_grouped_south = df_ws_grouped_south[~df_ws_grouped_south["id"].isin([0, 1000, 991, 9998])]
df_ws_grouped_south["label"] = df_ws_grouped_south["id"].apply(winter_spring_grouped)

final_df_ws_grouped_south = df_ws_grouped_south[model_band_names+["label"]]
train_spark_model(final_df_ws_grouped_south, 'ws_groot_strat_43000_6000',model_band_names)
