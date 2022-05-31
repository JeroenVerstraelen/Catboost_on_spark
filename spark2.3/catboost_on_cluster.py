from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from sklearn.utils.class_weight import compute_class_weight

spark = (SparkSession.builder
  .config("spark.tasks.cpus", "2")
  .appName("ClassifierTest")
  .getOrCreate()
)

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
    df = df.astype(int)
#     df["groupID"] = df["groupID"].astype(int)
#     df["zoneID"] = df["zoneID"].astype(int) ## floats kunnen niet als categorical feats
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
  print(nieuw_df.toPandas().head(5))

  (training_data, test_data) = nieuw_df.select("features", "label").randomSplit([0.7, 0.3], seed=24)

  train_pool = catboost_spark.Pool(training_data)
  test_pool = catboost_spark.Pool(test_data)

#   y_train = training_data.select("label").toPandas()["label"].to_numpy()
#   classes = np.unique(y_train)
#   weights = compute_class_weight(class_weight='balanced', classes=classes, y=y_train)
#   class_weights = dict(zip(classes, weights))
    
#   sorted_class_weights = {key: val for key,val in sorted(class_weights.items())}
#   final_class_weights = list(sorted_class_weights.values())
# #   sorted_numerical_cw = {n: val for n, val in enumerate(sorted_class_weights.values())}
#   print(sorted_numerical_cw)
#   final_class_weights[0] = 0.2
#   print(sorted_numerical_cw)

#   final_class_weights = [0.34952550814376093, 1.3517048412285269, 0.6427289603960396, 1.4905998851894375,  2.9241272522522523, 2.0526679841897235, 3.270308564231738, 0.963854862657758]
#   final_class_weights[0] = 3.5

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
    iterations=6000,
    # # learningRate=None,
    # # randomSeed=99,
    l2LeafReg=3.,
#     classWeightsMap=sorted_numerical_cw,
#     classWeightsList=final_class_weights,
    autoClassWeights=catboost_spark.EAutoClassWeightsType.Balanced,
#     depth=depth,
#     borderCount=200,
    # # growPolicy missing, min_data_in_leaf missing
    # baggingTemperature=1, #maar is al default + gooit float error, mss moet ik 1.0 doen als ik 'm wil zetten ? enfin is al default 1
    randomStrength=100.
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


### EEN MET FEATURES PROPOSED IN PROPOSAL
df_c_grouped = df.copy()
df_c_grouped = df_c_grouped[~df_c_grouped["id"].isin([0, 1000, 991, 9998, 1100, 1500, 1900])]
df_c_grouped["label"] = df_c_grouped["id"].apply(cereals_grouped)

final_df_c_grouped = df_c_grouped[model_band_names+["label"]]
train_spark_model(final_df_c_grouped, 'c_grouped_groot_int16',model_band_names)

