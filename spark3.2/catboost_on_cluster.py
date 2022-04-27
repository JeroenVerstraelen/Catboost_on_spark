from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import *

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

trainData = [
    Row(Vectors.dense(0.1, 0.2, 0.11), "1"),
    Row(Vectors.dense(0.97, 0.82, 0.33), "2"),
    Row(Vectors.dense(0.13, 0.22, 0.23), "1"),
    Row(Vectors.dense(0.8, 0.62, 0.0), "0")
]

trainDf = spark.createDataFrame(spark.sparkContext.parallelize(trainData), StructType(srcDataSchema))
trainPool = catboost_spark.Pool(trainDf)

evalData = [
    Row(Vectors.dense(0.22, 0.33, 0.9), "2"),
    Row(Vectors.dense(0.11, 0.1, 0.21), "0"),
    Row(Vectors.dense(0.77, 0.0, 0.0), "1")
]

evalDf = spark.createDataFrame(spark.sparkContext.parallelize(evalData), StructType(srcDataSchema))
evalPool = catboost_spark.Pool(evalDf)

classifier = catboost_spark.CatBoostClassifier()

# train a model
model = classifier.fit(trainPool, [evalPool])

# apply the model
predictions = model.transform(evalPool.data)
predictions.show()

# save the model
savedModelPath = "/data/users/Public/verstraj/catboost_models3.2/multiclass_model"
model.write().save(savedModelPath)

# load the model (can be used in a different Spark session)

loadedModel = catboost_spark.CatBoostClassificationModel.load(savedModelPath)

predictionsFromLoadedModel = loadedModel.transform(evalPool.data)
predictionsFromLoadedModel.show()
