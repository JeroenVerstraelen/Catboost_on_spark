from geopyspark import geopyspark_conf
from pyspark import SparkContext, SparkConf

conf = (geopyspark_conf(master="local[*]", appName="catboost")
        .set("spark.jars.packages", "ai.catboost:catboost-spark_3.1_2.12:1.0.4")
        )
SparkContext(conf=conf)
import catboost_spark

import numpy as np

from catboost import CatBoostClassifier, Pool

# initialize data
train_data = np.random.randint(0,
                               100,
                               size=(100, 10))

train_labels = np.random.randint(0,
                                 2,
                                 size=(100))

test_data = catboost_pool = Pool(train_data,
                                 train_labels)

model = CatBoostClassifier(iterations=2,
                           depth=2,
                           learning_rate=1,
                           loss_function='Logloss',
                           verbose=True)
# train the model
model.fit(train_data, train_labels)
# make the prediction using the resulting model
preds_class = model.predict(test_data)
preds_proba = model.predict_proba(test_data)
print("class = ", preds_class)
print("proba = ", preds_proba)




