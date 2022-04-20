#!/usr/bin/env bash

set -exo pipefail

pysparkPython="/bin/python3.6"
spark_job_name=Catboost_on_cluster
queue="openeo"
driverCores=1
sparkDriverJavaOptions="-Dhdp.version=3.1.4.0-315"

spark-submit \
   --master yarn --deploy-mode cluster \
   --queue ${queue} \
   --name ${spark_job_name} \
   --driver-memory 10G \
   --executor-memory 3G \
   --executor-cores 2 \
   --driver-cores ${driverCores} \
   --driver-java-options "${sparkDriverJavaOptions}" \
   --conf "spark.executor.extraJavaOptions=${sparkExecutorJavaOptions}" \
   --conf spark.yarn.submit.waitAppCompletion=false \
   --conf spark.driver.memoryOverhead=7g \
   --conf spark.executor.memoryOverhead=2g \
   --conf spark.driver.maxResultSize=2g \
   --conf spark.kryoserializer.buffer.max=1G \
   --conf spark.scheduler.mode="FAIR" \
   --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${pysparkPython} \
   --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${pysparkPython} \
   --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
   --conf spark.ui.view.acls.groups=vito \
   --conf spark.modify.acls.groups=vito \
   --conf spark.jars.packages="ai.catboost:catboost-spark_2.3_2.11:1.0.4" \
   --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
   catboost_on_cluster.py
