#!/usr/bin/env bash

set -exo pipefail

image="vito-docker-private.artifactory.vgt.vito.be/openeo-catboost:latest"

pysparkPython="/bin/python3.8"
#export HDP_VERSION=3.1.4.0-315
export SPARK_HOME=/opt/spark3_0_0
export PATH="$SPARK_HOME/bin:$PATH"

spark_job_name=Catboost_on_cluster
queue="openeo"
driverCores=1

yarn_runtime='docker'
sparkDriverJavaOptions="-Dscala.concurrent.context.numThreads=6 -Dhdp.version=3.1.4.0-315"
sparkExecutorJavaOptions="-Dscala.concurrent.context.numThreads=6"

${SPARK_HOME}/bin/spark-submit \
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
   --conf "spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/jre-11-openjdk" \
   --conf "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/jre-11-openjdk" \
   --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
   --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/etc/ipa/:/etc/ipa/:ro,/data/users:/data/users:rw \
   --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=${yarn_runtime} \
   --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${image}  \
   --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=${yarn_runtime} \
   --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${image}  \
   --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/users:/data/users:rw \
   --conf spark.ui.view.acls.groups=vito \
   --conf spark.modify.acls.groups=vito \
   --conf spark.jars.packages="ai.catboost:catboost-spark_3.0_2.12:1.0.4" \
   --conf spark.shuffle.service.name=spark_shuffle_320 --conf spark.shuffle.service.port=7557 \
   --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
   catboost_on_cluster.py
