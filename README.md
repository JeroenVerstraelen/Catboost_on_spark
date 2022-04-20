Steps to run on cluster:

    Clone repository to your uservm
    Authenticate with kinit
    Run ./submit23.sh (Ensure your kerberos user has the rights to write to every output folder in your script.)

For development, you can run the service:

    export SPARK_HOME=$(find_spark_home.py)
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    python catboost_on_cluster.py

Docker

    docker login vito-docker-private.artifactory.vgt.vito.be
    docker tag eae77adc20be vito-docker-private.artifactory.vgt.vito.be/openeo-catboost:latest
    docker push vito-docker-private.artifactory.vgt.vito.be/openeo-catboost:latest

