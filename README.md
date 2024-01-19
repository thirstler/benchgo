Running Benchmarks
==================

Trino
-----

Trino is run using the "benchgo" script. 

    benchgo --name trino-1t \
    --benchmark tpcds \
    --engine trino \
    --trino-coordinator http://10.73.1.61:8080 \
    --trino-catalog vast \
    --trino-schema "db0/tpcds/1g" \
    --tpcds-scale sf1000 \
    --prometheus-host http://10.73.1.41:9090

Spark
-----

Spark benchmarks are run by configuring the "benchgo_spark" script and then submitting the job to spark-submit or pyspark:

    # Needed if you're using a stand-alone pyspark
    export SPARK_HOME=/usr/local/spark3

    pyspark --master spark://node5:7077 \
    --driver-class-path $(echo /usr/local/vast-spark3/*.jar | tr ' ' ':') \
    --jars $(echo /usr/local/vast-spark3/*.jar | tr ' ' ',') \
    --driver-memory 32g < benchgo_spark

