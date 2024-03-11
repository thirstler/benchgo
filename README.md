Running Benchmarks
==================

Monitoring
----------
Output of benchmark scripts includes performance data polled from worker nodes to determine CPU utilization, network traffic and disk I/O during benchmarks. Prometheus is used for this, so you'll need to install Prometheus (and optionally Grafana) somewhere in the benchmark environment. Rough outline:

1. Install Prometheus on a system separate from any benchmarkeds systems
2. Install node_exporter and all worker systems and configure Prometheus to poll them under the same job (like "exec_cluster") using a 5 second polling interval.
3. If benchmarking VAST, install and run a node_exporter on each CNode and group them under another job ("cnode_cluster").

You'll specify the prometheus service when executing benchmark runs.

Trino
-----

Trino is run using the "benchgo" script. 

    benchgo --name vdb-trino-1t \
    --benchmark tpcds \
    --engine trino \
    --trino-coordinator http://10.73.1.41:8080 \
    --trino-catalog vast \
    --trino-schema "db0/tpcds/1t" \
    --tpcds-scale sf1000 \
    --prometheus-host http://10.73.1.41:9090

Spark
-----

Spark benchmarks are run by configuring the "benchgo_spark" script and then submitting the job to spark-submit or pyspark:

    # Needed if you're using a stand-alone pyspark
    export SPARK_HOME=/usr/local/spark3

    pyspark --driver-class-path $(echo /usr/local/vast-spark3/*.jar | tr ' ' ':') \
    --jars $(echo /usr/local/vast-spark3/*.jar | tr ' ' ',') < $(which benchgo_spark)


Iceberg

    export SPARK_HOME=/usr/local/spark3
    pyspark --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.4.3 < $(which benchgo_spark)

    
'''
 spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.4.3 \
   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
   --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
   --conf spark.sql.catalog.spark_catalog.type=hive \
   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
   --conf spark.sql.catalog.local.type=hive \
   --conf spark.hadoop.hive.metastore.uris=thrift://10.73.1.41:9083 \
   --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
   --conf spark.hadoop.fs.s3a.access.key="KTYJE7EBRPXFA8LW40RT" \
   --conf spark.hadoop.fs.s3a.secret.key="CrCK8xPdTNtUo+vXzXDukRFeDQYL7Q9XThEb3iQh" \
   --conf spark.hadoop.fs.s3a.path.style.access="true" \
   --conf spark.hadoop.fs.s3a.connection.ssl.enabled="false" \
   --conf spark.hadoop.fs.s3a.endpoint=http://local.tmphx.vast.lab:8070
'''

Generating Data for Transaction Tests
-------------------------------------

This is an example set of commands to generate the indicated data sets using a set of
3 servers each with 40 cores and 256GB of memory.

    
    export S3_ENDPOINT=http://localhost:8070
    export S3_BUCKET=object-data
    export KEY_PREFIX="trns_tbl"
    export AWS_ACCESS_KEY_ID="KTYJE7EBRPXFA8LW40RT"
    export AWS_SECRET_ACCESS_KEY="CrCK8xPdTNtUo+vXzXDukRFeDQYL7Q9XThEb3iQh"

    ##
    # Creates the following data sets
    # - 10 cols, 1m, 10m, 100m, 1b, 10b rows
    # - 100 cols, 1m, 10m, 100m, 1b, 10b rows
    # - 1000 cols, 1m, 10m, 100m, 1b, 10b rows
    # - 10000 cols, 1m, 10m, 100m, 1b, 10b rows

    ###########################################################################

    ##
    # 10 cols, 10m rows
    for n in {1..10}; do
        mkdata --endpoint ${S3_ENDPOINT} \
        --data \
        --jobs 40 \
        --job ${n} \
        --width-factor 1 \
        --s3out ${S3_BUCKET} \
        --s3prefix ${KEY_PREFIX}/1c1r \
        --records-per-job 25000
    done

    ##
    # 10 cols, 10m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1c10r \
            --records-per-job 250000 &
    done

    ##
    # 10 cols, 100m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1c100r \
            --records-per-job 2500000 &
    done

    ##
    # 10 cols 1000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1c1000r \
            --records-per-job 25000000 &
    done

    ##
    # 10, cols 10000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1c10000r \
            --records-per-job 250000000 &
    done
 
    ###########################################################################

    ##
    # 100, cols 1m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
        --data \
        --jobs 40 \
        --job ${n} \
        --width-factor 10 \
        --s3out ${S3_BUCKET} \
        --s3prefix ${KEY_PREFIX}/10c1r \
        --records-per-job 25000
    done

    ##
    # 100, cols 10m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 10 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/10c10r \
            --records-per-job 250000 &
    done

    ##
    # 100, cols 100m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 10 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/10c100r \
            --records-per-job 2500000 &
    done

    ##
    # 100, cols 1000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 10 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/10c1000r \
            --records-per-job 25000000 &
    done

    ##
    # 100, cols 10000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 10 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/10c10000r \
            --records-per-job 250000000 &
    done
  
    ###########################################################################

    ##
    # 1000, cols 1m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
        --data \
        --jobs 40 \
        --job ${n} \
        --width-factor 100 \
        --s3out ${S3_BUCKET} \
        --s3prefix ${KEY_PREFIX}/100c1r \
        --records-per-job 25000 &

    ##
    # 1000, cols 10m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 100 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/100c10r \
            --records-per-job 250000 &
    done

    ##
    # 1000, cols 100m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 100 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/100c100r \
            --records-per-job 2500000 &
    done

    ##
    # 1000, cols 1000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 100 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/100c1000r \
            --records-per-job 25000000 &
    done

    ##
    # 1000, cols 10000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 100 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/100c10000r \
            --records-per-job 250000000 &
    done
    

    ###########################################################################

    ##
    # 10,000, cols 1m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
        --data \
        --jobs 40 \
        --job ${n} \
        --width-factor 1000 \
        --s3out ${S3_BUCKET} \
        --s3prefix ${KEY_PREFIX}/1000c1r \
        --records-per-job 25000 &
    done

    ##
    # 1000, cols 10m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1000 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1000c10r \
            --records-per-job 250000 &
    done

    ##
    # 1000, cols 100m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1000 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1000c100r \
            --records-per-job 2500000 &
    done

    ##
    # 1000, cols 1000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1000 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1000c1000r \
            --records-per-job 25000000 &
    done

    ##
    # 1000, cols 10000m rows
    for n in {1..40}; do
        mkdata --endpoint ${S3_ENDPOINT} \
            --data \
            --jobs 40 \
            --job ${n} \
            --width-factor 1000 \
            --s3out ${S3_BUCKET} \
            --s3prefix ${KEY_PREFIX}/1000c10000r \
            --records-per-job 250000000 &
    done

