Running Benchmarks
==================

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

