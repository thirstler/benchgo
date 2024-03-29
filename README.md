Running Benchmarks
==================

Install
-------
I'd create a virtual environment for this

    python -mvenv ./venv
    source ./venv/bin/activate

Enter benchgo directory and
 
    python -mbuild
    pip install ./dist/benchgo-VERSION.tar.gz
    

Monitoring
----------
Output of some benchmark tests (TPC-DS) include performance data polled from worker nodes to determine CPU utilization, network traffic and disk I/O during benchmarks. Prometheus is used for this, so you'll need to install Prometheus (and optionally Grafana) somewhere in the benchmark environment. Rough outline:

1. Install Prometheus on a system separate from any benchmarking systems
2. Install node_exporter and all worker systems and configure Prometheus to poll them under the same job (like "exec_cluster") using a 5 second polling interval.
3. If benchmarking VAST, install and run a node_exporter on each CNode and group them under another job ("cnode_cluster").

You'll specify the prometheus service when executing benchmark runs.

Data
----
In the case of TPC-DS and TPC-DS-Selective tests data is not pre-generated.
You'll have to do that on your own ahead of time. I might imbed some ability 
to generate and place TPC-DS and TPC-H data with this utility later.

Tests/Benchmarks
----------------
Bit of a hodge-podge but you can run

Spark & Trino
- TPC-DS
- TPC-DS-Selective (custom)

Trino Only:
- Merge (delete & update)
- Update
- Insert tests

Spark Only:
- Dataframe ingest

SDK:
- Ingest

Trino
-----

Trino and SDK operations are run using the "benchgo" script. 

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

Spark benchmarks are run by editing an environment file (~/.benchgo/sparkenv)
and a YAML configuration file (~/.benchgo/benchgo_spark.yaml) to indicate the
tests you want to run. The tests are pre-canned so it is not a workload file
but rather where the spark job gets configuration - as well as set some
configuration items for the load you want to run.

    # Set up a sparkenv file - set's up the spark driver environment, not the
    # executors. Set to legit values, of course
    cat << EOF > ~/.benchgo/sparkenv
    SPARK_HOME=/usr/local/spark3
    VAST_CONNECTOR=/usr/local/vast-spark

    # becnchgo_spark.py needs these
    export SPARK_HOME VAST_CONNECTOR
    EOF

    # Create a template YAML file with 
    benchgo --gen-spark-config

    # Edit the config file to taste:
    vi ~/.benchgo/benchgo_spark.yaml

    # Run the job
    benchgo_spark

    

Generating Data for Transaction Tests
-------------------------------------

This is an example set of commands to generate the indicated data sets on a single server. You can adjust these commands to generate data on multiple servers.

    # For upload to an S3 bucket, set up some variables, otherwise you
    # can specify a directory for output.
    export S3_ENDPOINT=http://localhost:8070
    export S3_BUCKET=object-data
    export KEY_PREFIX="trns_tbl"
    export AWS_ACCESS_KEY_ID="KTYJE7EBRPXFA8LW40RT"
    export AWS_SECRET_ACCESS_KEY="CrCK8xPdTNtUo+vXzXDukRFeDQYL7Q9XThEb3iQh"


    ##
    # Small table with 13 cols and 1 million rows
    for n in {1..40}; do
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
    # Fat table with 10,022 cols and 1 billion rows
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

And of course you can do anything in between to arrive at a table of synthetic data of desired column width and row count.
