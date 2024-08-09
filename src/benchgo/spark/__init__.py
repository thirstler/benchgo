SPARK_BENCHGO_CONFIG_DIR='.benchgo'
SPARK_BENCHGO_CONFIG_FILE='benchgo_spark.yaml'
SPARK_BENCHGO_ENV_FILE='sparkenv'

ENV_TEMPLATE="""
# Needed for use of spark-submit outside of python env
SPARK_HOME=[/path/to/spark]
VAST_CONNECTOR=[/path/to/vast/connector/jars]
export SPARK_HOME VAST_CONNECTOR
"""

CONFIG_TEMPLATE="""config:
  job:

    # Set to false to suppress some output, you may also want to configure 
    # spark logging
    verbose: False

    # Handy setting that will dump all the configured options in this file
    # to the console so you can troubleshoot with and interactive spark shell
    dump_interactive: False

    # Basic spark settings
    app_name: "spark_benchmark"
    spark_master: "spark://localhost:7077"

    # Worker config, which choosing concurreny < 1 this is per query stream
    # and thus streams will block until resrouces are available
    num_exec: 3
    num_cores: 8
    exec_memory: "8g"

    # Driver options
    driver_memory: "1g"

  # Basic VAST Database Configuration (see vdb_confg section for tuning)
  vdb:
    # If "enable" is set to true it means that the catalog for VDB will
    # be configured for the resulting spark session. It's up to the benchmark
    # configuration to point to the intended catalog location. The default
    # catalog settings will point to the correct places for the supported table
    # formats.
    enable: true
    endpoint: "http://local:8070"
    endpoints: "http://endpoint1:80,http://endpoint2:80"
    access_key: ""
    secret_key: ""
    splits: 64
    subsplits: 10
    jars: "/usr/local/vast-spark3"

  # Base Iceberg configuration (see iceberg_config section for tuning)
  iceberg:
    # If "enable" is set to true it means that the catalog for Iceberg will
    # be configured for the resulting spark session. This utility assumes that
    # your backend storage is S3 with a Hive metastore. You may configure 
    # Iceberg tunings below (iceberg_config) to do whatever you want.
    enable: True
    package: "org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.4.3"
    jars: ""
    metastore_uri: "thrift://10.73.1.41:9083"
    access_key: ""
    secret_key: ""
    s3_endpoint: ""

  # Job monitoring set-up  
  prometheus:
    host: "http://10.73.1.41:9090"
    disable_ssl: True
  exec_monitor:
    enabled: True
    opts: "-javaagent:/usr/local/jmx_exporter/jmx_prometheus_javaagent-0.19.0.jar=9082:/usr/local/jmx_exporter/config.yaml -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8090 -Dcom.sun.management.jmxremote.rmi.port=8091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
    prometheus_job: "exec_1"
  driver_monitor:
    enabled: False
    opts: "-javaagent:/usr/local/jmx_exporter/jmx_prometheus_javaagent-0.19.0.jar=9084:/usr/local/jmx_exporter/config.yaml -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8092 -Dcom.sun.management.jmxremote.rmi.port=8093 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
    prometheus_job: ""
  cnode_monitor:
    enable: True
    prometheus_job: "cnodes"

  # Benchmark configuration. Selecting multiple benchmarks will result in all
  # benchmarks getting run. This probably isn't what you want. Use one.
  benchmarks:
    - "tpcds"
    #- "tpcds-step"
    #- "update/delete"
    #- "insert"
    #- "load"
    #- "merge"
    #- "gendata"

  # Configure TPC-DS benchmark. This will run all queries from dsqgen 
  # generated query streams.
  tpcds:
    # Check to make sure your selected scale factor matches the tables you're
    # pointed at (or just make sure the tables are correctly generated)
    tablecheck: True

    # Analyze for table statistics
    analyze_tables: False

    database_path: "ndb.db0.tpcds.sf1000"
    scale_factor: "sf1000"
    concurrency: 1
    explain: True
    clear_cache: True
    sleep_time_sec: 5

  # To step through each TPC-DS query for individual timings, use "tpcds-step"
  tpcds_step:
    database: "ndb.db0.tpcds"
    scale_factor: "sf1000"
    explain: True
    run_queries: "all"
    database_path: "spark_catalog.tpcds_1t"
    clear_cache: True
    sleep_time_sec: 5

  # Run update/delete benchmark
  update_delete:
    target_table: "ndb.db0.benchmark.target"
    batch_size:
      - 100
      - 1000
      - 10000
  insert:
    # Table format with either be "iceberg" or None
    table_format: "iceberg"
    target_table: "ndb.db0.benchmark.target"
    row_scale: 1
    col_scale: 1
    sparsity: 1.0
    iterations: 10
    batch_size:
      - 100
      - 1000
      - 10000
  merge:
    source_table: "ndb.db0.benchmark.source"
    dest_table: "ndb.db0.benchmark.dest"
  gendata:
    row_scale: 1
    col_scale: 1
    target_table: "ndb.db0.benchmark.dest"

  spark_config:
    spark.sql.adaptive.enabled: "true"

  vdb_config:
    spark.ndb.rowgroups_per_subsplit: 1
    spark.ndb.query_data_rows_per_split: 4000
    spark.ndb.retry_max_count: 3
    spark.ndb.retry_sleep_duration: 1
    spark.ndb.parallel_import: True
    spark.ndb.dynamic_filter_compaction_threshold: 100
    spark.ndb.dynamic_filtering_wait_timeout: 2
    spark.sql.catalog.ndb: "spark.sql.catalog.ndb.VastCatalog"
    spark.sql.extensions: "ndb.NDBSparkSessionExtension"

  iceberg_config:
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.spark_catalog: "org.apache.iceberg.spark.SparkSessionCatalog"
    spark.sql.catalog.spark_catalog.type: "hive"
    spark.sql.catalog.local: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.local.type: "hive"
    spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a: "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"

    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    #spark.hadoop.fs.s3a.experimental.input.fadvise: "random"
    #spark.hadoop.fs.s3a.block.size: "4M"
    #spark.hadoop.fs.s3a.readahead.range: "4M"

    # Generate S3 target config, comment-out if using AWS
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
"""