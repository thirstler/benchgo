from pathlib import Path

CONFIG_OUT_LOCATION="{}/.benchgo/benchgo_spark.yaml".format(Path.home())
CONFIG_TEMPLATE="""config:

  job:
    verbose: False
    dump_interactive: False
    app_name: "spark_benchmark"
    spark_master: "spark://localhost:7077"
    num_exec: 3
    num_cores: 8
    exec_memory: "8g"
    driver_memory: "1g"

  # Base VAST Database Configuration (see vdb_confg for tuning)
  vdb:
    enable: true
    endpoint: "http://local:8070" # primary endpoint
    endpoints: "http://endpoint1:80,http://endpoint2:80" # list of endpoints
    access_key: ""
    secret_key: ""
    splits: 64
    subsplits: 10
    jars: "/usr/local/vast-spark3"

  # Iceberg configuration
  iceberg:
    enable: True
    package: "org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.4.3"
    jars: ""
    metastore_uri: "thrift://10.73.1.41:9083"
    access_key: ""
    secret_key: ""
    s3_endpoint: ""

  # Benchmark configuration
  benchmarks:
    - "tpcds"
    #- "tpcds-selective"
    #- "update/delete"
    #- "insert"
    #- "load"
    #- "merge"
    #- "gendata"
  tpcds:
    database: "ndb.db0.tpcds"
    scale_factor: "sf1000"
    explain: True
    run_queries: "all"
    database_path: "spark_catalog.tpcds_1t"
    clear_cache: True
    sleep_time_sec: 5
  tpcds_selective:
    database: "ndb.db0.tpcds"
    scale_factor: "sf1000"
    explain: True
    run_queries: "all"
    database_path: "spark_catalog.tpcds_1t"
    clear_cache: True
    sleep_time_sec: 5
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
    spark.jars.packages: "org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.4.3"
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