import os, sys, json, random, datetime, time
from prometheus_api_client import PrometheusConnect
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from benchgo.tpcds_sf10000_queries import *
from benchgo.tpcds_sf1000_queries import *
from benchgo.tpcds_selective_queries import *
from benchgo.prometheus_handlers import *


def dump_interactive(scfg):

    sys.stdout.write("spark-sql --name {} \\\n".format(scfg.APP_NAME))
    sys.stdout.write("  --master {} \\\n".format(scfg.SPARK_MASTER))
    sys.stdout.write("  --conf spark.executor.memory={} \\\n".format(scfg.SPARK_EXEC_MEMORY))
    sys.stdout.write("  --conf spark.driver.memory={} \\\n".format(scfg.SPARK_DRIVER_MEMORY))
    sys.stdout.write("  --conf spark.executor.userClassPathFirst=true \\\n")
    sys.stdout.write("  --conf spark.driver.userClassPathFirst=true \\\n")
    if scfg.EXEC_MONITORING:
        sys.stdout.write("  --conf spark.executor.extraJavaOptions=\"{}\" \\\n".format(scfg.EXEC_MONITORING_OPTIONS))
    if scfg.DRIVER_MONITORING:
        sys.stdout.write("  --conf spark.driver.extraJavaOptions=\"{}\" \\\n".format(scfg.DRIVER_MONITORING_OPTIONS))
    if scfg.EXPLAIN:
        sys.stdout.write("  --conf spark.sql.debug.maxToStringFields=100 \\\n")

    if scfg.LOAD_VDB:
        sys.stdout.write("  --driver-class-path $(echo {}/*.jar | tr ' ' ':') \\\n".format(scfg.VDB_JARS))
        sys.stdout.write("  --jars $(echo {}/*.jar | tr ' ' ',') \\\n".format(scfg.VDB_JARS))
        for cfg in scfg.SPARK_VDB_CONFIG:
            sys.stdout.write("  --conf {}=\"{}\" \\\n".format(cfg[0], cfg[1]))
    
    if scfg.LOAD_ICEBERG:
        sys.stdout.write("  --packages {} \\\n".format(scfg.ICEBERG_PACKAGE))
        for cfg in scfg.SPARK_ICEBERG_CONFIG:
            sys.stdout.write("  --conf {}=\"{}\" \\\n".format(cfg[0], cfg[1]))
        for cfg in scfg.SPARK_S3A_CONFIG:
            sys.stdout.write("  --conf {}=\"{}\" \\\n".format(cfg[0], cfg[1]))
    if scfg.LOAD_ICEBERG and scfg.S3A_ENDPOINT:
        for cfg in scfg.SPARK_GENERIC_S3A_CONFIG:
            sys.stdout.write("  --conf {}=\"{}\" \\\n".format(cfg[0], cfg[1]))

    sys.stdout.flush()


def config_connect(scfg):

    conf = SparkConf()
    conf.setAppName(scfg.APP_NAME)
    conf.setMaster(scfg.SPARK_MASTER)
    conf.set("spark.executor.instances", scfg.SPARK_NUM_EXEC)
    conf.set("spark.executor.cores", scfg.SPARK_EXEC_CORES)
    conf.set("spark.executor.memory", scfg.SPARK_EXEC_MEMORY)
    conf.set("spark.driver.memory", scfg.SPARK_DRIVER_MEMORY)
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.executor.userClassPathFirst", "true")
    conf.set("spark.driver.userClassPathFirst", "true")
    if scfg.EXEC_MONITORING:
        conf.set("spark.executor.extraJavaOptions", scfg.EXEC_MONITORING_OPTIONS)
    if scfg.DRIVER_MONITORING:
        conf.set("spark.driver.extraJavaOptions", scfg.DRIVER_MONITORING_OPTIONS)
    if scfg.EXPLAIN:
        conf.set("spark.sql.debug.maxToStringFields", "100")

    if scfg.LOAD_VDB:
        for cfg in scfg.SPARK_VDB_CONFIG:
            conf.set(cfg[0], cfg[1])
    
    if scfg.LOAD_ICEBERG:
        for cfg in scfg.SPARK_ICEBERG_CONFIG:
            conf.set(cfg[0], cfg[1])
        for cfg in scfg.SPARK_S3A_CONFIG:
            conf.set(cfg[0], cfg[1])
    if scfg.LOAD_ICEBERG and scfg.S3A_ENDPOINT:
        for cfg in scfg.SPARK_GENERIC_S3A_CONFIG:
            conf.set(cfg[0], cfg[1])
    
    for c in conf.getAll():
        print("{: >40}: {}".format(c[0], c[1]))

    # Sloppy af
    session = SparkSession.builder.appName(scfg.APP_NAME).enableHiveSupport()
    spark = session.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark



def run_update_delete(scfg):

    spark = config_connect(scfg)

    sys.stdout.write("determine ID range...")
    sys.stdout.flush()
    res = spark.sql("SELECT MIN(id) AS min, MAX(id) AS max FROM {spark_catalog}.{spark_database}.{table}".format(
            spark_catalog=scfg.SPARK_CATALOG,
            spark_database=scfg.SPARK_DATABASE,
            table=scfg.TARGET_TABLE
        )
    )
    src_tbl_rows = res.collect()

    print("done ({})".format(int(src_tbl_rows[0][1])-int(src_tbl_rows[0][0])))
    ids = []
    for x in range(0,60000):
        ids.append(random.randrange(src_tbl_rows[0][0],src_tbl_rows[0][1]))

    results = {}

    ##
    # Updates/deletes
    record_ptr = 0
    for action in ["update", "delete"]:
        results[action] = {}
        for batch_sz in [100,1000,10000]:
            start = time.time()
            print("{} {} records in batches of {}".format(action, scfg.UPDATE_TEST_COUNT, batch_sz))
            tracking_list = []
            for count,row in enumerate(range(0,scfg.UPDATE_TEST_COUNT, batch_sz)):
                if action == "update":
                    query = "UPDATE {spark_catalog}.{spark_database}.{table} SET str_val_0='updated in batch size #{batchset}',float_val_0=3.14159265359,bool_val_0=true,int_val_0=42 WHERE ".format(
                        spark_catalog=scfg.SPARK_CATALOG,
                        spark_database=scfg.SPARK_DATABASE,
                        table=scfg.TARGET_TABLE,
                        batchset=batch_sz
                    )
                else:
                    query = "DELETE FROM {spark_catalog}.{spark_database}.{table} WHERE ".format(
                        spark_catalog=scfg.SPARK_CATALOG,
                        spark_database=scfg.SPARK_DATABASE,
                        table=scfg.TARGET_TABLE
                    )
                predicates = []
                for b in range(0, batch_sz):
                    predicates.append("id = {}".format(ids[record_ptr]))
                    record_ptr += 1
                query += " OR ".join(predicates)
                batch_start = time.time()
                try:
                    res = spark.sql(query)
                    elapsed = time.time()-batch_start
                    results[action][str(batch_sz)] = elapsed
                    tracking_list.append("{:.2f}".format(elapsed))
                    sys.stdout.write("\rbatch {}: {:.2f} sec       ".format(count+1, elapsed))
                    sys.stdout.flush()
                except Exception as e:
                    print("problem with {}, this benchmark is invalid".format(action.upper()))
                    if scfg.VERBOSE:
                        print(e)

            print("")
            print(",".join(tracking_list))
            print("{:.2f} seconds to {} {} records in batches of {}".format(time.time()-start, action, scfg.UPDATE_TEST_COUNT, batch_sz))
        
        clean_up_time = time.time()
        # After each set (update/delete), fix the hash-meal that gets left behind, include in timings
        """CALL spark_catalog.system.expire_snapshots(table => 'trns_tbl_ice.ice_t1c1r', older_than => TIMESTAMP '2024-02-14 22:36:28.867')"""

        print('expiring snapshots...')
        try: 
            spark.sql("CALL {spark_catalog}.system.expire_snapshots(table => '{spark_database}.{table}', older_than => TIMESTAMP '{timestamp}')".format(
                spark_catalog=scfg.SPARK_CATALOG,
                spark_database=scfg.SPARK_DATABASE,
                table=scfg.TARGET_TABLE,
                timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            ))
        except Exception as e:
            print("bad CALL (system.expire_snapshots), probably not an iceberg table, skipping")
            
        
        print('compacting files...')
        try:
            spark.sql("CALL {spark_catalog}.system.rewrite_data_files(table => '{spark_database}.{table}')".format(
                spark_catalog=scfg.SPARK_CATALOG,
                spark_database=scfg.SPARK_DATABASE,
                table=scfg.TARGET_TABLE
            ))
        except Exception as e:
            print("bad CALL (system.rewrite_data_files), probably not an iceberg table, skipping")

        print("clean-up after {action}: {time} sc".format(action=action, time="{:2f}".format(time.time()-clean_up_time)))

    print("timings by batch size:\n")
    print("----------------------\n")
    sys.stdout.write("operation")
    for r in results["update"]:
        sys.stdout.write(",{}".format(r))
    for r in results:
        sys.stdout.write("{},".format(r))
        cols = []
        for c in results[r]:
            cols.append("{:2f}".format(results[r][c]))
        print(",".join(cols))

    ##
    # Merge test
    

def run_tpcds(scfg):

    queries = []
    if scfg.BENCHMARK == "tpcds":
        if scfg.TPCDS_QUERY_SCALE_FACTOR == "sf10000":
            queries = tpcds_10t_queries.queries
        elif scfg.TPCDS_QUERY_SCALE_FACTOR == "sf1000":
            queries = tpcds_1t_queries.queries
    elif scfg.BENCHMARK == "tpcds_s":
        sq = tpcds_selective_queries()
        queries = sq.gen_all(scfg.TPCDS_QUERY_SCALE_FACTOR)


    prom = PrometheusConnect(
        url=scfg.PROMETHEUS_HOST,
        disable_ssl=True,
    )

    outdir = "/tmp/{}_{}".format(scfg.APP_NAME, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)

    benchmark_start_time = datetime.datetime.now()
    spark = config_connect(scfg)
    header = "row, query, id, elapsed, nodes, cpu, mem, rows, bytes, splits, exec cluster util, cnode cluster util, all cpu util, exec ingress, disk_r, disk_w"

    spark.sql("USE {catalog}.{database}".format(catalog=scfg.SPARK_CATALOG, database=scfg.SPARK_DATABASE))
    rowcount = 0

    run_queries = []
    if scfg.RUN_QUERIES != None:
        run_queries = scfg.RUN_QUERIES
    else:
        for q in queries:
            run_queries.append(q)

    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as th:
        print(header)
        th.write("{header}\n".format(header=header))
        th.flush()
        for q in run_queries:
            rowcount += 1
            if scfg.EXPLAIN:
                explain = spark.sql("EXPLAIN " + queries[q])
                explain_result = [str(row) for row in explain.collect()]
                with open("{outdir}/explain_{query}.txt".format(outdir=outdir, query=q), "w") as fh:
                    fh.write("".join(explain_result)+"\n")
                    fh.close()

            success = None
            try:
                if scfg.CLEAR_CACHE:
                    spark.sql("CLEAR CACHE")

                res = spark.sql(queries[q])
                then = datetime.datetime.now(tz=datetime.timezone.utc)
                result_string = "\n".join([str(row) for row in res.collect()])
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                elapsed = now.timestamp() - then.timestamp()
                spark.sparkContext._jvm.System.gc() # Force a GC 
                success = True
            except Exception as e:
                print(e)
                print("partial output in {outdir}".format(outdir=outdir))
                then = now = datetime.datetime.now(tz=datetime.timezone.utc)
                result_string = "\n"
                elapsed=0.0
                success = False

            # Need a gap to separate stats collection between queries and allow
            # Prometheus time to report
            time.sleep(scfg.SLEEP_BETWEEN_QUERIES)

            # Gather associated metrics
            try:
                exec_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": scfg.EXEC_PROMETHEUS_JOB, "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_cpu_data = None
                sys.stderr.write(str(e))
            
            try:
                cnode_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": scfg.CNODE_PROMETHEUS_JOB, "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                cnode_cpu_data = None
                sys.stderr.write(str(e))

            try:
                exec_network_data_in = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_InOctets',
                    label_config={"job": scfg.EXEC_PROMETHEUS_JOB},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_network_data_in = None
                sys.stderr.write(str(e))
            
            try:
                exec_network_data_out = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_OutOctets',
                    label_config={"job": scfg.EXEC_PROMETHEUS_JOB},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_network_data_out = None
                sys.stderr.write(str(e))
            
            try:
                exec_disk_reads = prom.get_metric_range_data(
                    metric_name='node_disk_read_bytes_total',
                    label_config={"job": scfg.EXEC_PROMETHEUS_JOB},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_disk_reads = None
                sys.stderr.write(str(e))
                
            try:
                exec_disk_writes = prom.get_metric_range_data(
                    metric_name='node_disk_written_bytes_total',
                    label_config={"job": scfg.EXEC_PROMETHEUS_JOB},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_disk_writes = None
                sys.stderr.write(str(e))

            exec_cpus = prom_node_cpu_count(exec_cpu_data)
            cnode_cpus = prom_node_cpu_count(cnode_cpu_data)
            tnet_in = prom_node_net_rate(exec_network_data_in)
            tnet_out = prom_node_net_rate(exec_network_data_out)
            exec_disk_r = prom_node_disk_rate(exec_disk_reads)
            exec_disk_w = prom_node_disk_rate(exec_disk_writes)
            execnet_quiet_in = tnet_in - tnet_out

            ttl_cpus = exec_cpus+cnode_cpus
            exec_cluster_rate = 1 - prom_node_cpu_util_rate(exec_cpu_data, "idle")
            cnode_cluster_rate = 1 - prom_node_cpu_util_rate(cnode_cpu_data, "idle")

            stats = "{rowcount:03d},{query},{query_id},{elapsed:.2f},{nodes},{cpu},{mem},{rows},{bytes},{splits},{exec_cluster_util},{cnode_cluster_util},{all_cpu_util},{exec_ingress_rate:.2f},{disk_r},{disk_w}".format(
                rowcount=rowcount,
                query=q + "" if success else "FAILED",
                query_id="n/a",
                elapsed=elapsed,
                nodes="null",
                cpu="null",
                mem="null",
                rows="null",
                bytes="null",
                splits="null",
                exec_cluster_util =  "{:.2f}".format(exec_cluster_rate)  if exec_cluster_rate  <= 1 else "",
                cnode_cluster_util = "{:.2f}".format(cnode_cluster_rate) if cnode_cluster_rate <= 1 else "",
                all_cpu_util = "{:.2f}".format(((exec_cluster_rate*exec_cpus) + (cnode_cluster_rate*cnode_cpus))/(ttl_cpus if ttl_cpus > 0 else 1)) if (exec_cluster_rate <= 1 and cnode_cluster_rate <= 1) else "",
                exec_ingress_rate=execnet_quiet_in,
                disk_r=exec_disk_r,
                disk_w=exec_disk_w
            )
              
            print("{stats}".format(stats=stats))
            th.write("{stats}\n".format(stats=stats))
            th.flush()

            with open("{outdir}/output_{query}.txt".format(outdir=outdir, query=q), "w") as fh:
                fh.write(result_string+"\n")
                fh.close()
            
            with open("{outdir}/node_series_{query}.json".format(outdir=outdir, query=q), "w") as fh:
                fh.write(json.dumps({
                    "exec_cpus": exec_cpu_data,
                    "cnode_cpus": cnode_cpu_data,
                    "tnet_in": exec_network_data_in,
                    "tnet_out": exec_network_data_out,
                    "trino_disk_r": exec_disk_reads,
                    "trino_disk_w": exec_disk_writes
                }))
                fh.close()

    benchmark_end_time = datetime.datetime.now()
    elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
    print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
    
    dump_stats(prom, benchmark_start_time, benchmark_end_time, outdir)
    
    spark.stop()
    print("done")
