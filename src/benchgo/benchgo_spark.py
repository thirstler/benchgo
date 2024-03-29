import os, sys, json, random, datetime, time, yaml
from prometheus_api_client import PrometheusConnect
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, countDistinct
from benchgo.tpcds_sf10000_queries import *
from benchgo.tpcds_sf1000_queries import *
from benchgo.tpcds_selective_queries import *
from benchgo.transaction_tables import *
from benchgo.prometheus_handlers import *
from pathlib import Path

class spcfg:

    active = None

    def __init__(self, section="default"):

        config_path = "{}/.benchgo/benchgo_spark.yaml".format(Path.home())
        with open(config_path, "r") as fh:
            self.cfg = yaml.safe_load(fh)["config"]

    def get(self, key_path):
        path = key_path.split(".")
        val = self.cfg
        for i in path:
            try:
                val = val[i]
            except KeyError:
                return None
            except TypeError:
                return None
            
        return val

    
    
def dump_interactive(scfg):
    """
    This is pretty fragile and needs to be updated manually. As it's for
    convenience purposes this is probably ok.  
    """
    
    conf_vals = config_block(scfg)
    sys.stdout.write("spark-sql ")
    if scfg.get("iceberg.enable"):
        try:
            sys.stdout.write("  --packages {} \\\n".format(os.environ["ICEBERG_PACKAGE"]))
        except:
            sys.stdout.write("  --packages {} \\\n".format("[ICEBERG PACKAGE NAME]"))


    if scfg.get("vdb.enable"):
        try:
            sys.stdout.write("  --driver-class-path $(echo {}/*.jar | tr ' ' ':') \\\n".format(os.environ["VAST_CONNECTOR"]))
            sys.stdout.write("  --jars $(echo {}/*.jar | tr ' ' ',') \\\n".format(os.environ["VAST_CONNECTOR"])) 
        except:
            sys.stdout.write("  --driver-class-path $(echo {}/*.jar | tr ' ' ':') \\\n".format("/path/to/vdb/jars"))
            sys.stdout.write("  --jars $(echo {}/*.jar | tr ' ' ',') \\\n".format("/path/to/vdb/jars")) 

    sys.stdout.write("  --master {} \\\n".format(scfg.get("job.spark_master")))
    sys.stdout.write("  --conf spark.executor.instances={} \\\n".format(scfg.get("job.num_exec")))
    sys.stdout.write("  --conf spark.executor.cores={} \\\n".format(scfg.get("job.num_cores")))
    sys.stdout.write("  --conf spark.executor.memory={} \\\n".format(scfg.get("job.exec_memory")))
    sys.stdout.write("  --conf spark.driver.memory={} \\\n".format(scfg.get("job.driver_memory")))

    for key, val in conf_vals:
        sys.stdout.write("  --conf {key}=\"{val}\" \\\n".format(key=key, val=val))

    sys.stdout.write("  --name {}\n\n".format(scfg.get("job.app_name")))

    sys.stdout.flush()


def config_block(scfg):
    conf_vals = []

    # Static stuff
    conf_vals.append(("spark.ui.showConsoleProgress", "false"))
    conf_vals.append(("spark.executor.userClassPathFirst", "true"))
    conf_vals.append(("spark.driver.userClassPathFirst", "true"))

    # Configured
    conf_vals.append(("spark.executor.instances", scfg.get("job.num_exec")))
    conf_vals.append(("spark.executor.cores", scfg.get("job.num_cores")))
    conf_vals.append(("spark.executor.memory", scfg.get("job.exec_memory")))
    conf_vals.append(("spark.driver.memory", scfg.get("job.driver_memory")))

    if scfg.get("exec_monitor.enabled"):
        conf_vals.append(("spark.executor.extraJavaOptions", scfg.get("exec_monitor.opts")))
    if scfg.get("driver_monitor.enabled"):
        conf_vals.append(("spark.driver.extraJavaOptions", scfg.get("driver_monitor.opts")))

    if scfg.get("tpcds.explain") or scfg.get("tpcds_selective.explain"):
        conf_vals.append(("spark.sql.debug.maxToStringFields", "100"))

    if scfg.get("vdb.enable"):

        # basic config
        conf_vals.append( ("spark.ndb.endpoint", scfg.get("vdb.endpoint")) )
        conf_vals.append( ("spark.ndb.data_endpoints", scfg.get("vdb.endpoints")) )
        conf_vals.append( ("spark.ndb.access_key_id", scfg.get("vdb.access_key")) )
        conf_vals.append( ("spark.ndb.secret_access_key", scfg.get("vdb.secret_key")) )
        conf_vals.append( ("spark.ndb.num_of_splits", scfg.get("vdb.splits")) )
        conf_vals.append( ("spark.ndb.num_of_sub_splits", scfg.get("vdb.subsplits")) )

        # Advanced and static config
        for key, val in scfg.get("vdb_config").items():
            conf_vals.append( (key, val) )

    if scfg.get("iceberg.enable"):

        # Basic config
        conf_vals.append( ("spark.hadoop.fs.s3a.endpoint", scfg.get("iceberg.s3_endpoint")) )
        conf_vals.append( ("spark.hadoop.hive.metastore.uris", scfg.get("iceberg.metastore_uri")) )
        conf_vals.append( ("spark.hadoop.fs.s3a.access.key", scfg.get("iceberg.access_key")) )
        conf_vals.append( ("spark.hadoop.fs.s3a.secret.key", scfg.get("iceberg.secret_key")) )

        # Advanced and static config
        for key, val in scfg.get("iceberg_config").items():
            conf_vals.append( (key, val) )
    
    return conf_vals


def config_connect(scfg):

    conf = SparkConf()
    
    conf.setAppName(scfg.get("job.app_name"))
    conf.setMaster(scfg.get("job.spark_master"))

    conf_vals = config_block(scfg)
    for c in conf_vals:
        print("{} -> {}".format(c[0], c[1]))
        conf.set(c[0], c[1])
    
    for c in conf.getAll():
        print("{: >40}: {}".format(c[0], c[1]))

    # Sloppy af
    session = SparkSession.builder.appName(scfg.get("job.app_name")).enableHiveSupport()
    spark = session.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def run_sql_inserts(scfg):

    outdir = "/tmp/{}_{}".format(scfg.get("job.app_name"), datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)
    th = open("{outdir}/timings.csv".format(outdir=outdir), "w")

    spark = config_connect(scfg)
    sys.stdout.write("get col width factor....")

    res = spark.read.table(scfg.get("insert.target_table")).limit(1)
    tw = len(res.columns)
    if tw > 10000:
        width_factor = 1000
    elif tw > 1000:
        width_factor = 100
    elif tw > 100:
        width_factor = 10
    elif tw > 10:
        width_factor = 1
    else:
        width_factor = -1

    print("({})".format(width_factor))
    col_counts = sf_cols(width_factor)

    sys.stdout.write("get row count factor...(range-size/row-count): ")
    sys.stdout.flush()

    res = spark.sql("SELECT MIN(id) AS min, MAX(id) AS max FROM {table}".format(table=scfg.get("insert.target_table"))).collect()
    print("{}/{}".format(int(res[0].max), int(res[0].min)))
    

    op_list = scfg.get("insert.batch_size")
    results = {}
    for o in op_list:
        results[o] = 0

    record_id = res[0].max
    th.write("insert timings\n")
    for batch_sz in op_list:
        th.write("record count,{}\n".format(scfg.get("insert.record_count")))
        th.write("batch size,{}\n".format(batch_sz))
        print("insert {} records in batches of {}".format(scfg.UPDATE_TEST_COUNT, batch_sz))
        
        for count in range(0, scfg.UPDATE_TEST_COUNT, batch_sz):
            rows = []
            tracking_list = []
            for br in range(0, batch_sz):
                record_id += 1
                cols = {}
                
                # Build row
                cols["id"] = record_id
                cols["record_id"] = "{}".format(''.join(random.choices(string.ascii_letters, k=16)))
                for s in range(0, int(col_counts.STR_COLS)):
                    cols["str_val_{}".format(s)] = ''.join(random.choices(string.ascii_letters, k=random.randrange(1,128)))
                for s in range(0, int(col_counts.FLOAT_COLS)):
                    cols["float_val_{}".format(s)] = random.random()*10000          
                for s in range(0, int(col_counts.INT_COLS)):
                    cols["int_val_{}".format(s)] = int(random.random()*(2**31)) if random.random() < 0.5 else -abs(int(random.random()*(2**31)))
                for s in range(0, int(col_counts.BOOL_COLS)):
                    cols["bool_val_{}".format(s)] = True if random.random() < 0.5 else False
                rows.append(cols)

            df = spark.createDataFrame(rows)
            
            try:
                batch_start = time.time()
                df.write.mode("append").save(scfg.TARGET_TABLE)
                elapsed = time.time()-batch_start
                results[batch_sz] += elapsed
                tracking_list.append("{:.2f}".format(elapsed))
                sys.stdout.write("\rbatch {}: {:.2f} sec       ".format(count+1, elapsed))
                sys.stdout.flush()
            except Exception as e:
                sys.stdout.write("\nprobem with insert: {}\n".format(e))

        th.write("discrete insert timings,")
        th.write("{}\n".format(",".join(tracking_list)))

        print("\r{:.2f} seconds to insert {} records in batches of {}".format(results[batch_sz], scfg.UPDATE_TEST_COUNT, batch_sz))

    th.write("overall timings by batch size\n")
    print("operation,{}".format(",".join([str(x) for x in op_list])))

    for o in results:
        th.write("{},{}\n".format(o, results[o]))

    th.close()


def run_inserts(scfg):

    spark = config_connect(scfg)

    record_count = scfg.get("insert.record_count")
    num_cores = scfg.get("job.num_cores")
    num_exec =scfg.get("job.num_exec")
    col_scale = scfg.get("insert.col_scale")
    sparsity = scfg.get("insert.sparsity")
    batch_size = scfg.get("insert.batch_size")
    target_table = scfg.get("insert.target_table")
    iterations = scfg.get("insert.iterations")

    # make a place
    spark.sql(mk_ddl_sql(col_scale, target_table, if_not_exists=True, table_format=scfg.get("insert.table_format")))

    for r in scfg.get("insert.batch_size"):

        timings=[]

        start = time.time()
        rdd = spark.sparkContext.parallelize(
            range(0, r), num_cores * num_exec).map(lambda x: mk_row(x, col_scale, sparsity),)
        df = spark.createDataFrame(rdd,
            schema=[x[0] for x in mk_schema(col_scale)]).cache()
        res = df.select(countDistinct("id")).collect()
        print("generated {} distinct rows in {:.2f} secs".format(res[0][0], time.time()-start))

        for test in range(0, iterations):

            start = time.time()
            df.writeTo(target_table).append()
            timings.append(time.time()-start) 
            sys.stdout.write("{}: {} records, {:.2f}s\n".format(test, r, timings[-1]))
        
        print("{:.2f} rows/s\n".format(r/(sum(timings)/len(timings))))


def run_update_delete(scfg):

    outdir = "/tmp/{}_{}".format(scfg.APP_NAME, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)

    spark = config_connect(scfg)

    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as th:

        th.write("update timings\n")
        sys.stdout.write("determine ID range...")
        sys.stdout.flush()

        res = spark.sql("SELECT MIN(id) AS min, MAX(id) AS max FROM {spark_catalog}.{spark_database}.{table}".format(
                spark_catalog=scfg.SPARK_CATALOG,
                spark_database=scfg.SPARK_DATABASE,
                table=scfg.TARGET_TABLE
            )
        )
        
        src_tbl_rows = res.collect()
        id_range_len = int(src_tbl_rows[0][1])-int(src_tbl_rows[0][0])
        print("done ({})".format(id_range_len))
        ids = []
        for x in range(0,60000):
            ids.append(random.randrange(src_tbl_rows[0][0],src_tbl_rows[0][1]))

        results = {}

        ##
        # Updates/deletes
        record_ptr = 0
        op_list = ["update", "delete"]
        batch_list = [100] # this is as big an SQL as spark can handle
        for action in op_list:
            results[action] = {}
            for batch_sz in batch_list:

                th.write("record count,{}\n".format(scfg.UPDATE_TEST_COUNT))
                th.write("batch size,{}\n".format(batch_sz))

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
                th.write("discrete update timings,{}\n".format(",".join(tracking_list)))
                                                               
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

        th.write("overall timings by batch size\n")
        for r in results:
            th.write("{},".format(r))
            cols = []
            for c in results[r]:
                cols.append("{:2f}".format(results[r][c]))
            th.write("{}\n".format(",".join(cols)))

        ##
        # Merge test
        sys.stdout.write("running merge delete tests....\nget 5 and 25 rowcount percentiles...")
        iceberg_df = spark.read.table("{}.{}.{}".format(scfg.SPARK_CATALOG, scfg.SPARK_DATABASE, scfg.TARGET_TABLE))

        # Side-tables
        p5_sample = iceberg_df.sample(0.05, seed=3)
        p25_sample = iceberg_df.sample(0.25, seed=4)


        th.write("\nmerge tests:\n")
        th.write("5th percentile record count, {}\n".format(p5_sample.count()))
        th.write("25th percentile record count, {}\n".format(p25_sample.count()))
        th.write("load and merge timings:\n")
        th.write("action/percentile,timing\n")
        
        plist = {"p5": p5_sample, "p25": p25_sample}
        for per in plist:

            sys.stdout.write("merge-delete benchmark, {} records...".format(plist[per]))
            sys.stdout.flush()

            target_table = "{}.{}.{}".format(scfg.SPARK_CATALOG, scfg.SPARK_DATABASE, scfg.TARGET_TABLE)
            side_table = "{}.{}.{}_{}".format(scfg.SPARK_CATALOG, scfg.SPARK_DATABASE, scfg.TARGET_TABLE, per)
            timer = time.time()
            plist[per].write \
                .format("iceberg") \
                .mode("overwrite") \
                .saveAsTable(side_table)
            elapsed = time.time()-timer

            print("\nload time ({}): {:.2f}".format(per, elapsed))
            th.write("load {},{:.2f}\n".format(per, elapsed))

            merge_q = "MERGE INTO {tgt_table} USING {src_table} ON {tgt_table}.id = {src_table}.id WHEN MATCHED THEN DELETE".format(
                tgt_table=target_table,
                src_table=side_table,
            )

            # timing #
            merge_start = time.time()
            spark.sql(merge_q)
            merge_end = time.time()
            # end timing #

            print("merge time ({}): {:.2f}".format(per, merge_end-merge_start))
            th.write("merge {},{:.2f}\n".format(per, merge_end-merge_start))

            spark.sql("DROP TABLE {}".format(side_table))



def run_tpcds_selective(scfg):

    queries = []
    sq = tpcds_selective_queries()
    queries = sq.gen_all(scfg.get("tpcds_selective.scale_factor"))

    scfg.active = scfg.get("tpcds_selective")
    run_sql_queries(scfg, queries)


def run_tpcds(scfg):

    queries = []

    if scfg.get("tpcds.scale_factor") == "sf10000":
        queries = tpcds_10t_queries.queries
    elif scfg.get("tpcds.scale_factor")  == "sf1000":
        queries = tpcds_1t_queries.queries

    scfg.active = scfg.get("tpcds")
    run_sql_queries(scfg, queries)


def run_sql_queries(scfg, queries):

    prom = PrometheusConnect(
        url=scfg.get("prometheus.host"),
        disable_ssl=scfg.get("prometheus.disable_ssl")
    )

    outdir = "/tmp/{}_{}".format(scfg.get("job.app_name"), datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)

    benchmark_start_time = datetime.datetime.now()
    spark = config_connect(scfg)
    header = "row, query, id, elapsed, nodes, cpu, mem, rows, bytes, splits, exec cluster util, cnode cluster util, all cpu util, exec ingress, disk_r, disk_w"

    spark.sql("USE {db_path}".format(db_path=scfg.active["database_path"]))

    rowcount = 0
    run_queries = []
    if scfg.active["run_queries"] != "all":
        run_queries = scfg.active["run_queries"]
    else:
        for q in queries:
            run_queries.append(q)

    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as th:
        print(header)
        th.write("{header}\n".format(header=header))
        th.flush()
        for q in run_queries:
            rowcount += 1
            if scfg.active["explain"]:
                explain = spark.sql("EXPLAIN " + queries[q])
                explain_result = [str(row) for row in explain.collect()]
                with open("{outdir}/explain_{query}.txt".format(outdir=outdir, query=q), "w") as fh:
                    fh.write("".join(explain_result)+"\n")
                    fh.close()

            success = None
            try:
                if scfg.active["clear_cache"]:
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
            time.sleep(scfg.active["sleep_time_sec"])

            # Gather associated metrics
            try:
                exec_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": scfg.get("exec_monitor.prometheus_job"), "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_cpu_data = None
                sys.stderr.write(str(e))
            
            try:
                cnode_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": scfg.get("cnode_monitor.prometheus_job"), "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                cnode_cpu_data = None
                sys.stderr.write(str(e))

            try:
                exec_network_data_in = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_InOctets',
                    label_config={"job": scfg.get("exec_monitor.prometheus_job")},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_network_data_in = None
                sys.stderr.write(str(e))
            
            try:
                exec_network_data_out = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_OutOctets',
                    label_config={"job": scfg.get("exec_monitor.prometheus_job")},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_network_data_out = None
                sys.stderr.write(str(e))
            
            try:
                exec_disk_reads = prom.get_metric_range_data(
                    metric_name='node_disk_read_bytes_total',
                    label_config={"job": scfg.get("exec_monitor.prometheus_job")},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                exec_disk_reads = None
                sys.stderr.write(str(e))
                
            try:
                exec_disk_writes = prom.get_metric_range_data(
                    metric_name='node_disk_written_bytes_total',
                    label_config={"job": scfg.get("exec_monitor.prometheus_job")},
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
    print("\ndone")
