import sys, datetime, time, json
from benchgo.tpcds import TPCDS
from benchgo.spark.util import spcfg, config_connect, config_block
from benchgo.queries.tpcds import tpcds_table_row_counts
from benchgo.queries.spark import *
from benchgo.prometheus_handler import PrometheusHandler

class SparkSQLTPCDS(TPCDS):

    def __init__(self) -> None:
        self.cfg = spcfg()
        self.engine = 'spark'
    

    def run_step(self):

        self.print_spark_config()

        if self.cfg.get("tpcds_step.tablecheck"):
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.cfg.get("tpcds_step.analyze_tables"):
            print("== analyzing tables ==")
            self.analyze_tables()

        self.logging_setup()

        self.prometheus_handler = PrometheusHandler(
            prometheus_host=self.cfg.get("prometheus.host"),
            exec_job=self.cfg.get("exec_monitor.prometheus_job"),
            cnode_job=self.cfg.get("cnode_monitor.prometheus_job"))

        print("running TCP-DS step-benchmark")
        print("db path:      {}".format(self.cfg.get('tpcds_step.database_path')))
        print("scale factor: {}".format(self.cfg.get('tpcds_step.scale_factor')))
        print()

        self.step_benchmark()

    def run(self):
        
        self.print_spark_config()

        if self.cfg.get("tpcds.tablecheck"):
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.cfg.get("tpcds.analyze_tables"):
            print("== analyzing tables ==")
            self.analyze_tables()

        self.logging_setup()
        
        self.prometheus_handler = PrometheusHandler(
            prometheus_host=self.cfg.get("prometheus.host"),
            exec_job=self.cfg.get("exec_monitor.prometheus_job"),
            cnode_job=self.cfg.get("cnode_monitor.prometheus_job"))

        print("running TCP-DS benchmark")
        print("db path:      {}".format(self.cfg.get('tpcds.database_path')))
        print("scale factor: {}".format(self.cfg.get('tpcds.scale_factor')))
        print("concurrency:  {}".format(self.cfg.get('tpcds.concurrency')))
        print()

        self.benchmark(self._benchmark_thread)

        return True
    

    def print_spark_config(self):
        spark_parameters = config_block(self.cfg)
        print("\nSpark Configuration:")
        for param in spark_parameters:
            print("{:>50}:{:<40}".format(param[0], param[1]))
        print("")


    def _benchmark_thread(self, id, queries):

        spark = config_connect(self.cfg, "{}_{}".format(self.cfg.get('job.app_name'), id))
        spark.sql('USE {}'.format(self.cfg.get('tpcds.database_path')))

        self.process_data[id]["started"] = time.time()
        for q, query in enumerate(queries):
            self.process_data[id]["current_query"] = query
            self.process_data[id]["query_count"] = q
            result = spark.sql(query)
            self.process_data[id]["result"] = result.collect()

        self.process_data[id]["finished"] = time.time()
        spark.stop()


    def tablecheck(self) -> bool:
        '''
        Makes sure the table row counts look correct and return false if they
        are not.
        '''
        spark = config_connect(self.cfg, 'table_check')

        row_info = tpcds_table_row_counts[self.cfg.get('tpcds.scale_factor')]
        spark.sql("USE {db_path}".format(db_path=self.cfg.get('tpcds.database_path')))

        check = True
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "SELECT COUNT(*) AS count FROM {}.{}".format(self.cfg.get('tpcds.database_path'), table)
            res = spark.sql(query)
            count = res.collect()[0]['count']
            
            if count == row_info[table]:
                print("ok")
            else:
                print(" NOK {}/{}".format(count, row_info[table]))
                check = False

        spark.stop()

        return check
    

    def analyze_tables(self) -> None:
        '''
        Analyze tables
        '''
        spark = config_connect(self.cfg, 'table_analysis')

        # just get the list of tables from somewhere:
        row_info = tpcds_table_row_counts[self.cfg.get('tpcds.scale_factor')]

        print("analyzing tables:")
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "ANALYZE TABLE {}.{} COMPUTE STATISTICS FOR ALL COLUMNS".format(self.cfg.get('tpcds.database_path'), table)
            res = spark.sql(query)
            collected = res.collect()
            print("ok")

        spark.stop()


    def step_query_setup(self):
        self.queries = []
        
        sf = self.cfg.get('tpcds_step.scale_factor')

        if sf == "sf10000":
            self.queries = SPARK_TPCDS_QUERY_SF10000
        elif sf == "sf1000":
            self.queries = SPARK_TPCDS_QUERY_SF1000
        else:
            sys.stderr.write("WARNING: scale factor {} not supported, using sf1000 queries\n".format(sf))
            self.queries = SPARK_TPCDS_QUERY_SF1000
            

    def step_benchmark(self):
        '''
        Old and only good for a limited number of scale factors and options.
        Useful for outputting info query-by-query
        '''
        self.step_query_setup()

        if self.cfg.get('tpcds_step.run_queries').upper() != "ALL":
            limit_queries = {}
            for q in [x.strip() for x in self.cfg.get('tpcds_step.run_queries').split(",")]:
                limit_queries[q] = self.queries[q]
            self.queries = limit_queries

        # Go        
        benchmark_start_time = datetime.datetime.now()

        spark = config_connect(self.cfg, 'step_benchmark')
        db_path = self.cfg.get("tpcds_step.database_path")
        print("using "+db_path)
        spark.sql("USE "+db_path)

        header = "row, query, id, time, nodes, cpu, mem, rows, bytes, splits, exec cluster util, cnode cluster util, all cpu util, exec ingress, disk_r, disk_w"
        
        self.result_log_fh.write(header+"\n")
        self.result_log_fh.flush()
        print(header)


        row_count = 0
        for query in self.queries:

            row_count += 1

            ###################################################################
            # Execute
            #print(self.queries[query])
            try:
                then = datetime.datetime.now(tz=datetime.timezone.utc)
                ed = spark.sql(self.queries[query])
                rows = ed.collect()
                now = datetime.datetime.now(tz=datetime.timezone.utc)

            except Exception as e:
                self.result_log_fh.write("{row},{query},{e},,,,,,,,,\n".format(row=row_count, query=query,e=e))
                self.result_log_fh.flush()
                continue
            
            time.sleep(int(self.cfg.get('tpcds_step.sleep_time_sec')))

            ###################################################################
            # Process output

            self.prometheus_handler.gather(then, now)
            timing = "{rowcount:03d},{query},{query_id},{time:.2f},{nodes},{cpu},{mem},{rows},{bytes},{splits},{t_cluster_util},{v_cluster_util},{agg_cpu_util},{tnet_quiet_in:.2f},{disk_r},{disk_w}".format(
                rowcount=row_count,
                query=query,
                query_id="",
                time=(now.timestamp()-then.timestamp()),
                nodes="",
                splits="",
                cpu="",
                rows="",
                bytes="",
                mem="",
                state="",
                t_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.exec_cluster_rate) if self.prometheus_handler.collection_data.exec_cluster_rate <= 1 else "",
                v_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.cnode_cluster_rate) if self.prometheus_handler.collection_data.cnode_cluster_rate <= 1 else "",
                agg_cpu_util="{:.2f}".format(
                    (   (self.prometheus_handler.collection_data.exec_cluster_rate  * self.prometheus_handler.collection_data.exec_cpus) + 
                        (self.prometheus_handler.collection_data.cnode_cluster_rate * self.prometheus_handler.collection_data.cnode_cpus)
                    ) / (self.prometheus_handler.collection_data.ttl_cpus if self.prometheus_handler.collection_data.ttl_cpus > 0 else 1)
                    ) if (self.prometheus_handler.collection_data.exec_cluster_rate <=1 and self.prometheus_handler.collection_data.cnode_cluster_rate <= 1) else "",
                tnet_quiet_in=self.prometheus_handler.collection_data.exec_net_quiet_in,
                disk_r=self.prometheus_handler.collection_data.exec_disk_r,
                disk_w=self.prometheus_handler.collection_data.exec_disk_w)
            
            self.result_log_fh.write(timing+"\n")
            self.result_log_fh.flush()
            print(timing)

            with open("{outdir}/output_{query}.txt".format(outdir=self.output_dir, query=query), "w") as fh:
                for row in rows:
                    fh.write(str(row))
                
            with open("{outdir}/node_series_{query}.json".format(outdir=self.output_dir, query=query), "w") as fh:
                fh.write(json.dumps({
                    "exec_cpus": self.prometheus_handler.collection_data.exec_cpu_data,
                    "cnode_cpus": self.prometheus_handler.collection_data.cnode_cpu_data,
                    "tnet_in": self.prometheus_handler.collection_data.exec_network_data_in,
                    "tnet_out": self.prometheus_handler.collection_data.exec_network_data_out,
                    "exec_disk_r": self.prometheus_handler.collection_data.exec_disk_reads,
                    "exec_disk_w": self.prometheus_handler.collection_data.exec_disk_writes
                }))

        benchmark_end_time = datetime.datetime.now()
        elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
        print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
        
        self.prometheus_handler.dump_stats(benchmark_start_time, benchmark_end_time, self.output_dir)

        print("\ndone")
