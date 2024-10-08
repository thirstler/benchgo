import argparse, json, datetime, time, requests, sys, trino
from benchgo.queries.trino import *
from benchgo.queries.tpcds import tpcds_table_row_counts
from benchgo.trino.util import *
from benchgo.util import prometheus_args, tpcds_args, global_args
from benchgo.tpcds import TPCDS
from benchgo.prometheus_handler import PrometheusHandler

class TrinoTPCDS(TPCDS):
    
    output_dir = None
    result_log_fh = None
    prometheus = None

    def __init__(self) -> None:
        parser = argparse.ArgumentParser(
            description="Run a TPC-DS Benchmark for Trino"
        )
        parser.add_argument("trino")
        parser.add_argument("tpcds")
        global_args(parser)
        tpcds_args(parser)
        trino_args(parser)
        prometheus_args(parser)
        self.args = parser.parse_args()
        self.engine = "trino"


    def query_prep(self, query, no_analyze=False, no_explain=False) -> str:
        prefix = ""
        if not no_explain:
            prefix += "EXPLAIN "
            if not no_analyze:
                prefix += "ANALYZE "

        return("{prefix}{query}".format(prefix=prefix, query=query))
    

    def run(self):

        if not self.args.skip_precheck:
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.args.analyze_tables:
            print("== analyzing tables ==")
            self.analyze_tables()

        self.logging_setup()

        self.prometheus_handler = PrometheusHandler(
            prometheus_host=self.args.prometheus_host,
            exec_job=self.args.exec_prometheus_job,
            cnode_job=self.args.cnode_prometheus_job)

        if self.args.step_query:
            print("running TCP-DS queries in order")
            print("trino path:   {}.{}".format(self.args.catalog, self.args.schema))
            print("scale factor: {}".format(self.args.scale_factor))
            print()
            self.step_benchmark()
        else:
            print("running TCP-DS benchmark")
            print("trino path:   {}.{}".format(self.args.catalog, self.args.schema))
            print("scale factor: {}".format(self.args.scale_factor))
            print("concurrency:  {}".format(self.args.concurrency))
            print()

            self.benchmark(self._benchmark_thread)

            #print("(Trino reports {:.2f} seconds aggregate query execution time)".format(max([sum(x["trino_timings"]) for x in self.process_data])))

        #print("output in {}/result_log.csv".format(self.output_dir))
    

    def query_setup(self):
        '''
        Only used by step_benchark()
        '''
        self.queries = []

        if self.args.scale_factor == "sf100000":
            pass
        elif self.args.scale_factor == "sf10000":
            self.queries = TRINO_TPCDS_QUERY_SF10000
        elif self.args.scale_factor == "sf1000":
            self.queries = TRINO_TPCDS_QUERY_SF1000
        else:
            sys.stderr.write("WARNING: scale factor {} not supported, using sf1000 queries\n".format(self.args.scale_factor))
            self.queries = TRINO_TPCDS_QUERY_SF1000
        #else self.args.scale_factor == "sf100":
        #    self.queries = TRINO_TPCDS_QUERY_SF100
        #elif self.args.scale_factor == "sf10":
        #    self.queries = TRINO_TPCDS_QUERY_SF10
        #elif self.args.scale_factor == "sf1":
        #    self.queries = TRINO_TPCDS_QUERY_SF1
        #else:
        #    self.queries = TRINO_TPCDS_QUERY_SF1000
            
        if len(self.queries) == 0:
            sys.stderr.write("scale factor {} not supported\n".format(self.args.scale_factor))
            sys.exit(1)


    def _benchmark_thread(self, id, queries):
        '''
        Actual executing and timing collection for this engine (Trino)
        '''
        tc = connection(self.args)
        self.process_data[id]["started"] = datetime.datetime.now().timestamp()
        #self.process_data[id]['trino_timings'] = []

        for q, query in enumerate(queries):
            self.process_data[id]["current_query"] = query
            self.process_data[id]["query_count"] = q

            try:
                qh = tc.execute(query)
            except trino.exceptions.TrinoQueryError:
                print('\nERROR: workload results invalid due to query error')
                continue
            except:
                print('\nERROR: workload results invalid due to unknown error')
                continue

            # Things that happen after this point skew wll timings so don't
            # add anything that takes much time. Also, benchgo needs to run 
            # near-line to the engine to avoid excess collection time
            result = qh.fetchall()

            # Update connector-specific stats
            self.process_data[id]["result"]    = result
            self.process_data[id]["q_time"]   += (qh.stats["elapsedTimeMillis"] - qh.stats["elapsedTimeMillis"])
            self.process_data[id]["q_nodes"]   = qh.stats["nodes"]
            self.process_data[id]["q_cpu"]    += qh.stats["cpuTimeMillis"]
            self.process_data[id]["q_mem"]     = max([qh.stats["peakMemoryBytes"], self.process_data[id]["q_mem"]])
            self.process_data[id]["q_bytes"]  += qh.stats["processedBytes"]
            self.process_data[id]["q_rows"]   += qh.stats["processedRows"]
            self.process_data[id]["q_splits"] += qh.stats["totalSplits"]

        self.process_data[id]["finished"] = datetime.datetime.now().timestamp()


    def step_benchmark(self):
        '''
        Old and only good for a limited number of scale factors and options.
        Useful for outputting info query-by-query
        '''
        self.query_setup()

        if self.args.run_queries:
            limit_queries = {}
            for q in [x.strip() for x in self.args.run_queries.split(",")]:
                limit_queries[q] = self.queries[q]
            self.queries = limit_queries

        # Go        
        benchmark_start_time = datetime.datetime.now()

        tc = connection(self.args)
        row_count = 0

        header = "row, query, id, time, nodes, cpu, mem, rows, bytes, splits, exec cluster util, cnode cluster util, all cpu util, exec ingress, disk_r, disk_w"
        
        self.result_log_fh.write(header+"\n")
        self.result_log_fh.flush()
        print(header)

        for query in self.queries:
            row_count += 1

            ###################################################################
            # Execute

            try:
                then = datetime.datetime.now(tz=datetime.timezone.utc)
                ed = tc.execute(self.query_prep(self.queries[query]))
                rows = tc.fetchall()
                now = datetime.datetime.now(tz=datetime.timezone.utc)
            except Exception as e:
                self.result_log_fh.write("{row},{query},{e},,,,,,,,,\n".format(row=row_count, query=query,e=e))
                self.result_log_fh.flush()
                continue
            
            time.sleep(int(self.args.sleep_between_queries))

            ###################################################################
            # Process output

            self.prometheus_handler.gather(then, now)

            timing = "{rowcount:03d},{query},{query_id},{time},{nodes},{cpu},{mem},{rows},{bytes},{splits},{t_cluster_util},{v_cluster_util},{agg_cpu_util},{tnet_quiet_in:.2f},{disk_r:.2f},{disk_w:.2f}".format(
                rowcount=row_count,
                query=query,
                query_id=ed.query_id,
                time=((ed.stats["elapsedTimeMillis"]-ed.stats["queuedTimeMillis"]))/1000,
                nodes=ed.stats["nodes"],
                splits=ed.stats["totalSplits"],
                cpu=ed.stats["cpuTimeMillis"],
                rows=ed.stats["processedRows"],
                bytes=ed.stats["processedBytes"],
                mem=ed.stats["peakMemoryBytes"],
                state=ed.stats["state"],
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

            req_session = requests.Session()
            req_session.auth = ("admin", "")
            response = req_session.get('{coordinator}/v1/query/{query_id}'.format(
                                            coordinator=self.args.coordinator,
                                            query_id=ed.query_id))

            with open("{outdir}/info_{query}.json".format(outdir=self.output_dir, query=query), "w") as fh:
                fh.write(response.text)
                fh.close()

            with open("{outdir}/output_{query}.txt".format(outdir=self.output_dir, query=query), "w") as fh:
                for row in rows: fh.write(",".join(row)+"\n")
                fh.close()
                
            with open("{outdir}/node_series_{query}.json".format(outdir=self.output_dir, query=query), "w") as fh:
                fh.write(json.dumps({
                    "exec_cpus": self.prometheus_handler.collection_data.exec_cpu_data,
                    "cnode_cpus": self.prometheus_handler.collection_data.cnode_cpu_data,
                    "tnet_in": self.prometheus_handler.collection_data.exec_network_data_in,
                    "tnet_out": self.prometheus_handler.collection_data.exec_network_data_out,
                    "exec_disk_r": self.prometheus_handler.collection_data.exec_disk_reads,
                    "exec_disk_w": self.prometheus_handler.collection_data.exec_disk_writes
                }))
                fh.close()

        benchmark_end_time = datetime.datetime.now()
        elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
        print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
        
        self.prometheus_handler.dump_stats(benchmark_start_time, benchmark_end_time, self.output_dir)

        print("\ndone")


    def tablecheck(self) -> bool:
        '''
        Makes sure the table row counts look correct and return false if they are
        not.
        '''
        row_info = tpcds_table_row_counts[self.args.scale_factor]
        check = True
        tc = connection(self.args)
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "SELECT COUNT(*) FROM {}.\"{}\".{}".format(self.args.catalog, self.args.schema, table)
            tc.execute(query)
            rows = tc.fetchall()

            if int(rows[0][0] == row_info[table]):
                print("ok")
            else:
                print(" NOK {}/{}".format(rows[0][0], row_info[table]))
                check = False
        
        return check
    

    def analyze_tables(self) -> None:
        '''
        Analyze tables
        '''
        # just get the list of tables from somewhere:
        row_info = tpcds_table_row_counts[self.args.scale_factor]

        tc = connection(self.args)
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "ANALYZE {}.\"{}\".{}".format(self.args.catalog, self.args.schema, table)
            tc.execute(query)
            #rows = tc.fetchall()
            print("done")
        

