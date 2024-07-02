import argparse, json, datetime, time, requests, sys, os, threading, math
from benchgo.queries.trino import *
from benchgo.trino import tpcds_table_row_counts
from benchgo.trino.util import *
from benchgo.util import prometheus_args, tpcds_args, global_args
from benchgo.tpcds import TPCDS


class TrinoTPCDS(TPCDS):
    
    # Declaratons
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
    

    def query_setup(self):
        self.queries = []

        if self.args.scale_factor == "sf100000":
            pass
        elif self.args.scale_factor == "sf10000":
            self.queries = TRINO_TPCDS_QUERY_SF10000
        elif self.args.scale_factor == "sf1000":
            self.queries = TRINO_TPCDS_QUERY_SF1000
        elif self.args.scale_factor == "sf100":
            pass
        elif self.args.scale_factor == "sf10":
            pass
        elif self.args.scale_factor == "sf1":
            pass
            
        if len(self.queries) == 0:
            sys.stderr.write("scale factor {} not supported\n".format(self.args.scale_factor))
            sys.exit(1)


    def run(self):

        if not self.args.skip_precheck:
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.args.analyze_tables:
            self.analyze_tables()


        self.logging_setup()
        self.prometheus_connect()

        if self.args.step_query:
            self.step_benchmark()
        else:
            self.benchmark()

        print("output in {}/result_log.csv".format(self.output_dir))

    def _benchmark_thread(self, id, queries):
        tc = connection(self.args)
        self.thread_data[id]['started'] = datetime.datetime.now()
        for q, query in enumerate(queries):
            self.thread_data[id]['query_count'] = q
            self.thread_data[id]['current_query'] = query
            result = tc.execute(query)
            getme = result.fetchall()
        self.thread_data[id]['finished'] = datetime.datetime.now()



    def benchmark(self):
        
        print("running TCP-DS benchmark on Trino")
        print("trino path:   {}.{}".format(self.args.catalog, self.args.schema))
        print("scale factor: {}".format(self.args.scale_factor))
        print("concurrenty:  {}".format(self.args.concurrency))
        print()

        here = os.path.dirname(__file__)
        threads = []
        self.thread_data = []
        for t, stream in enumerate(range(0, int(self.args.concurrency))):
            file_path = os.path.join(here, '../queries/trino/tpcds/{scale_factor}/{concurrency}/query_{stream}.json'.format(
                scale_factor=self.args.scale_factor,
                concurrency=self.args.concurrency,
                stream=stream))
            with open(file_path, "r") as jf:
                queries = json.load(jf) 
            threads.append(threading.Thread(target=self._benchmark_thread, args=(t, queries,)))
            self.thread_data.append({
                'started': None,
                'finished': None,
                'query_count': 0,
                'current_query': ''
            })
        
        benchmark_start_time = datetime.datetime.now()
        for t in threads:
            t.start()

        # Poll
        print("elapsed time   | stream percent of queries finished...")
        print("---------------|--------------------------------------")
        while True:
            
            tc = int(self.args.concurrency)
            for t in threads:
                if not t.is_alive():
                    tc -= 1
            if tc == 0: break
            time.sleep(1)
            nownow = datetime.datetime.now()
            delta = nownow - benchmark_start_time
            sys.stdout.write("\r{} | ".format(delta))
            sys.stdout.write(" | ".join([ "{}%".format( math.ceil(x["query_count"]*100/103) ) for i, x in enumerate(self.thread_data)]))

        
        for t in threads:
            t.join()

        print("\ndone")

            


    def step_benchmark(self):
        
        # Grab queries for the selected scale factor
        self.query_setup()

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

            timing = "{rowcount:03d},{query},{query_id},{time},{nodes},{cpu},{mem},{rows},{bytes},{splits},{t_cluster_util},{v_cluster_util},{agg_cpu_util},{tnet_quiet_in:.2f},{disk_r},{disk_w}".format(
                rowcount=row_count,
                nodes=ed.stats["nodes"],
                splits=ed.stats["totalSplits"],
                time=((ed.stats["elapsedTimeMillis"]-ed.stats["queuedTimeMillis"]))/1000,
                cpu=ed.stats["cpuTimeMillis"],
                rows=ed.stats["processedRows"],
                bytes=ed.stats["processedBytes"],
                mem=ed.stats["peakMemoryBytes"],
                state=ed.stats["state"],
                query_id=ed.query_id,
                query=query,
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
                    "trino_cpus": self.prometheus_handler.collection_data.exec_cpu_data,
                    "cnode_cpus": self.prometheus_handler.collection_data.cnode_cpu_data,
                    "tnet_in": self.prometheus_handler.collection_data.exec_network_data_in,
                    "tnet_out": self.prometheus_handler.collection_data.exec_network_data_out,
                    "trino_disk_r": self.prometheus_handler.collection_data.exec_disk_reads,
                    "trino_disk_w": self.prometheus_handler.collection_data.exec_disk_writes
                }))
                fh.close()

        benchmark_end_time = datetime.datetime.now()
        elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
        print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
        
        self.prometheus_handler.dump_stats(benchmark_start_time, benchmark_end_time, self.output_dir)

        print("done")

    def tablecheck(self) -> bool:
        '''
        Makes sure the table row counts look correct and return false if they are
        not.
        '''
        row_info = tpcds_table_row_counts[self.args.scale_factor]
        check = True
        tc = connection(self.args)
        print("checking tables:")
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
        print("analyzing tables:")
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "ANALYZE {}.\"{}\".{}".format(self.args.catalog, self.args.schema, table)
            tc.execute(query)
            rows = tc.fetchall()
            print("done")
        

