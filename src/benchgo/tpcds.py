import sys, datetime, os, math, time, threading, json
from benchgo.prometheus_handler import PrometheusHandler
from multiprocessing import Process, Array, Value
from ctypes import c_double, c_int32

class TPCDS:
    '''
    base class for any TPC-DS benchmark regardless of engine, should 
    implement all of the generic stuff with engine specific routines in the
    '''

    queries = None
    args = None

    def query_prep(self, query) -> str:
        prefix = ""
        if not self.args.no_explain:
            prefix += "EXPLAIN "
            if not self.args.no_analyze:
                prefix += "ANALYZE "

        return("{prefix}{query}".format(prefix=prefix, query=query))
    

    def prometheus_connect(self) -> bool:

        if not self.args.prometheus_host == None:
            self.prometheus_handler = PrometheusHandler(self.args)
            return True
        else:
            return False


    def logging_setup(self):
        # Set up ouput dir and main result log

        # Spark is special
        if self.engine == 'spark':
            job_name = self.cfg.get('job.app_name')
        else:
            job_name = self.args.name

        self.output_dir = "/tmp/{}_{}".format(job_name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

        try:
            os.mkdir(self.output_dir)
        except Exception as ops:
            sys.stderr.write("problem creating output dir {}: {}\n".format(self.output_dir, ops))
            sys.exit(1)
        
        try:
            self.result_log_fh = open("{outdir}/result_log.csv".format(outdir=self.output_dir), "w")
        except Exception as ops:
            sys.stderr.write("problem creating log file: {}\n".format(ops))
            sys.exit(1)

    def output_header(self):
        print("elapsed time   | stream percent of queries finished...")
        print("---------------|--------------------------------------")


    def output_status(self):
        
        nownow = datetime.datetime.now()
        delta = nownow - self.benchmark_start_time
        sys.stdout.write("\r{} | ".format(delta))
        sys.stdout.write(" | ".join([ "{}%".format( math.ceil(x["query_count"].value*100/103) ) for i, x in enumerate(self.process_data)]))

    def output_results(self):

        start = min([x['started'] for x in self.process_data])
        end = max([x['finished'] for x in self.process_data])
        print(start, end)
        #benchtime = end - start

        self.prometheus_handler.gather(start, end)
        print("elapsed, exec cluster util, cnode cluster util, agg util, cluster ingress, disk writes, disk reads")
        print("{benchtime}, {t_cluster_util}, {v_cluster_util}, {agg_cpu_util}, {tnet_quiet_in}, {disk_r}, {disk_w}".format(
                benchtime=end,
                t_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.exec_cluster_rate) if self.prometheus_handler.collection_data.exec_cluster_rate <= 1 else "",
                v_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.cnode_cluster_rate) if self.prometheus_handler.collection_data.cnode_cluster_rate <= 1 else "",
                agg_cpu_util="{:.2f}".format(
                    (   (self.prometheus_handler.collection_data.exec_cluster_rate  * self.prometheus_handler.collection_data.exec_cpus) + 
                        (self.prometheus_handler.collection_data.cnode_cluster_rate * self.prometheus_handler.collection_data.cnode_cpus)
                    ) / (self.prometheus_handler.collection_data.ttl_cpus if self.prometheus_handler.collection_data.ttl_cpus > 0 else 1)
                    ) if (self.prometheus_handler.collection_data.exec_cluster_rate <=1 and self.prometheus_handler.collection_data.cnode_cluster_rate <= 1) else "",
                tnet_quiet_in=self.prometheus_handler.collection_data.exec_net_quiet_in,
                disk_r=self.prometheus_handler.collection_data.exec_disk_r,
                disk_w=self.prometheus_handler.collection_data.exec_disk_w))
        

    def poll_processes(self, processes) -> bool:

        while True:
            tc = int(self.concurrency)
            for t in processes:
                if not t.is_alive():
                    tc -= 1
            if tc == 0: break
            time.sleep(1)
            # show status
            self.output_status()

        return True
    
    
    def benchmark(self, proc_function):

        here = os.path.dirname(__file__)
        processes = []
        self.process_data = []

        if self.engine == 'spark':
            self.concurrency = int(self.cfg.get('tpcds.concurrency'))
            self.scale_factor = self.cfg.get('tpcds.scale_factor')
        else:
            self.concurrency = int(self.args.concurrency)
            self.scale_factor = self.args.scale_factor

        for t, stream in enumerate(range(0, int(self.concurrency))):
            file_path = os.path.join(here, 'queries/{engine}/tpcds/{scale_factor}/{concurrency}/query_{stream}.json'.format(
                engine=self.engine,
                scale_factor=self.scale_factor,
                concurrency=self.concurrency,
                stream=stream))
            with open(file_path, "r") as jf:
                queries = json.load(jf) 

            self.process_data.append({
                'started': Value(c_double, 0),
                'finished': Value(c_double, 0),
                'query_count': Value(c_int32, 0),
                'current_query': Value(c_int32, 0),
            })
            processes.append(Process(target=proc_function, 
                                    args=(t, queries, self.process_data[-1]['started'],
                                        self.process_data[-1]['finished'],
                                        self.process_data[-1]['query_count'],
                                        self.process_data[-1]['current_query'])))
        
        self.benchmark_start_time = datetime.datetime.now()
        for t in processes:
            t.start()

        
        self.output_header()

        # Poll threads
        self.poll_processes(processes)
  
        for t in processes:
            t.join()
        print()
        
        self.output_results()

        print("\ndone")