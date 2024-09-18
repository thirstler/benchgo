import sys, os, math, time, json
from datetime import datetime
#from multiprocessing import Process, Array, Value
from threading import Thread
from ctypes import c_double, c_int32

class TPCDS:
    '''
    base class for any TPC-DS benchmark regardless of engine, should 
    implement generic timers and reports from benchmark worker processes
    defined child classes.
    '''
    queries = None


    def logging_setup(self):
        # Set up ouput dir and main result log

        # Spark is special
        if self.engine == 'spark':
            job_name = self.cfg.get('job.app_name')
        else:
            job_name = self.args.name

        self.output_dir = "/tmp/{}_{}".format(job_name, datetime.now().strftime("%Y%m%d%H%M%S"))

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
        
        nownow = datetime.now()
        delta = nownow - self.benchmark_start_time
        sys.stdout.write("\r{} | ".format(delta))
        sys.stdout.write(" | ".join([ "{}%".format(math.ceil(x["query_count"]*100/103) ) for i, x in enumerate(self.process_data)]))

    def output_results(self):
        
        # Start and finish timings are gathered from the individual process
        # timings to exclude thread polling overhead in the main thread
        start = min([x['started'] for x in self.process_data])
        end = max([x['finished'] for x in self.process_data])

        # Collate any results that the engine may have been able to add
        # will be a bunch of zeros, of not
        q_nodes = max([x["q_nodes"] for x in self.process_data])
        q_time = sum([x["q_time"] for x in self.process_data])
        q_cpu = sum([x["q_cpu"] for x in self.process_data])q_splits
        q_bytes = sum([x["q_bytes"] for x in self.process_data])
        q_mem = max([x["q_mem"] for x in self.process_data])
        q_rows = sum([x["q_rows"] for x in self.process_data])
        q_splits = sum([x["q_splits"] for x in self.process_data])
        extended = True if any([q_time, q_nodes, q_cpu, q_mem, q_rows, q_splits, q_bytes]) > 0 else False

        self.prometheus_handler.gather(datetime.fromtimestamp(start), datetime.fromtimestamp(end))

        print("\n# Benchmark runtime and cluster load statistics")
        print("benchmark time, exec cluster util, cnode cluster util, agg util, cluster ingress, disk writes, disk reads", end='')
        if extended:
            print(", nodes, ttl wall, ttl CPU, ttl bytes, max mem, ttl rows, ttl splits")
        else: print()

        print("{benchtime:.2f}, {t_cluster_util}, {v_cluster_util}, {agg_cpu_util}, {tnet_quiet_in:.2f}, {disk_r:.2f}, {disk_w:.2f}".format(
                benchtime=end-start,
                t_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.exec_cluster_rate) if self.prometheus_handler.collection_data.exec_cluster_rate <= 1 else "",
                v_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.cnode_cluster_rate) if self.prometheus_handler.collection_data.cnode_cluster_rate <= 1 else "",
                agg_cpu_util="{:.2f}".format(
                    (   (self.prometheus_handler.collection_data.exec_cluster_rate  * self.prometheus_handler.collection_data.exec_cpus) + 
                        (self.prometheus_handler.collection_data.cnode_cluster_rate * self.prometheus_handler.collection_data.cnode_cpus)
                    ) / (self.prometheus_handler.collection_data.ttl_cpus if self.prometheus_handler.collection_data.ttl_cpus > 0 else 1)
                    ) if (self.prometheus_handler.collection_data.exec_cluster_rate <=1 and self.prometheus_handler.collection_data.cnode_cluster_rate <= 1) else "",
                tnet_quiet_in=self.prometheus_handler.collection_data.exec_net_quiet_in,
                disk_r=self.prometheus_handler.collection_data.exec_disk_r,
                disk_w=self.prometheus_handler.collection_data.exec_disk_w), end='')    
        if extended:
            print(f", {q_nodes}, {q_time}, {q_cpu}, {q_bytes}, {q_mem}, {q_rows}, {q_splits}")
        else: print()


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
                'started': 0,
                'finished': 0,
                'query_count': 0,
                'current_query': 0,
                'result': 0,
                'q_time': 0,
                'q_nodes': 0,
                'q_cpu': 0,
                'q_mem': 0,
                'q_rows': 0,
                'q_bytes': 0,
                'q_splits': 0
            })
            processes.append(Thread(target=proc_function, args=(t, queries)))
        
        self.benchmark_start_time = datetime.now()
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