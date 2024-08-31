import sys, sqlite3, zlib, json
from prometheus_api_client import PrometheusConnect

class PrometheusHandler:

    class window:
        start = 0
        stop = 0
    
    class collection_data:
        exec_cpus = 0
        exec_disk_r = 0
        exec_disk_w = 0
        exec_cluster_rate = 0
        cnode_cpus = 0
        exec_net_in = 0
        exec_net_out = 0
        ttl_cpus = 0
        exec_net_quiet_in = 0
        cnode_cluster_rate = 0
        exec_cpu_data = None
        cnode_cpu_data = None
        exec_network_data_in = None
        exec_network_data_out = None
        exec_disk_reads = None
        exec_disk_writes = None

    def __init__(self,
                 prometheus_host="http://localhost:9090",
                 exec_job=None,
                 cnode_job=None):
        '''
        Very basic, unauthenticated prometheus connection
        '''
        self.prometheus = PrometheusConnect(
            url=prometheus_host,
            disable_ssl=True,
        )
        self.exec_job = exec_job
        self.cnode_job = cnode_job

    def set_range(self, start, stop):
        self.window.start = start
        self.window.stop = stop


    def get_metric(self, metric, label_config):
        metric_value = None
        # Gather associated metrics
        try:
            metric_value = self.prometheus.get_metric_range_data(
                metric_name=metric,
                label_config=label_config,
                start_time=self.window.start,
                end_time=self.window.stop
            )
        except:
            pass

        return metric_value
    

    def gather(self, start, stop):

        # Set the window
        self.window.start = start
        self.window.stop = stop

        # Gather associated metrics
        self.collection_data.exec_cpu_data = self.get_metric('node_cpu_seconds_total', {"job": self.exec_job, "mode": "idle"})
        self.collection_data.cnode_cpu_data = self.get_metric('node_cpu_seconds_total', {"job": self.cnode_job, "mode": "idle"})
        self.collection_data.exec_network_data_in = self.get_metric('node_netstat_IpExt_InOctets', {"job": self.exec_job})
        self.collection_data.exec_network_data_out = self.get_metric('node_netstat_IpExt_OutOctets', {"job": self.exec_job})
        self.collection_data.exec_disk_reads = self.get_metric('node_disk_read_bytes_total', {"job": self.exec_job})
        self.collection_data.exec_disk_writes = self.get_metric('node_disk_written_bytes_total', {"job": self.exec_job})

        # Compile aggregations
        self.collection_data.exec_cpus = self.agg_node_cpu_count(self.collection_data.exec_cpu_data)
        self.collection_data.cnode_cpus = self.agg_node_cpu_count(self.collection_data.cnode_cpu_data)
        self.collection_data.exec_net_in = self.agg_node_net_rate(self.collection_data.exec_network_data_in)
        self.collection_data.exec_net_out = self.agg_node_net_rate(self.collection_data.exec_network_data_out)
        self.collection_data.exec_disk_r = self.agg_node_disk_rate(self.collection_data.exec_disk_reads)
        self.collection_data.exec_disk_w = self.agg_node_disk_rate(self.collection_data.exec_disk_writes)
        self.collection_data.exec_net_quiet_in = self.collection_data.exec_net_in - self.collection_data.exec_net_out
        self.collection_data.ttl_cpus = self.collection_data.exec_cpus + self.collection_data.cnode_cpus
        self.collection_data.exec_cluster_rate = 1 - self.agg_node_cpu_util_rate(self.collection_data.exec_cpu_data, "idle")
        self.collection_data.cnode_cluster_rate = 1 - self.agg_node_cpu_util_rate(self.collection_data.cnode_cpu_data, "idle")


    def agg_node_cpu_util_rate(self, data, mode) -> float:
        if data == None: return 0
        rate = 0
        ttl_p = 0
        cpus = {}

        for metric in data:

            if metric["metric"]["mode"] == mode:
                if len(metric["values"]) < 2: continue
                cpu_id = "{}:{}".format(metric["metric"]["instance"], metric["metric"]["cpu"])
                try:
                    cpus[cpu_id] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
                except:
                    cpus[cpu_id] = 0
                ttl_p += cpus[cpu_id]

        try:
            rate = ttl_p/len(cpus)
        except:
            rate = -1

        return rate


    def agg_node_net_rate(self, data):
        if data == None: return 0
        ttl_r = 0
        instances = {}

        for metric in data:
            if len(metric["values"]) < 2: continue
            instance = "{}".format(metric["metric"]["instance"])
            try:
                instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
            except:
                instances[instance]  = 0

            ttl_r += instances[instance]

        return ttl_r


    def agg_node_disk_rate(self, data):
        if data == None: return 0
        ttl_r = 0
        instances = {}
        for metric in data:
            if len(metric["values"]) < 2: continue
            instance = "{}".format(metric["metric"]["instance"])
            try:
                instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
            except:
                instances[instance] = 0

            ttl_r += instances[instance]

        return ttl_r


    def agg_node_cpu_count(self, data, ht=True):
        if data == None: return 0
        cpu_count = 0
        for metric in data:
            if metric["metric"]["mode"] == "idle":
                cpu_count += 1
                
        return cpu_count

    def dump_stats(self, start, stop, outdir):

        print("archiving all collected Prometheus metrics for job to SQLite db at {}/prometheus_archive.db".format(outdir))
        everything = self.prometheus.all_metrics()
        con = sqlite3.connect("{}/prometheus_archive.db".format(outdir))
        cur = con.cursor()
        cur.execute("CREATE TABLE dump (metric, data)")
        count=0
        complete=0
        batch = []
        for p in everything:
            metric = self.prometheus.get_metric_range_data(
                metric_name=p,
                start_time=start,
                end_time=stop
            )
            batch.append((p, zlib.compress(bytes(json.dumps(metric), 'utf-8'))))
            if count==100:
                cur.executemany("INSERT INTO dump VALUES(?, ?)", batch)
                sys.stdout.write('\r' + str(complete) + '/' +  str(len(everything)))
                count = 0
                batch.clear()
            count+=1
            complete+=1
        cur.executemany("INSERT INTO dump VALUES(?, ?)", batch)
        sys.stdout.write('\r' + str(complete) + '/' +  str(len(everything)))
        con.commit()
        cur.close()
