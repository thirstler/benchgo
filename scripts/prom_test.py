#!/bin/env python3
import datetime
from prometheus_api_client import PrometheusConnect
import json
import sqlite3
import sys
import zlib

def prom_node_cpu_util_rate(data, mode):
    ttl_p = 0
    cpus = {}

    for metric in data:

        if metric["metric"]["mode"] == mode:
            if len(metric["values"]) < 2: continue
            cpu_id = "{}:{}".format(metric["metric"]["instance"], metric["metric"]["cpu"])
            cpus[cpu_id] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
            ttl_p += cpus[cpu_id]

    try:
        rate = ttl_p/len(cpus)
    except:
        rate = -1

    return rate

def prom_node_net_rate(data):
    ttl_r = 0
    instances = {}

    for metric in data:

        if len(metric["values"]) < 2: continue
        instance = "{}".format(metric["metric"]["instance"])
        instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
        ttl_r += instances[instance]

    return ttl_r

def prom_node_disk_rate(data):
    ttl_r = 0
    instances = {}
    for metric in data:
        instance = "{}".format(metric["metric"]["instance"])
        instances[instance] = (float(metric["values"][-1][1]) - float(metric["values"][0][1])) /  (float(metric["values"][-1][0]) - float(metric["values"][0][0]))
        ttl_r += instances[instance]

    return ttl_r


def prom_node_cpu_count(data, ht=True):
    cpu_count = 0
    for metric in data:
        if metric["metric"]["mode"] == "idle":
            cpu_count += 1
            
    return cpu_count

prom = PrometheusConnect(
    url="http://10.73.1.41:9090",
    disable_ssl=True,
)

now = datetime.datetime.now()
then = datetime.datetime.now() - datetime.timedelta(seconds=30)
print(now, then)

trino_network_data_in = prom.get_metric_range_data(
        metric_name='node_netstat_IpExt_InOctets',
        label_config={"job": "trino_1"},
        start_time=then,
        end_time=now
    )
trino_network_data_out = prom.get_metric_range_data(
        metric_name='node_netstat_IpExt_OutOctets',
        label_config={"job": "trino_1"},
        start_time=then,
        end_time=now
    )

trino_disk_reads = prom.get_metric_range_data(
        metric_name='node_disk_read_bytes_total',
        label_config={"job": "trino_1"},
        start_time=then,
        end_time=now
    )
trino_disk_writes = prom.get_metric_range_data(
        metric_name='node_disk_written_bytes_total',
        label_config={"job":"trino_1"}, 
        start_time=then,
        end_time=now
    )

"""
everything = prom.all_metrics()
con = sqlite3.connect("prometheus_dump.db")
cur = con.cursor()
cur.execute("CREATE TABLE dump (metric, data)")
count=0
complete=0
batch = []
for p in everything:
    metric = prom.get_metric_range_data(
        metric_name=p,
        start_time=then,
        end_time=now
    )
    batch.append((p, zlib.compress(bytes(json.dumps(metric), 'utf-8'))))
    if count==100:
        cur.executemany("INSERT INTO dump VALUES(?, ?)", batch)
        sys.stdout.write(' ' + str(complete) + '/' +  str(len(everything)) + ' \r' )
        count = 0
        batch.clear()
    count+=1
    complete+=1

print()
con.commit()
con.close()
"""
#print(json.dumps(trino_disk_reads, indent=2))
disk_r = prom_node_disk_rate(trino_disk_reads)
disk_w = prom_node_disk_rate(trino_disk_writes)

print("disk r/w: " + str(disk_r), disk_w)

tnet_in = prom_node_net_rate(trino_network_data_in)
tnet_out = prom_node_net_rate(trino_network_data_out)
print("net in/out/agg_out: " + str(tnet_in), tnet_out, tnet_in-tnet_out)

trino_cpu_data = prom.get_metric_range_data(
	metric_name='node_cpu_seconds_total',
	label_config={"job": "trino_1", "mode": "idle"},
	start_time=then,
	end_time=now,
)
cnode_cpu_data = prom.get_metric_range_data(
	metric_name='node_cpu_seconds_total',
	label_config={"job": "vast_cnodes", "mode": "idle"},
	#start_time=then,
	#end_time=now
)

#print(json.dumps(trino_cpu_data, indent=2))
trino_cpus = prom_node_cpu_count(trino_cpu_data)
cnode_cpus = prom_node_cpu_count(cnode_cpu_data)
print(trino_cpus, cnode_cpus)
print(prom_node_cpu_util_rate(trino_cpu_data, "idle"))
