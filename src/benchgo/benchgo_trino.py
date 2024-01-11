from prometheus_api_client import PrometheusConnect
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from urllib.parse import urlparse
from benchgo.tpcds_10t_queries import *
from benchgo.tpcds_1t_queries import *
from benchgo.prometheus_handlers import *
import os
import datetime
import json
import time
import sqlite3
import sys
import zlib

# Configuration
TRINO_SESSION={
    "vast.expression_projection_pushdown": True,
    "vast.num_of_subsplits": 10,
    "vast.num_of_splits": 64
}

def connection(args):

    endpoint = urlparse(args.trino_coordinator)
    if endpoint.scheme == "https" and args.trino_password != None:
        authscheme=BasicAuthentication(args.trino_user, args.trino_password)
    else:
        authscheme=None

    conn = connect(
        host=endpoint.netloc.split(":")[0],
        port=endpoint.netloc.split(":")[1],
        user=args.trino_user,
        auth=authscheme,
        catalog=args.trino_catalog,
        schema=args.trino_schema,
        session_properties=TRINO_SESSION
    )
    cur = conn.cursor()
    return cur

def dump_stats(prom, start, stop, outdir):

    print("archiving all collected Prometheus metrics for job to SQLite db at {}/prometheus_archive.db".format(outdir))
    everything = prom.all_metrics()
    con = sqlite3.connect("{}/prometheus_dump.db".format(outdir))
    cur = con.cursor()
    cur.execute("CREATE TABLE dump (metric, data)")
    count=0
    complete=0
    batch = []
    for p in everything:
        metric = prom.get_metric_range_data(
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

def run(args):
    if args.tpcds_scale == "sf10000":
        queries = tpcds_10t_queries.queries
    elif args.tpcds_scale == "sf1000":
        queries = tpcds_1t_queries.queries

    prom = PrometheusConnect(
            url=args.prometheus_host,
            disable_ssl=True,
        )
    
    benchmark_start_time = datetime.datetime.now()

    tc = connection(args)
    outdir = "/tmp/{}_{}".format(args.name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)
    row_count = 0
    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as th:
        header = "row, query, id, time, nodes, cpu, mem, rows, bytes, splits, trino cluster util, cnode cluster util, all cpu util, trino ingress rate, disk_r, disk_w"
        th.write(header+"\n")
        print(header)
        for query in queries:
            row_count += 1
            try:
                then = datetime.datetime.now(tz=datetime.UTC)
                ed = tc.execute("EXPLAIN ANALYZE {query}".format(query=queries[query]))
                rows = tc.fetchall()
                now = datetime.datetime.now(tz=datetime.UTC)
            except:
                th.write("{query},,,,,,,,,,\n".format(query=query))
                continue
            
            time.sleep(args.sleep_between_queries)

            # Gather associated metrics
            trino_cpu_data = prom.get_metric_range_data(
                metric_name='node_cpu_seconds_total',
                label_config={"job": args.trino_prometheus_job, "mode": "idle"},
                start_time=then,
                end_time=now
            )
            cnode_cpu_data = prom.get_metric_range_data(
                metric_name='node_cpu_seconds_total',
                label_config={"job": args.cnode_prometheus_job, "mode": "idle"},
                start_time=then,
                end_time=now
            )
            trino_network_data_in = prom.get_metric_range_data(
                metric_name='node_netstat_IpExt_InOctets',
                label_config={"job": args.trino_prometheus_job},
                start_time=then,
                end_time=now
            )
            trino_network_data_out = prom.get_metric_range_data(
                metric_name='node_netstat_IpExt_OutOctets',
                label_config={"job": args.trino_prometheus_job},
                start_time=then,
                end_time=now
            )
            trino_disk_reads = prom.get_metric_range_data(
                metric_name='node_disk_read_bytes_total',
                label_config={"job": args.trino_prometheus_job},
                start_time=then,
                end_time=now
            )
            trino_disk_writes = prom.get_metric_range_data(
                metric_name='node_disk_written_bytes_total',
                label_config={"job": args.trino_prometheus_job},
                start_time=then,
                end_time=now
            )
            trino_cpus = prom_node_cpu_count(trino_cpu_data)
            cnode_cpus = prom_node_cpu_count(cnode_cpu_data)
            tnet_in = prom_node_net_rate(trino_network_data_in)
            tnet_out = prom_node_net_rate(trino_network_data_out)
            trino_disk_r = prom_node_disk_rate(trino_disk_reads)
            trino_disk_w = prom_node_disk_rate(trino_disk_writes)
            tnet_quiet_in = tnet_in - tnet_out

            ttl_cpus = trino_cpus+cnode_cpus
            trino_cluster_rate = 1 - prom_node_cpu_util_rate(trino_cpu_data, "idle")
            cnode_cluster_rate = 1 - prom_node_cpu_util_rate(cnode_cpu_data, "idle")

            timing = "{rowcount:03d}, {query}, {query_id}, {time}, {nodes}, {cpu}, {mem}, {rows}, {bytes}, {splits}, {t_cluster_util:.2f}, {v_cluster_util:.2f}, {agg_cpu_util:.2f}, {tnet_quiet_in:.2f}, {disk_r}, {disk_w}".format(
                rowcount=row_count,
                nodes=ed.stats["nodes"],
                splits=ed.stats["totalSplits"],
                time=(ed.stats["elapsedTimeMillis"]-ed.stats["queuedTimeMillis"]),
                cpu=ed.stats["cpuTimeMillis"],
                rows=ed.stats["processedRows"],
                bytes=ed.stats["processedBytes"],
                mem=ed.stats["peakMemoryBytes"],
                state=ed.stats["state"],
                query_id=ed.query_id,
                query=query,
                t_cluster_util=trino_cluster_rate,
                v_cluster_util=cnode_cluster_rate,
                agg_cpu_util=((trino_cluster_rate*trino_cpus) + (cnode_cluster_rate*cnode_cpus))/(ttl_cpus if ttl_cpus > 0 else 1),
                tnet_quiet_in=tnet_quiet_in,
                disk_r=trino_disk_r,
                disk_w=trino_disk_w)
            
            th.write(timing+"\n")
            th.flush()
            print(timing)

            with open("{outdir}/stats_{query}.json".format(outdir=outdir, query=query), "w") as fh:
                fh.write(json.dumps(ed.stats))
                fh.close()

            with open("{outdir}/output_{query}.txt".format(outdir=outdir, query=query), "w") as fh:
                for row in rows: fh.write(",".join(row)+"\n")
                fh.close()
                
            with open("{outdir}/node_series_{query}.json".format(outdir=outdir, query=query), "w") as fh:
                for row in rows: fh.write(json.dumps({
                    "trino_cpus": trino_cpu_data,
                    "cnode_cpus": cnode_cpu_data,
                    "tnet_in": trino_network_data_in,
                    "tnet_out": trino_network_data_out,
                    "trino_disk_r": trino_disk_reads,
                    "trino_disk_w": trino_disk_writes
                }))
                fh.close()

        th.close()

    benchmark_end_time = datetime.datetime.now()
    elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
    print("elapsed: {}s".format(str(elapsed_benchmark_time.seconds)))
    
    dump_stats(prom, benchmark_start_time, benchmark_end_time, outdir)
    print("done")

