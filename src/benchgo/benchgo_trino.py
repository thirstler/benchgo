from prometheus_api_client import PrometheusConnect
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from urllib.parse import urlparse
from benchgo.tpcds_sf10000_queries import *
from benchgo.tpcds_sf1000_queries import *
from benchgo.prometheus_handlers import *
import os
import datetime
import json
import time

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
        header = "row, query, id, time, nodes, cpu, mem, rows, bytes, splits, exec cluster util, cnode cluster util, all cpu util, exec ingress, disk_r, disk_w"
        th.write(header+"\n")
        print(header)
        for query in queries:
            row_count += 1
            try:
                then = datetime.datetime.now(tz=datetime.timezone.utc)
                ed = tc.execute("EXPLAIN ANALYZE {query}".format(query=queries[query]))
                rows = tc.fetchall()
                now = datetime.datetime.now(tz=datetime.timezone.utc)
            except Exception as e:
                th.write("{row},{query},{e},,,,,,,,,\n".format(row=row_count, query=query,e=e))
                th.flush()
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

            timing = "{rowcount:03d}, {query}, {query_id}, {time}, {nodes}, {cpu}, {mem}, {rows}, {bytes}, {splits}, {t_cluster_util}, {v_cluster_util}, {agg_cpu_util}, {tnet_quiet_in:.2f}, {disk_r}, {disk_w}".format(
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
                t_cluster_util="{:.2f}".format(trino_cluster_rate) if trino_cluster_rate <= 1 else "",
                v_cluster_util="{:.2f}".format(cnode_cluster_rate) if cnode_cluster_rate <= 1 else "",
                agg_cpu_util="{:.2f}".format(((trino_cluster_rate*trino_cpus) + (cnode_cluster_rate*cnode_cpus))/(ttl_cpus if ttl_cpus > 0 else 1)) if (trino_cluster_rate <=1 and cnode_cluster_rate <= 1) else "",
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
                fh.write(json.dumps({
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

