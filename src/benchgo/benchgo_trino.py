from prometheus_api_client import PrometheusConnect
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from trino.transaction import IsolationLevel
from urllib.parse import urlparse
from benchgo.tpcds_sf10000_queries import *
from benchgo.tpcds_sf1000_queries import *
from benchgo.prometheus_handlers import *
from benchgo.tpcds_selective_queries import *
from multiprocessing import Process
import os
import datetime
import json
import time
import requests
import random
import string
import sys
import trino

# Configuration
TRINO_SESSION={
    "task_writer_count": 64
}
# Base CPU utilization from VAST cnodes
CNODE_BASE_CPU_UTIL=0.36
UPDATE_TEST_COUNT=10000

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
        session_properties=TRINO_SESSION,
        isolation_level=IsolationLevel.AUTOCOMMIT
    )
    cur = conn.cursor()
    return cur

def run(args):

    if args.benchmark == "tpcds" or args.benchmark == "tpcds_s":
        tpcds(args)
    elif args.benchmark == "update/delete":
        update_delete(args)


def _update_slice(args, slice, offset, count, randkey, concurrancy):
    query_tmplt = "UPDATE {table} SET record_id='{record_id}' WHERE "
    qh = []
    conn = connection(args) 
    if UPDATE_TEST_COUNT % concurrancy != 0:
        print("concurrancy/update_count has a remainder, this won't work like you want")
    for n in range(0, count, concurrancy):
        match = []
        for c in range(0, concurrancy):
            match.append("id={id}".format(id=random.randrange(slice, slice+offset)))
        

        query = query_tmplt.format(table=args.update_del_table, record_id=randkey) + " OR ".join(match)
        qh.append(
            conn.execute(query)
        )
        sys.stdout.write(".")
        sys.stdout.flush()
    for h in qh:
        res = h.fetchall()
        sys.stdout.write("~")
        sys.stdout.flush()

    conn.close()
 

def update_delete(args):

    tc = connection(args)

    sys.stdout.write("determine ID range...")
    sys.stdout.flush()
    res = tc.execute("SELECT MIN(id) AS min, MAX(id) AS max FROM {table}".format(table=args.update_del_table))
    src_tbl_rows = tc.fetchall()
    print("done ({})".format(int(src_tbl_rows[0][1])-int(src_tbl_rows[0][0])))
    ids = []
    for x in range(0,60000):
        ids.append(random.randrange(src_tbl_rows[0][0],src_tbl_rows[0][1]))

    results = {}
    ##
    # Updates/deletes
    record_ptr = 0
    for action in ["update", "delete"]:
        results[action] = {}
        for batch_sz in [10, 100, 1000, 10000]:
            start = time.time()
            print("{} {} records in batches of {}".format(action, UPDATE_TEST_COUNT, batch_sz))
            for count,row in enumerate(range(0,UPDATE_TEST_COUNT, batch_sz)):
                if action == "update":
                    query = "UPDATE {table} SET str_val_0='tag! you are it!',float_val_0=3.14159265359,bool_val_0=true,int_val_0=42 WHERE ".format(table=args.update_del_table)
                else:
                    query = "DELETE FROM {table} WHERE ".format(table=args.update_del_table)
                predicates = []
                for b in range(0, batch_sz):
                    predicates.append("id = {}".format(ids[record_ptr]))
                    record_ptr += 1
                query += " OR ".join(predicates)
                batch_start = time.time()
                try:
                    tc.execute(query)
                    tc.fetchall()
                    elapsed = time.time()-batch_start
                    results[action][str(batch_sz)] = elapsed
                    sys.stdout.write("\rbatch {}: {:.2f} sec       ".format(count+1, elapsed))
                except trino.exceptions.TrinoUserError as e:
                    sys.stdout.write("\rprobem with query: {}".format(e))

            print("")
            print("{:.2f} seconds to {} {} records in batches of {}".format(time.time()-start, action, UPDATE_TEST_COUNT, batch_sz))

    sys.stdout.write("timings by batch size:\n")
    sys.stdout.write("----------------------\n")
    sys.stdout.write("operation,10,100,1000,10,000\n")
    for r in results:
        sys.stdout.write("{},".format(r))
        cols = []
        for c in results[r]:
            cols.append(c)
        sys.stdout.write(",".join(cols))



def tpcds(args):
    queries = []
    if args.benchmark == "tpcds":
        if args.tpcds_scale == "sf10000":
            queries = tpcds_10t_queries.queries
        elif args.tpcds_scale == "sf1000":
            queries = tpcds_1t_queries.queries
    elif args.benchmark == "tpcds_s":
        sq = tpcds_selective_queries()
        queries = sq.gen_all(args.tpcds_scale)
    
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
            try:
                trino_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": args.trino_prometheus_job, "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                trino_cpu_data = None
                sys.stderr.write(str(e))

            try:
                cnode_cpu_data = prom.get_metric_range_data(
                    metric_name='node_cpu_seconds_total',
                    label_config={"job": args.cnode_prometheus_job, "mode": "idle"},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                cnode_cpu_data = None
                sys.stderr.write(str(e))

            try:
                trino_network_data_in = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_InOctets',
                    label_config={"job": args.trino_prometheus_job},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                trino_network_data_in = None
                sys.stderr.write(str(e))

            try:
                trino_network_data_out = prom.get_metric_range_data(
                    metric_name='node_netstat_IpExt_OutOctets',
                    label_config={"job": args.trino_prometheus_job},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                trino_network_data_out = None
                sys.stderr.write(str(e))

            try:
                trino_disk_reads = prom.get_metric_range_data(
                    metric_name='node_disk_read_bytes_total',
                    label_config={"job": args.trino_prometheus_job},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                trino_disk_reads = None
                sys.stderr.write(str(e))

            try:
                trino_disk_writes = prom.get_metric_range_data(
                    metric_name='node_disk_written_bytes_total',
                    label_config={"job": args.trino_prometheus_job},
                    start_time=then,
                    end_time=now
                )
            except Exception as e:
                trino_disk_writes = None
                sys.stderr.write(str(e))


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
                t_cluster_util="{:.2f}".format(trino_cluster_rate) if trino_cluster_rate <= 1 else "",
                v_cluster_util="{:.2f}".format(cnode_cluster_rate) if cnode_cluster_rate <= 1 else "",
                agg_cpu_util="{:.2f}".format(((trino_cluster_rate*trino_cpus) + (cnode_cluster_rate*cnode_cpus))/(ttl_cpus if ttl_cpus > 0 else 1)) if (trino_cluster_rate <=1 and cnode_cluster_rate <= 1) else "",
                tnet_quiet_in=tnet_quiet_in,
                disk_r=trino_disk_r,
                disk_w=trino_disk_w)
            
            th.write(timing+"\n")
            th.flush()
            print(timing)

            req_session = requests.Session()
            req_session.auth = ("admin", "")
            response = req_session.get('{trino_coordinator}/v1/query/{query_id}'.format(
                                            trino_coordinator=args.trino_coordinator,
                                            query_id=ed.query_id))

            with open("{outdir}/info_{query}.json".format(outdir=outdir, query=query), "w") as fh:
                fh.write(response.text)
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
    print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
    
    dump_stats(prom, benchmark_start_time, benchmark_end_time, outdir)
    print("done")

