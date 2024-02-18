from prometheus_api_client import PrometheusConnect
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from trino.transaction import IsolationLevel
from urllib.parse import urlparse
from benchgo.tpcds_sf10000_queries import *
from benchgo.tpcds_sf1000_queries import *
from benchgo.prometheus_handlers import *
from benchgo.tpcds_selective_queries import *
from benchgo.transaction_tables import sf_cols
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
SIDE_TABLE_DDL="""CREATE TABLE {table} (id BIGINT)"""

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

    # Output file
    outdir = "/tmp/{}_{}".format(args.name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)
    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as fh:
        if args.benchmark == "tpcds" or args.benchmark == "tpcds_s":
            tpcds(args, fh)
        elif args.benchmark == "update/delete":
            update_delete(args, fh)


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
 

def update_delete(args, th):

    tc = connection(args)

    sys.stdout.write("get col width factor....")
    res = tc.execute("SELECT * FROM {table} LIMIT 1".format(table=args.update_del_table))
    sf = tc.fetchall()
    if len(sf[0]) > 10000:
        width_factor = 1000
    elif len(sf[0]) > 1000:
        width_factor = 100
    elif len(sf[0]) > 100:
        width_factor = 10
    elif len(sf[0]) > 10:
        width_factor = 1
    else:
        width_factor = -1
    print("({})".format(width_factor))
    col_counts = sf_cols(width_factor)

    sys.stdout.write("get row count factor...")
    sys.stdout.flush()
    res = tc.execute("SELECT MIN(id) AS min, MAX(id) AS max FROM {table}".format(table=args.update_del_table))
    src_tbl_rows = tc.fetchall()
    print("({})".format(int(src_tbl_rows[0][1])-int(src_tbl_rows[0][0])+1))
    ids = []

    sys.stdout.write("select ids...")
    sys.stdout.flush()
    for x in range(0,60000):
        while True:
            nn = random.randrange(src_tbl_rows[0][0],src_tbl_rows[0][1])
            if nn not in ids:
                ids.append(nn)
                break
    print("{} unique ids".format(len(ids)))
    
    ###########################################################################
    # Updates/deletes
    op_list = [100, 1000, 10000]
    results = {}
    record_ptr = 0
    for action in ["update", "delete"]:
        th.write("{} timings\n".format(action))
        results[action] = {}
        for batch_sz in op_list:
            th.write("record count,{}\n".format(UPDATE_TEST_COUNT))
            th.write("batch size,{}\n".format(batch_sz))

            start = time.time()
            print("{} {} records in batches of {}".format(action, UPDATE_TEST_COUNT, batch_sz))
            tracking_list = []
            for count,row in enumerate(range(0,UPDATE_TEST_COUNT, batch_sz)):
                if action == "update":
                    updates = []
                    
                    strids = [x for x in range(0, col_counts.STR_COLS)]
                    floadids = [x for x in range(0, col_counts.FLOAT_COLS)]
                    intids = [x for x in range(0, col_counts.INT_COLS)]
                    boolids = [x for x in range(0, col_counts.BOOL_COLS)]
                    random.shuffle(strids)
                    random.shuffle(floadids)
                    random.shuffle(intids)
                    random.shuffle(boolids)

                    for s in range(0, int(col_counts.STR_COLS*0.25)+1):
                        updates.append("str_val_{}='{}'".format(
                            strids[s],
                            "{}".format(''.join(random.choices(string.ascii_letters, k=random.randrange(1,128))))))
                    for s in range(0, int(col_counts.FLOAT_COLS*0.25)+1):
                        updates.append("float_val_{}={}".format(
                            floadids[s],
                            random.random()*10000))
                    for s in range(0, int(col_counts.INT_COLS*0.25)+1):
                        updates.append("int_val_{}={}".format(
                            intids[s],
                            int(random.random()*(2**31)) if random.random() < 0.5 else -abs(int(random.random()*(2**31)))))
                    for s in range(0, int(col_counts.FLOAT_COLS*0.25)+1):
                        updates.append("bool_val_{}={}".format(
                            boolids[s],
                            "true" if random.random() < 0.5 else "false"))
                    
                    query = "UPDATE {table} SET {updates} WHERE ".format(
                        table=args.update_del_table,
                        updates=",".join(updates)
                    )

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
                    tracking_list.append("{:.2f}".format(elapsed))
                    sys.stdout.write("\rbatch {}: {:.2f} sec       ".format(count+1, elapsed))
                    sys.stdout.flush()
                except trino.exceptions.TrinoUserError as e:
                    sys.stdout.write("\rprobem with query: {}\n".format(e))

            th.write("discrete {} timings,".format(action))
            th.write("{}\n".format(",".join(tracking_list)))

            print("\r{:.2f} seconds to {} {} records in batches of {}".format(time.time()-start, action, UPDATE_TEST_COUNT, batch_sz))

    th.write("overall timings by batch size\n")
    print("operation,{}".format(",".join([str(x) for x in op_list])))
    for r in results:
        th.write("{},".format(r))
        cols = []
        for c in results[r]:
            cols.append("{:2f}".format(results[r][c]))
        th.write("{}\n".format(",".join(cols)))

    ###########################################################################
    # Merge tests
    sys.stdout.write("running merge delete tests....\nget 5 and 25 percentiles...")
    tc.execute("SELECT COUNT(*) FROM {}".format(args.update_del_table))
    rows = tc.fetchall()
    p5 = int(rows[0][0]*0.05)
    p25 = int(rows[0][0]*0.25)

    print("done ({} and {})".format(p5, p25))
    th.write("\nmerge tests:\n")
    th.write("5th percentile record count, {}\n".format(p5))
    th.write("25th percentile record count, {}\n".format(p25))
    th.write("load and merge timings:\n")
    th.write("action,percentile,timing\n")
    # Create side-tables
    plist = {"p5": p5, "p25": p25}
    for per in plist:

        sys.stdout.write("merge benchmark, {} records...".format(plist[per]))
        sys.stdout.flush()
        side_table = '{catalog}.{schema}.{table}'.format(
            catalog=args.trino_catalog,
            schema=args.trino_schema,
            table="side_{}".format(plist[per])
        )
        tc.execute(SIDE_TABLE_DDL.format(table=side_table))
        sidepopq = "INSERT INTO {tgt_table} SELECT id FROM {src_table} ORDER BY RANDOM() LIMIT {numrows}".format(
            tgt_table=side_table,
            src_table=args.update_del_table,
            numrows=plist[per]
        )

        # timing #
        loadstart = time.time()
        tc.execute(sidepopq)
        tc.fetchall()
        loadend = time.time()
        # end timing #

        print("\nload time ({}): {:.2f}".format(per, loadend-loadstart))
        th.write("load,{},{:.2f}\n".format(per, loadend-loadstart))

        merge_q = "MERGE INTO {tgt_table} USING {src_table} ON {tgt_table}.id = {src_table}.id WHEN MATCHED THEN DELETE".format(
            tgt_table=args.update_del_table,
            src_table=side_table,
        )

        # timing #
        merge_start = time.time()
        tc.execute(merge_q)
        tc.fetchall()
        merge_end = time.time()
        # end timing #

        print("merge time ({}): {:.2f}".format(per, merge_end-merge_start))
        th.write("merge,{},{:.2f}\n".format(per, loadend-loadstart))

        tc.execute("DROP TABLE {}".format(side_table))


    if args.merge_from_table:
        th.write("Update merge test\n")

        query = "SELECT COUNT(*) FROM {source}".format(source=args.merge_from_table)
        tc.execute(query)
        res = tc.fetchall()
        print("merging {} records into {} from {}".format(res[0][0], args.update_del_table, args.merge_from_table))

        query = "MERGE INTO {tgt_table} AS a USING {src_table} AS b ON a.id=b.id WHEN MATCHED THEN UPDATE SET {vals}".format(
                tgt_table=args.update_del_table,
                src_table=args.merge_from_table,
                vals=",".join(["str_val_{x}=b.str_val_{x}".format(x=x) for x in range(0,col_counts.STR_COLS)])+","+
                    ",".join(["float_val_{x}=b.float_val_{x}".format(x=x) for x in range(0,col_counts.FLOAT_COLS)])+","+
                    ",".join(["int_val_{x}=b.int_val_{x}".format(x=x) for x in range(0,col_counts.INT_COLS)])+","+
                    ",".join(["bool_val_{x}=b.bool_val_{x}".format(x=x) for x in range(0,col_counts.BOOL_COLS)])
            )

        merge_start = time.time()
        tc.execute(query)
        tc.fetchall()
        merge_end = time.time()
        print("merge time: {:.2f}s".format(merge_end-merge_start))
        th.write("merge records,{}\n".format(res[0][0]))
        th.write("merge time,{:.2f}\n".format(merge_end-merge_start))

    else:
        print("no source table for UPDATE merge test specified, skipping")

def tpcds(args, th):
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

    benchmark_end_time = datetime.datetime.now()
    elapsed_benchmark_time = benchmark_end_time - benchmark_start_time
    print("elapsed: {}s (NOT a performance timing metric)".format(str(elapsed_benchmark_time.seconds)))
    
    dump_stats(prom, benchmark_start_time, benchmark_end_time, outdir)
    print("done")

