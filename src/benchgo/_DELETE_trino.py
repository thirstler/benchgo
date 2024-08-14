import os, datetime, json, time, requests, random, string, sys, datetime, argparse
from trino.dbapi import connect
from trino.transaction import IsolationLevel
from trino.auth import BasicAuthentication
from urllib.parse import urlparse
from benchgo.queries.trino.tpcds_sf10000_queries import *
from benchgo.queries.trino.tpcds_sf1000_queries import *
from benchgo.prometheus_handler import *
from benchgo.queries.trino.tpcds_selective_queries import *
from benchgo.data import sf_cols
from multiprocessing import Process


# Configuration
TRINO_SESSION={
    "task_writer_count": 64,
    "task_concurrency": 64
}
# Base CPU utilization from VAST cnodes
CNODE_BASE_CPU_UTIL=0.36
UPDATE_TEST_COUNT=10000
SIDE_TABLE_DDL="""CREATE TABLE {table} (id BIGINT)"""
MERGE_TEMPLATE="""MERGE INTO {tgt_table} AS a USING {src_table} AS b 
ON a.{key}=b.{key}
WHEN MATCHED
    THEN UPDATE SET {update_list}
WHEN NOT MATCHED
    THEN INSERT ({insert_field_list}) VALUES ({insert_list})"""

MERGE_TEMPLATE_NI="""MERGE INTO {tgt_table} AS a USING {src_table} AS b 
ON a.{key}=b.{key}
WHEN MATCHED
    THEN UPDATE SET {update_list}
"""
MERGE_TEMPLATE_NU="""MERGE INTO {tgt_table} AS a USING {src_table} AS b 
ON a.{key}=b.{key}
WHEN NOT MATCHED
    THEN INSERT ({insert_field_list}) VALUES ({insert_list})"""

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

def run_trino():

    parser = argparse.ArgumentParser(
        description="Trino workload generator"
    )
    parser.add_argument("trino", action="store_true")
    parser.add_argument("tpcds", action="store_true")
    parser.add_argument("tpch", action="store_true")
    parser.add_argument("transaction", action="store_true")
    parser.add_argument("throughput", action="store_true")

    parser.add_argument("--name", default="benchgo", help="name this run something - will show up in output directory")
    parser.add_argument("--benchmark", default=None, help="benchmark task to run (tpcds, insert, update/delete)")
    parser.add_argument("--trino-coordinator", help="Trino coordinator URI (e.g.: http://trino_coord:8080)")
    parser.add_argument("--trino-password", default=None, help="Trino password")
    parser.add_argument("--trino-user", default='admin', help="Trino username")
    parser.add_argument("--trino-catalog", help="catalog housing target tpcds database")
    parser.add_argument("--trino-schema", help="schema housing target tpcds database")

    # Prometheus options
    parser.add_argument("--prometheus-host", help="URL for prometheus host (e.g.: http://myhost:9090)")
    parser.add_argument("--trino-prometheus-job", default="trino_1", help="Prometheus job name for Trino cluster's node_exporter data")
    parser.add_argument("--cnode-prometheus-job", default="vast_cnodes", help="Prometheus job name for CNode cluster's node_exporter data")





    


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
    actions = ["update", "delete"]
    op_list = [100, 1000, 10000]
    results = {}
    for a in actions:
        results[a] = {}
        for o in op_list:
            results[a][str(o)] = 0

    record_ptr = 0
    for action in actions:
        th.write("{} timings\n".format(action))
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
                    for s in range(0, int(col_counts.BOOL_COLS*0.25)+1):
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
                
                try:
                    batch_start = time.time()
                    tc.execute(query)
                    tc.fetchall()
                    elapsed = time.time()-batch_start
                    results[action][str(batch_sz)] += elapsed
                    tracking_list.append("{:.2f}".format(elapsed))
                    sys.stdout.write("\rbatch {}: {:.2f} sec       ".format(count+1, elapsed))
                    sys.stdout.flush()
                except trino.exceptions.TrinoUserError as e:
                    sys.stdout.write("\rprobem with query: {}\n".format(e))

            th.write("discrete {} timings,".format(action))
            th.write("{}\n".format(",".join(tracking_list)))

            print("\r{:.2f} seconds to {} {} records in batches of {}".format(results[action][str(batch_sz)], action, UPDATE_TEST_COUNT, batch_sz))

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
    sys.stdout.write("running merge delete tests....\nget 5 and 25 rowcount percentiles...")
    tc.execute("SELECT COUNT(*) FROM {}".format(args.update_del_table))
    rows = tc.fetchall()
    p5 = int(rows[0][0]*0.05)
    p25 = int(rows[0][0]*0.25)

    print("done ({} and {})".format(p5, p25))
    th.write("\nmerge tests:\n")
    th.write("5th percentile record count, {}\n".format(p5))
    th.write("25th percentile record count, {}\n".format(p25))
    th.write("load and merge timings:\n")
    th.write("action/percentile,timing\n")
    # Create side-tables
    plist = {"p5": p5, "p25": p25}
    for per in plist:

        sys.stdout.write("merge-delete benchmark, {} records...".format(plist[per]))
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
        th.write("load {},{:.2f}\n".format(per, loadend-loadstart))

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
        th.write("merge {},{:.2f}\n".format(per, merge_end-merge_start))

        tc.execute("DROP TABLE {}".format(side_table))


    if args.merge_from_table:
        th.write("Update merge test\n")

        query = "SELECT COUNT(*) FROM {source}".format(source=args.merge_from_table)
        tc.execute(query)
        srcrecords = tc.fetchall()[0][0]
        print("merging {} records into {} from {}".format(srcrecords, args.update_del_table, args.merge_from_table))
        
        query = "SELECT COUNT(*) FROM {source}".format(source=args.update_del_table)
        before_rec = tc.execute(query).fetchall()[0][0]

        query = MERGE_TEMPLATE.format(
                key="id",
                tgt_table=args.update_del_table,
                src_table=args.merge_from_table,
                update_list=",".join(["str_val_{x}=b.str_val_{x}".format(x=x) for x in range(0,col_counts.STR_COLS)])+","+
                    ",".join(["float_val_{x}=b.float_val_{x}".format(x=x) for x in range(0,col_counts.FLOAT_COLS)])+","+
                    ",".join(["int_val_{x}=b.int_val_{x}".format(x=x) for x in range(0,col_counts.INT_COLS)])+","+
                    ",".join(["bool_val_{x}=b.bool_val_{x}".format(x=x) for x in range(0,col_counts.BOOL_COLS)]),
                insert_field_list=",".join(["str_val_{x}".format(x=x) for x in range(0,col_counts.STR_COLS)])+","+
                    ",".join(["float_val_{x}".format(x=x) for x in range(0,col_counts.FLOAT_COLS)])+","+
                    ",".join(["int_val_{x}".format(x=x) for x in range(0,col_counts.INT_COLS)])+","+
                    ",".join(["bool_val_{x}".format(x=x) for x in range(0,col_counts.BOOL_COLS)]),
                insert_list=",".join(["b.str_val_{x}".format(x=x) for x in range(0,col_counts.STR_COLS)])+","+
                    ",".join(["b.float_val_{x}".format(x=x) for x in range(0,col_counts.FLOAT_COLS)])+","+
                    ",".join(["b.int_val_{x}".format(x=x) for x in range(0,col_counts.INT_COLS)])+","+
                    ",".join(["b.bool_val_{x}".format(x=x) for x in range(0,col_counts.BOOL_COLS)])
            )

        try:
            merge_start = time.time()
            tc.execute(query)
            tc.fetchall()
            merge_end = time.time()

            query = "SELECT COUNT(*) FROM {source}".format(source=args.update_del_table)
            after_rec = tc.execute(query).fetchall()[0][0]

            print("merge time: {:.2f}s".format(merge_end-merge_start))
            th.write("merge record source rows,{}\n".format(srcrecords))
            th.write("destination size before,{}\n".format(before_rec))
            th.write("destination size after,{}\n".format(after_rec))
            th.write("increase,{:.2f}\n".format(after_rec-before_rec))
            th.write("merge time,{:.2f}\n".format(merge_end-merge_start))
        except:
            print("gah, fuck. merge failed. oh well...")
            th.write("merge record source rows,-1\n")
            th.write("destination size before,-1\n")
            th.write("destination size after,-1\n")
            th.write("increase,-1.0\n")
            th.write("merge time,-1.0\n")

    else:
        print("no source table for UPDATE merge test specified, skipping")




def merge_tables(args):

    tc = connection(args)

    # get source and dest schemas + sanity check
    try:
        src_schema = tc.execute("DESCRIBE {}".format(args.source_table)).fetchall()
    except Exception as e:
        print(e)
        print("\n\nmissing source table? ({})".format(args.source_table))
        exit(1)
        

    try:
        tgt_schema = tc.execute("DESCRIBE {}".format(args.dest_table)).fetchall()
    except Exception as e:
        print(e)
        print("\n\nmissing destination table? ({})".format(args.dest_table))
        exit(1)

    
    src_schema.sort()
    tgt_schema.sort()

    if src_schema != tgt_schema:
        print("source and target schema do not match:")
        print("-"*12 + "source" + "-"*12 +  "|" + "-"*9 + "destination" + "-"*10)
        for r in range(0, len(src_schema) if len(src_schema) > len(tgt_schema) else len(tgt_schema)):
            if len(src_schema) > r:
                sys.stdout.write("{:<30}".format("{}:{}".format(src_schema[r][0], src_schema[r][1])))
            else:
                sys.stdout.write("{:^30}".format("-"))
            sys.stdout.write("|")
            if len(tgt_schema) > r:
                sys.stdout.write("{:<30}\n".format("{}:{}".format(tgt_schema[r][0], tgt_schema[r][1])))
            else:
                sys.stdout.write("{:^30}\n".format("-"))
        sys.stdout.flush()

        if args.force:
            pass
        else:
            exit(1)
    
    if not args.noupdate and not args.noinsert:
        field_updates = ",".join(["{x}=b.{x}".format(x=x[0]) for x in src_schema if x != args.on_key])
        insert_field_list = ",".join(["{}".format(x[0]) for x in src_schema if x != args.on_key])
        insert_list = ",".join(["b.{}".format(x[0]) for x in src_schema if x != args.on_key])
        query = MERGE_TEMPLATE.format(
            tgt_table=args.dest_table,
            src_table=args.source_table,
            key=args.on_key,
            update_list=field_updates,
            insert_field_list=insert_field_list,
            insert_list=insert_list
        )
    
    if not args.noupdate and args.noinsert:
        field_updates = ",".join(["{x}=b.{x}".format(x=x[0]) for x in src_schema if x != args.on_key])
        query = MERGE_TEMPLATE_NI.format(
            tgt_table=args.dest_table,
            src_table=args.source_table,
            key=args.on_key,
            update_list=field_updates
        )
    
    if args.noupdate and not args.noinsert:
        insert_field_list = ",".join(["{}".format(x[0]) for x in src_schema if x != args.on_key])
        insert_list = ",".join(["b.{}".format(x[0]) for x in src_schema if x != args.on_key])
        query = MERGE_TEMPLATE_NU.format(
            tgt_table=args.dest_table,
            src_table=args.source_table,
            key=args.on_key,
            insert_field_list=insert_field_list,
            insert_list=insert_list
        )

        res = tc.execute(query)
        res.fetchall()
        print("done?")
            
    
