import datetime, sys, os, math, time, getpass, platform, pandas as pd
from pathlib import Path
from benchgo.transaction_tables import *
from multiprocessing import Process, Array
import pyarrow
from vastdb.api import VastdbApi
import configparser

def stage_block(stage, stage_gate):

    while True:
        time.sleep(0.001)
        for c in stage_gate:
            if c == stage: continue
        break


def get_creds(args):

    if args.aws_profile:
        config = configparser.ConfigParser()
        config.read(Path.home() / ".aws" / "credentials")
        access_key = config[args.aws_profile]['aws_access_key_id']
        secret_key = config[args.aws_profile]['aws_secret_access_key']
    else:
        access_key = args.access_key
        secret_key = args.secret_key

    return access_key, secret_key

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "{:.2f} {}".format(s, size_name[i])

def _insert_proc(p_id, stage_gate, results, start, end, width_factor, sparsity, creds, endpoints, table_target, batch_size, streaming):
    
    # P0 will write to console
    writer = True if p_id[0] == 0 else False

    # Start VDB session for this process
    vastdb_session = VastdbApi(host=endpoints, access_key=creds[0], secret_key=creds[1])

    end = (math.ceil((end-start)/batch_size)*batch_size)+start
    share = end - start
    if writer:
        sys.stdout.write("generating random data...")
        sys.stdout.flush()

    data = []
    records = 0
    bytes = 0
    schema = [x[0] for x in mk_schema(width_factor)]
    pdframes = []
    for id in range(start, end):
        row = mk_row(id, width_factor, sparsity)
        bytes += sys.getsizeof(row)
        data.append(row)
        records += 1

        if records % 1000 == 0 and writer:
            sys.stdout.write("\rgenerating random data...({:.1f}%) ({:<8})".format( (records/share)*100, convert_size(bytes*p_id[1])))
            sys.stdout.flush()

        if records % 10000 == 0 or id == (end-1):
            pdframes.append(pd.DataFrame(data, columns=schema))
            data.clear()

    pdframes = pd.concat(pdframes)

    # Store it
    #dataframe.to_parquet('/tmphx-70/file_share/jason/wiki/{}_out'.format(p_id[0]), compression='gzip')
    
    # Done with stage 0, advance to 1
    stage_gate[p_id[0]] = 1

    # synch with everyone else
    stage_block(0, stage_gate)

    if writer:
        print("\n{} records generated".format(records*p_id[1]))
        sys.stdout.write("writing data...")
        sys.stdout.flush()

    # insert with controlled batch size
    start = time.time()
    if streaming:
        for o in range(0, share, batch_size):
            vastdb_session.insert(table_target[0],
                                    table_target[1],
                                    table_target[2],
                                    pdframes[o:o+batch_size].to_dict(orient='list'))
    else:
        vastdb_session.insert(table_target[0],
                                table_target[1],
                                table_target[2],
                                pdframes.to_dict(orient='list'))
        
    elapsed = time.time()-start

    if writer:
        print("done")

    results[p_id[0]] = records/elapsed

    
def prep_environment(args):

    access_key, secret_key = get_creds(args)

    if args.vdb_table == None:
        user = getpass.getuser()
        host = platform.node()
        table = 'benchgo_{}_{}'.format(user, host)
    else:
        table = args.vdb_table

    vastdb_session = VastdbApi(host=args.vast_endpoints, access_key=access_key, secret_key=secret_key)

    tables = vastdb_session.list_tables(args.vdb_database, args.vdb_schema)

    for t in tables[2]:
        if table == t.name:
            if args.force:
                vastdb_session.drop_table(args.vdb_database, args.vdb_schema, table)
                print("dropped table {}.{}.{}".format(args.vdb_database, args.vdb_schema, table))
            else:
                print("table exists ({}.{}.{}), exiting".format(args.vdb_database, args.vdb_schema, table))
                exit()
    
    schema = pyarrow_schema(int(args.width_factor))
    vastdb_session.create_table(args.vdb_database, args.vdb_schema, table, arrow_schema=schema)
    print("created table {}.{}.{}".format(args.vdb_database, args.vdb_schema, table))


    return (args.vdb_database, args.vdb_schema, table)


def multi_insert(args):

    access_key, secret_key = get_creds(args)
    endpoints = args.vast_endpoints
    total_rows = int(args.insert_rows)
    procs =  int(args.agent_procs)
    rows_per_proc = math.ceil(total_rows/procs)
    stage_gate = Array("i", procs)
    results = Array("d", procs)
    dg_proc = []
    offset = 0

    if args.reuse:
        table_target = (args.vdb_database, args.vdb_schema, args.vdb_table)
        print("using existing table (assuming you know what you're doing)")
    else:
        table_target = prep_environment(args)

    for p in range(0, procs):
        dg_proc.append(Process(target=_insert_proc, args=((p,procs-1),
                                                          stage_gate,
                                                          results,
                                                          offset,
                                                          offset+rows_per_proc,
                                                          int(args.width_factor),
                                                          float(args.sparsity),
                                                          (access_key,secret_key),
                                                          endpoints,
                                                          table_target,
                                                          int(args.insert_batch),
                                                          args.streaming)))
        offset += rows_per_proc
        dg_proc[-1].start()

    for p in range(0, procs):
        dg_proc[p].join()
    
    print("{:.2f} rec/s".format(sum([x for x in results])))

def run_inserts(args, fh):

    multi_insert(args)



def run_updates(args, fh):
    pass

def run_deletes(args, fh):
    pass


def run_sdk(args):

    # Output file
    outdir = "/tmp/{}_{}".format(args.name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(outdir)
    with open("{outdir}/timings.csv".format(outdir=outdir), "w") as fh:
        if args.benchmark == "insert":
            run_inserts(args, fh)
        elif args.benchmark == "update/delete":
            run_updates(args, fh)
            run_deletes(args, fh)
        else:
            print("did you select a benchmark? ({} selected)".format(args.benchmark ))
        fh.close()