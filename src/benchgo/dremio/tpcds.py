import argparse, json, datetime, time, requests, sys
from benchgo.queries.trino import *
from benchgo.util import prometheus_args, tpcds_args, global_args
from benchgo.dremio.util import dremio_args
from benchgo.tpcds import TPCDS
from pyarrow import flight
from pyarrow.flight import FlightClient


class DremioTPCDS(TPCDS):
    
    # Declaratons
    output_dir = None
    result_log_fh = None
    prometheus = None

    def __init__(self) -> None:
        parser = argparse.ArgumentParser(
            description="Run a TPC-DS Benchmark for Dremio"
        )
        parser.add_argument("dremio")
        parser.add_argument("tpcds")
        global_args(parser)
        tpcds_args(parser)
        dremio_args(parser)
        prometheus_args(parser)
        self.args = parser.parse_args()
        self.engine = "dremio"
    

    def run(self):

        if not self.args.skip_precheck:
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.args.analyze_tables:
            self.analyze_tables()


        self.logging_setup()
        self.prometheus_connect()

        

        if self.args.step_query:
            print("running TCP-DS queries in order")
            print("scale factor: {}".format(self.args.scale_factor))
            print()
            self.step_benchmark()
        else:
            print("running TCP-DS benchmark")
            print("trino path:   {}.{}".format(self.args.catalog, self.args.schema))
            print("scale factor: {}".format(self.args.scale_factor))
            print("concurrenty:  {}".format(self.args.concurrency))
            print()

            self.benchmark(self._benchmark_thread)

            print("(Trino reports {:.2f} seconds aggregate query execution time)".format(max([sum(x["trino_timings"]) for x in self.thread_data])))

        print("output in {}/result_log.csv".format(self.output_dir))

    def _benchmark_thread(self, id, queries):
        '''
        Actual executing and timing collection for this engine (Trino)
        '''
        tc = connection(self.args)
        self.thread_data[id]['started'] = datetime.datetime.now()
        self.thread_data[id]['trino_timings'] = []
        for q, query in enumerate(queries):
            self.thread_data[id]['query_count'] = q
            self.thread_data[id]['current_query'] = query
            result = tc.execute(query)

            # Things that happen after this point skew wll timings so don't
            # add anything that takes much time. Also, this needs to run 
            # near-line to the engine to avoid results movement to skew things
            getme = result.fetchall()
            self.thread_data[id]['trino_timings'].append(((result.stats["elapsedTimeMillis"]-result.stats["queuedTimeMillis"]))/1000)


        self.thread_data[id]['finished'] = datetime.datetime.now()



    def tablecheck(self) -> bool:
        '''
        Makes sure the table row counts look correct and return false if they are
        not.
        '''
        row_info = tpcds_table_row_counts[self.args.scale_factor]
        check = True
        tc = connection(self.args)
        print("checking tables:")
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "SELECT COUNT(*) FROM {}.\"{}\".{}".format(self.args.catalog, self.args.schema, table)
            tc.execute(query)
            rows = tc.fetchall()

            if int(rows[0][0] == row_info[table]):
                print("ok")
            else:
                print(" NOK {}/{}".format(rows[0][0], row_info[table]))
                check = False
        
        return check
    
    def analyze_tables(self) -> None:
        '''
        Analyze tables
        '''
        # just get the list of tables from somewhere:
        row_info = tpcds_table_row_counts[self.args.scale_factor]

        tc = connection(self.args)
        print("analyzing tables:")
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "ANALYZE {}.\"{}\".{}".format(self.args.catalog, self.args.schema, table)
            tc.execute(query)
            rows = tc.fetchall()
            print("done")
        

