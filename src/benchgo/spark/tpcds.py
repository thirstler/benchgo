import sys, datetime, time
from benchgo.tpcds import TPCDS
from benchgo.util import global_args, tpcds_args, prometheus_args, vastdb_api_args
from benchgo.spark.util import spark_args, spcfg, config_connect
from benchgo.queries.tpcds import tpcds_table_row_counts

class SparkSQLTPCDS(TPCDS):

    def __init__(self) -> None:
        self.cfg = spcfg()
        self.engine = 'spark'
    
    def run(self):
        
        if self.cfg.get("tpcds.tablecheck"):
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.cfg.get("tpcds.analyze_tables"):
            print("== analyzing tables ==")
            self.analyze_tables()


        self.logging_setup()
        #if not self.prometheus_connect():
        #    print("(no prometheus host specified, skipping stats gathering)")

        print("running TCP-DS benchmark")
        print("db path:      {}".format(self.cfg.get('tpcds.database_path')))
        print("scale factor: {}".format(self.cfg.get('tpcds.scale_factor')))
        print("concurrency:  {}".format(self.cfg.get('tpcds.concurrency')))
        print()

        self.benchmark(self._benchmark_proc)
        #self.config_connect()

        #print("output in {}/result_log.csv".format(self.output_dir))

        return True
    
    def _benchmark_proc(self, id, queries, started, finished, query_count, current_query):

        spark = config_connect(self.cfg, "{}_{}".format(self.cfg.get('job.app_name'), id))
        spark.sql('USE {}'.format(self.cfg.get('tpcds.database_path')))

        started.value = time.time()
        for q, query in enumerate(queries):
            query_count.value = q
            #current_query.value = query
            result = spark.sql(query)
            data = result.collect()

        finished.value = time.time()



    def tablecheck(self) -> bool:
        '''
        Makes sure the table row counts look correct and return false if they are
        not.
        '''
        spark = config_connect(self.cfg, 'table_check')

        row_info = tpcds_table_row_counts[self.cfg.get('tpcds.scale_factor')]
        spark.sql("USE {db_path}".format(db_path=self.cfg.get('tpcds.database_path')))

        check = True
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "SELECT COUNT(*) AS count FROM {}.{}".format(self.cfg.get('tpcds.database_path'), table)
            res = spark.sql(query)
            count = res.collect()[0]['count']
            
            if count == row_info[table]:
                print("ok")
            else:
                print(" NOK {}/{}".format(count, row_info[table]))
                check = False

        spark.stop()

        return check
    
    def analyze_tables(self) -> None:
        '''
        Analyze tables
        '''
        spark = config_connect(self.cfg, 'table_analysis')

        # just get the list of tables from somewhere:
        row_info = tpcds_table_row_counts[self.cfg.get('tpcds.scale_factor')]

        print("analyzing tables:")
        for table in row_info:
            sys.stdout.write("{}...".format(table))
            sys.stdout.flush()
            query = "ANALYZE TABLE {}.{} COMPUTE STATISTICS FOR ALL COLUMNS".format(self.cfg.get('tpcds.database_path'), table)
            res = spark.sql(query)
            collected = res.collect()
            print("ok")

        spark.stop()
        
