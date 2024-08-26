from benchgo.filter import Filter
from benchgo.util import prometheus_args, global_args, filter_args
from benchgo.trino.util import trino_args
import benchgo.trino.util as trino_util
from benchgo.data import TransactionTblSchema
from benchgo.prometheus_handler import PrometheusHandler
from datetime import datetime
import argparse, sys, time

class TrinoFilter(Filter):

    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Run a filter-throughput test. Exercises scan/filter/project operations"
        )
        parser.add_argument("trino")
        parser.add_argument("filter")
        global_args(parser)
        filter_args(parser)
        trino_args(parser)
        prometheus_args(parser)
        self.args = parser.parse_args()
        self.engine = "trino"
        self.trino_conn = trino_util.connection(self.args)
        self.schema = TransactionTblSchema(int(self.args.width_factor))
        self.prometheus_handler = PrometheusHandler(
            prometheus_host=self.args.prometheus_host,
            exec_job=self.args.exec_prometheus_job,
            cnode_job=self.args.cnode_prometheus_job)
    
    def tablecheck(self) -> bool:

        query = 'SELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'.format(
            catalog=self.args.catalog,
            schema=self.args.schema,
            table=self.args.target_table)
        
        res = self.trino_conn.execute(query)
        data = res.fetchall()
        if int(data[0][0]/1000000) != int(self.args.row_factor):
            print("failed row count tests (got {}, expected {})".format(data[0][0], int(self.args.row_factor)*1000000))
            return False
        
        query = 'DESCRIBE "{catalog}"."{schema}"."{table}"'.format(
            catalog=self.args.catalog,
            schema=self.args.schema,
            table=self.args.target_table)
        
        res = self.trino_conn.execute(query)
        data = res.fetchall()
        if len(data) != self.schema.col_count:
            print("failed column count tests (got {}, expected {})".format(len(data), self.schema.col_count))
            return False

        
        return True
        
    def analyze_table(self) -> bool:
        print('analyzing table "{catalog}"."{schema}"."{table}"...'.format(catalog=self.args.catalog,
            schema=self.args.schema,
            table=self.args.target_table), end='', flush=True)
        
        query = 'ANALYZE "{catalog}"."{schema}"."{table}"'.format(
            catalog=self.args.catalog,
            schema=self.args.schema,
            table=self.args.target_table)
        self.trino_conn.execute(query)
        print("done")

    def header(self):
        print("\njob, current_time, timing, rows_returned, exec_cluster_util, storage_cluster_util, aggregate_util, network_in, network_out, disk_read, disk_write")

    
    def select_numeric(self, ratio:float=1.0, bitwidth:int=32, select_col:str="int_val_0", use_sql=False, sel_float=False):

        # get a slice of the int space
        slice_start, slice_end = self.get_rnd_ks_slice(ratio=ratio, bitwidth=bitwidth, ret_float=sel_float)
        
        rc = 0

        query = 'SELECT {fields} FROM "{catalog}"."{schema}"."{table}" WHERE {sel_col} BETWEEN {start} AND {end}'.format(
                    fields=",".join([f"COUNT({x})" for x in self.schema.field_list]),
                    catalog=self.args.catalog,
                    schema=self.args.schema,
                    table=self.args.target_table,
                    sel_col=select_col,
                    start=slice_start,
                    end=slice_end
                )

        s_ts = time.time()
        res = self.trino_conn.execute(query)
        data = res.fetchall() # execution time include collection
        e_ts = time.time()
        rc = data[0][0]

        return (e_ts-s_ts), rc


    def run(self):

        if not self.args.skip_precheck:
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.args.analyze_table:
            self.analyze_table()

        self.header()

        int_test_ratios = [0.00001, 0.0001, 0.001, 0.01, 0.1]
        #int_test_ratios = [1.0]
        self.print_select_numeric(int_test_ratios, 64, "bigint_val_0")
        #self.print_select_numeric(int_test_ratios, 32, "int_val_0")
        #self.print_get_by_substr(["abcdef", "abcde", "abcd", "abc", "ab"], "record_id")
        #self.print_select_numeric(int_test_ratios, 64, "double_val_0", sel_float=True)
        #self.print_select_numeric(int_test_ratios, 32, "float_val_0", sel_float=True)
        #self.print_select_by_words(self.get_words(1), "str_val_0")
        #self.print_select_by_words(self.get_words(2), "str_val_0")
        #self.print_select_by_words(self.get_words(4), "str_val_0")