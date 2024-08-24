import time, sys, random
from benchgo.spark.util import config_connect, spcfg, config_block
from benchgo.data import TransactionTblSchema
from pyspark import StorageLevel
from datetime import datetime
from benchgo.filter import Filter
from benchgo.prometheus_handler import PrometheusHandler

class SparkFilter(Filter):


    def __init__(self):
        self.spark_cfg = spcfg()
        self.spark = config_connect(self.spark_cfg, "throughput_tests")
        self.verbose = False
        self.schema = TransactionTblSchema(self.spark_cfg.get("throughput.width_factor"))
        self.prometheus_handler = PrometheusHandler(
            prometheus_host=self.spark_cfg.get('prometheus.host'),
            exec_job=self.spark_cfg.get('exec_monitor.prometheus_job'),
            cnode_job=self.spark_cfg.get('cnode_monitor.prometheus_job'))


    def warm_up_and_check(self, df):

        print("checking row count...", end='')

        rows = df.count()
        if rows/1000000 == float(self.spark_cfg.get("throughput.row_factor")):
            print("OK ({} rows) ".format(rows))
        else:
            print("NOK ({} rows) ".format(rows))

        print("checking column count...", end='')
        col_count = len(df.columns)
        if col_count == self.schema.col_count:
            print("OK ({} cols) ".format(col_count))
        else:
            print("NOK ({} cols) ".format(col_count))

    def print_spark_config(self):
        spark_parameters = config_block(self.spark_cfg)
        print("\nSpark Configuration:")
        for param in spark_parameters:
            print("{:>50}:{:<40}".format(param[0], param[1]))
        print("")


    def header(self):

        print("\njob, current_time, timing, rows_returned, exec_cluster_util, storage_cluster_util, aggregate_util, network_in, network_out, disk_read, disk_write")


    def print_select_numeric(self, ratios:dict, bitwidth:int, field:str, sel_float=False):

        for slice in ratios:
            print("select_numeric({}, {}%),".format(field, slice*100), end="", flush=True)
            then = datetime.now()
            print("{},".format(then), end="", flush=True)

            # Do the thing!
            timing, row_count = self.select_numeric(ratio=slice, bitwidth=bitwidth, select_col=field, use_sql=False, sel_float=sel_float)

            print("{:.4f},".format(timing), end="")
            print("{},".format(row_count), end="")
            self.print_prometheus_stats(then, datetime.now())


    def print_get_by_substr(self, strings:dict, col:str):

        for string in strings:
            print("select_by_substr('{}'),".format(string), end="", flush=True)
            then = datetime.now()
            print("{},".format(then), end="", flush=True)

            # Do the thing!
            timing, row_count = self.select_by_substr(string, col, use_sql=True)

            print("{:.4f},".format(timing), end="")
            print("{},".format(row_count), end="")
            self.print_prometheus_stats(then, datetime.now())


    def print_prometheus_stats(self, then, now):

        self.prometheus_handler.gather(then, now)

        load_stats = "{t_cluster_util},{v_cluster_util},{agg_cpu_util},{tnet_quiet_in:.2f},{disk_r},{disk_w}".format(
            t_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.exec_cluster_rate) if self.prometheus_handler.collection_data.exec_cluster_rate <= 1 else "",
            v_cluster_util="{:.2f}".format(self.prometheus_handler.collection_data.cnode_cluster_rate) if self.prometheus_handler.collection_data.cnode_cluster_rate <= 1 else "",
            agg_cpu_util="{:.2f}".format(
                (   (self.prometheus_handler.collection_data.exec_cluster_rate  * self.prometheus_handler.collection_data.exec_cpus) +
                    (self.prometheus_handler.collection_data.cnode_cluster_rate * self.prometheus_handler.collection_data.cnode_cpus)
                ) / (self.prometheus_handler.collection_data.ttl_cpus if self.prometheus_handler.collection_data.ttl_cpus > 0 else 1)
                ) if (self.prometheus_handler.collection_data.exec_cluster_rate <=1 and self.prometheus_handler.collection_data.cnode_cluster_rate <= 1) else "",
            tnet_quiet_in=self.prometheus_handler.collection_data.exec_net_quiet_in,
            disk_r=self.prometheus_handler.collection_data.exec_disk_r,
            disk_w=self.prometheus_handler.collection_data.exec_disk_w)

        print(load_stats)


    def get_words(self, num_words:int) -> dict:

        with open("/usr/share/dict/words") as fh:
            words = fh.readlines()

        wordlist = []
        for x in range(num_words):
            wordlist.append(random.choice(words).strip())
        
        return wordlist


    def run(self):

        self.table_df = self.spark.read.table(self.spark_cfg.get("throughput.database_path"))

        self.print_spark_config()

        if self.spark_cfg.get("throughput.analyze_tables"):
            self.spark.sql('ANALYZE TABLE {} COMPUTE STATISTICS FOR ALL COLUMNS'.format(self.spark_cfg.get("throughput.database_path")))

        self.warm_up_and_check(self.table_df)

        self.header()

        int_test_ratios = [0.00001, 0.0001, 0.001, 0.01, 0.1]
        self.print_select_numeric(int_test_ratios, 64, "bigint_val_0")
        self.print_select_numeric(int_test_ratios, 32, "int_val_0")
        self.print_get_by_substr(["abcdef", "abcde", "abcd", "abc", "ab"], "record_id")
        self.print_select_numeric(int_test_ratios, 64, "double_val_0", sel_float=True)
        self.print_select_numeric(int_test_ratios, 32, "float_val_0", sel_float=True)
        self.print_select_by_words(self.get_words(1), "str_val_0")
        self.print_select_by_words(self.get_words(2), "str_val_0")
        self.print_select_by_words(self.get_words(4), "str_val_0")

        self.spark.stop()

    def print_select_by_words(self, words, column):

        then = datetime.now()
        print("select_by_words('{}'),".format(len(words)), end="", flush=True)
        then = datetime.now()
        print("{},".format(then), end="", flush=True)
        
        self.select_by_words(words, column)
        timing, row_count = self.select_by_words(words, column)

        print("{:.4f},".format(timing), end="")
        print("{},".format(row_count), end="")
        self.print_prometheus_stats(then, datetime.now())


    def select_by_words(self, words, select_col, andor="OR", use_sql=True):

        words_sql = f" {andor} ".join(["{} LIKE '%{}%'".format(select_col, x) for x in words])
        s_ts = time.time()
        rc = 0
        try:
            if use_sql:
                filtered_df = self.spark.sql('SELECT {} FROM {} WHERE {}'.format(
                    ",".join(self.schema.field_list),
                    self.spark_cfg.get('throughput.database_path'),
                    words_sql
                ))
            else:
                pass # not implemented

            filtered_df.persist(StorageLevel.MEMORY_ONLY) # Force read
            rc = filtered_df.count()
            e_ts = time.time()
            filtered_df.unpersist()
            del filtered_df

        except Exception as e:
            sys.stderr.write(f"{e}\n")
            return -1, 0
        
        return (e_ts-s_ts), rc
        

    def select_by_substr(self, string, select_col, use_sql=False):

        s_ts = time.time()
        rc = 0
        try:
            if use_sql:
                filtered_df = self.spark.sql('SELECT {} FROM {} WHERE {} LIKE \'{}%\''.format(
                    ",".join(self.schema.field_list),
                    self.spark_cfg.get('throughput.database_path'),
                    select_col,
                    string,
                ))
            else:
                filtered_df = self.table_df.filter(
                    self.table_df[select_col].startsWith(string)
                ).select(self.schema.field_list)

            filtered_df.persist(StorageLevel.MEMORY_ONLY) # Force read
            rc = filtered_df.count()
            e_ts = time.time()
            filtered_df.unpersist()
            del filtered_df

        except Exception as e:
            if self.verbose: sys.stderr.write(f"{e}\n")
            return None

        return (e_ts-s_ts), rc


    def select_numeric(self, ratio:float=1.0, bitwidth:int=32, select_col:str="int_val_0", use_sql=False, sel_float=False):

        # get a slice of the int space
        slice_start, slice_end = self.get_rnd_ks_slice(ratio=ratio, bitwidth=bitwidth, ret_float=sel_float)

        s_ts = time.time()
        rc = 0
        try:
            if use_sql:
                filtered_df = self.spark.sql('SELECT {} FROM {} WHERE {} BETWEEN {} AND {}'.format(
                    ",".join(self.schema.field_list),
                    self.spark_cfg.get('throughput.database_path'),
                    select_col,
                    slice_start,
                    slice_end
                ))
            else:
                filtered_df = self.table_df.filter(
                    self.table_df[select_col].between(slice_start, slice_end)
                ).select(self.schema.field_list)

            filtered_df.persist(StorageLevel.MEMORY_ONLY) # Force read
            rc = filtered_df.count()
            e_ts = time.time()

            filtered_df.unpersist()
            del filtered_df

        except Exception as e:
            if self.verbose: sys.stderr.write(f"{e}\n")
            return None, None

        return (e_ts-s_ts), rc
    

    def get_part_sz(self, part):
        return [sys.getsizeof(part)]