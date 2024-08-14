import time, sys, random
from benchgo.spark.util import config_connect, spcfg, config_block
from benchgo.data import TransactionTblSchema
from pyspark import StorageLevel

class SparkThroughput():
    
    def __init__(self):
        self.spark_cfg = spcfg()
        self.spark = config_connect(self.spark_cfg, "throughput_tests")
        self.verbose = False
        self.schema = TransactionTblSchema(self.spark_cfg.get("throughput.width_factor"))

    def warm_up_and_check(self, df):
        
        print("checking row count...", end='')

        rows = df.count()
        if rows/1000000 == float(self.spark_cfg.get("throughput.row_factor")):
            print("OK ({} rows) ".format(rows))
        else:
            print("NOK ({} rows) ".format(rows))
        
        col_count = len(df.columns)
        if col_count == self.schema.col_count:
            print("OK ({} cols) ".format(col_count))
        else:
            print("NOK ({} cols) ".format(col_count))

    def print_spark_config(self):
        spark_parameters = config_block(self.cfg)
        print("\nSpark Configuration:")
        for param in spark_parameters:
            print("{:>50}:{:<40}".format(param[0], param[1]))
        print("")

    def run(self):
        
        self.table_df = self.spark.read.table(self.spark_cfg.get("throughput.database_path"))
        
        self.print_spark_config()

        self.warm_up_and_check(self.table_df)

        # Select by integer (0.001%)
        print("select_by_integer(0.001%): {}".format(self.select_by_integer(ratio=0.00001, bitwidth=64, select_col="bigint_val_0")))

        # Select by integer (0.01%)
        print("select_by_integer(0.01%): {}".format(self.select_by_integer(ratio=0.0001, bitwidth=64, select_col="bigint_val_0")))

        # Select by integer (0.1%)
        print("select_by_integer(0.1%): {}".format(self.select_by_integer(ratio=0.001, bitwidth=64, select_col="bigint_val_0")))

        # Select by integer (1%)
        print("select_by_integer(1%): {}".format(self.select_by_integer(ratio=0.01, bitwidth=64, select_col="bigint_val_0")))

        # Select by integer (10%)
        print("select_by_integer(10%): {}".format(self.select_by_integer(ratio=0.1, bitwidth=64, select_col="bigint_val_0")))

        # Select by integer (50%)
        #print("select_by_integer() 50\%: {}".format(self.select_by_integer(ratio=.5)))


    def get_slice(self, ratio:float=1.0, bitwidth:int=32):

        target_length = (2**bitwidth) * ratio
        target_offset = int(random.random() * random.randrange(-2**(bitwidth-1), 2**(bitwidth-1)))

        return target_offset, int(target_offset+target_length)


    def select_by_integer(self, ratio:float=1.0, bitwidth:int=32, select_col:str="int_val_0", use_sql=False) -> float:

        # get a slice of the int space, start over if it exceeds the bounds
        # of the (signed) space.
        while True:
            slice_start, slice_end = self.get_slice(ratio, bitwidth)
            if slice_end < (2**(bitwidth-1)):
                break

        s_ts = time.time()
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
            print("{} records, sql=={}, slice==({}, {})".format(filtered_df.count(), use_sql, slice_start, slice_end))
            e_ts = time.time()
        except Exception as e:
            if self.verbose: sys.stderr.write(f"{e}\n")
            return None

        return (e_ts-s_ts)


