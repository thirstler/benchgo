#!/bin/env pyspark

from benchgo.spark.util import spcfg, dump_interactive

if __name__ == "__main__":

    
    config = spcfg()
    if config.get("job.dump_interactive"):
        dump_interactive(spcfg())
        exit(0)
        
    benchmarks = config.get("benchmarks")
    
    if "tpcds" in benchmarks:
        from benchgo.spark.tpcds import SparkSQLTPCDS
        SparkSQLTPCDS().run()
    
    if "tpcds_step" in benchmarks:
        from benchgo.spark.tpcds import SparkSQLTPCDS
        print("running tocds_step benchmark")
        SparkSQLTPCDS().run_step()

    if "throughput" in benchmarks:
        from benchgo.spark.throughput import SparkThroughput
        SparkThroughput().run()

    #if "insert" in benchmarks:
    #    run_inserts(config)
