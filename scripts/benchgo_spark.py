#!/bin/env pyspark

from benchgo.benchgo_spark import *

if __name__ == "__main__":

    os.environ['PYSPARK_PYTHON'] = '/home/vastdata/venv/bin/python'
    
    config = spcfg()
    if config.get("job.dump_interactive"):
        dump_interactive(spcfg())
        exit(0)
        
    benchmarks = config.get("benchmarks")


    
    if "tpcds" in benchmarks:
        run_tpcds(config)
    if "insert" in benchmarks:
        run_inserts(config)
