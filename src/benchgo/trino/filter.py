from benchgo.filter import Filter
from benchgo.util import prometheus_args, global_args
from benchgo.trino.util import trino_args
import argparse, sys

class TrinoFilter(Filter):

    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Run a TPC-DS Benchmark for Trino"
        )
        parser.add_argument("trino")
        parser.add_argument("filter")
        global_args(parser)
        trino_args(parser)
        prometheus_args(parser)
        self.args = parser.parse_args()
        self.engine = "trino"


    def run(self):

        if not self.args.skip_precheck:
            print("== checking tables ==")
            if not self.tablecheck():
                sys.stderr.write("you have a table problem, fix it or --skip-precheck if you know what you're doing\n")
                sys.exit(1)
            else:
                print("tables look good")

        if self.args.analyze_tables:
            print("== analyzing tables ==")
            self.analyze_tables()
    
    def tablecheck(self):

        