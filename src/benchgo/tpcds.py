import sys, datetime, os
from benchgo.prometheus_handler import PrometheusHandler

class TPCDS:
    '''
    base class for any TPC-DS benchmark regardless of engine
    '''

    queries = None
    args = None

    def query_prep(self, query) -> str:
        prefix = ""
        if not self.args.no_explain:
            prefix += "EXPLAIN "
            if not self.args.no_analyze:
                prefix += "ANALYZE "

        return("{prefix}{query}".format(prefix=prefix, query=query))
    

    def prometheus_connect(self):
        self.prometheus_handler = PrometheusHandler(self.args)


    def logging_setup(self):
        # Set up ouput dir and main result log
        self.output_dir = "/tmp/{}_{}".format(self.args.name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        try:
            os.mkdir(self.output_dir)
        except Exception as ops:
            sys.stderr.write("problem creating output dir {}: {}\n".format(self.output_dir, ops))
            sys.exit(1)
        
        try:
            self.result_log_fh = open("{outdir}/result_log.csv".format(outdir=self.output_dir), "w")
        except Exception as ops:
            sys.stderr.write("problem creating log file: {}\n".format(ops))
            sys.exit(1)
