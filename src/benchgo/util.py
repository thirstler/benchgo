def prometheus_args(parser) -> None:

    # Prometheus options
    parser.add_argument("--prometheus-host", default=None, help="URL for prometheus host (e.g.: http://myhost:9090)")
    parser.add_argument("--exec-prometheus-job", default="trino_1", help="Prometheus job name for execution cluster's node_exporter data")
    parser.add_argument("--cnode-prometheus-job", default="vast_cnodes", help="Prometheus job name for CNode cluster's node_exporter data")
    parser.add_argument("--sleep-between-queries", default="5", help="gap in seconds between queries to avoid overlap in stats collection")


def vastdb_api_args(parser) -> None:
    parser.add_argument("--access-key", default=None, help="API access key")
    parser.add_argument("--secret-key", default=None, help="API secret key")
    parser.add_argument("--endpoints", default=None, help="comma-delimited list of API endpoints for database access")
    parser.add_argument("--database", default=None, help="VAST database/bucket name")
    parser.add_argument("--schema", default=None, help="schema or schema path")


def vastdb_sdk_args(parser) -> None:
    # SDK options
    parser.add_argument("--vdb-database", default=None, help="VAST database/bucket name")
    parser.add_argument("--vdb-schema", default=None, help="schema or schema path")
    parser.add_argument("--vdb-table", default=None, help="target table for tests - table will be created if it doesn't exit")


def data_load_args(parser) -> None:
    # Insert options
    parser.add_argument("--width-factor", default="1", help="column width of data generated, 1~=10 cols, 2~=20, 10~=100, etc...")
    parser.add_argument("--sparsity", default="1.0", help="sparsity of generated data, 1.0 = 100%% data, 0.25 = 25%% data, 75%% NULLs")
    parser.add_argument("--agent-procs", default="4", help="num procs to run per node (for SDK benchmarks, relevant to data generation and benchmark run)")
    parser.add_argument("--insert-rows", default="10000", help="total number of rows to write (amount of data generated)")
    parser.add_argument("--insert-batch", default="1000", help="batch size for inserts")


def tpcds_args(parser) -> None:
    parser.add_argument("--analyze-tables", action="store_true", help="runs ANALYZE on tables to populate statistics for cost-based optimizers")
    parser.add_argument("--skip-precheck", action="store_true", help="skip table checks to a) make sure the selected scale-factor is correct and b) make sure the tables are complete")
    parser.add_argument("--scale-factor", help="TPC-DS scale factor sf1000, sf10000, sf100000; WARNING: this option selects the scale factor for queries, not the data set")
    parser.add_argument("--no-explain", action="store_true", help="do not include EXPLAIN in queries")
    parser.add_argument("--no-analyze", action="store_true", help="do not include ANALYZE in queries")
    parser.add_argument("--step-query", action="store_true", help="run queries individually, not a benchmark but captures query plans and (sometimes) detailed information")
    parser.add_argument("--run-queries", default=None, help="comma-delimited list of queries to run when using --step-query (query42,query73...)")
    parser.add_argument("--concurrency", default="1", help="specify concurrency for benchmark run")


def global_args(parser) -> None:
    parser.add_argument("--name", default="benchgo", help="name this workload; determines the base name of the output directory")
    parser.add_argument("--outdir", default="/tmp/", help="specify output directory for benchmark results")


def transaction_options(parser) -> None:
    parser.add_argument("--update-del-tests-sf", default="sf1", help="scale factor for tests (sf1,sf10,sf100,sf1000 or sf10000)")
    parser.add_argument("--update-del-table", default=None, help="full path of table to run tests on (only works on tables prepared with mkdata utility)")
    parser.add_argument("--merge-from-table", default=None, help="table (full path), to use as source for merge testing")
    parser.add_argument("--force", action="store_true", help="force various operations (table overwrites, etc), TRIPLE CAUTION")
    parser.add_argument("--reuse", action="store_true", help="use existing tables/resources rather than create new ones")


def filter_args(parser) -> None:
    parser.add_argument("--target-table", required=True, help="name of the benchmark table")
    parser.add_argument("--width-factor", required=True, help="width factor to be benchmarked (e.g.: 1,10,100,1000)")
    parser.add_argument("--row-factor", required=True, help="row factor to be benchmarked, 1=1m rows (e.g.: 1000,10000,100000))")
    parser.add_argument("--tests", default="_ALL_", help="int,int64,float,float64,substring,words,1row (defaults to all tests)")
    parser.add_argument("--skip-precheck", action="store_true", help="tables are checked by default to make sure they match the benchmark, this skips that checks")
    parser.add_argument("--analyze-table", action="store_true", help="analyze the table before benchmark")
    



    