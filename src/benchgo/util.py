import datetime, sys, os
from benchgo.prometheus_handler import PrometheusHandler

def prometheus_args(parser) -> None:

    # Prometheus options
    parser.add_argument("--prometheus-host", help="URL for prometheus host (e.g.: http://myhost:9090)")
    parser.add_argument("--exec-prometheus-job", default="trino_1", help="Prometheus job name for execution cluster's node_exporter data")
    parser.add_argument("--cnode-prometheus-job", default="vast_cnodes", help="Prometheus job name for CNode cluster's node_exporter data")
    parser.add_argument("--sleep-between-queries", default="5", help="gap in seconds between queries to avoid overlap in stats collection")


def vastdb_sdk_args(parser) -> None:
    # SDK options
    parser.add_argument("--aws-profile", default=None, help="use an AWS profile for credentials to the VAST Database")
    parser.add_argument("--access-key", default=None, help="access key, falls-back to env AWS_ACCESS_KEY_ID")
    parser.add_argument("--secret-key", default=None, help="secret key, falls-back to env AWS_SECRET_ACCESS_KEY")
    parser.add_argument("--aws-region", default='us-east-1', help="AWS region")
    parser.add_argument("--vast-endpoints", default=None, help="comma-delimited list of API endpoints for database access")
    parser.add_argument("--vdb-database", default=None, help="VAST database/bucket name")
    parser.add_argument("--vdb-schema", default=None, help="schema or schema path")
    parser.add_argument("--vdb-table", default=None, help="target table for tests - table will be created if it doesn't exit")
    parser.add_argument("--streaming", action="store_true", help="simulate streaming performance (vs throughput)")


def data_load_args(parser) -> None:
    # Insert options
    parser.add_argument("--width-factor", default="1", help="column width of data generated, 1~=10 cols, 2~=20, 10~=100, etc...")
    parser.add_argument("--sparsity", default="1.0", help="sparsity of generated data, 1.0 = 100%% data, 0.25 = 25%% data, 75%% NULLs")
    parser.add_argument("--agent-procs", default="4", help="num procs to run per node (for SDK benchmarks, relevant to data generation and benchmark run)")
    parser.add_argument("--insert-rows", default="10000", help="total number of rows to write (amount of data generated)")
    parser.add_argument("--insert-batch", default="1000", help="batch size for inserts")

def tpcds_args(parser) -> None:
    parser.add_argument("--analyze-tables", action="store_true", help="runs ANALYZE on tables to populate statistcs for cost-based optimizers")
    parser.add_argument("--skip-precheck", action="store_true", help="skip table checks to a) make sure the selected scale-factor is correct and b) make sure the tables are complete")
    parser.add_argument("--scale-factor", help="TPC-DS scale factor sf1000, sf10000, sf100000; WARNING: this option selects the scale factor for queries, not the data set")
    parser.add_argument("--no-explain", action="store_true", help="do not include EXPLAIN in queries")
    parser.add_argument("--no-analyze", action="store_true", help="do not include ANALYZE in queries")

def global_args(parser) -> None:
    parser.add_argument("--name", default="benchgo", help="name this workload; determines the base name of the output directory")


def transaction_options(parser) -> None:
    parser.add_argument("--update-del-tests-sf", default="sf1", help="scale factor for tests (sf1,sf10,sf100,sf1000 or sf10000)")
    parser.add_argument("--update-del-table", default=None, help="full path of table to run tests on (only works on tables prepared with mkdata utility)")
    parser.add_argument("--merge-from-table", default=None, help="table (full path), to use as source for merge testing")
    parser.add_argument("--force", action="store_true", help="force various operations (table overwrites, etc), TRIPLE CAUTION")
    parser.add_argument("--reuse", action="store_true", help="use existing tables/resources rather than create new ones")






    