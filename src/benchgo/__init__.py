BENCHGO_VERSION='0.2'
ROOT_HELP='''
Execute a benchmark for a given engine using the syntax

    benchgo [engine] [benchmark] [options]

E.g.:

    benchgo trino tpcds --help

The following this list of implemented and plannded benchmarks:

    # Trino workloads
    benchgo trino tpcds --help
    benchgo trino tpch --help           [Not Implimented]
    benchgo trino transaction --help    [Not Implimented]
    benchgo trino throughput --help     [Not Implimented]

    # SparkSQL workloads
    benchgo sparksql tpcds --help
    benchgo sparksql tpch --help        [Not Implimented]
    benchgo sparksql transaction --help [Not Implimented]
    benchgo sparksql throughput --help  [Not Implimented]

    # Spark data frame workloads
    benchgo sparkdf transaction --help [Not Implimented]
    benchgo sparkdf throughput --help  [Not Implimented]

    # Dremio 
    benchgo dremio tpcds --help         [Not Implimented]
    benchgo dremio tpch --help          [Not Implimented]
    benchgo dremio throughput --help    [Not Implimented]

    # VAST DB SDK 
    benchgo vastdbsdk transaction --help   [Not Implimented]
    benchgo vastdbsdk throughput --help    [Not Implimented]
'''
TRINO_HELP='''
Execute a benchmark with the Trino engine

    benchgo trino [benchmark] [options]

 The following this list of implemented and plannded benchmarks:

    benchgo trino tpcds --help
    benchgo trino tpch --help           [Not Implimented]
    benchgo trino transaction --help    [Not Implimented]
    benchgo trino throughput --help     [Not Implimented]   
'''
SQPARKSQL_HELP='''
Execute a benchmark with the SparkSQL engine

    benchgo sparksql [benchmark] [options]

 The following this list of implemented and plannded benchmarks:

    benchgo sparksql tpcds --help
    benchgo sparksql tpch --help           [Not Implimented]
    benchgo sparksql transaction --help    [Not Implimented]
    benchgo sparksql throughput --help     [Not Implimented]   
'''
SPARKDF_HELP='''
Execute a benchmark with the Spark data frame interface

    benchgo sparkdf [benchmark] [options]

 The following this list of implemented and plannded benchmarks:
    benchgo sparkdf transaction --help    [Not Implimented]
    benchgo sparkdf throughput --help     [Not Implimented]   
'''
DREMIO_HELP='''
Execute a benchmark with the Dremio engine

    benchgo dremio [benchmark] [options]

 The following this list of implemented and plannded benchmarks:

    benchgo dremio tpcds --help
    benchgo dremio tpch --help           [Not Implimented]
    benchgo dremio throughput --help     [Not Implimented]   
'''
VASTDBSDK_HELP='''
Execute a benchmark with the VAST DataBase Python SDK

    benchgo vastdbsdk [benchmark] [options]

 The following this list of implemented and plannded benchmarks:
    benchgo vastdbsdk transaction --help    [Not Implimented]
    benchgo vastdbsdk throughput --help     [Not Implimented]
'''