BENCHGO_VERSION='0.2'
ROOT_HELP='''
Execute a benchmark for a given engine using the syntax

    benchgo [engine] [benchmark] [options]

E.g.:

    benchgo trino tpcds --help

The following this list of implemented and planned benchmarks:

    # Generate data for transaction and throughput/filter tests
    benchgo mkdata transaction --help
    
    # Trino workloads
    benchgo trino tpcds --help
    benchgo trino tpch --help           [Not Implimented]
    benchgo trino transaction --help    [Not Implimented]
    benchgo trino throughput --help     [Not Implimented]

    # Spark workloads (see help on use of spark scripts)
    benchgo spark --help

    # Dremio 
    benchgo dremio tpcds --help         [Not Implimented]
    benchgo dremio tpch --help          [Not Implimented]
    benchgo dremio throughput --help    [Not Implimented]

    # VAST DB SDK 
    benchgo vastdbsdk transaction --help   [Not Implimented]
    benchgo vastdbsdk throughput --help    [Not Implimented]

'''
MKDATA_HELP='''
Create data for transaction and throughput workloads

    benchgo mkdata transaction --help
    
'''
TRINO_HELP='''
Execute a benchmark with the Trino engine

    benchgo trino [benchmark] [options]

List of implemented and planned benchmarks:

    benchgo trino tpcds --help
    benchgo trino tpch --help           [Not Implimented]
    benchgo trino filter --help
    benchgo trino transaction --help    [Not Implimented]

'''
SPARK_HELP='''
Working with spark is different from the other engines since benchmark 
profiles need to be wrapped in spark-submit. Also, all configuration for
the engine itself is done through the driver program making the list of 
command line options very lengthy. For this reason you need to populate
a configuration file with all of your spark options and benchmark profile
settings and then use the benchgo_spark wrapper to execute:

To create an empty configuration use:

    benchgo spark genconfig

This will populate an empty config file and environment file:

    ~/.benchgo/benchgo_spark.yaml
    ~/.benchgo/sparkenv

Configure these files and run:

    benchgo_spark

'''
DREMIO_HELP='''
Execute a benchmark with the Dremio engine

    benchgo dremio [benchmark] [options]

List of implemented and planned benchmarks:

    benchgo dremio tpcds --help
    benchgo dremio tpch --help           [Not Implimented]
    benchgo dremio throughput --help     [Not Implimented]

'''
VASTDBSDK_HELP='''
Execute a benchmark with the VAST DataBase Python SDK

    benchgo vastdbsdk [benchmark] [options]

List of implemented and planned benchmarks:

    benchgo vastdbsdk transaction --help    [Not Implimented]
    benchgo vastdbsdk throughput --help     [Not Implimented]

'''