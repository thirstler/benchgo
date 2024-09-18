BENCHGO_VERSION='0.2'
ROOT_HELP='''
Execute a benchmark for a given engine using the syntax

    benchgo [engine] [benchmark] [options]

E.g.:

    benchgo trino tpcds --help

The following this list of engines are supported

    trino
    spark
    mkdata
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
    benchgo trino filter --help

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
NOT IMPLEMENTED
Execute a benchmark with the Dremio engine

    benchgo dremio [benchmark] [options]


'''
VASTDBSDK_HELP='''
NOT IMPLEMENTED
Execute a benchmark with the VAST DataBase Python SDK

    benchgo vastdbsdk [benchmark] [options]
'''
DICT_FILE='words.txt'
VDB_SCHEMA_DELIMITER='/'