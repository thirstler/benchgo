import os, yaml, sys
from pathlib import Path
from benchgo.spark import CONFIG_TEMPLATE, ENV_TEMPLATE, SPARK_BENCHGO_CONFIG_DIR, SPARK_BENCHGO_CONFIG_FILE, SPARK_BENCHGO_ENV_FILE
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def spark_args(parser) -> None:
    parser.add_argument("--spark-home", default="/opt/spark", help="set SPARK_HOME (required)")
    parser.add_argument("--master", default="spark://localhost:7077", help="Spark master address")
    parser.add_argument("--executors", default='1', help="number of spark executors")
    parser.add_argument("--exec-cores", default='1', help="executor CPU cores")
    parser.add_argument("--exec-memory", default='16g', help="executor memory")
    parser.add_argument("--driver-memory", default='4g', help="executor memory")


class spcfg:

    active = None

    def __init__(self):

        config_path = "{}/{}/{}".format(Path.home(), SPARK_BENCHGO_CONFIG_DIR, SPARK_BENCHGO_CONFIG_FILE)
        with open(config_path, "r") as fh:
            self.cfg = yaml.safe_load(fh)["config"]

    def get(self, key_path):
        path = key_path.split(".")
        val = self.cfg
        for i in path:
            try:
                val = val[i]
            except KeyError:
                return None
            except TypeError:
                return None
            
        return val

def config_connect(scfg, job_name):

    conf = SparkConf()
    
    conf.setAppName(job_name)
    conf.setMaster(scfg.get("job.spark_master"))

    conf_vals = config_block(scfg)
    for c in conf_vals:
        conf.set(c[0], c[1])

    # Sloppy af
    #session = SparkSession.builder.appName(job_name).enableHiveSupport()
    session = SparkSession.builder.appName(job_name)
    spark = session.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def config_block(scfg):
    
    conf_vals = []

    # Static stuff
    conf_vals.append(("spark.ui.showConsoleProgress", "false"))
    conf_vals.append(("spark.executor.userClassPathFirst", "true"))
    conf_vals.append(("spark.driver.userClassPathFirst", "true"))

    # Configured
    conf_vals.append(("spark.executor.instances", scfg.get("job.num_exec")))
    conf_vals.append(("spark.executor.cores", int(scfg.get("job.num_cores"))))
    conf_vals.append(("spark.executor.memory", scfg.get("job.exec_memory")))
    conf_vals.append(("spark.driver.memory", scfg.get("job.driver_memory")))

    # Defaults
    for key, val in scfg.get("spark_config").items():
            conf_vals.append( (key, val) )

    if scfg.get("exec_monitor.enabled"):
        conf_vals.append(("spark.executor.extraJavaOptions", scfg.get("exec_monitor.opts")))
    if scfg.get("driver_monitor.enabled"):
        conf_vals.append(("spark.driver.extraJavaOptions", scfg.get("driver_monitor.opts")))

    if scfg.get("tpcds.explain") or scfg.get("tpcds_selective.explain"):
        conf_vals.append(("spark.sql.debug.maxToStringFields", "100"))

    if scfg.get("vdb.enable"):

        # basic config
        conf_vals.append( ("spark.ndb.endpoint", scfg.get("vdb.endpoint")) )
        conf_vals.append( ("spark.ndb.data_endpoints", scfg.get("vdb.endpoints")) )
        conf_vals.append( ("spark.ndb.access_key_id", scfg.get("vdb.access_key")) )
        conf_vals.append( ("spark.ndb.secret_access_key", scfg.get("vdb.secret_key")) )
        conf_vals.append( ("spark.ndb.num_of_splits", scfg.get("vdb.splits")) )
        conf_vals.append( ("spark.ndb.num_of_sub_splits", scfg.get("vdb.subsplits")) )

        # Advanced and static config
        for key, val in scfg.get("vdb_config").items():
            conf_vals.append( (key, val) )

    if scfg.get("iceberg.enable"):

        # Basic config
        conf_vals.append( ("spark.hadoop.fs.s3a.endpoint", scfg.get("iceberg.s3_endpoint")) )
        conf_vals.append( ("spark.hadoop.hive.metastore.uris", scfg.get("iceberg.metastore_uri")) )
        conf_vals.append( ("spark.hadoop.fs.s3a.access.key", scfg.get("iceberg.access_key")) )
        conf_vals.append( ("spark.hadoop.fs.s3a.secret.key", scfg.get("iceberg.secret_key")) )

        # Advanced and static config
        for key, val in scfg.get("iceberg_config").items():
            conf_vals.append( (key, val) )
    
    return conf_vals


def dump_interactive(scfg):
    """
    This is pretty fragile and needs to be updated manually. As it's for
    convenience purposes this is probably ok.  
    """
    
    conf_vals = config_block(scfg)
    sys.stdout.write("spark-sql ")
    if scfg.get("iceberg.enable"):
        try:
            sys.stdout.write("  --packages {} \\\n".format(os.environ["ICEBERG_PACKAGE"]))
        except:
            sys.stdout.write("  --packages {} \\\n".format("[ICEBERG PACKAGE NAME]"))


    if scfg.get("vdb.enable"):
        try:
            sys.stdout.write("  --driver-class-path $(echo {}/*.jar | tr ' ' ':') \\\n".format(scfg.get("vdb.jars")))
            sys.stdout.write("  --jars $(echo {}/*.jar | tr ' ' ',') \\\n".format(scfg.get("vdb.jars"))) 
        except:
            sys.stdout.write("  --driver-class-path $(echo {}/*.jar | tr ' ' ':') \\\n".format("/path/to/vdb/jars"))
            sys.stdout.write("  --jars $(echo {}/*.jar | tr ' ' ',') \\\n".format("/path/to/vdb/jars")) 

    sys.stdout.write("  --master {} \\\n".format(scfg.get("job.spark_master")))
    sys.stdout.write("  --conf spark.executor.instances={} \\\n".format(scfg.get("job.num_exec")))
    sys.stdout.write("  --conf spark.cores.max={} \\\n".format(scfg.get("job.num_cores")))
    sys.stdout.write("  --conf spark.executor.memory={} \\\n".format(scfg.get("job.exec_memory")))
    sys.stdout.write("  --conf spark.driver.memory={} \\\n".format(scfg.get("job.driver_memory")))

    for key, val in conf_vals:
        sys.stdout.write("  --conf {key}=\"{val}\" \\\n".format(key=key, val=val))

    sys.stdout.write("  --name {}\n\n".format(scfg.get("job.app_name")))

    sys.stdout.flush()


def gen_config():

    benchgo_config_home="{}/{}".format(Path.home(), SPARK_BENCHGO_CONFIG_DIR)
    spark_env_loc="{}/{}".format(benchgo_config_home, SPARK_BENCHGO_ENV_FILE)
    config_out_loc="{}/{}".format(benchgo_config_home, SPARK_BENCHGO_CONFIG_FILE)

    if not os.path.isdir(benchgo_config_home):
        try:
            os.mkdir(benchgo_config_home)
        except Exception as e:
            print(e)
    
    if not os.path.isfile(config_out_loc):
        try:
            with open(config_out_loc, "x") as fh:
                fh.write(CONFIG_TEMPLATE)
            print("template written to {}, you need to configure it".format(config_out_loc))
        except Exception as e:
            print(e)
    else:
        print('config file exists')

    if not os.path.isfile(spark_env_loc):
        try:
            with open(spark_env_loc, "x") as fh:
                fh.write(ENV_TEMPLATE)
            print("env file written to {}, you need to configure it".format(spark_env_loc))
        except Exception as e:
            print(e)
    else:
        print('env file exists')