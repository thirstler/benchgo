import random, string, math, pyarrow, argparse, sys, uuid, os, hashlib, vastdb
import pyarrow as pa
from ibis import _
#import ibis


SCHEMA_DELIMITER='/'

class TransactionTblSchema:

    str_len_range = (1,128)

    def __init__(self, width_factor, sparsity=1.0, string_range=(1,256)):
        
        self.sparsity = sparsity

        with open('/usr/share/dict/words', 'r') as fh:
            words = fh.readlines()
            self.words = [s[:-1] for s in words]
            del words
        
        # define some quick generators
        self.rand_rec_id = lambda:   ''.join(random.choices(string.hexdigits, k=16))
        self.rand_str6 = lambda:     None if random.random() > self.sparsity else ' '.join([ random.choice(self.words) for w in range(1, 6)])
        self.rand_sfloat32 = lambda: None if random.random() > self.sparsity else (random.random()*(2**(32)))-(2**(32-1))
        self.rand_sfloat64 = lambda: None if random.random() > self.sparsity else (random.random()*(2**(64)))-(2**(64-1))
        self.rand_int32 = lambda:    None if random.random() > self.sparsity else int(random.random()*(2**32))-(2**(32-1))
        self.rand_int64 = lambda:    None if random.random() > self.sparsity else int(random.random()*(2**64))-(2**(64-1))
        self.rand_bool = lambda:     None if random.random() > self.sparsity else True if random.random() < 0.5 else False

        # annoying map to keep data types alined between arrow and various SQL types
        self.type_map = {
            "string": {
                "arrow": pyarrow.string(),
                "spark_sql": "VARCHAR(255)",
                "gen_function": self.rand_str6
            },
            "float32": {
                "arrow": pyarrow.float32(),
                "spark_sql": "REAL",
                "gen_function": self.rand_sfloat32
            },
            "float64": {
                "arrow": pyarrow.float64(),
                "spark_sql": "DOUBLE",
                "gen_function": self.rand_sfloat64
            },
            "boolean": {
                "arrow": pyarrow.bool_(),
                "spark_sql": "BOOLEAN",
                "gen_function": self.rand_bool
            },
            "int32": {
                "arrow": pyarrow.int32(),
                "spark_sql": "INTEGER",
                "gen_function": self.rand_int32
            },
            "int64": {
                "arrow": pyarrow.int64(),
                "spark_sql": "BIGINT",
                "gen_function": self.rand_int64
            }
        }

        # Actual schema for tables with width factor multipliers. Change this
        # and change the resultant/expected tables for data generation and
        # benchmark runs.
        self.schema = {
            "id": (0, "int64"),          # mandatory, do not change
            "record_id": (0, "string"),  # mandatory, do not change
            "str_val": (math.ceil(width_factor * 0.5), "string"),
            "float_val": (width_factor*2, "float32"),
            "double_val": (width_factor, "float64"),
            "bool_val": (width_factor*2, "boolean"),
            "int_val": (width_factor*2, "int32"),
            "bigint_val": (width_factor, "int64")
        }
        self.col_count = len(self.mk_schema(dtype="spark_sql"))
        
        self.field_list = []
        for s in self.schema:
            for r in range(self.schema[s][0]):
                self.field_list.append("{}_{}".format(s, r))

    def mk_schema(self, dtype=None) -> dict:
        '''
        Type is one of: arrow, spark_sql, trino or dremio
        '''
        if dtype not in ["arrow", "spark_sql", "trino", "dremio"]: return None

        columns = []

        # Always present
        columns.append(("id", self.type_map["int64"][dtype]))
        columns.append(("record_id", self.type_map["string"][dtype]))
        for col_t in self.schema:
            for i in range(self.schema[col_t][0]):
                columns.append((
                    "{}_{}".format(col_t, i),
                    self.type_map[self.schema[col_t][1]][dtype]
                ))

        return columns
    
    def pyarrow_schema(self):
        return pyarrow.schema(self.mk_schema(dtype="arrow"))
    

'''
def mk_schema(width_factor):

    columns = [("id", "BIGINT"), ("record_id", "VARCHAR(255)")]

    # String data types:
    for n in range(0, width_factor):
        columns.append( ("str_val_{}".format(n), "VARCHAR(255)") )
    # Float data types:
    for n in range(0, width_factor*5):
        columns.append( ("float_val_{}".format(n), "REAL") )
    # Boolean data types:
    for n in range(0, width_factor*2):
        columns.append( ("bool_val_{}".format(n), "BOOLEAN") )
    # Integer data types:
    for n in range(0, width_factor*3):
        columns.append( ("int_val_{}".format(n), "INTEGER") )

    return columns
    
    
def pyarrow_schema(width_factor):
    columns = [("id", pyarrow.int64()), ("record_id", pyarrow.string())]

    # String data types:
    for n in range(0, width_factor):
        columns.append( ("str_val_{}".format(n), pyarrow.string()) )
    # Float data types:
    for n in range(0, width_factor*5):
        columns.append( ("float_val_{}".format(n), pyarrow.float32()) )
    # Boolean data types:
    for n in range(0, width_factor*2):
        columns.append( ("bool_val_{}".format(n), pyarrow.bool_()) )
    # Integer data types:
    for n in range(0, width_factor*3):
        columns.append( ("int_val_{}".format(n), pyarrow.int32()) )

    return pyarrow.schema(columns)
'''


def write_data(args, data, schema):

    obj_id = uuid.uuid4()
    if args.s3out:
        if len(args.prefix) > 1:
            if args.prefix[-1] != '/':
                prefix = args.prefix + '/'
            else:
                prefix = args.prefix
                
        full_key = "{}{}".format(prefix, obj_id)
            
        if args.endpoint:
            s3con = boto3.session.Session()
            client = s3con.client(
                service_name='s3',
                endpoint_url=args.endpoint
            )
        else:
            client = boto3.client('s3')
        
        sys.stdout.write("({endpoint}) uploading to s3://{bucket}/{key}...".format(endpoint=args.endpoint, bucket=args.s3out, key=full_key))
        sys.stdout.flush()

        textblob = ""
        for row in data:
            textblob += "|".join([str(x) if x != None else "" for x in row]) + '\n'

        client.put_object(Body=bytes("\n".join(textblob),"utf-8"), Bucket=args.s3out, Key=full_key)
        print("done")

    elif args.dirout:

        dirout = args.dirout[:-1] if args.dirout[-1] == '/' else args.dirout
        if not os.path.exists(dirout):
            os.makedirs(dirout)
            print("(directory {} created)".format(dirout))

        fn = "{dir}/{uuid}".format(dir=dirout, uuid=obj_id)

        fn.replace('//', '/') # just in case

        with open(fn, "w") as fh:
            print(fn)
            for row in data:
                textdata = "|".join([str(x) if x != None else "" for x in row])
                fh.write("{}\n".format(textdata))
            fh.close()


    elif args.vdbout:

        session = vastdb.connect(
            endpoint=args.endpoint,
            access=args.access_key,
            secret=args.secret_key)
        
        # Get the pyarrow schema
        schema = schema.pyarrow_schema()

        # Turn into columns 
        columns = list(zip(*data))
        arrays = [pa.array(column, type=schema.field(i).type) for i, column in enumerate(columns)]
        record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

        # None shall pass without my permission
        # vdbschema = get_schema_path(session, args.vdbout, args.prefix.split(SCHEMA_DELIMITER))
        with session.transaction() as tx:
            vdb_bucket = tx.bucket("db0")
            vdb_schema = schema_dive(vdb_bucket, args.prefix.split(SCHEMA_DELIMITER))
            if args.table_name == '_AUTO_':
                table_name = "benchmark_wf{}_rf{}_sp{}".format(args.width_factor, int((int(args.records_per_job)*int(args.jobs))/1000000), args.sparsity)
            else:
                table_name = args.table_name
            
            # Quietly create the table
            if args.job == "1":
                try:
                    table = vdb_schema.create_table(table_name, schema)
                    print("create the target table")
                except:
                    pass

            table = vdb_schema.table(table_name)

            table.insert(record_batch)

'''
def get_schema_path(session, bucket_name, schema_path) -> object:
    with session.transaction() as tx:
        bucket = tx.bucket(bucket_name)
        schema = schema_dive(bucket.schema(schema_path[0]), schema_path[1:])
        if schema == None:
            return None
        return schema

def check_for_schema(session, args) -> bool:

    with session.transaction() as tx:
        bucket = tx.bucket(args.vdbout)
        names = args.prefix.split(SCHEMA_DELIMITER)
        schema = schema_dive(bucket.schema(names[0]), names[1:])
        if schema == None:
            return False
        return True
'''      

def schema_dive(bucket, schemas):
    schema = bucket.schema(schemas[0])
    return _schema_dive(schema, schemas[1:])

def _schema_dive(schema, names) -> object:
    '''Test to see if the schema path exists'''
    if len(names) == 0: return schema
    this_name = names.pop(0)
    try:
        schema = schema.schema(this_name)
    except:
        return None
    
    return _schema_dive(schema, names)


def parse_ip_list(endpoints) -> list:
    iplist = []
    iprange = endpoints.split(':')
    if len(iprange) == 2:
        octet1 = iprange[0].split('.')
        octet2 = iprange[0].split('.')
        for i in range(int(octet1[3]), int(octet2[3])+1):
            iplist.append('.'.join(octet1[:2]) + str(i))
    else:
        iplist.append(endpoints)

    return iplist



def create_transaction_table(width_factor=1, table_path="trns_tbl"):
    print(mk_ddl_sql(width_factor, table_path))


def mk_ddl_sql(width_factor, table_path, if_not_exists=False, table_format=None):

    ddl = "CREATE TABLE{ine}{table} (\n    ".format(
        ine=" IF NOT EXISTS " if if_not_exists else "",
        table=table_path)

    schema = TransactionTblSchema(width_factor).mk_schema(dtype="spark_sql")

    columns = []
    for field, type in schema:
        columns.append("{} {}".format(field, type))

    ddl = ddl + ",\n    ".join(columns) + "\n)"

    if table_format:

        ddl += " USING {table_format}".format(table_format=table_format)

    return ddl


def mk_dict_row(id, width_factor, sparsity, cardinality):
    dict_row = {}
    row = mk_row(id, width_factor, sparsity, cardinality)
    schema = TransactionTblSchema(width_factor).mk_schema(dtype="spark_sql")
    for i, item in enumerate(schema):
        if row[i] != None:
            dict_row[item[0]] = row[i]
    return dict_row


    

def mk_row(id, table_def):
    
    # Mandatory fields
    col_data = [id, table_def.rand_rec_id()]

    for col_class in table_def.schema:

        itr = table_def.schema[col_class][0]
        dtype = table_def.schema[col_class][1]
        gen_func = table_def.type_map[dtype]["gen_function"]

        if itr > 0: # skip mandatory fields

            values =  [gen_func() for x in range(itr)]

            for x in values:
                col_data.append(x)
    
    return col_data
        

def _mk_row(id, width_factor, sparsity, cardinality):

    record_id = "{}".format(''.join(random.choices(string.ascii_letters, k=16)))
    col_data = (id, record_id)


    # Strings
    for n in range(0, col_count.STR_COLS):
        
        if random.random() < sparsity:
            if n == 0 and cardinality != 0:
                col_data += (numeric_to_hash(random.randrange(0, cardinality), 64),)
            else:
                col_data += (''.join(random.choices(string.ascii_letters, k=random.randrange(1,128))),)
        else:
            col_data += (None,)

    # Floats
    for n in range(0, col_count.FLOAT_COLS):
        
        if random.random() < sparsity:
            if n == 0 and cardinality != 0:
                col_data += (random.randrange(0, cardinality)/cardinality,)
            else:
                col_data += (random.random()*10000,)
        else:
            col_data += (None,)

    # Boolean data types:
    for n in range(0, col_count.BOOL_COLS):
        
        if random.random() < sparsity:
            col_data += (True if random.random() < 0.5 else False,)
        else:
            col_data += (None,)
    
    # Integer data types:
    for n in range(0, col_count.INT_COLS):
        
        if random.random() < sparsity:
            if n == 0 and cardinality != 0:
                col_data += (random.randrange(0,cardinality),)
            else:
                col_data += (int(random.random()*(2**32))-(2**31),)
        else:
            col_data += (None,)

    return col_data


def _mk_data(args, width_factor=1, job=1, sparsity=1.0, multiplier=100000, limit=1073741824) -> None:

    start = (job-1) * multiplier
    end = job * multiplier
    size_in_ch=0

    table_def = TransactionTblSchema(width_factor, sparsity=sparsity)
    data_block = []
    for i, id in enumerate(range(start, end)):

        data = mk_row(id, table_def)

        # byte sizes are really vague 
        size_in_ch += sys.getsizeof(data) + (width_factor*40)

        data_block.append(data)

        #if i % 10000 == 0:
        #    sys.stdout.write("\r{} bytes".format(size_in_ch))
        #    sys.stdout.flush()

        # size limit reached, write the block
        if size_in_ch > int(limit):
            write_data(args, data_block, table_def)
            size_in_ch = 0
            data_block.clear()
    
    
    # Write the stragglers
    write_data(args, data_block, table_def)

    return None

def numeric_to_hash(value: int, length: int) -> str:

    value_str = str(value).encode()
    hash_obj = hashlib.sha256(value_str)
    hex_hash = hash_obj.hexdigest()
    if length <= len(hex_hash):
        return hex_hash[:length]
    else:
        return hex_hash.ljust(length, '0') 
    

def mk_data():

    parser = argparse.ArgumentParser(
        description="Create data and DDL for benchgo benchmarks"
    )

    # Get these out of the way
    parser.add_argument("mkdata")
    parser.add_argument("transaction")

    # Data generation
    parser.add_argument("--ddl", action="store_true", help="generate DDL for the selected column width factor")
    parser.add_argument("--table-path", default="trns_abl", help="full path to table in generated SQL")
    parser.add_argument("--data", action="store_true", help="generate data for benchmarks")
    parser.add_argument("--width-factor", default="1", help="width factor for data/ddl generation factor of 1=10 cols, factor of 10 = 100 cols, etc")
    parser.add_argument("--jobs", default="1", help="when generating data, this is the total number of parallel jobs that you will run (1 job defaults to 1,000,000 records)")
    parser.add_argument("--job", default="1", help="this is the job ID of THIS job")
    parser.add_argument("--records-per-job", default="1000000", help="number of rows/records to generate")
    parser.add_argument("--sparsity", default="1.0", help="data sparsity in the table, e.g: 0.1 will populate aprox 10 perc of the fields")
    parser.add_argument("--cardinality", default="0", help="cardinality factor for predefined columns, 0==random-value (high card) or integer setting the number of distinct vales")

    # Output options
    parser.add_argument("--dirout", default=None, help="place resultant CSV files in here")
    parser.add_argument("--s3out", default=None, help="place resultant CSV in this S3 bucket (use environment variables for credentials)")
    parser.add_argument("--vdbout", default=None, help="write data to this VDB database")
    parser.add_argument("--prefix", default="", help="place data in this S3 prefix or VDB schema path, depending on --s3out or --vdbout")
    parser.add_argument("--table-name", default="_AUTO_", help="when using --vdbout, specify a table name (automatic if not provided)")
    parser.add_argument("--endpoint", default=None, help="custom S3 endpoint (non AWS) or VDB endpoint (e.g.: http://10.0.0.1:8080)")
    parser.add_argument("--byteslimit", default=1073741824, help="python bytes limit for batches and threshold for triggering a new object")
    parser.add_argument("--access-key", help="VDB access key (use environment for S3)")
    parser.add_argument("--secret-key", help="VDB secret key (use environment for S3)")

    args = parser.parse_args()

    if args.ddl:
        create_transaction_table(width_factor=int(args.width_factor), table_path=args.table_path)
    
    if args.data:
        sys.stdout.write("generating data...\n")
        sys.stdout.flush()
        _mk_data(
            args,
            width_factor=int(args.width_factor),
            job=int(args.job),
            sparsity=float(args.sparsity),
            multiplier=int(args.records_per_job),
            limit=int(args.byteslimit)
        )
        print("\ndone")
        

            

