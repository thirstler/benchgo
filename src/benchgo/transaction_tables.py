import random
import string
import boto3
import sys
import uuid
import pyarrow

class sf_cols:
    
    STR_COLS = 0
    FLOAT_COLS = 0
    INT_COLS = 0
    BOOL_COLS= 0

    def __init__(self, wf):
        self.STR_COLS =  (wf*1) if (wf*1 <= 20) else 20
        self.FLOAT_COLS = wf*5
        self.INT_COLS = wf*3
        self.BOOL_COLS= wf*2


def write_data(args, data):
    obj_id = uuid.uuid4()
    if args.s3out:
        if len(args.s3prefix) > 1:
            if args.s3prefix[-1] != '/':
                s3prefix = args.s3prefix + '/'
            else:
                s3prefix = args.s3prefix
                
        full_key = "{}{}".format(s3prefix, obj_id)
            
        if args.endpoint:
            s3con = boto3.session.Session()
            client = s3con.client(
                service_name='s3',
                endpoint_url=args.endpoint,
            )
        else:
            client = boto3.client('s3')
        
        sys.stdout.write("uploading to s3://{bucket}/{key}...".format(bucket=args.s3out, key=full_key))
        sys.stdout.flush()
        client.put_object(Body=bytes("\n".join(data),"utf-8"), Bucket=args.s3out, Key=full_key)
        print("done")

    elif args.dirout:
        fn = "{dir}/{uuid}".format(dir=args.dirout, uuid=obj_id)
        with open(fn, "w") as fh:
            print(fn)
            for row in data:
                fh.write("{}\n".format(row))
            fh.close()


def create_transaction_table(width_factor=1, table_path="trns_tbl"):
    print(mk_ddl_sql(width_factor, table_path))

def mk_ddl_sql(width_factor, table_path, if_not_exists=False, table_format=None):

    ddl = "CREATE TABLE{ine}{table} (\n    ".format(
        ine=" IF NOT EXISTS " if if_not_exists else "",
        table=table_path)

    schema = mk_schema(width_factor)

    columns = []
    for field, type in schema:
        columns.append("{} {}".format(field, type))

    ddl = ddl + ",\n    ".join(columns) + "\n)"

    if table_format:

        ddl += " USING {table_format}".format(table_format=table_format)

    return ddl


    
def pyarrow_schema(width_factor):

    columns = [("id", pyarrow.int64()), ("record_id", pyarrow.string())]

    # String data types:
    str_cols = (width_factor*1) if (width_factor*1 <= 20) else 20
    for n in range(0, str_cols):
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
    


def mk_schema(width_factor):

    columns = [("id", "BIGINT"), ("record_id", "VARCHAR(255)")]

    # String data types:
    str_cols = (width_factor*1) if (width_factor*1 <= 20) else 20
    for n in range(0, str_cols):
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
    

def mk_dict_row(id, width_factor, sparsity):
    dict_row = {}
    row = mk_row(id, width_factor, sparsity)
    schema = mk_schema(width_factor)
    for i, item in enumerate(schema):
        if row[i] != None:
            dict_row[item[0]] = row[i]
    return dict_row


def mk_row(id, width_factor, sparsity):

    record_id = "{}".format(''.join(random.choices(string.ascii_letters, k=16)))
    col_data = (id, record_id)
    col_count = sf_cols(width_factor)

    # Strings
    for n in range(0, col_count.STR_COLS):

        if random.random() < sparsity:
            col_data += (''.join(random.choices(string.ascii_letters, k=random.randrange(1,128))),)
        else:
            col_data += ("",)

    # Floats
    for n in range(0, col_count.FLOAT_COLS):
        if random.random() < sparsity:
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
            col_data += (int(random.random()*(2**31)) if random.random() < 0.5 else -abs(int(random.random()*(2**31))),)
        else:
            col_data += (None,)


    return col_data


def mk_data(args, width_factor=1, job=1, sparsity=1.0, multiplier=100000, limit=1073741824):

    start = (job-1) * multiplier
    end = job * multiplier
    size_in_ch=0

    data_block = []
    for id in range(start, end):

        data = mk_row(id, width_factor, sparsity)

        row = "|".join([str(x) for x in data])
        size_in_ch += len(row)
        data_block.append(row)

        #if len(data_block) % 1000 == 0:
        #    sys.stdout.write("\r{} bytes".format(size_in_ch))

        if size_in_ch > int(limit):
            write_data(args, data_block)
            size_in_ch = 0
            data_block.clear()

    write_data(args, data_block)

    return data_block

