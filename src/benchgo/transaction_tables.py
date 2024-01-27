import random
import string
import boto3
import sys
import uuid

def write_data(args, data):
    obj_id = uuid.uuid4()
    if args.s3out:
        if len(args.s3prefix) > 1:
            if args.s3prefix[-1] != '/':
                s3prefix = args.s3prefix + '/'
            else:
                s3prefix = args.s3prefix
        full_key = s3prefix + "{}".format(obj_id)
            
        
        if args.endpoint:
            session = boto3.session.Session()
            client = session.client(
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
        with open("{dir}/{uuid}".format(args.dirout, uuid=obj_id)) as fh:
            for row in data:
                fh.write("{}\n".format(row))
            fh.close()


def create_transaction_table(width_factor=1, table_path="trns_tbl"):

    ddl = "CREATE TABLE {table} (".format(table=table_path)

    columns = []
    # index/id
    columns.append("id BIGINT")

    # String record ID
    columns.append("record_id VARCHAR(255)")

    # String data types:
    str_cols = (width_factor*1) if (width_factor*1 <= 20) else 20
    for n in range(0, str_cols):
        columns.append("str_val_{} VARCHAR(255)".format(n))

    # Float data types:
    for n in range(0, width_factor*5):
        columns.append("float_val_{} REAL".format(n))
    
    # Boolean data types:
    for n in range(0, width_factor*2):
        columns.append("bool_val_{} BOOLEAN".format(n))
    
    # Integer data types:
    for n in range(0, width_factor*3):
        columns.append("int_val_{} INTEGER".format(n))

    ddl = ddl + ",".join(columns) + ")"

    print(ddl)


def mk_data(args, width_factor=1, jobs=1, job=1, sparsity=1.0, multiplier=100000, limit=1073741824):

    records = jobs*multiplier

    r10000 = 10000/records
    r1000 = 1000/records
    r100 = 100/records

    start = (job-1) * multiplier
    end = job * multiplier
    size_in_ch=0

    data_block = []
    for id in range(start, end):

        prefix = None
        if random.random() < r100:
            prefix = "r100xx"
        elif random.random() < r1000+r100:
            prefix = "r1000x"
        elif random.random() < r10000+r1000:
            prefix = "r10000"

        record_id = "{}".format(''.join(random.choices(string.ascii_letters, k=16)))

        if prefix:
            record_id = "{}{}".format(prefix, record_id[6:])
            #print(record_id)

        row = "{id}|{record_id}|".format(
            id=id,
            record_id=record_id
        )

        col_data = []

        # Strings
        str_cols = (width_factor*1) if (width_factor*1 <= 20) else 20
        for n in range(0, str_cols):

            if random.random() < sparsity:
                col_data.append("{}".format(''.join(random.choices(string.ascii_letters, k=random.randrange(1,128)))))
            else:
                col_data.append("")

        # Floats
        for n in range(0, width_factor*5):
            if random.random() < sparsity:
                col_data.append("{}".format(random.random()*10000))
            else:
                col_data.append("")
   
    
        # Boolean data types:
        for n in range(0, width_factor*2):
            if random.random() < sparsity:
                col_data.append("{}".format("true" if random.random() < 0.5 else "false"))
            else:
                col_data.append("")
        
        # Integer data types:
        for n in range(0, width_factor*3):
            if random.random() < sparsity:
                col_data.append("{}".format(
                    int(random.random()*(2**31)) if random.random() < 0.5 else -abs(int(random.random()*(2**31)))
                ))
            else:
                col_data.append("")
        
        row += "|".join(col_data)
        size_in_ch += len(row)

        data_block.append(row)

        if len(data_block) % 1000 == 0:
            sys.stdout.write("\r{} bytes".format(size_in_ch))

        if size_in_ch > int(limit):
            write_data(args, data_block)
            size_in_ch = 0
            data_block.clear()

    write_data(args, data_block)
    return data_block

