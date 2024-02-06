from vastdb import vastdb_api
import pyarrow as pa
from vastdb.vastdb_api import VastdbApi


def get_conn(args):
    return VastdbApi(
        host=args.vast_ips, 
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=False,
        auth_type=vastdb_api.AuthType.SIGV4,
    )


def run(args):
    
    vdb_conn=get_conn(args)

    ##
    # Insert tests
    pass
