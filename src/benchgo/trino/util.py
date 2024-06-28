from trino.dbapi import connect
from trino.transaction import IsolationLevel
from trino.auth import BasicAuthentication
from urllib.parse import urlparse
from benchgo.trino import DEFAULT_TRINO_SESSION


def connection(args):

    endpoint = urlparse(args.coordinator)
    if endpoint.scheme == "https" and args.password != None:
        authscheme=BasicAuthentication(args.user, args.password)
    else:
        authscheme=None

    if args.session_properties:
        session_prop = eval(args.session_properties) if type(args.session_properties) == str else args.session_properties
    else:
        session_prop = DEFAULT_TRINO_SESSION

    conn = connect(
        host=endpoint.netloc.split(":")[0],
        port=endpoint.netloc.split(":")[1],
        user=args.user,
        auth=authscheme,
        catalog=args.catalog,
        schema=args.schema,
        session_properties=session_prop,
        isolation_level=IsolationLevel.AUTOCOMMIT
    )
    cur = conn.cursor()
    return cur


def trino_args(parser) -> None:
    parser.add_argument("--coordinator", help="Trino coordinator URI (e.g.: http://trino_coord:8080)")
    parser.add_argument("--password", default=None, help="Trino password NOT IMPLIMENTED")
    parser.add_argument("--user", default='admin', help="Trino username (requires unauthenticated requests atm)")
    parser.add_argument("--catalog", help="catalog housing target tpcds database that matches the selected scale factor")
    parser.add_argument("--schema", help="schema housing target tpcds database that matches the selected scale factor")
    parser.add_argument("--session-properties", default=DEFAULT_TRINO_SESSION, help="override default session settings (e.g.: {\"task_concurrency\": 64})")
