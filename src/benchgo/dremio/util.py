def dremio_args(parser) -> None:
    parser.add_argument("--coordinator", help="Trino coordinator URI (e.g.: http://dremio_coord:8080)")
    parser.add_argument("--password", default=None, help="Dremio password NOT IMPLIMENTED")
    parser.add_argument("--user", default='admin', help="Dremio username")
    parser.add_argument("--catalog", help="catalog housing target tpcds database that matches the selected scale factor")
    parser.add_argument("--schema", help="schema housing target tpcds database that matches the selected scale factor")