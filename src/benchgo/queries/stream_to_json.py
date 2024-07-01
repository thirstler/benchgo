#!/bin/python3

import sys, json, re
fh=open(sys.argv[1], 'r')
query_text=fh.read()
queries=query_text.split(';')
print(json.dumps(queries[:-1]))
