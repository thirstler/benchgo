#!/bin/python3

'''
Convert raw query streams to json
'''
import json, os

engines=['trino', 'dremio', 'spark']
benchmarks=["tpcds"]
scale_factors=['sf1', 'sf10', 'sf100', 'sf1000', 'sf10000', 'sf100000']
query_streams=[1, 2, 4, 8]

for engine in engines:
    for benchmark in benchmarks:
        for scale_factor in scale_factors:
            for query_stream in query_streams:
                for stream in range(0, query_stream):
                    infile = f'./{engine}/{benchmark}/{scale_factor}/{query_stream}/query_{stream}.sql'
                    if not os.path.isfile(infile):
                        print(f"not present: {infile}")
                        continue
                    with open(infile, 'r') as rh:
                        query_text=rh.read()
                        queries=query_text.split(';')
                        with open(f'./{engine}/{benchmark}/{scale_factor}/{query_stream}/query_{stream}.json', 'w') as wh:
                            wh.write(json.dumps(queries[:-1]))
                        
                    print(f"processed: {infile}")
