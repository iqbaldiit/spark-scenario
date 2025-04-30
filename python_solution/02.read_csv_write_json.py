'''
Python - Write a script that reads a large CSV file in chunks (e.g., 1000 rows at a time),
converts each chunk to JSON, and writes it to a new JSON file (one JSON array per chunk).
'''

import pandas as pd

nSize=0
for chunk in pd.read_csv("<your csv file path>",chunksize=1000):
    sJson_file_name="../fileName_"+str(nSize)+".json"
    chunk.to_json(sJson_file_name, orient='records', indent=2)
    nSize+=1
