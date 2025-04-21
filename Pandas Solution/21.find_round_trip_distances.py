'''
You are given a table Routes representing one-way travel distances between cities. Each row in the table contains:
from: the origin city
to: the destination city
dist: the one-way distance from to

Write a query using PySpark or Spark SQL to find all city pairs that have round trips, i.e., a route exists from city A to city B and also from city B to city A.
For each of these city pairs, compute the roundtrip distance, which is the sum of both one-way distances between the cities.
Return the result with:
from (city A) to (city B)
roundtrip_dist (sum of A→B and B→A)
You should return only one direction of each round trip pair (e.g., return only SEA → SF but not SF → SEA).

Input:

    +----+---+----+
    |from| to|dist|
    +----+---+----+
    | SEA| SF| 300|
    | CHI|SEA|2000|
    |  SF|SEA| 300|
    | SEA|CHI|2000|
    | SEA|LND| 500|
    | LND|SEA| 500|
    | LND|CHI|1000|
    | CHI|NDL| 180|
    +----+---+----+

Expected Output:

    +----+---+--------------+
    |from| to|roundtrip_dist|
    +----+---+--------------+
    | SEA| SF|           600|
    | CHI|SEA|          4000|
    | LND|SEA|          1000|
    +----+---+--------------+

'''

import pandas
import pandas as pd

# Sample Data
data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]

columns=["from", "to", "dist"]

# convert the worker list to data frame
df=pd.DataFrame(data,columns=columns)
print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

df=pd.merge(df,df,left_on=["from","to"],right_on=["to","from"],suffixes=("_a","_b"))
df=df[df["from_a"]<df["to_a"]]
df["roundtrip_dist"]=df["dist_a"]+df["dist_b"]
df=df[["from_a","to_a","roundtrip_dist"]]
df.columns=["from","to","roundtrip_dist"]

print(df)


