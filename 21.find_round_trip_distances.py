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

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

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
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1: ((DSL))

df=(df.alias("A").join(df.alias("B"),(col("A.from")==col("B.to"))
                       & (col("A.to")==col("B.from")))
    .where(col("A.from")<col("A.to"))
    .select("A.from","A.to",(col("A.dist")+col("B.dist")).alias("roundtrip_dist"))
    .orderBy(col("roundtrip_dist").asc())
)

df.show()



# # # # # #### ================ Approach->3 : ((SQL))
# df.createOrReplaceTempView("tbl")
# sSQL="""
#     SELECT *  FROM (
#     SELECT A.from, A.to, A.dist+B.dist AS roundtrip_dist
#     FROM tbl A
#     JOIN tbl B ON A.from=B.to AND A.to=B.from
#     WHERE A.from<A.to)tab
#     ORDER BY roundtrip_dist
# """
#
# df=spark.sql(sSQL)
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()