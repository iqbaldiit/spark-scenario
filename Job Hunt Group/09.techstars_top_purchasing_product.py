'''
    Title: Top Purchasing Customers Per Store

    You are given a table "Sales" with the with store_id, customer_id   and  purchase_amt Column

    Write a SQL query to find the top purchasing customer(s) for each store based on their total purchase amount.
    If multiple customers have the same highest purchase amount for a store, include all of them.


Input:
    +--------+-----------+------------+
    |store_id|customer_id|purchase_amt|
    +--------+-----------+------------+
    |       1|        101|         500|
    |       1|        101|         300|
    |       1|        102|         500|
    |       2|        101|        1000|
    |       2|        102|         900|
    |       3|        101|        1000|
    |       3|        102|        1000|
    +--------+-----------+------------+

Expected Output:

    +--------+-----------+------------------+
    |store_id|customer_id|total_purchase_amt|
    +--------+-----------+------------------+
    |       1|        101|               800|
    |       2|        101|              1000|
    |       3|        102|              1000|
    |       3|        101|              1000|
    +--------+-----------+------------------+

'''


from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, 101, 500), (1, 101, 300), (1, 102, 500),
    (2, 101, 1000), (2, 102, 900),
    (3, 101, 1000), (3, 102, 1000)
]

columns=["store_id", "customer_id", "purchase_amt"]

# convert list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
df.printSchema()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))

# summation of purchase amount based on customer and store
df=(df.groupby("store_id","customer_id")
    .agg(sum("purchase_amt").alias("total_purchase_amt"))
    .orderBy("store_id")
)

# make rank to find out the top purchaser per store
window=Window.partitionBy(col("store_id")).orderBy(col("total_purchase_amt").desc())
df=(df.withColumn("rk",dense_rank().over(window))
    .where(col("rk")==1)
    .drop("rk")
)
df.show()


# # # # #### ================ Approach->1 : ((SQL))

# df.createOrReplaceTempView("Sales")
#
# sSQL="""
#     WITH tab AS (
#         SELECT store_id, customer_id, sum (purchase_amt) AS total_purchase_amt
#         FROM Sales GROUP BY store_id, customer_id
#     ),tab_rk AS (
#         SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY total_purchase_amt DESC) AS rk
#         FROM  tab
#     )
#     SELECT store_id, customer_id, total_purchase_amt FROM tab_rk WHERE rk=1
#     ORDER BY store_id
# """
# df=spark.sql(sSQL)
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()