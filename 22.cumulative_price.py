'''
Title: Cumulative Price by Product ID and Date

You are given a table "Sales" with the following schema:
pid, date and price

Each row represents the sale of a product (pid) on a specific date (date) for a certain price (price).
Write a query to generate a new column new_price which represents the cumulative sum of price for each pid, ordered by date.

Input:
    +---+------+-----+
    |pid|  date|price|
    +---+------+-----+
    |  1|26-May|  100|
    |  1|27-May|  200|
    |  1|28-May|  300|
    |  2|29-May|  400|
    |  3|30-May|  500|
    |  3|31-May|  600|
    +---+------+-----+

Expected Output:
    +---+------+-----+---------+
    |pid|  date|price|new_price|
    +---+------+-----+---------+
    |  1|26-May|  100|      100|
    |  1|27-May|  200|      300|
    |  1|28-May|  300|      600|
    |  2|29-May|  400|      400|
    |  3|30-May|  500|      500|
    |  3|31-May|  600|     1100|
    +---+------+-----+---------+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, "26-May", 100),
    (1, "27-May", 200),
    (1, "28-May", 300),
    (2, "29-May", 400),
    (3, "30-May", 500),
    (3, "31-May", 600),
]

columns = ["pid", "date", "price"]

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1: ((DSL))
#
window=Window.partitionBy("pid").orderBy(col("date").asc())
df=df.withColumn("new_price", sum("price").over(window))
df.show()

# # # # # #### ================ Approach->3 : ((SQL))
# df.createOrReplaceTempView("tbl")
# sSQL="""
#     SELECT *
#     , SUM(price) OVER (PARTITION BY pid ORDER BY date ASC) AS new_price
#     FROM tbl
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