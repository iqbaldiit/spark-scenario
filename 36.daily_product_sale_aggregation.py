'''
Title: Group Products Sold by Date

Given a table of daily product sales, write a query to:
1. Group products sold on the same date into an array
2. Count the number of products sold each day (including duplicates)
3. Order the results by sell_date

Input:
    +------------+------------+
    | sell_date  | product    |
    +------------+------------+
    | 2020-05-30 | Headphone  |
    | 2020-06-01 | Pencil     |
    | 2020-06-02 | Mask       |
    | 2020-05-30 | Basketball |
    | 2020-06-01 | Book       |
    | 2020-06-02 | Mask       |
    | 2020-05-30 | T-Shirt    |
    +------------+------------+

Expected Output:

    +------------+---------------------------+-----------+
    | sell_date  | products                 | num_sold  |
    +------------+---------------------------+-----------+
    | 2020-05-30 | [T-Shirt, Basketball]    | 3         |
    | 2020-06-01 | [Pencil, Book]           | 2         |
    | 2020-06-02 | [Mask]                   | 1         |
    +------------+---------------------------+-----------+


Explanation:
    1. 2020-05-30: 3 products sold (T-Shirt, Basketball, Headphone)
    2. 2020-06-01: 2 products sold (Pencil, Book)
    3. 2020-06-02: 1 unique product sold (Mask appears twice but counted once)
    4. Products are sorted alphabetically in the array

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Book"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

columns = ["sell_date", "product"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns)


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
df=df.groupBy("sell_date").agg(
    collect_set("product").alias("products")
    ,count_distinct("product").alias("num_sold")
).orderBy("sell_date")

df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#     SELECT sell_date
#     ,COLLECT_SET(product) AS products
#     ,COUNT(DISTINCT product) AS num_sold
#     FROM tbl GROUP BY sell_date
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


















