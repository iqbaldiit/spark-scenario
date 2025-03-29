'''
    You are given a table Sales that contains sales data for different products over multiple years.
    Write a Solution to find the rows corresponding to the highest quantity sold for each product.
    If multiple rows have the same highest quantity for a product, return all of them.
    The result should be sorted in descending order by year and quantity.

input:
    +-------+----------+----+--------+-----+
    |sale_id|product_id|year|quantity|price|
    +-------+----------+----+--------+-----+
    |      1|       100|2010|      25| 5000|
    |      2|       100|2011|      16| 5000|
    |      3|       100|2012|       8| 5000|
    |      4|       200|2010|      10| 9000|
    |      5|       200|2011|      15| 9000|
    |      6|       200|2012|      20| 7000|
    |      7|       300|2010|      20| 7000|
    |      8|       300|2011|      18| 7000|
    |      9|       300|2012|      20| 7000|
    +-------+----------+----+--------+-----+

Expected Output :-

        |sale_id|product_id|year|quantity|price|
        +-------+----------+----+--------+-----+
        |      6|       200|2012|      20| 7000|
        |      9|       300|2012|      20| 7000|
        |      8|       300|2011|      18| 7000|
        |      1|       100|2010|      25| 5000|
        +-------+----------+----+--------+-----+

Solution Explanation: The problem stated that, we have to find out the highest quantity of product sold yearly.
                        The result should be sorted by year and quantity.

Approach-> : (Using window function)
    1. Create a window
    2. Rank the product based on quantity.
    2. choose highest rank.
    3. print

'''
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]

columns = ["sale_id", "product_id", "year","quantity","price"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)

print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (Using window (DSL))

window=Window.partitionBy("year").orderBy(col("quantity").desc())
df=df.withColumn("rank",dense_rank().over(window))
df=df.filter(col("rank")==1).orderBy(col("year").desc(),col("quantity").desc())\
    .select("sale_id","product_id","year","quantity","price")
df.show()

# # # # #### ================ Approach->1 : (Using window (SQL))
# df.createOrReplaceTempView("Sales")
#
# df=spark.sql(" WITH cte AS ("
#              " SELECT *, "
#              " DENSE_RANK() OVER (PARTITION BY year ORDER BY quantity DESC) rk"
#              " FROM Sales)"
#              " SELECT sale_id,product_id,year,quantity,price FROM cte "
#              " WHERE rk=1 ORDER BY year DESC, quantity DESC"
#              )
#
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()