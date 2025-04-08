'''
Accenture Interview Scenario:

You are given a dataset containing sales data for different stores across various months. Each row contains the store name,
the month, and the sales amount. Your task is to calculate the cumulative sales for each store, considering the monthly sales, using PySpark.

You should also:
1. Filter out stores with sales lower than 1000 in any month.
2. Calculate the total sales for each store over all months.
3. Sort the results by the total sales in descending order.

schema and dataset
data = [ ("Store A", "2024-01", 800), ("Store A", "2024-02", 1200), ("Store A", "2024-03", 900)
    , ("Store B", "2024-01", 1500), ("Store B", "2024-02", 1600), ("Store B", "2024-03", 1400)
    , ("Store C", "2024-01", 700), ("Store C", "2024-02", 1000), ("Store C", "2024-03", 800) ]

Create DataFrame df = spark.createDataFrame(data,

["Store", "Month", "Sales"])

Input:
        +-------+-------+-----+
        |  Store|  Month|Sales|
        +-------+-------+-----+
        |Store A|2024-01|  800|
        |Store A|2024-02| 1200|
        |Store A|2024-03|  900|
        |Store B|2024-01| 1500|
        |Store B|2024-02| 1600|
        |Store B|2024-03| 1400|
        |Store C|2024-01|  700|
        |Store C|2024-02| 1000|
        |Store C|2024-03|  800|
        +-------+-------+-----+

Expected output:
        +-------+-------+-----+----------------+----------+
        |  Store|  Month|Sales|cumulative_sales|total_sale|
        +-------+-------+-----+----------------+----------+
        |Store B|2024-01| 1500|            1500|      4500|
        |Store B|2024-02| 1600|            3100|      4500|
        |Store B|2024-03| 1400|            4500|      4500|
        +-------+-------+-----+----------------+----------+

Solution Explanation: The problem stated that we have to calculate the cumulative sales per store per month. cumulative sales means
                      running total of the sales over time (in this case it would be month). Imagine Day 1 Sales= 10 USD, Day 2 Sales= 15 USD
                      Day 3 sales =5 USD. So Day 1  cumulative sales = 10 (As no sales before the date), Day 2 cumulative sales=10+15=25
                      (previous cumulative+ today's total sales), Day 3 cumulative sales= 5+25=30.

                      In this case, we have to remove those stores with all records whose one of the sales amount < 1000,
                       and sorting by total sale by store in descending.

Approach :
    1. we have find out the removed stores list considering sale amount <1000
    2. find those records whose store not in removed store list. (Left Anti)
    3. Create a window and calculate for cumulative sales for store by month
    4. find total sale by store using group by
    5. joining with cumulative record set
    6. show result order by total sale descending order.

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [ ("Store A", "2024-01", 800), ("Store A", "2024-02", 1200), ("Store A", "2024-03", 900)
    , ("Store B", "2024-01", 1500), ("Store B", "2024-02", 1600), ("Store B", "2024-03", 1400)
    , ("Store C", "2024-01", 700), ("Store C", "2024-02", 1000), ("Store C", "2024-03", 800) ]

columns = ["Store", "Month", "Sales"]

# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# find out the removed stores list considering sale amount <1000
removed_stores = df.filter(col("Sales") < 1000).select("Store").distinct()
df = df.join(removed_stores, on="Store", how="left_anti")
#df.show()

# create window function for Cumulative sales
window=Window.partitionBy("Store").orderBy("Month").rowsBetween(Window.unboundedPreceding,0)

# calculate cumulative sales
df=df.withColumn("cumulative_sales",sum("sales").over(window))
#df.show()

# find out the total sales
total_sale_df=df.groupBy("Store").agg(sum(col("Sales")).alias("total_sale"))
#total_sale_df.show()

df=df.join(total_sale_df,"Store","inner").orderBy(col("total_sale").desc())
df.show()


# # # # # #### ================ Approach->1 : (SQL))
# df.createOrReplaceTempView("tbl_Sales")
# sSQL="""
#     WITH CTE AS (
#     SELECT * FROM tbl_Sales WHERE Store NOT IN (
#     SELECT Store FROM tbl_Sales WHERE Sales<1000))
#     SELECT * FROM (SELECT *
#     , SUM(Sales) OVER (PARTITION BY Store ORDER BY Month) AS cumulative_sales
#     , SUM(Sales) OVER (PARTITION BY Store) AS total_sales
#     FROM CTE)tab ORDER BY total_sales DESC
#
# """
# df=spark.sql(sSQL)
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()