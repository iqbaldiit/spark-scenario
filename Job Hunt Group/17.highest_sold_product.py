'''
    Title: Find Employee who incremented

You are given two tables: Sales and Product. Each row represents a sale transaction of a product in a specific year.
The total sale value for a row is calculated as: quantity * price

Question 1: Total Sales by Product
    Write a query to calculate the total sales amount of each product across all years.
    Requirements:
        1. Total sales = SUM(quantity * price)
        2. Return product_id, product_name, and total_sales
        3. Include products even if they have no sales (total sales should be 0)
        4. Order the result by product_id

Question 2: Top Product Sold in Each Year
    Write a query to find the top-selling product for each year.
    Requirements:
        1. Top-selling product is defined as the product with the highest total quantity sold in that year
        2. If multiple products have the same highest quantity, return all of them
        3. Return year, product_id, product_name, and total_quantity
        4. Order the result by year ascending

Input:
    +-------+----------+----+--------+-----+
    |sale_id|product_id|year|quantity|price|
    +-------+----------+----+--------+-----+
    |      1|       100|2010|      25| 5000|
    |      2|       100|2011|      16| 5000|
    |      3|       100|2012|       8| 5000|
    |      4|       200|2010|      10| 9000|
    |      5|       200|2011|      15| 9000|
    |      6|       200|2012|      20| 9000|
    |      7|       300|2010|      20| 7000|
    |      8|       300|2011|      18| 7000|
    |      9|       300|2012|      20| 7000|
    +-------+----------+----+--------+-----+

    +----------+------------+
    |product_id|product_name|
    +----------+------------+
    |       100|       Nokia|
    |       200|      IPhone|
    |       300|     Samsung|
    |       400|          LG|
    +----------+------------+

Expected Output Question 1:

    +----------+------------+-----------+
    |product_id|product_name|total_sales|
    +----------+------------+-----------+
    |       100|       Nokia|     245000|
    |       200|      IPhone|     405000|
    |       300|     Samsung|     406000|
    +----------+------------+-----------+

Expected Output Question 1:

    +----+----------+------------+---------+
    |year|product_id|product_name|total_qty|
    +----+----------+------------+---------+
    |2010|       100|       Nokia|       25|
    |2011|       300|     Samsung|       18|
    |2012|       200|      IPhone|       20|
    |2012|       300|     Samsung|       20|
    +----+----------+------------+---------+


'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

# Sales DataFrame
sales_data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 9000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]

sales_columns = ["sale_id", "product_id", "year", "quantity", "price"]
sales_df = spark.createDataFrame(sales_data, sales_columns)

# Product DataFrame
product_data = [
    (100, "Nokia"),
    (200, "IPhone"),
    (300, "Samsung"),
    (400, "LG")
]

product_columns = ["product_id", "product_name"]
product_df = spark.createDataFrame(product_data, product_columns)

print()
print("==========Input Data=============")

sales_df.show()
product_df.show()
print()
print("==========Expected output for Question 1 =============")

# # # # #### ================ Approach->1 : (DSL)

total_sale_df=(sales_df.groupby("product_id")
       .agg(sum(col("quantity")*col("price")).alias("total_sales"))
)

q1_df=(total_sale_df.alias("ts").join(product_df.alias("p"),"product_id")
       .select(col("p.product_id")
               ,col("p.product_name")
               ,col("ts.total_sales")
       ).fillna(0)
       .orderBy(col("product_id").asc())
)
q1_df.show()

# # # # # #### ================ Approach->2 : ((SQL))



print()
print("==========Expected output for Question 2 =============")

# # # # #### ================ Approach->1 : (DSL)

yearly_sale_df=(sales_df.groupby("year","product_id")
               .agg(sum(col("quantity")).alias("total_qty"))
               )

# create window for rank
rk_window=Window.partitionBy("year").orderBy(desc("total_qty"))

highest_sold_df=(
    yearly_sale_df
    .withColumn("rank",dense_rank().over(rk_window))
    .where(col("rank")==1)
)

q2_df=(
    highest_sold_df.join(product_df,"product_id")
    .select("year", "product_id", "product_name", "total_qty")
    .orderBy(col("year"))
)

q2_df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()