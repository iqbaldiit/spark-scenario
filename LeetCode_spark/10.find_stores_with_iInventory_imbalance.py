# Source: https://leetcode.com/problems/find-stores-with-inventory-imbalance/solutions/7026586/simple-best-solution-by-iqbaldiit-409p/
'''
	Table: stores

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| store_id    | int     |
	| store_name  | varchar |
	| location    | varchar |
	+-------------+---------+
	store_id is the unique identifier for this table.
	Each row contains information about a store and its location.
	Table: inventory

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| inventory_id| int     |
	| store_id    | int     |
	| product_name| varchar |
	| quantity    | int     |
	| price       | decimal |
	+-------------+---------+
	inventory_id is the unique identifier for this table.
	Each row represents the inventory of a specific product at a specific store.
	Write a solution to find stores that have inventory imbalance - stores where the most expensive product has lower stock than the cheapest product.

	For each store, identify the most expensive product (highest price) and its quantity
	For each store, identify the cheapest product (lowest price) and its quantity
	A store has inventory imbalance if the most expensive product's quantity is less than the cheapest product's quantity
	Calculate the imbalance ratio as (cheapest_quantity / most_expensive_quantity)
	Round the imbalance ratio to 2 decimal places
	Only include stores that have at least 3 different products
	Return the result table ordered by imbalance ratio in descending order, then by store name in ascending order.

	The result format is in the following example.



	Example:

	Input:

	stores table:

	+----------+----------------+-------------+
	| store_id | store_name     | location    |
	+----------+----------------+-------------+
	| 1        | Downtown Tech  | New York    |
	| 2        | Suburb Mall    | Chicago     |
	| 3        | City Center    | Los Angeles |
	| 4        | Corner Shop    | Miami       |
	| 5        | Plaza Store    | Seattle     |
	+----------+----------------+-------------+
	inventory table:

	+--------------+----------+--------------+----------+--------+
	| inventory_id | store_id | product_name | quantity | price  |
	+--------------+----------+--------------+----------+--------+
	| 1            | 1        | Laptop       | 5        | 999.99 |
	| 2            | 1        | Mouse        | 50       | 19.99  |
	| 3            | 1        | Keyboard     | 25       | 79.99  |
	| 4            | 1        | Monitor      | 15       | 299.99 |
	| 5            | 2        | Phone        | 3        | 699.99 |
	| 6            | 2        | Charger      | 100      | 25.99  |
	| 7            | 2        | Case         | 75       | 15.99  |
	| 8            | 2        | Headphones   | 20       | 149.99 |
	| 9            | 3        | Tablet       | 2        | 499.99 |
	| 10           | 3        | Stylus       | 80       | 29.99  |
	| 11           | 3        | Cover        | 60       | 39.99  |
	| 12           | 4        | Watch        | 10       | 299.99 |
	| 13           | 4        | Band         | 25       | 49.99  |
	| 14           | 5        | Camera       | 8        | 599.99 |
	| 15           | 5        | Lens         | 12       | 199.99 |
	+--------------+----------+--------------+----------+--------+
	Output:

	+----------+----------------+-------------+------------------+--------------------+------------------+
	| store_id | store_name     | location    | most_exp_product | cheapest_product   | imbalance_ratio  |
	+----------+----------------+-------------+------------------+--------------------+------------------+
	| 3        | City Center    | Los Angeles | Tablet           | Stylus             | 40.00            |
	| 1        | Downtown Tech  | New York    | Laptop           | Mouse              | 10.00            |
	| 2        | Suburb Mall    | Chicago     | Phone            | Case               | 25.00            |
	+----------+----------------+-------------+------------------+--------------------+------------------+
	Explanation:

	Downtown Tech (store_id = 1):
	Most expensive product: Laptop ($999.99) with quantity 5
	Cheapest product: Mouse ($19.99) with quantity 50
	Inventory imbalance: 5 < 50 (expensive product has lower stock)
	Imbalance ratio: 50 / 5 = 10.00
	Has 4 products (≥ 3), so qualifies
	Suburb Mall (store_id = 2):
	Most expensive product: Phone ($699.99) with quantity 3
	Cheapest product: Case ($15.99) with quantity 75
	Inventory imbalance: 3 < 75 (expensive product has lower stock)
	Imbalance ratio: 75 / 3 = 25.00
	Has 4 products (≥ 3), so qualifies
	City Center (store_id = 3):
	Most expensive product: Tablet ($499.99) with quantity 2
	Cheapest product: Stylus ($29.99) with quantity 80
	Inventory imbalance: 2 < 80 (expensive product has lower stock)
	Imbalance ratio: 80 / 2 = 40.00
	Has 3 products (≥ 3), so qualifies
	Stores not included:
	Corner Shop (store_id = 4): Only has 2 products (Watch, Band) - doesn't meet minimum 3 products requirement
	Plaza Store (store_id = 5): Only has 2 products (Camera, Lens) - doesn't meet minimum 3 products requirement
	The Results table is ordered by imbalance ratio in descending order, then by store name in ascending order
'''
from six import integer_types

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
store_data = [
    ('1', 'Downtown Tech', 'New York')
    ,('2', 'Suburb Mall', 'Chicago')
    ,('3', 'City Center', 'Los Angeles')
    ,('4', 'Corner Shop', 'Miami')
    ,('5', 'Plaza Store', 'Seattle')
]


store_columns = ["store_id", "store_name","location"]

# convert list to data frame
store_df = spark.createDataFrame(store_data,store_columns)

inv_data = [
    ('1', '1', 'Laptop', '5', '999.99')
    ,('2', '1', 'Mouse', '50', '19.99')
    ,('3', '1', 'Keyboard', '25', '79.99')
    ,('4', '1', 'Monitor', '15', '299.99')
    ,('5', '2', 'Phone', '3', '699.99')
    ,('6', '2', 'Charger', '100', '25.99')
    ,('7', '2', 'Case', '75', '15.99')
    ,('8', '2', 'Headphones', '20', '149.99')
    ,('9', '3', 'Tablet', '2', '499.99')
    ,('10', '3', 'Stylus', '80', '29.99')
    ,('11', '3', 'Cover', '60', '39.99')
    ,('12', '4', 'Watch', '10', '299.99')
    ,('13', '4', 'Band', '25', '49.99')
    ,('14', '5', 'Camera', '8', '599.99')
    ,('15', '5', 'Lens', '12', '199.99')
]


inv_columns = ["inventory_id", "store_id","product_name","quantity","price"]

# convert list to data frame
inv_df = spark.createDataFrame(inv_data,inv_columns)

print()
print("==========Input Data=============")

store_df.show()
inv_df.show()

print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)

# find expensive product
exp_product_df=(
    inv_df.groupby("store_id").agg(max(col("price").cast("float")).alias("price")
                                    ,count_distinct("product_name").alias("product_count")).alias("e")
    .where(col("product_count")>2)
    .join(inv_df.alias("i"),on=["store_id","price"])
    .select("e.store_id",col("i.product_name").alias("most_exp_product"),col("i.quantity").alias("ex_qty"))
)

# find cheapst product
cheap_product_df=(
    inv_df.groupby("store_id").agg(min(col("price").cast("float")).alias("price")
                                   ,count_distinct("product_name").alias("product_count")).alias("c")
    .where(col("product_count")>2)
    .join(inv_df.alias("i"),on=["store_id","price"])
    .select("c.store_id",col("i.product_name").alias("cheapest_product"),col("i.quantity").alias("cp_qty"))
)

# final output
df=(
    exp_product_df.alias("e").join(
        cheap_product_df.alias("c"),
        (col("e.store_id") == col("c.store_id")) &
        (col("e.ex_qty") < col("c.cp_qty"))
    ).join(store_df.alias("s"),"store_id")
    .withColumn("imbalance_ratio",round(col("c.cp_qty")/col("e.ex_qty"),2))
    .select("e.store_id","s.store_name","s.location","e.most_exp_product","c.cheapest_product","imbalance_ratio")
    .orderBy(desc("imbalance_ratio"),"s.store_name")
)

df.show()

# # # # #### ================ Approach->2 : (SQL)
# store_df.createOrReplaceTempView("stores")
# inv_df.createOrReplaceTempView("inventory")
#
#
# sSQL="""
#     WITH exp_product AS(
#         SELECT e.store_id, i.product_name AS most_exp_product,CAST(e.price AS FLOAT), i.quantity FROM inventory i
#         INNER JOIN (SELECT store_id,MAX(CAST(price AS FLOAT)) price FROM inventory GROUP BY store_id HAVING COUNT(DISTINCT product_name)>2) e
#         ON e.store_id=i.store_id AND e.price=i.price
#     )
#     , cheap_product AS (
#         SELECT c.store_id, i.product_name AS cheapest_product,CAST(c.price AS FLOAT), i.quantity FROM inventory i
#         INNER JOIN (SELECT store_id,MIN(CAST(price AS FLOAT)) price FROM inventory GROUP BY store_id  HAVING COUNT(DISTINCT product_name)>2) c
#         ON c.store_id=i.store_id AND c.price=i.price
#     )
#     , final AS(
#         SELECT e.store_id,s.store_name,s.location,e.most_exp_product,c.cheapest_product
#         ,ROUND(c.quantity/e.quantity,2) AS imbalance_ratio
#         FROM exp_product e
#         INNER JOIN cheap_product c ON e.store_id=c.store_id
#         INNER JOIN stores s ON e.store_id=s.store_id
#         WHERE e.quantity<c.quantity
#     )
#     SELECT * FROM final ORDER BY imbalance_ratio DESC, store_name
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()