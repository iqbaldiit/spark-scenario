# Source: https://leetcode.com/problems/find-product-recommendation-pairs/solutions/6945275/simple-best-solution-by-iqbaldiit-t2jc/
'''
	Table: ProductPurchases

	+-------------+------+
	| Column Name | Type |
	+-------------+------+
	| user_id     | int  |
	| product_id  | int  |
	| quantity    | int  |
	+-------------+------+
	(user_id, product_id) is the unique identifier for this table.
	Each row represents a purchase of a product by a user in a specific quantity.
	Table: ProductInfo

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| product_id  | int     |
	| category    | varchar |
	| price       | decimal |
	+-------------+---------+
	product_id is the unique identifier for this table.
	Each row assigns a category and price to a product.
	Amazon wants to understand shoPPng patterns across product categories. Write a solution to:

	Find all category pairs (where category1 < category2)
	For each category pair, determine the number of unique customers who purchased products from both categories
	A category pair is considered reportable if at least 3 different customers have purchased products from both categories.

	Return the result table of reportable category pairs ordered by customer_count in descending order, and in case of a tie, by category1 in ascending order lexicographically, and then by category2 in ascending order.

	The result format is in the following example.



	Example:

	Input:

	ProductPurchases table:

	+---------+------------+----------+
	| user_id | product_id | quantity |
	+---------+------------+----------+
	| 1       | 101        | 2        |
	| 1       | 102        | 1        |
	| 1       | 201        | 3        |
	| 1       | 301        | 1        |
	| 2       | 101        | 1        |
	| 2       | 102        | 2        |
	| 2       | 103        | 1        |
	| 2       | 201        | 5        |
	| 3       | 101        | 2        |
	| 3       | 103        | 1        |
	| 3       | 301        | 4        |
	| 3       | 401        | 2        |
	| 4       | 101        | 1        |
	| 4       | 201        | 3        |
	| 4       | 301        | 1        |
	| 4       | 401        | 2        |
	| 5       | 102        | 2        |
	| 5       | 103        | 1        |
	| 5       | 201        | 2        |
	| 5       | 202        | 3        |
	+---------+------------+----------+
	ProductInfo table:

	+------------+-------------+-------+
	| product_id | category    | price |
	+------------+-------------+-------+
	| 101        | Electronics | 100   |
	| 102        | Books       | 20    |
	| 103        | Books       | 35    |
	| 201        | Clothing    | 45    |
	| 202        | Clothing    | 60    |
	| 301        | Sports      | 75    |
	| 401        | Kitchen     | 50    |
	+------------+-------------+-------+
	Output:

	+-------------+-------------+----------------+
	| category1   | category2   | customer_count |
	+-------------+-------------+----------------+
	| Books       | Clothing    | 3              |
	| Books       | Electronics | 3              |
	| Clothing    | Electronics | 3              |
	| Electronics | Sports      | 3              |
	+-------------+-------------+----------------+
	Explanation:

	Books-Clothing:
	User 1 purchased products from Books (102) and Clothing (201)
	User 2 purchased products from Books (102, 103) and Clothing (201)
	User 5 purchased products from Books (102, 103) and Clothing (201, 202)
	Total: 3 customers purchased from both categories
	Books-Electronics:
	User 1 purchased products from Books (102) and Electronics (101)
	User 2 purchased products from Books (102, 103) and Electronics (101)
	User 3 purchased products from Books (103) and Electronics (101)
	Total: 3 customers purchased from both categories
	Clothing-Electronics:
	User 1 purchased products from Clothing (201) and Electronics (101)
	User 2 purchased products from Clothing (201) and Electronics (101)
	User 4 purchased products from Clothing (201) and Electronics (101)
	Total: 3 customers purchased from both categories
	Electronics-Sports:
	User 1 purchased products from Electronics (101) and Sports (301)
	User 3 purchased products from Electronics (101) and Sports (301)
	User 4 purchased products from Electronics (101) and Sports (301)
	Total: 3 customers purchased from both categories
	Other category pairs like Clothing-Sports (only 2 customers: Users 1 and 4) and Books-Kitchen (only 1 customer: User 3) have fewer than 3 shared customers and are not included in the result.
	The result is ordered by customer_count in descending order. Since all pairs have the same customer_count of 3, they are ordered by category1 (then category2) in ascending order.
'''
from six import integer_types

from spark_session import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
product_purchases_data = [
    ('1', '101', '2'),('1', '102', '1'),('1', '103', '3')
    ,('2', '101', '1'),('2', '102', '5'),('2', '104', '1')
    ,('3', '101', '2'),('3', '103', '1'),('3', '105', '4')
    ,('4', '101', '1'),('4', '102', '1'),('4', '103', '2')
    ,('4', '104', '3'),('5', '102', '2'),('5', '104', '1')
]


product_purchases_columns = ["user_id", "product_id","quantity"]

# convert list to data frame
product_purchases_df = spark.createDataFrame(product_purchases_data,product_purchases_columns).cache()

# Sample data
product_info_data = [
    ('101', 'Electronics', '100'),('102', 'Books', '20'),('103', 'Clothing', '35')
    ,('104', 'Kitchen', '50'),('105', 'Sports', '75')
]

product_info_columns=["product_id","category","price"]

# convert list to data frame
product_info_df = spark.createDataFrame(product_info_data,product_info_columns).cache()

print()
print("==========Input Data=============")

product_info_df.show()
product_purchases_df.show()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
user_products_df= (product_purchases_df.join(product_info_df,'product_id')
     .select('user_id','product_id','category').distinct()
)

df=(user_products_df.alias("uc1").join(user_products_df.alias("uc2"),"user_id")
    .where(col("uc1.product_id")<col("uc2.product_id"))
    .select(col("user_id")
            ,col("uc1.product_id").alias("product1_id")
            ,col("uc2.product_id").alias("product2_id")
            ,col("uc1.category").alias("product1_category")
            ,col("uc2.category").alias("product2_category"))
)

df=(df.groupby(col('product1_id'),col('product2_id'),col('product1_category'),col('product2_category'))
    .agg(countDistinct(col('user_id')).alias('customer_count'))
    .where(col('customer_count')>=3)
    .orderBy(desc('customer_count'),'product1_id','product2_id')
)
df.show()

# # # # #### ================ Approach->2 : (SQL)
# product_purchases_df.createOrReplaceTempView("ProductPurchases")
# product_info_df.createOrReplaceTempView("ProductInfo")
#
# sSQL="""
#     WITH UserProducts AS (
#         SELECT DISTINCT PP.user_id,PP.product_id, P.category FROM ProductPurchases PP
#         INNER JOIN ProductInfo P ON PP.product_id = P.product_id
#     ),ProductPairs AS(
#         SELECT	UP1.product_id AS product1_id
#             ,UP2.product_id AS product2_id
#             ,UP1.category AS product1_category
#             ,UP2.category AS product2_category
#             ,COUNT(UP1.user_id) customer_count
#         FROM UserProducts UP1
#         INNER JOIN UserProducts UP2 ON UP1.user_id=UP2.user_id AND UP1.product_id<UP2.product_id
#         GROUP BY UP1.product_id,UP2.product_id,UP1.category,UP2.category
#         HAVING COUNT(UP1.user_id)>=3
#     )
#     SELECT * FROM ProductPairs ORDER BY customer_count DESC,product1_id ASC,product2_id ASC;
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()