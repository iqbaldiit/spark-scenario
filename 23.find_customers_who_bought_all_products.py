'''
Title: Find Customers Who Bought All Products

Question:

You are given two tables:

1. "CustomerProduct" that contains the columns customer_id and product_key, indicating which product each customer has purchased.
2. "Product" that contains all available product_keys in the store.

Write a query to return the IDs of customers who have purchased all the products listed in the Product table.
Return the result in any order.

input:
        +-----------+-----------+
        |customer_id|product_key|
        +-----------+-----------+
        |          1|          5|
        |          2|          6|
        |          3|          5|
        |          3|          6|
        |          1|          6|
        +-----------+-----------+
        +-----------+
        |product_key|
        +-----------+
        |          5|
        |          6|
        +-----------+

Expected Output:
        +-----------+
        |customer_id|
        +-----------+
        |          1|
        |          3|
        +-----------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# start timer to see execution time
start_timer()

# Sample Data
customer_data = [
    (1, 5),
    (2, 6),
    (3, 5),
    (3, 6),
    (1, 6),
]
product_data = [(5,), (6,)]

# Create DataFrames
df_customer = spark.createDataFrame(customer_data, ["customer_id", "product_key"])
df_product = spark.createDataFrame(product_data, ["product_key"])

print()
print("==========Input Data=============")

df_customer.show()
df_product.show()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1: ((DSL))
# get the total distinct product count to filter
total_product=df_product.distinct().count()
# print(total_product)

df=(df_customer.groupby("customer_id")
    .agg(count_distinct("product_key").alias("p_count"))
    .where(col("p_count")==total_product)
    .select("customer_id")
)

df.show()

# # # # # #### ================ Approach->3 : ((SQL))
# df_customer.createOrReplaceTempView("Customer")
# df_product.createOrReplaceTempView("Product")
# sSQL="""
#
#     SELECT customer_id FROM Customer
#     GROUP BY customer_id
#     HAVING COUNT(DISTINCT product_key)=(SELECT COUNT(*) FROM Product)
#
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