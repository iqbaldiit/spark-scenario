'''
    Title: Top K Customers by Total Spend

    You are given a dataset containing customer transaction details. Each transaction record consists of a customer_id and
    a transaction_amount. Your task is to find the top 3 customers who spent the most, ranked by total spend in descending order.


Input:
    +-----------+------------------+
    |customer_id|transaction_amount|
    +-----------+------------------+
    |       C001|               500|
    |       C002|              1000|
    |       C003|               750|
    |       C001|               300|
    |       C002|               500|
    |       C003|               250|
    |       C004|               200|
    +-----------+------------------+

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
    ('C001', 500), ('C002', 1000), ('C003', 750),
    ('C001', 300), ('C002', 500), ('C003', 250), ('C004', 200)
]

columns=["customer_id", "transaction_amount"]

# convert list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
df.printSchema()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))

df=(df.groupby("customer_id")
    .agg(sum("transaction_amount").alias("total_spend"))
    .orderBy(col("total_spend").desc())
    .limit(3)
)

df.show()


# # # # # #### ================ Approach->1 : ((SQL))
#
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#         SELECT customer_id,SUM(transaction_amount) AS total_spend FROM tbl
#         GROUP BY customer_id
#         ORDER BY total_spend DESC
#         LIMIT 3
#
# """
# df=spark.sql(sSQL)
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()