'''
    Title: Monthly Transaction Summary by Country

    You are given a dataset containing transaction information with the following column
    id, country, state, amount,trans_date.

    Write a query for each month and country, return the following aggregated details:

        1. month (in format YYYY-MM)
        2. country
        3. trans_count: total number of transactions
        4. approved_count: number of approved transactions
        5. trans_total_amount: total transaction amount
        6. approved_total_amount: total amount of approved transactions

    Return the result ordered by month ascending, then country ascending.


Input:
    +---+-------+--------+------+----------+
    | id|country|   state|amount|trans_date|
    +---+-------+--------+------+----------+
    |121|     US|approved|  1000|2018-12-18|
    |122|     US|declined|  2000|2018-12-19|
    |123|     US|approved|  2000|2019-01-01|
    |124|     DE|approved|  2000|2019-01-07|
    +---+-------+--------+------+----------+

Expected Output:

    +-------+-------+-----------+--------------+------------------+---------------------+
    |  month|country|trans_count|approved_count|trans_total_amount|approved_total_amount|
    +-------+-------+-----------+--------------+------------------+---------------------+
    |2018-12|     US|          2|             1|              3000|                 1000|
    |2019-01|     DE|          1|             1|              2000|                 2000|
    |2019-01|     US|          1|             1|              2000|                 2000|
    +-------+-------+-----------+--------------+------------------+---------------------+


'''


from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (121, 'US', 'approved', 1000, '2018-12-18'),
    (122, 'US', 'declined', 2000, '2018-12-19'),
    (123, 'US', 'approved', 2000, '2019-01-01'),
    (124, 'DE', 'approved', 2000, '2019-01-07'),
]

columns=['id', 'country', 'state', 'amount', 'trans_date']

# convert list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
df.printSchema()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : ((DSL))

# get month and year extracting date
df=df.withColumn("month",date_format(col("trans_date"),"yyyy-MM"))

df=(df.groupby("month","country").agg(
    count("id").alias("trans_count")
    ,sum(when(col("state")=="approved",1).otherwise(0)).alias("approved_count")
    ,sum(col("amount")).alias("trans_total_amount")
    ,sum(when(col("state")=="approved",col("amount")).otherwise(0)).alias("approved_total_amount")
)).orderBy("month","country")

df.show()


# # # # #### ================ Approach->1 : ((SQL))

# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#         SELECT DATE_FORMAT(trans_date,'yyyy-MM') AS month
#         ,country
#         ,COUNT(*) AS trans_count
#         ,SUM(CASE WHEN state='approved' THEN 1 ELSE 0 END) AS approved_count
#         ,SUM (amount) AS trans_total_amount
#         ,SUM(CASE WHEN state='approved' THEN amount ELSE 0 END) AS approved_total_amount
#         FROM tbl
#         GROUP BY month,country
#         ORDER BY month,country
#
# """
# df=spark.sql(sSQL)
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()