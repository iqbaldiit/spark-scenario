'''
TCS Interview Scenario:

You are given a table called Customer with the following columns:
cust_id,cust_name,cust_phone,cust_email

You need to remove duplicates and keep the minimum possible number of unique customer records
(i.e., deduplicate based on repeated values).
Assume a duplicate is defined by having the same cust_name, cust_phone, and cust_email.
In case of duplicates, keep the record with the smallest cust_id.

Input:
    +-------+---------+----------+-----------------+
    |cust_id|cust_name|cust_phone|       cust_email|
    +-------+---------+----------+-----------------+
    |    101|    Alice|1234567890|  alice@email.com|
    |    102|      Bob|2345678901|    bob@email.com|
    |    103|    Alice|1234567890|  alice@email.com|
    |    104|  Charlie|3456789012|charlie@email.com|
    |    105|      Bob|2345678901|    bob@email.com|
    |    106|    David|4567890123|  david@email.com|
    +-------+---------+----------+-----------------+

Expected output:
    +-------+---------+----------+-----------------+
    |cust_id|cust_name|cust_phone|       cust_email|
    +-------+---------+----------+-----------------+
    |    101|    Alice|1234567890|  alice@email.com|
    |    102|      Bob|2345678901|    bob@email.com|
    |    104|  Charlie|3456789012|charlie@email.com|
    |    106|    David|4567890123|  david@email.com|
    +-------+---------+----------+-----------------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (101, "Alice", "1234567890", "alice@email.com"),
    (102, "Bob", "2345678901", "bob@email.com"),
    (103, "Alice", "1234567890", "alice@email.com"),
    (104, "Charlie", "3456789012", "charlie@email.com"),
    (105, "Bob", "2345678901", "bob@email.com"),
    (106, "David", "4567890123", "david@email.com"),
]

columns = ["cust_id", "cust_name", "cust_phone","cust_email"]

# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

#create window partition by duplicates and order by cust_id
window=Window.partitionBy("cust_name", "cust_phone", "cust_email").orderBy(col("cust_id").asc())

# find the row number within thw window
df=df.withColumn("row_id",row_number().over(window))

# keep the first row id as the requirement is keeping small cust_id
df=df.filter(df.row_id==1)

# drop column row_id
df=df.drop("row_id")
df.show()



# # # # # # #### ================ Approach->2 : (SQL))
# df.createOrReplaceTempView("tbl")
# sSQL="""
#     WITH CTE AS (
#         SELECT *
#         ,ROW_NUMBER() OVER (PARTITION BY cust_name,cust_phone,cust_email ORDER BY cust_id ASC) AS row_id
#         FROM tbl
#     )
#     SELECT cust_id,cust_name,cust_phone,cust_email FROM CTE
# """
# df=spark.sql(sSQL)
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()