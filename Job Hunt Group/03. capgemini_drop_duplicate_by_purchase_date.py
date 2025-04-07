'''
Capgemini Interview Question:

You are provided with a DataFrame containing customer purchase data.
Your task is to: Identify and remove duplicate rows based on the customer_id column.
If duplicates exist for a customer_id, retain the row with the most recent purchase_date.

schema and dataset
data = [(1, "Laptop", "2024-08-01"), (1, "Mouse", "2024-08-05"), (2, "Keyboard", "2024-08-02"), (2, "Monitor", "2024-08-03")]
columns = ["customer_id", "product", "purchase_date"]

Input:

        +-----------+--------+-------------+
        |customer_id| product|purchase_date|
        +-----------+--------+-------------+
        |          1|  Laptop|   2024-08-01|
        |          1|   Mouse|   2024-08-05|
        |          2|Keyboard|   2024-08-02|
        |          2| Monitor|   2024-08-03|
        +-----------+--------+-------------+

Output:

        +-----------+-------+-------------+
        |customer_id|product|purchase_date|
        +-----------+-------+-------------+
        |          1|  Mouse|   2024-08-05|
        |          2|Monitor|   2024-08-03|
        +-----------+-------+-------------+

Solution Explanation: The problem stated that, we have to keep only the last record of the customer and rest would be deleted.
                    if there is multiple record by the same date same customer keep only one.

Approach:
    1. create a window for make serial number based on purchase date from last for a particular customer.
    2. filter record whose serial number is first
    3. print result.
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [(1, "Laptop", "2024-08-01"), (1, "Mouse", "2024-08-05"), (2, "Keyboard", "2024-08-02"), (2, "Monitor", "2024-08-03")]
columns = ["customer_id", "product", "purchase_date"]

# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)
#
# create window
window=Window.partitionBy("customer_id").orderBy(col("purchase_date").desc())

# find row serial number of the dataset based on purchase date per customer
df=df.withColumn("sl",row_number().over(window))

# keep the first rows for each customer
df=df.filter(col("sl")==1).select("customer_id","product","purchase_date")
df.show()

# # # # #### ================ Approach->1 : (SQL))
# df.createOrReplaceTempView("Purchase")
# sSQL="""
#     WITH CTE AS (
#     SELECT *
#         , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY purchase_date DESC) AS SL
#     FROM Purchase)
#     SELECT customer_id,product,purchase_date FROM CTE WHERE SL=1
# """
# df=spark.sql(sSQL)
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()