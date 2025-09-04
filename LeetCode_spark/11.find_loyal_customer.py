# Source:
'''
	Table: customer_transactions

	+------------------+---------+
	| Column Name      | Type    |
	+------------------+---------+
	| transaction_id   | int     |
	| customer_id      | int     |
	| transaction_date | date    |
	| amount           | decimal |
	| transaction_type | varchar |
	+------------------+---------+
	transaction_id is the unique identifier for this table.
	transaction_type can be either 'purchase' or 'refund'.
	Write a solution to find loyal customers. A customer is considered loyal if they meet ALL the following criteria:

	Made at least 3 purchase transactions.
	Have been active for at least 30 days.
	Their refund rate is less than 20% .
	Return the result table ordered by customer_id in ascending order.

	The result format is in the following example.



	Example:

	Input:

	customer_transactions table:

	+----------------+-------------+------------------+--------+------------------+
	| transaction_id | customer_id | transaction_date | amount | transaction_type |
	+----------------+-------------+------------------+--------+------------------+
	| 1              | 101         | 2024-01-05       | 150.00 | purchase         |
	| 2              | 101         | 2024-01-15       | 200.00 | purchase         |
	| 3              | 101         | 2024-02-10       | 180.00 | purchase         |
	| 4              | 101         | 2024-02-20       | 250.00 | purchase         |
	| 5              | 102         | 2024-01-10       | 100.00 | purchase         |
	| 6              | 102         | 2024-01-12       | 120.00 | purchase         |
	| 7              | 102         | 2024-01-15       | 80.00  | refund           |
	| 8              | 102         | 2024-01-18       | 90.00  | refund           |
	| 9              | 102         | 2024-02-15       | 130.00 | purchase         |
	| 10             | 103         | 2024-01-01       | 500.00 | purchase         |
	| 11             | 103         | 2024-01-02       | 450.00 | purchase         |
	| 12             | 103         | 2024-01-03       | 400.00 | purchase         |
	| 13             | 104         | 2024-01-01       | 200.00 | purchase         |
	| 14             | 104         | 2024-02-01       | 250.00 | purchase         |
	| 15             | 104         | 2024-02-15       | 300.00 | purchase         |
	| 16             | 104         | 2024-03-01       | 350.00 | purchase         |
	| 17             | 104         | 2024-03-10       | 280.00 | purchase         |
	| 18             | 104         | 2024-03-15       | 100.00 | refund           |
	+----------------+-------------+------------------+--------+------------------+
	Output:

	+-------------+
	| customer_id |
	+-------------+
	| 101         |
	| 104         |
	+-------------+
	Explanation:

	Customer 101:
	Purchase transactions: 4 (IDs: 1, 2, 3, 4)
	Refund transactions: 0
	Refund rate: 0/4 = 0% (less than 20%)
	Active period: Jan 5 to Feb 20 = 46 days (at least 30 days)
	Qualifies as loyal
	Customer 102:
	Purchase transactions: 3 (IDs: 5, 6, 9)
	Refund transactions: 2 (IDs: 7, 8)
	Refund rate: 2/5 = 40% (exceeds 20%)
	Not loyal
	Customer 103:
	Purchase transactions: 3 (IDs: 10, 11, 12)
	Refund transactions: 0
	Refund rate: 0/3 = 0% (less than 20%)
	Active period: Jan 1 to Jan 3 = 2 days (less than 30 days)
	Not loyal
	Customer 104:
	Purchase transactions: 5 (IDs: 13, 14, 15, 16, 17)
	Refund transactions: 1 (ID: 18)
	Refund rate: 1/6 = 16.67% (less than 20%)
	Active period: Jan 1 to Mar 15 = 73 days (at least 30 days)
	Qualifies as loyal
	The result table is ordered by customer_id in ascending order.
'''
from six import integer_types

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    ('1', '101', '2024-01-05', '150.0', 'purchase')
    ,('2', '101', '2024-01-15', '200.0', 'purchase')
    ,('3', '101', '2024-02-10', '180.0', 'purchase')
    ,('4', '101', '2024-02-20', '250.0', 'purchase')
    ,('5', '102', '2024-01-10', '100.0', 'purchase')
    ,('6', '102', '2024-01-12', '120.0', 'purchase')
    ,('7', '102', '2024-01-15', '80.0', 'refund')
    ,('8', '102', '2024-01-18', '90.0', 'refund')
    ,('9', '102', '2024-02-15', '130.0', 'purchase')
    ,('10', '103', '2024-01-01', '500.0', 'purchase')
    ,('11', '103', '2024-01-02', '450.0', 'purchase')
    ,('12', '103', '2024-01-03', '400.0', 'purchase')
    ,('13', '104', '2024-01-01', '200.0', 'purchase')
    ,('14', '104', '2024-02-01', '250.0', 'purchase')
    ,('15', '104', '2024-02-15', '300.0', 'purchase')
    ,('16', '104', '2024-03-01', '350.0', 'purchase')
    ,('17', '104', '2024-03-10', '280.0', 'purchase')
    ,('18', '104', '2024-03-15', '100.0', 'refund')
]


columns = ["transaction_id", "customer_id","transaction_date","amount","transaction_type"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

#  # # # # # #### ================ Approach->1 : (DSL)
#
# df=(df.groupby("customer_id").agg(
#         sum(when(col("transaction_type")=="purchase",1).otherwise(0)).alias("pur_count")
#         ,sum(when(col("transaction_type")=="refund",1).otherwise(0)).alias("ref_count")
#         ,max("transaction_date").alias("max_date")
#         ,min("transaction_date").alias("min_date")
#     ).withColumn("active_days",datediff(col("max_date"),col("min_date")))
#     .withColumn("refund_rate",col("ref_count")/(col("ref_count")+col("pur_count")))
#     .where((col("pur_count")>2)
#             & (col("active_days")>29)
#             & (col("refund_rate")<0.2)
#     )
#     .select("customer_id")
#     .orderBy(asc("customer_id"))
# )
# df.show()

# # # #### ================ Approach->2 : (SQL)
df.createOrReplaceTempView("customer_transactions")

sSQL="""
    WITH loyal_cus AS (
        SELECT customer_id 
        ,COUNT (CASE WHEN transaction_type='purchase' THEN 1 END) pur_count
        ,COUNT (CASE WHEN transaction_type='refund' THEN 1 END) ref_count
        ,MAX(transaction_date) AS max_date
        ,MIN(transaction_date) AS min_date
        FROM customer_transactions
        GROUP BY customer_id
    )
    SELECT customer_id FROM loyal_cus
    WHERE pur_count>2
    AND DATEDIFF(DAY,min_date,max_date)>29
    AND ref_count*1.00/(pur_count+ref_count)*1.00<0.2
    ORDER BY customer_id
"""
df=spark.sql(sSQL)
df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()