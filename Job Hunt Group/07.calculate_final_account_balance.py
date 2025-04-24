'''
Title: Calculate Final Account Balances

you are give a table "TRANSACTION" with the following column
transaction_id, account_id, amount, transaction_type

Each row in this table represents a single bank transaction for a particular account.
The transaction_type is either 'Deposit' or 'Withdrawal'.

Write a query to report the final account balance for each account.
A Deposit increases the balance and a Withdrawal decreases it.

Return the result table ordered by account_id.

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#start timer to see execution time
start_timer()

# Sample Data
data = [
    (123, 11, 10, 'Deposit'),
    (124, 11, 20, 'Deposit'),
    (126, 21, 20, 'Deposit'),
    (125, 11, 5, 'Withdrawal'),
    (128, 21, 10, 'Withdrawal'),
    (130, 31, 98, 'Deposit'),
    (132, 31, 36, 'Withdrawal'),
    (140, 21, 16, 'Deposit')
]

columns = ["transaction_id", "account_id", "amount", "transaction_type"]

# convert data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # #### ================ Approach->1 : (DSL)

df=(df.withColumn("Deposit",
                    when(col("transaction_type")=="Deposit", col("amount")).otherwise(0))
    .withColumn("Withdrawal",
                when(col("transaction_type")=="Withdrawal", col("amount")).otherwise(0))
)

df=(df.groupBy("account_id")
        .agg((sum("Deposit")- sum("Withdrawal")).alias("FINAL_ACC_BALANCE"))
        .orderBy(col("account_id").asc())
)


df.show()


# # # # #### ================ Approach->2.0 : (Using SQL (Window))
# # create temp table
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#     SELECT account_id
#     ,SUM ( CASE WHEN  transaction_type = 'Deposit'
#             THEN amount
#             ELSE -amount END) AS FINAL_ACC_BALANCE
#     FROM tbl
#     GROUP BY account_id ORDER BY account_id ASC
# """
#
# df=spark.sql(sSQL)
# df.show()


## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()














