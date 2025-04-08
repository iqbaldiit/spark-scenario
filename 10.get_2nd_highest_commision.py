'''
    You are given a PySpark DataFrame df with the following schema:
    emp_id, commission_amt, month_last_date
    Each row represents an employee's commission earned at the end of a month.
    Your task is to return a DataFrame containing the row with the second highest commission amount
    for each employee. If an employee has only one record, exclude them from the output.

Input:
        +-----+-------------+-------------+
        |emp_id|commission_amt|month_last_date|
        +-----+-------------+-------------+
        |    1|          300|  31-Jan-2021|
        |    1|          400|  28-Feb-2021|
        |    1|          200|  31-Mar-2021|
        |    2|         1000|  31-Oct-2021|
        |    2|          900|  31-Dec-2021|
        +-----+-------------+-------------+

Expected output:

        +-----+-------------+-------------+
        |emp_id|commission_amt|month_last_date|
        +-----+-------------+-------------+
        |    1|          200|  31-Mar-2021|
        |    2|         1000|  31-Oct-2021|
        +-----+-------------+-------------+

Solution Explanation: The problem stated that we have to find the second highest commission for each employee

Approach :
    1. Create a window partition by emp_id and order by commission
    2. Find rank by dense_rank
    3. filter where rank =2
    4. show result

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
columns= ["emp_id", "commission_amt", "month_last_date"]
# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# create window function for rank
window=Window.partitionBy("emp_id").orderBy(col("commission_amt").desc())

# add rank for getting 2nd highest commission
df=df.withColumn("rank",dense_rank().over(window))
#df.show()

# filter the 2nnd highest commission
df=df.filter(df.rank ==2).select("emp_id","commission_amt","month_last_date")
df.show()


# # # # # # #### ================ Approach->1 : (SQL))
# df.createOrReplaceTempView("tbl_Commission")
# sSQL="""
#     SELECT emp_id,commission_amt,month_last_date
#     FROM (
#     SELECT *, DENSE_RANK() OVER (PARTITION BY emp_id ORDER BY commission_amt DESC) rk FROM
#     tbl_Commission) tab WHERE rk=2
#
# """
# df=spark.sql(sSQL)
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()