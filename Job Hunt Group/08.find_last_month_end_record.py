'''
Title: Find the Record with the Earliest Month-End Date per Employee

ou are given a table containing employees’ commission amounts along with the month-end date for each record.
Your task is to find the record with the earliest month-end date for each employee (emp_id).
Return the emp_id, commission_amt, and the corresponding month_last_date.

Input:
        +------+--------------+---------------+
        |emp_id|commission_amt|month_last_date|
        +------+--------------+---------------+
        |     1|           300|    31-Jan-2021|
        |     1|           400|    28-Feb-2021|
        |     1|           200|    31-Mar-2021|
        |     2|          1000|    31-Oct-2021|
        |     2|           900|    31-Dec-2021|
        +------+--------------+---------------+

Output:
        +------+--------------+---------------+
        |emp_id|commission_amt|month_last_date|
        +------+--------------+---------------+
        |     1|           300|     2021-01-31|
        |     2|          1000|     2021-10-31|
        +------+--------------+---------------+

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
    (2, 900, "31-Dec-2021"),
]


columns = ["emp_id", "commission_amt", "month_last_date"]

# convert  to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# First, cast month_last_date to DateType
df = df.withColumn("month_last_date", to_date("month_last_date", "dd-MMM-yyyy"))

# Create a window
window = Window.partitionBy("emp_id").orderBy(col("month_last_date").asc())

# Add row number
df = df.withColumn("rn", row_number().over(window))

# Filter where row number is 1
df = df.filter(col("rn") == 1).select("emp_id", "commission_amt", "month_last_date")


df.show()

# # # # # #### ================ Approach->2 : ((SQL))

# df.createOrReplaceTempView("tbl")
# sSQL="""
#
#     SELECT userid, collect_list(page) AS page
#     FROM tbl GROUP BY userid
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

















