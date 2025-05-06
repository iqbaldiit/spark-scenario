'''
Title: Identify Employees Sharing the Same Salary

Given a table of employee information with the following columns
worker_id, first_name, last_name, salary, joining_date, depart

write a query to find all employees who share the same salary
with at least one other employee in the company.

Input:
    +--------+---------+--------+------+-------------------+------+
    |worker_id|first_name|last_name|salary|     joining_date|depart|
    +--------+---------+--------+------+-------------------+------+
    |     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
    |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
    |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
    |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
    |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
    +--------+---------+--------+------+-------------------+------+

Expected Output:

    +--------+---------+--------+------+-------------------+------+
    |worker_id|first_name|last_name|salary|    joining_date|depart|
    +--------+---------+--------+------+-------------------+------+
    |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
    |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
    |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
    |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
    +--------+---------+--------+------+-------------------+------+


Explanation:
    1. Employees with worker_id 002 and 003 both earn 300000
    2. Employees with worker_id 004 and 005 both earn 500000
    3. Employee with worker_id 001 earns 100000 (unique salary) and is excluded

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]

columns = ["worker_id", "first_name", "last_name", "salary", "joining_date", "depart"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns)


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
window=Window.orderBy(col("salary"))
df=(df.withColumn("cnt",count("salary").over(window))
    .where(col("cnt")>1)
    .drop("cnt")
)


df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#     WITH CTE AS (
#         SELECT salary,COUNT(*) AS cnt
#         FROM tbl GROUP BY salary HAVING COUNT(*)>1
#     )
#     SELECT T.* FROM CTE C
#     INNER JOIN tbl T ON T.salary=C.salary
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


















