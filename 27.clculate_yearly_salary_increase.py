'''
Title: Calculate Yearly Salary Increases

you are given a table name Employee with the following columns
emp_id, salary and year

Write a solution to calculate yearly salary increases for each employee based on their previous year's salary.
Given a table of employee salaries by year, create a new column that shows the salary increase from the previous year.
For the first year of data for each employee, the increase should be 0.

Input:
    +-----+------+----+
    |emp_id|salary|year|
    +-----+------+----+
    |    1| 60000|2018|
    |    1| 70000|2019|
    |    1| 80000|2020|
    |    2| 60000|2018|
    |    2| 65000|2019|
    |    2| 65000|2020|
    |    3| 60000|2018|
    |    3| 65000|2019|
    +-----+------+----+

Expected Output:
    +------+------+----+---------------+
    |emp_id|salary|year|increase_salary|
    +------+------+----+---------------+
    |     1| 60000|2018|              0|
    |     1| 70000|2019|          10000|
    |     1| 80000|2020|          10000|
    |     2| 60000|2018|              0|
    |     2| 65000|2019|           5000|
    |     2| 65000|2020|              0|
    |     3| 60000|2018|              0|
    |     3| 65000|2019|           5000|
    +------+------+----+---------------+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, 60000, 2018), (1, 70000, 2019), (1, 80000, 2020),
    (2, 60000, 2018), (2, 65000, 2019), (2, 65000, 2020),
    (3, 60000, 2018), (3, 65000, 2019)
]

columns = ["emp_id", "salary", "year"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns)


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)
window=Window.partitionBy("emp_id").orderBy("year")

df=(df.withColumn("pre_Salary",lag("salary",1,0).over(window))
    .withColumn("increase_salary"
                ,when(col("pre_Salary")==0,0)
                .otherwise((col("salary")-col("pre_Salary"))))
    .drop("pre_Salary")
    .orderBy(col("emp_id").asc())
)

df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#     SELECT tab.emp_id,tab.salary,tab.year
#     ,CASE WHEN tab.pre_salary IS NULL THEN 0
#         ELSE tab.salary-tab.pre_salary END AS  increase_salary
#     FROM  (SELECT *, LAG (salary) OVER (PARTITION BY emp_id ORDER BY year) AS pre_salary
#     FROM tbl)tab
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