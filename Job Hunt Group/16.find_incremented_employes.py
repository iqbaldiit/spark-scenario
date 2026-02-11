'''
    Title: Find Employee who incremented

You are given a table employee_salaries with the following columns:
    1. employee_id (INT): Unique identifier for an employee
    2. salary (INT): Salary of the employee for a given year
    3. year (INT): The year the salary was paid

Each row represents an employee’s salary in a specific year. A year-over-year raise is defined as a year in which
an employee’s salary is strictly higher than their salary in the immediately preceding year.

Write a query to find all employees who have received at least 3 year-over-year salary raises.


Explanation :
    1. Salaries must be compared year by year for the same employee
    2. Only increases in salary count as raises
    3. The years do not need to be consecutive, but comparisons are always made with the previous available year
    4. Return only the employee_id of employees who meet the condition
    5. The result should contain unique employee IDs


Input:
    +-----------+------+----+
    |employee_id|salary|year|
    +-----------+------+----+
    |          1| 60000|2018|
    |          1| 70000|2019|
    |          1| 80000|2020|
    |          1| 90000|2021|
    |          2| 60000|2018|
    |          2| 65000|2019|
    |          2| 65000|2020|
    |          3| 60000|2018|
    |          3| 65000|2019|
    +-----------+------+----+

Expected Output:

    +-----------+
    |employee_id|
    +-----------+
    |          1|
    +-----------+


'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()


# Sample Data
data = [
    (1, 60000, 2018),
    (1, 70000, 2019),
    (1, 80000, 2020),
    (1, 90000, 2021),
    (2, 60000, 2018),
    (2, 65000, 2019),
    (2, 65000, 2020),
    (3, 60000, 2018),
    (3, 65000, 2019)
]


columns = ["employee_id", "salary", "year"]

# convert  to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# create a window
window=Window.partitionBy("employee_id").orderBy(asc("year"))

# add a column with next salary
df = (df.withColumn("next_salary", lead("salary",1).over(window)) # find next year salary with lead function
      .where(col("next_salary")>col("salary")) # filter the salary is incremented or not
      .groupby(col("employee_id")).count() # count how many times incremented
      .where(col("count")>=3) # as per question, filter 3 times and over
      .select("employee_id") # as per question only employee
      .orderBy(col("employee_id").asc())
)

df.show()

# # # # # #### ================ Approach->2 : ((SQL))

# df.createOrReplaceTempView("employee_salaries")
# sSQL="""
#
#     WITH nxt_sal AS(
#     SELECT *,LEAD (salary) OVER (Partition BY employee_id ORDER BY year) AS next_salary
#     FROM employee_salaries
#     ),
#     raises AS (
#         SELECT employee_id,COUNT(employee_id) AS no_of_increment FROM nxt_sal WHERE next_salary>salary
#         GROUP BY employee_id
#     )
#     SELECT employee_id FROM raises WHERE no_of_increment>=3
#
# """
#
# df=spark.sql(sSQL)
# df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()