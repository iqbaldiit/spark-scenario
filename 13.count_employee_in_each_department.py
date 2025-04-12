'''
Title: Count Employees in Each Department

You are given a table name  employees with the following column.
emp_id: Integer — Unique identifier for each employee.
emp_name: String — Name of the employee.
dept: String — Name of the department the employee belongs to.

Write a solution in PySpark, Spark SQL, and Pandas to find the total number of employees
in each department. Return the result with two columns:
dept: The name of the department.
total: The total number of employees in that department.
Sort the result by department name in ascending order

Input :-
        +------+--------+-----------+
        |emp_id|emp_name|       dept|
        +------+--------+-----------+
        |     1|    Jhon|Development|
        |     2|     Tim|Development|
        |     3|   David|    Testing|
        |     4|     Sam|    Testing|
        |     5|   Green|    Testing|
        |     6|  Miller| Production|
        |     7|  Brevis| Production|
        |     8|  Warner| Production|
        |     9|    Salt| Production|
        +------+--------+-----------+

Expected Output :-
        +-----------+-----+
        |       dept|total|
        +-----------+-----+
        |Development|    2|
        | Production|    4|
        |    Testing|    3|
        +-----------+-----+

'''
from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()

data = [(1, "Jhon", "Development"),
        (2, "Tim", "Development"),
        (3, "David", "Testing"),
        (4, "Sam", "Testing"),
        (5, "Green", "Testing"),
        (6, "Miller", "Production"),
        (7, "Brevis", "Production"),
        (8, "Warner", "Production"),
        (9, "Salt", "Production")]

columns = ["emp_id", "emp_name", "dept"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))

df=df.groupBy("dept").agg(count("emp_id").alias("total")).orderBy(col("dept").asc())
df.show()

# # # # # #### ================ Approach->2 : ( (SQL))
# # create temp table
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#         SELECT dept, COUNT(emp_id) AS total FROM tbl
#         GROUP BY dept ORDER BY dept
# """
# df=spark.sql(sSQL)
# df.show()



# to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()
