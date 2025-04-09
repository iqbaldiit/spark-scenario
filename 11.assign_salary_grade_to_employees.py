'''
    You are given a table called Emp_table containing information about employees, including
    their employee ID, name, and salary.

    Your task is to write a SQL query that returns all employee records with an additional column grade
    based on the following salary grading system:
    1. Salary greater than 10,000 → Grade 'A'
    2. Salary between 5,000 and 10,000 (inclusive) → Grade 'B'
    3. Salary less than 5,000 → Grade 'C'

    Input:
            +------+---------------+------+
            |emp_id|       emp_name|salary|
            +------+---------------+------+
            |     1|           Jhon|  4000|
            |     2|      Tim David| 12000|
            |     3|Json Bhrendroff|  7000|
            |     4|         Jordon|  8000|
            |     5|          Green| 14000|
            |     6|         Brewis|  6000|
            +------+---------------+------+

    Expected Output:
            +------+---------------+------+-----+
            |emp_id|       emp_name|salary|grade|
            +------+---------------+------+-----+
            |     1|           Jhon|  4000|    C|
            |     2|      Tim David| 12000|    A|
            |     3|Json Bhrendroff|  7000|    B|
            |     4|         Jordon|  8000|    B|
            |     5|          Green| 14000|    A|
            |     6|         Brewis|  6000|    B|
            +------+---------------+------+-----+

Solution Explanation: The problem stated very clearly that we have assign grade to employees as per their rules.

Approach:
    1. As per the requirement use case when statement in SQL and when-otherwise in dsl.
'''

from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()

data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]

columns = ["emp_id", "emp_name", "salary"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : ((DSL))

# df=(df.withColumn("grade",
#                   when(df.salary>10000,"A")
#                   .when((df.salary>=5000) & (df.salary<=10000), "B")
#                   .otherwise("C")
#                  ))
# df.show()

# # # # #### ================ Approach->2 : ( (SQL))
# create temp table
df.createOrReplaceTempView("tbl")

sQuery="""
    SELECT *, CASE WHEN salary >10000 THEN 'A'
            WHEN salary BETWEEN 5000 AND 10000 THEN 'B'
            ELSE 'C' END AS grade 
    FROM tbl
"""

df=spark.sql(sQuery)

df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()
