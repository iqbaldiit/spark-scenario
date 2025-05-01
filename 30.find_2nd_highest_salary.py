'''
Title: Find 2nd highest salary

You are given two tables name
1. Employee (emp_id|name|dept_id| salary)
2. Department (dept_id1|dept_name)

Write a SQL query to extract second most salary for each department. The result is order by dept_id

Input:
        +------+----+-------+-------+
        |emp_id|name|dept_id| salary|
        +------+----+-------+-------+
        |     1|   A|      A|1000000|
        |     2|   B|      A|2500000|
        |     3|   C|      G| 500000|
        |     4|   D|      G| 800000|
        |     5|   E|      W|9000000|
        |     6|   F|      W|2000000|
        +------+----+-------+-------+

        +--------+---------+
        |dept_id1|dept_name|
        +--------+---------+
        |       A|    AZURE|
        |       G|      GCP|
        |       W|      AWS|
        +--------+---------+

Expected Output:

        +------+----+---------+-------+
        |emp_id|name|dept_name| salary|
        +------+----+---------+-------+
        |     1|   A|    AZURE|1000000|
        |     6|   F|      AWS|2000000|
        |     3|   C|      GCP| 500000|
        +------+----+---------+-------+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
emp_data = [
        (1, 'A', 'A', 1000000),
        (2, 'B', 'A', 2500000),
        (3, 'C', 'G', 500000),
        (4, 'D', 'G', 800000),
        (5, 'E', 'W', 9000000),
        (6, 'F', 'W', 2000000)
]

emp_columns = ["emp_id", "name", "dept_id", "salary"]

dept_data = [('A', 'AZURE'), ('G', 'GCP'), ('W', 'AWS')]
dept_columns = ["dept_id1", "dept_name"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
emp_df = spark.createDataFrame(emp_data,emp_columns)
dept_df = spark.createDataFrame(dept_data,dept_columns)


print()
print("==========Input Data=============")

emp_df.show()
dept_df.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)

window=Window.partitionBy("dept_id").orderBy(col("salary").desc())

df=(emp_df.withColumn("rk", dense_rank().over(window))
        .where(col("rk")==2)
        .join(dept_df,emp_df.dept_id==dept_df.dept_id1,"inner")
        .select("emp_id","name","dept_name","salary")
        .orderBy(col("dept_id"))
)

df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# emp_df.createOrReplaceTempView("Employee")
# dept_df.createOrReplaceTempView("Department")
#
# sSQL="""
#     SELECT tab.emp_id,tab.name, tab.dept_name,tab.salary
#     FROM (
#             SELECT *
#             ,DENSE_RANK() OVER (PARTITION BY E.dept_id ORDER BY E.salary DESC) AS rk
#             FROM Employee E
#             INNER JOIN Department D ON E.dept_id=D.dept_id1
#     ) tab WHERE tab.rk=2 ORDER BY tab.dept_id ASC
# """
#
# df=spark.sql(sSQL)
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()