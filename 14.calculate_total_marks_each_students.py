'''
    Title: Calculate Total Marks for Each Student

    You are given a table name "student_marks" with the following column
    roll_no--> role number of each student
    , name--> Name of the student
    , telugu, english, maths, science, social --> all are marks in Integer value.

    Write a solution to calculate the total marks obtained by each student across all subjects.
    Return a table with the original columns and an additional column:
    total: The sum of marks from telugu, english, maths, science, and social.

Input:
    |roll_no|  name|telugu|english|maths|science|social|
    +------+------+------+-------+-----+-------+------+
    |203040|rajesh|    10|     20|   30|     40|    50|
    +------+------+------+-------+-----+-------+------+

Output:
    +------+------+------+-------+-----+-------+------+-----+
    |roll_no|  name|telugu|english|maths|science|social|total|
    +------+------+------+-------+-----+-------+------+-----+
    |203040|rajesh|    10|     20|   30|     40|    50|  150|
    +------+------+------+-------+-----+-------+------+-----+

Solution 1: We can sum of the marks of all subject like [telugu + english + maths + science + social + total]
Solution 2: We can sum all the fields except first 2 columns as column may increase horizontally.
Solution 3. We can use reduce function and operator to find the summation

Approach for solution 2:
    1. Get all columns list except first 2.
    2. Make the column list into a string variable with + operator
    3. set the columns string as variable to the query.
    3. show()

Approach for solution 3:
    1. Get all columns list except first 2.
    2. Make the column list
    3. User reduce function with column operator
    3. show()
'''

from spark_session import *
from pyspark.sql.functions import *
from functools import reduce
import operator

# start timer to see execution time
start_timer()

data = [(203040, "rajesh", 10, 20, 30, 40, 50)]

columns = ["rol_no", "name", "telugu", "english", "maths", "science", "social"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1: Solution 1: : ((DSL))
#
# df=df.withColumn("total",expr("telugu + english + maths + science + social"))
# df.show()

# # # # #### ================ Approach->1: Solution 2: : ((DSL))

# course_columns=df.columns[2:] # get columns except first 2
# sColumns=" + ".join(course_columns) # make string with operator
# df = df.withColumn("total", expr(f"{sColumns}"))
# #df = df.withColumn("total", reduce(operator.add, [col(c) for c in course_columns]))
# df.show()

# # # # # #### ================ Approach->2 : Solution 1: ( (SQL))
# # create temp table
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#         SELECT *, telugu + english + maths + science + social AS total FROM tbl
# """
# df=spark.sql(sSQL)
# df.show()

# # # # # #### ================ Approach->2 : Solution 2: ( (SQL))
# # create temp table
# df.createOrReplaceTempView("tbl")
# course_columns=df.columns[2:] # get columns except first 2
# sColumns=" + ".join(course_columns) # make string with operator
#
# sSQL=f"""
#         SELECT *, {sColumns} AS total FROM tbl
# """
# df=spark.sql(sSQL)
# df.show()



# to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()
