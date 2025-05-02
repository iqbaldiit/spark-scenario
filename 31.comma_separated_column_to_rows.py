'''
Title: Transform Comma-Separated Columns into Rows

Given a DataFrame with multiple columns containing comma-separated values,
write a solution to transform these columns into rows while preserving their order and including an empty row.

Input:
    +----+-----+--------+-----------+
    |col1| col2|    col3|       col4|
    +----+-----+--------+-----------+
    |  m1|m1,m2|m1,m2,m3|m1,m2,m3,m4|
    +----+-----+--------+-----------+

Expected Output:

        +-----------+
        |        col|
        +-----------+
        |         m1|
        |      m1,m2|
        |   m1,m2,m3|
        |m1,m2,m3,m4|
        |           |
        +-----------+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

columns = ["col1", "col2", "col3", "col4"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns)


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
df=df.select(explode(array(col("col1"),col("col2"),col("col3"),col("col4")))).alias("col")
df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df.createOrReplaceTempView("tbl")
#
# sSQL="""
#     SELECT
#         EXPLODE(
#             SPLIT(
#                 CONCAT(col1,'-',col2,'-',col3,'-',col4)
#             ,'-')
#         ) AS col
#     FROM tbl
#
# """
#
# df=spark.sql(sSQL)

# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()


















