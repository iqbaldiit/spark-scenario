'''
Title: Find Grandparent Relationships in Family Tree

Given a table representing parent-child relationships,
write a SQL query to find each child with their immediate parent and grandparent.

Input:
    +-----+------+
    |child|parent|
    +-----+------+
    |    A|    AA|
    |    B|    BB|
    |    C|    CC|
    |   AA|   AAA|
    |   BB|   BBB|
    |   CC|   CCC|
    +-----+------+

Expected Output:

    +-----+------+-----------+
    |child|parent|grandparent|
    +-----+------+-----------+
    |    A|    AA|        AAA|
    |    C|    CC|        CCC|
    |    B|    BB|        BBB|
    +-----+------+-----------+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [("A", "AA"), ("B", "BB"), ("C", "CC"),
        ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

columns = ["child", "parent"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns)


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
# df=(df.alias("C").join(df.alias("G"),col("C.parent")==col("G.child"),"inner")
#         .select("C.child","C.parent",col("G.parent").alias("grandparent"))
# )
# df.show()

# # # # # #### ================ Approach->2 : ((SQL))

df.createOrReplaceTempView("tbl")

sSQL="""
    SELECT C.child,C.parent,G.parent AS  grandparent
    FROM tbl C 
    INNER JOIN tbl G ON C.parent=G.child
"""

df=spark.sql(sSQL)

df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()