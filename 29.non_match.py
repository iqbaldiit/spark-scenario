'''

Input:
    +---+
    |col|
    +---+
    |  1|
    |  2|
    |  3|
    +---+

    +----+
    |col1|
    +----+
    |   1|
    |   2|
    |   3|
    |   4|
    |   5|
    +----+

Output :-
    +---+
    |col|
    +---+
    |  1|
    |  2|
    |  4|
    |  5|
    +---+
'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data1 = [(1,), (2,), (3,)]
df1 = spark.createDataFrame(data1, ["col"])
data2 = [(1,), (2,), (3,), (4,), (5,)]
df2 = spark.createDataFrame(data2, ["col1"])



print()
print("==========Input Data=============")

df1.show()
df2.show()


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)
maxdf = df1.agg(max("col").alias("max"))
maxdf.show()

maxsalary = maxdf.select(col("max")).first()[0]

joindf = df1.join(df2, df1["col"] == df2["col1"], "outer").drop("col")
joindf.show()

finaldf = joindf.filter(col("col1") != maxsalary).withColumnRenamed("col1", "col").orderBy("col")
finaldf.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()