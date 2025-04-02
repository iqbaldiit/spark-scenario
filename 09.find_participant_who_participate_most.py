'''
    You are given a table 'Participants' containing participant names and their rankings across multiple events.
    The rank column contains a list of rankings achieved by each participant.

    write a solution to find the participant(s) who have achieved rank 1 the most times.
    If there is a tie, return all such participants.

    Return only the name(s) of the participant(s) with the highest count of rank 1.

    Input :-
        +----+---------------+
        |name|           rank|
        +----+---------------+
        |   a|   [1, 1, 1, 3]|
        |   b|   [1, 2, 3, 4]|
        |   c|[1, 1, 1, 1, 4]|
        |   d|            [3]|
        +----+---------------+

    Expected Output :-
        +----+
        |name|
        +----+
        |   c|
        +----+

    Solution Explanation: The problem stated that, we have to find out who participate most rank 1.

    Approach:
        1. explode the rank column
        2. count of rank where rank=1
        3. find max of the count
        4. print name of the result.
'''
from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Sample Data
data = [
    ("a", [1, 1, 1, 3]),
    ("b", [1, 2, 3, 4]),
    ("c", [1, 1, 1, 1, 4]),
    ("d", [3])
]
columns=["name", "rank"]

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (using Explode (DSL))

df=df.withColumn("rank",explode(col("rank")))
df=df.filter(col("rank")==1)
df=df.groupBy(col("name")).agg(count(col("rank")).alias("rk_count"))
df=df.orderBy(col("rk_count").desc()).limit(1).select("name")

df.show()

# # # #### ================ Approach->2 : (using explode (SQL))

# df.createOrReplaceTempView("Participants")
# df=spark.sql("WITH cte AS ("
#              " SELECT name,rk FROM Participants"
#              " LATERAL VIEW explode(rank) as rk )"
#              " SELECT name FROM"
#              " (SELECT name, COUNT(rk) rk_count FROM cte WHERE rk=1"
#              " GROUP BY name)tab ORDER BY rk_count DESC LIMIT (1)"
# )
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()