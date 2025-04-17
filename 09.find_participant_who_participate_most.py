'''
    You are given a table 'Participants' containing participant names and their
    rankings across multiple events.
    The rank column contains a list of rankings achieved by each participant.

    write a solution to find the participant(s) who have achieved rank 1 the most times.
    If there is a tie, return all such participants.

    Return only the name(s) of the participant(s) with the highest count of rank 1.

    Input :-
        +----+---------------+
        |name|           rank|
        +----+---------------+
        |   a|   [1, 1, 1, 3]| 3
        |   b|   [1, 2, 3, 4]| 1
        |   c|[1, 1, 1, 1, 4]| 4
        |   d|            [3]|
        +----+---------------+

    Expected Output :-
        +----+
        |name|
        +----+
        |   c|
        +----+


1.  explode
2.  filter rank=1
3.  group by (name) count (rank)
4. max


    Solution Explanation: The problem stated that, we have to find out who participate most rank 1.

    Approach (using explode):
        1. explode the rank column
        2. count of rank where rank=1
        3. find max of the count  or use DENSE_RANK
        4. filter and print name.

    Approach (Using Size function)
        1. get the size of the rank array where rank=1
        2. find max count or use DENSE_RANK
        3. match and print

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
max_rk=df.agg(max(col("rk_count")).alias("max_rk")).collect()[0][0]
df=df.filter(col("rk_count")==max_rk).select("name")
df.show()

# # # # # # #### ================ Approach->2 : (using size and filter (DSL))
#df=df.withColumn("rk_count",expr("size(filter(rank,x->x=1))")) # get count of 1
# max_rk=df.agg(max(col("rk_count")).alias("max_rk")).collect()[0][0] # find max rank count
# df=df.filter(col("rk_count")==max_rk).select("name")
# df.show()

# # # # # # #### ================ Approach->3 : (using explode (SQL))
# # #
# df.createOrReplaceTempView("Participants")
# df=spark.sql("WITH cte AS ("
#              " SELECT name,rk FROM Participants"
#              " LATERAL VIEW explode(rank) as rk )"
#              " SELECT tab2.name FROM ("
#              " SELECT tab.name, DENSE_RANK() OVER (ORDER BY tab.rk_count DESC) crk FROM ("
#              " SELECT name, COUNT(rk) rk_count FROM cte WHERE rk=1"
#              " GROUP BY name "
#              " )tab)tab2 WHERE tab2.crk=1"
# )
#
# df.show()

# # # # # # #### ================ Approach->4 : (size and filter (SQL))
# # #
# df.createOrReplaceTempView("Participants")
# df=spark.sql(" SELECT name FROM ("
#              " SELECT name, DENSE_RANK() OVER (ORDER BY rk_count DESC) AS crk FROM ("
#              " SELECT *, SIZE(FILTER(rank, x->x=1)) AS rk_count FROM Participants"
#              " )tab)tab2 WHERE crk=1"
# )
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()