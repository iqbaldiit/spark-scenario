'''
    You are given a table "Teams" with a single column "teams"
    Each row in the table represents a unique team participating in a tournament. Your task is to generate all possible
    unique match combinations where each team plays against every other team exactly once.

    Return a result table with a single column matches, where each row contains a match in the format "Team_A Vs Team_B".

    Input :-
            +----------+
            |     teams|
            +----------+
            |     India|
            |  Pakistan|
            |  SriLanka|
            |Bangladesh|
            +----------+

1. Self join

    Expected Output :-
            +--------------------+
            |             matches|
            +--------------------+
            |   India Vs Pakistan|
            |   India Vs SriLanka|
            |Pakistan Vs SriLanka|
            | Bangladesh Vs India|
            |Bangladesh Vs Pak...|
            |Bangladesh Vs Sri...|
            +--------------------+

    Solution Explanation: The problem state that, there is table name "teams". Now we have to create unique tournament match.

    Approach:
    1. In this case we have use self join where teams is not the same
    2. print

'''

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Sample Data
data=[("India",), ("Pakistan",), ("SriLanka",), ("Bangladesh",)] #
columns=["teams"]

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (Using self join (DSL))

df=df.alias("tA").join(df.alias("tB"), col("tA.teams")>col("tB.teams"))\
    .select(concat_ws(" Vs ",col("tA.teams") , col("tB.teams")).alias("matches"))

df.show()

# # # # #### ================ Approach->2 : (Using self join (SQL))
# df.createOrReplaceTempView("Tournament")
#
# df=spark.sql("SELECT CONCAT(TA.teams, ' Vs ',TB.teams) AS matches "
#              " FROM Tournament TA"
#              " INNER JOIN Tournament TB ON TA.teams<TB.teams"
# )
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()