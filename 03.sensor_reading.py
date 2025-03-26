"""
You are given a dataset containing sensor readings recorded at different timestamps.
Each sensor records multiple values over time.

The goal is to compute the difference between consecutive readings for each sensor_id, ensuring that:
1. The values column is updated as the difference between the current value and the next value.
2. If there is no next value available, assume it as 0 (default value for LEAD function).
3. Only rows where the next value exists should be retained in the final output.

Input:
        +--------+----------+------+
        |sensor_id| timestamp|values|
        +--------+----------+------+
        |    1111|2021-01-15|    10|
        |    1111|2021-01-16|    15|
        |    1111|2021-01-17|    30|
        |    1112|2021-01-15|    10|
        |    1112|2021-01-15|    20|
        |    1112|2021-01-15|    30|
        +--------+----------+------+

Output:
    +--------+----------+------+
    |sensor_id| timestamp|values|
    +--------+----------+------+
    |    1111|2021-01-15|     5|
    |    1111|2021-01-16|    15|
    |    1112|2021-01-15|    10|
    |    1112|2021-01-15|    10|
    +--------+----------+------+

Solution Explanation: The problem stated that, we have to provide a dataset with
    1.the value difference from next
    2. if there is no next value, should not include the result dataset

Approach-> : (Using window function)
    1. create a window by sensor and values
    2. use LEAD to find the next value
    3. remove if there is no next value
    4. make a value of difference
    5. print

"""
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()


data = [
    (1111, "2021-01-15", 10),
    (1111, "2021-01-16", 15),
    (1111, "2021-01-17", 30),
    (1112, "2021-01-15", 10),
    (1112, "2021-01-15", 20),
    (1112, "2021-01-15", 30)
]

columns = ["sensor_id", "timestamp", "values"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (Using window function (DSL))
# create window
window=Window.partitionBy("sensor_id").orderBy("values")

# get the ext values
df=df.withColumn("next_values", lead("values",1,0).over(window))

# remove the record who has no next_values
df=df.filter("next_values>0")


# get the difference of values
df=df.withColumn("values",col("next_values")-col("values"))\
    .select("sensor_id","timestamp","values")
df.show()


# # # # #### ================ Approach->2 : (Using window function (SQL))
# # create temp table
# df.createOrReplaceTempView("Sensors")
# df=spark.sql(""
#              " WITH CTE AS ("
#              " SELECT *, LEAD(values,1,0) OVER (PARTITION BY sensor_id ORDER BY values ASC) AS next_values"
#              " FROM Sensors)"
#              " SELECT sensor_id, timestamp, (next_values-values) AS values "
#              " FROM CTE WHERE next_values>0"
# "")
#
# df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()