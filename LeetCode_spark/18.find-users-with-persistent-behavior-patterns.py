# Source(MSSQL) : https://leetcode.com/problems/find-users-with-persistent-behavior-patterns/solutions/8537763/simple-best-solution-by-iqbaldiit-rd18/
# Source (Postgress) : https://leetcode.com/problems/find-users-with-persistent-behavior-patterns/solutions/8537836/simple-best-solution-by-iqbaldiit-oucr/
'''
	Table: activity

	+--------------+---------+
	| Column Name  | Type    |
	+--------------+---------+
	| user_id      | int     |
	| action_date  | date    |
	| action       | varchar |
	+--------------+---------+
	(user_id, action_date, action) is the primary key (unique value) for this table.
	Each row represents a user performing a specific action on a given date.
	Write a solution to identify behaviorally stable users based on the following definition:

	A user is considered behaviorally stable if there exists a sequence of at least 5 consecutive days such that:
	The user performed exactly one action per day during that period.
	The action is the same on all those consecutive days.
	If a user has multiple qualifying sequences, only consider the sequence with the maximum length.
	Return the result table ordered by streak_length in descending order, then by user_id in ascending order.

	The result format is in the following example.



	Example:

	Input:

	activity table:

	+---------+-------------+--------+
	| user_id | action_date | action |
	+---------+-------------+--------+
	| 1       | 2024-01-01  | login  |
	| 1       | 2024-01-02  | login  |
	| 1       | 2024-01-03  | login  |
	| 1       | 2024-01-04  | login  |
	| 1       | 2024-01-05  | login  |
	| 1       | 2024-01-06  | logout |
	| 2       | 2024-01-01  | click  |
	| 2       | 2024-01-02  | click  |
	| 2       | 2024-01-03  | click  |
	| 2       | 2024-01-04  | click  |
	| 3       | 2024-01-01  | view   |
	| 3       | 2024-01-02  | view   |
	| 3       | 2024-01-03  | view   |
	| 3       | 2024-01-04  | view   |
	| 3       | 2024-01-05  | view   |
	| 3       | 2024-01-06  | view   |
	| 3       | 2024-01-07  | view   |
	+---------+-------------+--------+
	Output:

	+---------+--------+---------------+------------+------------+
	| user_id | action | streak_length | start_date | end_date   |
	+---------+--------+---------------+------------+------------+
	| 3       | view   | 7             | 2024-01-01 | 2024-01-07 |
	| 1       | login  | 5             | 2024-01-01 | 2024-01-05 |
	+---------+--------+---------------+------------+------------+
	Explanation:

	User 1:
	Performed login from 2024-01-01 to 2024-01-05 on consecutive days
	Each day has exactly one action, and the action is the same
	Streak length = 5 (meets minimum requirement)
	The action changes on 2024-01-06, ending the streak
	User 2:
	Performed click for only 4 consecutive days
	Does not meet the minimum streak length of 5
	Excluded from the result
	User 3:
	Performed view for 7 consecutive days
	This is the longest valid sequence for this user
	Included in the result
	The Results table is ordered by streak_length in descending order, then by user_id in ascending order
'''
from tkinter.constants import FIRST

from pandas.core.computation.expressions import where
from pyspark.sql.functions import month, unix_timestamp, datediff
from six import integer_types

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    (1, '2024-01-01', 'login'),
    (1, '2024-01-02', 'login'),
    (1, '2024-01-03', 'login'),
    (1, '2024-01-04', 'login'),
    (1, '2024-01-05', 'login'),
    (1, '2024-01-06', 'logout'),
    (2, '2024-01-01', 'click'),
    (2, '2024-01-02', 'click'),
    (2, '2024-01-03', 'click'),
    (2, '2024-01-04', 'click'),
    (3, '2024-01-01', 'view'),
    (3, '2024-01-02', 'view'),
    (3, '2024-01-03', 'view'),
    (3, '2024-01-04', 'view'),
    (3, '2024-01-05', 'view'),
    (3, '2024-01-06', 'view'),
    (3, '2024-01-07', 'view')
]

columns = ["user_id","action_date","action"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")
df.show()

print()
print("==========Expected output=============")

# #  # # # # # #### ================ Approach->1 : (DSL)

win_lead=Window.partitionBy("user_id","action").orderBy("user_id","action","action_date")
win_count=Window.partitionBy("user_id","action_date")

df=(df.withColumn("next_date",lead("action_date").over(win_lead))
    .withColumn("action_count",count("*").over(win_count))
    .withColumn("date_diff",datediff(col("next_date"),col("action_date")))
    )

df=(df.groupBy("user_id","action","action_count","date_diff").agg(
    (1+sum("date_diff")).alias("streak_length")
    ,min("action_date").alias("start_date")
    ,max("next_date").alias("end_date")
).where((col("action_count")==1)
        & (col("date_diff")==1)
        & (col("streak_length")>=5))
    .select("user_id","action","streak_length","start_date","end_date")
    .orderBy(desc("streak_length"),asc("user_id"))
)

df.show()

# # # # #### ================ Approach->2 : (SQL)
# df.createOrReplaceTempView("activity")
#
# sSQL="""
#     WITH tbl_lead AS (
#     SELECT *
#     , LEAD(action_date) OVER (PARTITION BY user_id,action ORDER BY user_id,action,action_date) AS next_date
#     , COUNT(action) OVER (PARTITION BY user_id,action_date) AS action_count
#     FROM activity
#     )
#     SELECT user_id,action
#     ,SUM(DATEDIFF(DAY,action_date,next_date))+1 AS streak_length
#     ,MIN(action_date) start_date
#     ,MAX(next_date) end_date
#     FROM tbl_lead WHERE action_count=1 AND DATEDIFF(DAY,action_date,next_date)=1
#     GROUP BY user_id,action
#     HAVING SUM(DATEDIFF(DAY,action_date,next_date))+1>=5
#     ORDER BY streak_length DESC, user_id ASC
# """
# df=spark.sql(sSQL)
# df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()