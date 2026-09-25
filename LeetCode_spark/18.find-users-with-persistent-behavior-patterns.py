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
    (1, 101, 'like'),
    (1, 102, 'like'),
    (1, 103, 'like'),
    (1, 104, 'wow'),
    (1, 105, 'like'),
    (2, 201, 'like'),
    (2, 202, 'wow'),
    (2, 203, 'sad'),
    (2, 204, 'like'),
    (2, 205, 'wow'),
    (3, 301, 'love'),
    (3, 302, 'love'),
    (3, 303, 'love'),
    (3, 304, 'love'),
    (3, 305, 'love')
]

columns = ["user_id","content_id","reaction"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")
df.show()

print()
print("==========Expected output=============")

# #  # # # # # #### ================ Approach->1 : (DSL)

df_tot_rec=(df.groupBy("user_id").agg(count("*").alias("total_reactions"))
            .where(col("total_reactions")>=5))

df_dom_reaction=df.groupBy("user_id","reaction").agg(count("*").alias("reaction_count"))

df=((df_tot_rec.alias("tr").join(df_dom_reaction.alias("dr"),"user_id","inner")
    .withColumn("reaction_ratio",round(col("dr.reaction_count")/col("tr.total_reactions"),2))
    .where(col("reaction_ratio")>=0.6)
    ).select("tr.user_id",col("dr.reaction").alias("dominant_reaction"),"reaction_ratio")
    .orderBy(desc("reaction_ratio"),asc("tr.user_id")))

df.show()

# # # # #### ================ Approach->2 : (SQL)
# df.createOrReplaceTempView("reactions")
#
#
# sSQL="""
#     WITH tbl_total_reaction AS(
#         SELECT user_id,COUNT(1) AS total_reactions FROM reactions GROUP BY user_id HAVING COUNT(1)>=5
#     ), tbl_dom_reaction AS (
#         SELECT user_id,reaction,COUNT(1) AS reaction_count FROM reactions GROUP BY user_id ,reaction
#     ), tbl_result AS (
#         SELECT tr.user_id,dr.reaction AS dominant_reaction
#         ,ROUND(1.00*dr.reaction_count/tr.total_reactions,2) AS reaction_ratio
#         FROM tbl_total_reaction tr
#         INNER JOIN tbl_dom_reaction dr ON tr.user_id=dr.user_id
#         WHERE 1.00*dr.reaction_count/tr.total_reactions>=0.6
#     )
#     SELECT * FROM tbl_result ORDER BY reaction_ratio DESC, user_id ASC
# """
# df=spark.sql(sSQL)
# df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()