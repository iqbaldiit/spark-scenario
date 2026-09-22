# Source: https://leetcode.com/problems/find-emotionally-consistent-users/solutions/8529427/simple-best-solution-by-iqbaldiit-ioug/
'''
	Table: reactions

	+--------------+---------+
	| Column Name  | Type    |
	+--------------+---------+
	| user_id      | int     |
	| content_id   | int     |
	| reaction     | varchar |
	+--------------+---------+
	(user_id, content_id) is the primary key (unique value) for this table.
	Each row represents a reaction given by a user to a piece of content.
	Write a solution to identify emotionally consistent users based on the following requirements:

	For each user, count the total number of reactions they have given.
	Only include users who have reacted to at least 5 different content items.
	A user is considered emotionally consistent if at least 60% of their reactions are of the same type.
	Return the result table ordered by reaction_ratio in descending order and then by user_id in ascending order.

	Note:

	reaction_ratio should be rounded to 2 decimal places
	The result format is in the following example.



	Example:

	Input:

	reactions table:

	+---------+------------+----------+
	| user_id | content_id | reaction |
	+---------+------------+----------+
	| 1       | 101        | like     |
	| 1       | 102        | like     |
	| 1       | 103        | like     |
	| 1       | 104        | wow      |
	| 1       | 105        | like     |
	| 2       | 201        | like     |
	| 2       | 202        | wow      |
	| 2       | 203        | sad      |
	| 2       | 204        | like     |
	| 2       | 205        | wow      |
	| 3       | 301        | love     |
	| 3       | 302        | love     |
	| 3       | 303        | love     |
	| 3       | 304        | love     |
	| 3       | 305        | love     |
	+---------+------------+----------+
	Output:

	+---------+-------------------+----------------+
	| user_id | dominant_reaction | reaction_ratio |
	+---------+-------------------+----------------+
	| 3       | love              | 1.00           |
	| 1       | like              | 0.80           |
	+---------+-------------------+----------------+
	Explanation:

	User 1:
	Total reactions = 5
	like appears 4 times
	reaction_ratio = 4 / 5 = 0.80
	Meets the 60% consistency requirement
	User 2:
	Total reactions = 5
	Most frequent reaction appears only 2 times
	reaction_ratio = 2 / 5 = 0.40
	Does not meet the consistency requirement
	User 3:
	Total reactions = 5
	'love' appears 5 times
	reaction_ratio = 5 / 5 = 1.00
	Meets the consistency requirement
	The Results table is ordered by reaction_ratio in descending order, then by user_id in ascending order.
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