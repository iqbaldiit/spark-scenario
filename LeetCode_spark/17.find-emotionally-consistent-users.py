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
    (1, 'Write a blog outline', 120),
    (1, 'Generate SQL query', 80),
    (1, 'Summarize an article', 200),
    (2, 'Create resume bullet', 60),
    (2, 'Improve LinkedIn bio', 70),
    (3, 'Explain neural networks', 300),
    (3, 'Generate interview Q&A', 250),
    (3, 'Write cover letter', 180),
    (3, 'Optimize Python code', 220)
]

columns = ["user_id","prompt","tokens"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")
df.show()

print()
print("==========Expected output=============")

# #  # # # # # #### ================ Approach->1 : (DSL)

df=(df.groupBy("user_id").agg((count('*').alias("prompt_count"))
                             ,round(avg("tokens"),2).alias("avg_tokens")
                             ,max("tokens").alias("max_token"))
    .where((col("prompt_count")>=3) & (col("max_token")>col("avg_tokens")))
    .orderBy(desc("avg_tokens"),asc("user_id")))

df.show()

# # # # #### ================ Approach->2 : (SQL)
# df.createOrReplaceTempView("prompts")
#
#
# sSQL="""
#     WITH tbl_summary AS (
#         SELECT user_id, COUNT(*) AS prompt_count, ROUND(AVG(tokens*1.00),2) AS avg_tokens, MAX(tokens*1.00) AS max_token
#         FROM prompts GROUP BY user_id
#     )
#     SELECT user_id,prompt_count,avg_tokens FROM tbl_summary
#     WHERE prompt_count>=3 AND max_token>avg_tokens
#     ORDER BY avg_tokens DESC, user_id ASC
# """
# df=spark.sql(sSQL)
# df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()