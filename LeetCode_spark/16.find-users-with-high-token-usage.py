# Source: https://leetcode.com/problems/most-common-course-pairs/solutions/8511786/easy-and-best-solution-by-iqbaldiit-kyfp/
'''
	Table: prompts

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| user_id     | int     |
	| prompt      | varchar |
	| tokens      | int     |
	+-------------+---------+
	(user_id, prompt) is the primary key (unique value) for this table.
	Each row represents a prompt submitted by a user to an AI system along with the number of tokens consumed.
	Write a solution to analyze AI prompt usage patterns based on the following requirements:

	For each user, calculate the total number of prompts they have submitted.
	For each user, calculate the average tokens used per prompt (Rounded to 2 decimal places).
	Only include users who have submitted at least 3 prompts.
	Only include users who have submitted at least one prompt with tokens greater than their own average token usage.
	Return the result table ordered by average tokens in descending order, and then by user_id in ascending order.

	The result format is in the following example.



	Example:

	Input:

	prompts table:

	+---------+--------------------------+--------+
	| user_id | prompt                   | tokens |
	+---------+--------------------------+--------+
	| 1       | Write a blog outline     | 120    |
	| 1       | Generate SQL query       | 80     |
	| 1       | Summarize an article     | 200    |
	| 2       | Create resume bullet     | 60     |
	| 2       | Improve LinkedIn bio     | 70     |
	| 3       | Explain neural networks  | 300    |
	| 3       | Generate interview Q&A   | 250    |
	| 3       | Write cover letter       | 180    |
	| 3       | Optimize Python code     | 220    |
	+---------+--------------------------+--------+
	Output:

	+---------+---------------+------------+
	| user_id | prompt_count  | avg_tokens |
	+---------+---------------+------------+
	| 3       | 4             | 237.5      |
	| 1       | 3             | 133.33     |
	+---------+---------------+------------+
	Explanation:

	User 1:
	Total prompts = 3
	Average tokens = (120 + 80 + 200) / 3 = 133.33
	Has a prompt with 200 tokens, which is greater than the average
	Included in the result
	User 2:
	Total prompts = 2 (less than the required minimum)
	Excluded from the result
	User 3:
	Total prompts = 4
	Average tokens = (300 + 250 + 180 + 220) / 4 = 237.5
	Has prompts with 300 and 250 tokens, both greater than the average
	Included in the result
	The Results table is ordered by avg_tokens in descending order, then by user_id in ascending order
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

# # #  # # # # # #### ================ Approach->1 : (DSL)
#
# df_top_user=(df.groupBy("user_id").agg(
#     count("course_id").alias("course_count")
#     ,avg("course_rating").alias("avg_rating")
#     )
#     .where((col("course_count")>=5) & (col("avg_rating")>=4))
#     .select("user_id")
# )
#
# df_top_course=(df.join(df_top_user,on="user_id",how="inner")
#                .withColumn("second_course",lead("course_name")
#                            .over(Window.partitionBy("user_id")
#                                  .orderBy("completion_date"))
#                )
#                .where(col("second_course").isNotNull())
#                .select(col("course_name").alias("first_course"),col("second_course"))
# )
#
# df_result=(df_top_course.groupby("first_course","second_course")
#            .agg(count("*").alias("transition_count"))
#            .orderBy(desc("transition_count"),asc("first_course"),asc("second_course"))
# )
#
# df_result.show(truncate=False)

# # # #### ================ Approach->2 : (SQL)
df.createOrReplaceTempView("prompts")


sSQL="""
    WITH tbl_summary AS (
        SELECT user_id, COUNT(*) AS prompt_count, ROUND(AVG(tokens*1.00),2) AS avg_tokens, MAX(tokens*1.00) AS max_token   
        FROM prompts GROUP BY user_id
    )
    SELECT user_id,prompt_count,avg_tokens FROM tbl_summary 
    WHERE prompt_count>=3 AND max_token>avg_tokens
    ORDER BY avg_tokens DESC, user_id ASC
"""
df=spark.sql(sSQL)
df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()