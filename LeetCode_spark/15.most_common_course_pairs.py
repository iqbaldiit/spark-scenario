# Source: https://leetcode.com/problems/most-common-course-pairs/solutions/8511786/easy-and-best-solution-by-iqbaldiit-kyfp/
'''
Table: course_completions

+-------------------+---------+
| Column Name       | Type    |
+-------------------+---------+
| user_id           | int     |
| course_id         | int     |
| course_name       | varchar |
| completion_date   | date    |
| course_rating     | int     |
+-------------------+---------+
(user_id, course_id) is the combination of columns with unique values for this table.
Each row represents a completed course by a user with their rating (1-5 scale).
Write a solution to identify skill mastery pathways by analyzing course completion sequences among top-performing students:

Consider only top-performing students (those who completed at least 5 courses with an average rating of 4 or higher).
For each top performer, identify the sequence of courses they completed in chronological order.
Find all consecutive course pairs (Course A → Course B) taken by these students.
Return the pair frequency, identifying which course transitions are most common among high achievers.
Return the result table ordered by pair frequency in descending order and then by first course name and second course name in ascending order.

The result format is in the following example.



Example:

Input:

course_completions table:

+---------+-----------+------------------+-----------------+---------------+
| user_id | course_id | course_name      | completion_date | course_rating |
+---------+-----------+------------------+-----------------+---------------+
| 1       | 101       | Python Basics    | 2024-01-05      | 5             |
| 1       | 102       | SQL Fundamentals | 2024-02-10      | 4             |
| 1       | 103       | JavaScript       | 2024-03-15      | 5             |
| 1       | 104       | React Basics     | 2024-04-20      | 4             |
| 1       | 105       | Node.js          | 2024-05-25      | 5             |
| 1       | 106       | Docker           | 2024-06-30      | 4             |
| 2       | 101       | Python Basics    | 2024-01-08      | 4             |
| 2       | 104       | React Basics     | 2024-02-14      | 5             |
| 2       | 105       | Node.js          | 2024-03-20      | 4             |
| 2       | 106       | Docker           | 2024-04-25      | 5             |
| 2       | 107       | AWS Fundamentals | 2024-05-30      | 4             |
| 3       | 101       | Python Basics    | 2024-01-10      | 3             |
| 3       | 102       | SQL Fundamentals | 2024-02-12      | 3             |
| 3       | 103       | JavaScript       | 2024-03-18      | 3             |
| 3       | 104       | React Basics     | 2024-04-22      | 2             |
| 3       | 105       | Node.js          | 2024-05-28      | 3             |
| 4       | 101       | Python Basics    | 2024-01-12      | 5             |
| 4       | 108       | Data Science     | 2024-02-16      | 5             |
| 4       | 109       | Machine Learning | 2024-03-22      | 5             |
+---------+-----------+------------------+-----------------+---------------+
Output:

+------------------+------------------+------------------+
| first_course     | second_course    | transition_count |
+------------------+------------------+------------------+
| Node.js          | Docker           | 2                |
| React Basics     | Node.js          | 2                |
| Docker           | AWS Fundamentals | 1                |
| JavaScript       | React Basics     | 1                |
| Python Basics    | React Basics     | 1                |
| Python Basics    | SQL Fundamentals | 1                |
| SQL Fundamentals | JavaScript       | 1                |
+------------------+------------------+------------------+
Explanation:

User 1: Completed 6 courses with average rating 4.5 (qualifies as top performer)
User 2: Completed 5 courses with average rating 4.4 (qualifies as top performer)
User 3: Completed 5 courses but average rating is 2.8 (does not qualify)
User 4: Completed only 3 courses (does not qualify)
Course Pairs Among Top Performers:
User 1: Python Basics → SQL Fundamentals → JavaScript → React Basics → Node.js → Docker
User 2: Python Basics → React Basics → Node.js → Docker → AWS Fundamentals
Most common transitions: Node.js → Docker (2 times), React Basics → Node.js (2 times)
Results are ordered by transition_count in descending order, then by first_course in ascending order, and then by second_course in ascending order.

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
    (1, 101, 'Python Basics', '2024-01-05', 5),
    (1, 102, 'SQL Fundamentals', '2024-02-10', 4),
    (1, 103, 'JavaScript', '2024-03-15', 5),
    (1, 104, 'React Basics', '2024-04-20', 4),
    (1, 105, 'Node.js', '2024-05-25', 5),
    (1, 106, 'Docker', '2024-06-30', 4),
    (2, 101, 'Python Basics', '2024-01-08', 4),
    (2, 104, 'React Basics', '2024-02-14', 5),
    (2, 105, 'Node.js', '2024-03-20', 4),
    (2, 106, 'Docker', '2024-04-25', 5),
    (2, 107, 'AWS Fundamentals', '2024-05-30', 4),
    (3, 101, 'Python Basics', '2024-01-10', 3),
    (3, 102, 'SQL Fundamentals', '2024-02-12', 3),
    (3, 103, 'JavaScript', '2024-03-18', 3),
    (3, 104, 'React Basics', '2024-04-22', 2),
    (3, 105, 'Node.js', '2024-05-28', 3),
    (4, 101, 'Python Basics', '2024-01-12', 5),
    (4, 108, 'Data Science', '2024-02-16', 5),
    (4, 109, 'Machine Learning', '2024-03-22', 5)
]

columns = ["user_id","course_id","course_name","completion_date","course_rating"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")
df.show()

print()
print("==========Expected output=============")

# #  # # # # # #### ================ Approach->1 : (DSL)

df_top_user=(df.groupBy("user_id").agg(
    count("course_id").alias("course_count")
    ,avg("course_rating").alias("avg_rating")
    )
    .where((col("course_count")>=5) & (col("avg_rating")>=4))
    .select("user_id")
)

df_top_course=(df.join(df_top_user,on="user_id",how="inner")
               .withColumn("second_course",lead("course_name")
                           .over(Window.partitionBy("user_id")
                                 .orderBy("completion_date"))
               )
               .where(col("second_course").isNotNull())
               .select(col("course_name").alias("first_course"),col("second_course"))
)

df_result=(df_top_course.groupby("first_course","second_course")
           .agg(count("*").alias("transition_count"))
           .orderBy(desc("transition_count"),asc("first_course"),asc("second_course"))
)

df_result.show(truncate=False)



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()