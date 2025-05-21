'''
	Table: Scores

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| student_id  | int     |
	| subject     | varchar |
	| score       | int     |
	| exam_date   | varchar |
	+-------------+---------+
	(student_id, subject, exam_date) is the primary key for this table.
	Each row contains information about a student's score in a specific subject on a particular exam date. score is between 0 and 100 (inclusive).
	Write a solution to find the students who have shown improvement. A student is considered to have shown improvement if they meet both of these conditions:

	Have taken exams in the same subject on at least two different dates
	Their latest score in that subject is higher than their first score
	Return the result table ordered by student_id, subject in ascending order.

	The result format is in the following example.



	Example:

	Input:

	Scores table:

	+------------+----------+-------+------------+
	| student_id | subject  | score | exam_date  |
	+------------+----------+-------+------------+
	| 101        | Math     | 70    | 2023-01-15 |
	| 101        | Math     | 85    | 2023-02-15 |
	| 101        | Physics  | 65    | 2023-01-15 |
	| 101        | Physics  | 60    | 2023-02-15 |
	| 102        | Math     | 80    | 2023-01-15 |
	| 102        | Math     | 85    | 2023-02-15 |
	| 103        | Math     | 90    | 2023-01-15 |
	| 104        | Physics  | 75    | 2023-01-15 |
	| 104        | Physics  | 85    | 2023-02-15 |
	+------------+----------+-------+------------+
	Output:

	+------------+----------+-------------+--------------+
	| student_id | subject  | first_score | latest_score |
	+------------+----------+-------------+--------------+
	| 101        | Math     | 70          | 85           |
	| 102        | Math     | 80          | 85           |
	| 104        | Physics  | 75          | 85           |
	+------------+----------+-------------+--------------+
	Explanation:

	Student 101 in Math: Improved from 70 to 85
	Student 101 in Physics: No improvement (dropped from 65 to 60)
	Student 102 in Math: Improved from 80 to 85
	Student 103 in Math: Only one exam, not eligible
	Student 104 in Physics: Improved from 75 to 85

'''
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    (101, "Math", 70, "2023-01-15"),
    (101, "Math", 85, "2023-02-15"),
    (101, "Physics", 65, "2023-01-15"),
    (101, "Physics", 60, "2023-02-15"),
    (102, "Math", 80, "2023-01-15"),
    (102, "Math", 85, "2023-02-15"),
    (103, "Math", 90, "2023-01-15"),
    (104, "Physics", 75, "2023-01-15"),
    (104, "Physics", 85, "2023-02-15")
]

columns = ["student_id", "subject", "score", "exam_date"]

# convert list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (DSL)

window=Window.partitionBy("student_id","subject").orderBy("exam_date")

df=(df.withColumn("first_score",first_value("score").over(window))
    .withColumn("latest_score", last_value("score").over(window.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))
    .where(col("latest_score")>col("first_score"))
    .select("student_id", "subject", "first_score", "latest_score")
    .distinct()
    .orderBy("student_id", "subject")
)
df.show()

# # # # #### ================ Approach->2 : (SQL)
# df.createOrReplaceTempView("Scores")
#
# sSQL="""
#
#     WITH tab AS(
#         SELECT *
#         ,FIRST_VALUE(score) OVER (PARTITION BY student_id, subject Order by exam_date) AS first_score
#         ,LAST_VALUE(score) OVER (PARTITION BY student_id, subject Order by exam_date
#                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_score
#         FROM Scores
#     )
#     SELECT DISTINCT student_id,subject,first_score,latest_score FROM tab WHERE latest_score>first_score
#     ORDER BY student_id,subject
#
# """
# df=spark.sql(sSQL)
# df.show()
## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()



