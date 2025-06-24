# Source: https://leetcode.com/problems/find-consistently-improving-employees/solutions/6841397/simple-best-solution/
'''
--Source (MySQL, MSSQL, Oracle, PostgreSQL): https://leetcode.com/problems/find-consistently-improving-employees/solutions/6841397/simple-best-solution-by-iqbaldiit-5zao/

	Table: employees

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| employee_id | int     |
	| name        | varchar |
	+-------------+---------+
	employee_id is the unique identifier for this table.
	Each row contains information about an employee.
	Table: performance_reviews

	+-------------+------+
	| Column Name | Type |
	+-------------+------+
	| review_id   | int  |
	| employee_id | int  |
	| review_date | date |
	| rating      | int  |
	+-------------+------+
	review_id is the unique identifier for this table.
	Each row represents a performance review for an employee. The rating is on a scale of 1-5 where 5 is excellent and 1 is poor.
	Write a solution to find employees who have consistently improved their performance over their last three reviews.

	An employee must have at least 3 review to be considered
	The employee's last 3 reviews must show strictly increasing ratings (each review better than the previous)
	Use the most recent 3 reviews based on review_date for each employee
	Calculate the improvement score as the difference between the latest rating and the earliest rating among the last 3 reviews
	Return the result table ordered by improvement score in descending order, then by name in ascending order.

	The result format is in the following example.



	Example:

	Input:

	employees table:

	+-------------+----------------+
	| employee_id | name           |
	+-------------+----------------+
	| 1           | Alice Johnson  |
	| 2           | Bob Smith      |
	| 3           | Carol Davis    |
	| 4           | David Wilson   |
	| 5           | Emma Brown     |
	+-------------+----------------+
	performance_reviews table:

	+-----------+-------------+-------------+--------+
	| review_id | employee_id | review_date | rating |
	+-----------+-------------+-------------+--------+
	| 1         | 1           | 2023-01-15  | 2      |
	| 2         | 1           | 2023-04-15  | 3      |
	| 3         | 1           | 2023-07-15  | 4      |
	| 4         | 1           | 2023-10-15  | 5      |
	| 5         | 2           | 2023-02-01  | 3      |
	| 6         | 2           | 2023-05-01  | 2      |
	| 7         | 2           | 2023-08-01  | 4      |
	| 8         | 2           | 2023-11-01  | 5      |
	| 9         | 3           | 2023-03-10  | 1      |
	| 10        | 3           | 2023-06-10  | 2      |
	| 11        | 3           | 2023-09-10  | 3      |
	| 12        | 3           | 2023-12-10  | 4      |
	| 13        | 4           | 2023-01-20  | 4      |
	| 14        | 4           | 2023-04-20  | 4      |
	| 15        | 4           | 2023-07-20  | 4      |
	| 16        | 5           | 2023-02-15  | 3      |
	| 17        | 5           | 2023-05-15  | 2      |
	+-----------+-------------+-------------+--------+
	Output:

	+-------------+----------------+-------------------+
	| employee_id | name           | improvement_score |
	+-------------+----------------+-------------------+
	| 2           | Bob Smith      | 3                 |
	| 1           | Alice Johnson  | 2                 |
	| 3           | Carol Davis    | 2                 |
	+-------------+----------------+-------------------+
	Explanation:

	Alice Johnson (employee_id = 1):
	Has 4 reviews with ratings: 2, 3, 4, 5
	Last 3 reviews (by date): 2023-04-15 (3), 2023-07-15 (4), 2023-10-15 (5)
	Ratings are strictly increasing: 3 → 4 → 5
	Improvement score: 5 - 3 = 2
	Carol Davis (employee_id = 3):
	Has 4 reviews with ratings: 1, 2, 3, 4
	Last 3 reviews (by date): 2023-06-10 (2), 2023-09-10 (3), 2023-12-10 (4)
	Ratings are strictly increasing: 2 → 3 → 4
	Improvement score: 4 - 2 = 2
	Bob Smith (employee_id = 2):
	Has 4 reviews with ratings: 3, 2, 4, 5
	Last 3 reviews (by date): 2023-05-01 (2), 2023-08-01 (4), 2023-11-01 (5)
	Ratings are strictly increasing: 2 → 4 → 5
	Improvement score: 5 - 2 = 3
	Employees not included:
	David Wilson (employee_id = 4): Last 3 reviews are all 4 (no improvement)
	Emma Brown (employee_id = 5): Only has 2 reviews (needs at least 3)
	The output table is ordered by improvement_score in descending order, then by name in ascending order.
'''
from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
employees_data = [
    (1, "Alice Johnson"),
    (2, "Bob Smith"),
    (3, "Carol Davis"),
    (4, "David Wilson"),
    (5, "Emma Brown")
]

employees_columns = ["employee_id", "name",]

# convert list to data frame
employees_df = spark.createDataFrame(employees_data,employees_columns)

# Sample product data
performance_reviews_data = [
    (1, 1, "2023-01-15", 2),
    (2, 1, "2023-04-15", 3),
    (3, 1, "2023-07-15", 4),
    (4, 1, "2023-10-15", 5),
    (5, 2, "2023-02-01", 3),
    (6, 2, "2023-05-01", 2),
    (7, 2, "2023-08-01", 4),
    (8, 2, "2023-11-01", 5),
    (9, 3, "2023-03-10", 1),
    (10, 3, "2023-06-10", 2),
    (11, 3, "2023-09-10", 3),
    (12, 3, "2023-12-10", 4),
    (13, 4, "2023-01-20", 4),
    (14, 4, "2023-04-20", 4),
    (15, 4, "2023-07-20", 4),
    (16, 5, "2023-02-15", 3),
    (17, 5, "2023-05-15", 2)
]

performance_reviews_columns = ["review_id", "employee_id", "review_date", "rating"]

# convert list to data frame
performance_reviews_df = spark.createDataFrame(performance_reviews_data,performance_reviews_columns)

print()
print("==========Input Data=============")

employees_df.show()
performance_reviews_df.show()
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)

# create a window to get most recent review
window=Window.partitionBy("employee_id").orderBy(desc("review_date"))

# rank the dataset to find the last three reviews
df=(performance_reviews_df.withColumn("rk", row_number().over(window))
    .where(col("rk")<=3)
)

# get the valid employee who has at least 3 reviews
valid_emp_df=(performance_reviews_df.groupby("employee_id").agg(count("*").alias("review_counts"))
              .where(col("review_counts")>=3)
              )

# get valid employee with review scores
df=df.join(valid_emp_df,"employee_id")

# check the employee is improving continuously and find improvement scores
df=(df.groupby("employee_id").pivot("rk").agg(max("rating"))
    .where((col("1")>col("2")) & (col("2")>col("3")))
    .withColumn("improvement_score", col("1")-col("3"))
    .drop(col("1"),col("2"),col("3"))
    )

# get the final result as per requirements
df=(df.join(employees_df,"employee_id")
    .select("employee_id","name","improvement_score")
    .orderBy(desc("improvement_score"),asc("name"))
    )
df.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()