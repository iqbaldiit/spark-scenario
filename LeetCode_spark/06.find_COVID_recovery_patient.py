# Source : https://leetcode.com/problems/find-covid-recovery-patients/solutions/6890813/simple-best-solution-by-iqbaldiit-b259/
'''
	Table: patients

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| patient_id  | int     |
	| patient_name| varchar |
	| age         | int     |
	+-------------+---------+
	patient_id is the unique identifier for this table.
	Each row contains information about a patient.
	Table: covid_tests

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| test_id     | int     |
	| patient_id  | int     |
	| test_date   | date    |
	| result      | varchar |
	+-------------+---------+
	test_id is the unique identifier for this table.
	Each row represents a COVID test result. The result can be Positive, Negative, or Inconclusive.
	Write a solution to find patients who have recovered from COVID - patients who tested positive but later tested negative.

	A patient is considered recovered if they have at least one Positive test followed by at least one Negative test on a later date
	Calculate the recovery time in days as the difference between the first positive test and the first negative test after that positive test
	Only include patients who have both positive and negative test results
	Return the result table ordered by recovery_time in ascending order, then by patient_name in ascending order.

	The result format is in the following example.



	Example:

	Input:

	patients table:

	+------------+--------------+-----+
	| patient_id | patient_name | age |
	+------------+--------------+-----+
	| 1          | Alice Smith  | 28  |
	| 2          | Bob Johnson  | 35  |
	| 3          | Carol Davis  | 42  |
	| 4          | David Wilson | 31  |
	| 5          | Emma Brown   | 29  |
	+------------+--------------+-----+
	covid_tests table:

	+---------+------------+------------+--------------+
	| test_id | patient_id | test_date  | result       |
	+---------+------------+------------+--------------+
	| 1       | 1          | 2023-01-15 | Positive     |
	| 2       | 1          | 2023-01-25 | Negative     |
	| 3       | 2          | 2023-02-01 | Positive     |
	| 4       | 2          | 2023-02-05 | Inconclusive |
	| 5       | 2          | 2023-02-12 | Negative     |
	| 6       | 3          | 2023-01-20 | Negative     |
	| 7       | 3          | 2023-02-10 | Positive     |
	| 8       | 3          | 2023-02-20 | Negative     |
	| 9       | 4          | 2023-01-10 | Positive     |
	| 10      | 4          | 2023-01-18 | Positive     |
	| 11      | 5          | 2023-02-15 | Negative     |
	| 12      | 5          | 2023-02-20 | Negative     |
	+---------+------------+------------+--------------+
	Output:

	+------------+--------------+-----+---------------+
	| patient_id | patient_name | age | recovery_time |
	+------------+--------------+-----+---------------+
	| 1          | Alice Smith  | 28  | 10            |
	| 3          | Carol Davis  | 42  | 10            |
	| 2          | Bob Johnson  | 35  | 11            |
	+------------+--------------+-----+---------------+
	Explanation:

	Alice Smith (patient_id = 1):
	First positive test: 2023-01-15
	First negative test after positive: 2023-01-25
	Recovery time: 25 - 15 = 10 days
	Bob Johnson (patient_id = 2):
	First positive test: 2023-02-01
	Inconclusive test on 2023-02-05 (ignored for recovery calculation)
	First negative test after positive: 2023-02-12
	Recovery time: 12 - 1 = 11 days
	Carol Davis (patient_id = 3):
	Had negative test on 2023-01-20 (before positive test)
	First positive test: 2023-02-10
	First negative test after positive: 2023-02-20
	Recovery time: 20 - 10 = 10 days
	Patients not included:
	David Wilson (patient_id = 4): Only has positive tests, no negative test after positive
	Emma Brown (patient_id = 5): Only has negative tests, never tested positive
	Output table is ordered by recovery_time in ascending order, and then by patient_name in ascending order.
'''
from six import integer_types

from spark_session import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
patients_data = [
    (1, "Alice Smith", 28),
    (2, "Bob Johnson", 35),
    (3, "Carol Davis", 42),
    (4, "David Wilson", 31),
    (5, "Emma Brown", 29)
]

patients_columns = ["patient_id", "patient_name","age"]

# convert list to data frame
patients_df = spark.createDataFrame(patients_data,patients_columns)

# Sample data
covid_tests_data = [
    Row(1, 1, "2023-01-15", "Positive"),
    Row(2, 1, "2023-01-25", "Negative"),
    Row(3, 2, "2023-02-01", "Positive"),
    Row(4, 2, "2023-02-05", "Inconclusive"),
    Row(5, 2, "2023-02-12", "Negative"),
    Row(6, 3, "2023-01-20", "Negative"),
    Row(7, 3, "2023-02-10", "Positive"),
    Row(8, 3, "2023-02-20", "Negative"),
    Row(9, 4, "2023-01-10", "Positive"),
    Row(10, 4, "2023-01-18", "Positive"),
    Row(11, 5, "2023-02-15", "Negative"),
    Row(12, 5, "2023-02-20", "Negative")
]

covid_tests_schema=StructType([
    StructField("test_id", IntegerType(),nullable=False)
    ,StructField("patient_id", IntegerType(),nullable=False)
    ,StructField("test_date", StringType(),nullable=False)
    ,StructField("result", StringType(),nullable=False)
])

# convert list to data frame
covid_test_df = spark.createDataFrame(covid_tests_data,schema=covid_tests_schema)

print()
print("==========Input Data=============")

patients_df.show()
covid_test_df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# get those patient who are positive
positive_df=(covid_test_df.where(col("result")=="Positive")
             .groupby("patient_id").agg(min("test_date").alias("pos_date"))
             )

df=(covid_test_df.alias("N")
             .join(positive_df.alias("P"),"patient_id")
             .where((col("N.test_date")>col("P.pos_date")) & (col("result")=="Negative"))
             .groupby("N.patient_id","P.pos_date").agg(min("N.test_date").alias("neg_date"))
             .withColumn("recovery_time", datediff(col("neg_date"),col("P.pos_date")))
             .join(patients_df.alias("PT"),"patient_id")
             .select("PT.patient_id","PT.patient_name","PT.age","recovery_time")
             .orderBy(asc("recovery_time"),asc("patient_name"))
)


df.show()



# # # # #### ================ Approach->2 : (SQL)
# patients_df.createOrReplaceTempView("patients")
# covid_test_df.createOrReplaceTempView("covid_tests")
#
# sSQL="""
#     WITH pos AS (
#         SELECT patient_id, MIN(test_date) AS pos_date
#         FROM covid_tests
#         WHERE result = 'Positive'
#         GROUP BY patient_id
#     ),
#     neg AS (
#         SELECT CT.patient_id, pos.pos_date, MIN(CT.test_date) AS neg_date
#         FROM covid_tests CT
#         JOIN pos ON CT.patient_id = pos.patient_id
#         WHERE CT.result = 'Negative' AND CT.test_date > pos.pos_date
#         GROUP BY CT.patient_id, pos.pos_date
#     ),
#     result AS (
#         SELECT N.patient_id, P.patient_name, P.age,
#                DATEDIFF(DAY,N.pos_date,N.neg_date) AS recovery_time
#         FROM neg N
#         JOIN patients P ON P.patient_id = N.patient_id
#     )
#     SELECT *
#     FROM result
#     ORDER BY recovery_time, patient_name;
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()