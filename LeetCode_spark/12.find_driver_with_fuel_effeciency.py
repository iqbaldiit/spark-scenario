# Source: https://leetcode.com/problems/find-drivers-with-improved-fuel-efficiency/solutions/7168120/simple-best-solution-by-iqbaldiit-15js/
'''
	Table: drivers

	+-------------+---------+
	| Column Name | Type    |
	+-------------+---------+
	| driver_id   | int     |
	| driver_name | varchar |
	+-------------+---------+
	driver_id is the unique identifier for this table.
	Each row contains information about a driver.
	Table: trips

	+---------------+---------+
	| Column Name   | Type    |
	+---------------+---------+
	| trip_id       | int     |
	| driver_id     | int     |
	| trip_date     | date    |
	| distance_km   | decimal |
	| fuel_consumed | decimal |
	+---------------+---------+
	trip_id is the unique identifier for this table.
	Each row represents a trip made by a driver, including the distance traveled and fuel consumed for that trip.
	Write a solution to find drivers whose fuel efficiency has improved by comparing their average fuel efficiency in the first half of the year with the second half of the year.

	Calculate fuel efficiency as distance_km / fuel_consumed for each trip
	First half: January to June, Second half: July to December
	Only include drivers who have trips in both halves of the year
	Calculate the efficiency improvement as (second_half_avg - first_half_avg)
	Round all results to 2 decimal places
	Return the result table ordered by efficiency improvement in descending order, then by driver name in ascending order.

	The result format is in the following example.



	Example:

	Input:

	drivers table:

	+-----------+---------------+
	| driver_id | driver_name   |
	+-----------+---------------+
	| 1         | Alice Johnson |
	| 2         | Bob Smith     |
	| 3         | Carol Davis   |
	| 4         | David Wilson  |
	| 5         | Emma Brown    |
	+-----------+---------------+
	trips table:

	+---------+-----------+------------+-------------+---------------+
	| trip_id | driver_id | trip_date  | distance_km | fuel_consumed |
	+---------+-----------+------------+-------------+---------------+
	| 1       | 1         | 2023-02-15 | 120.5       | 10.2          |
	| 2       | 1         | 2023-03-20 | 200.0       | 16.5          |
	| 3       | 1         | 2023-08-10 | 150.0       | 11.0          |
	| 4       | 1         | 2023-09-25 | 180.0       | 12.5          |
	| 5       | 2         | 2023-01-10 | 100.0       | 9.0           |
	| 6       | 2         | 2023-04-15 | 250.0       | 22.0          |
	| 7       | 2         | 2023-10-05 | 200.0       | 15.0          |
	| 8       | 3         | 2023-03-12 | 80.0        | 8.5           |
	| 9       | 3         | 2023-05-18 | 90.0        | 9.2           |
	| 10      | 4         | 2023-07-22 | 160.0       | 12.8          |
	| 11      | 4         | 2023-11-30 | 140.0       | 11.0          |
	| 12      | 5         | 2023-02-28 | 110.0       | 11.5          |
	+---------+-----------+------------+-------------+---------------+
	Output:

	+-----------+---------------+------------------+-------------------+------------------------+
	| driver_id | driver_name   | first_half_avg   | second_half_avg   | efficiency_improvement |
	+-----------+---------------+------------------+-------------------+------------------------+
	| 2         | Bob Smith     | 11.24            | 13.33             | 2.10                   |
	| 1         | Alice Johnson | 11.97            | 14.02             | 2.05                   |
	+-----------+---------------+------------------+-------------------+------------------------+
	Explanation:

	Alice Johnson (driver_id = 1):
	First half trips (Jan-Jun): Feb 15 (120.5/10.2 = 11.81), Mar 20 (200.0/16.5 = 12.12)
	First half average efficiency: (11.81 + 12.12) / 2 = 11.97
	Second half trips (Jul-Dec): Aug 10 (150.0/11.0 = 13.64), Sep 25 (180.0/12.5 = 14.40)
	Second half average efficiency: (13.64 + 14.40) / 2 = 14.02
	Efficiency improvement: 14.02 - 11.97 = 2.05
	Bob Smith (driver_id = 2):
	First half trips: Jan 10 (100.0/9.0 = 11.11), Apr 15 (250.0/22.0 = 11.36)
	First half average efficiency: (11.11 + 11.36) / 2 = 11.24
	Second half trips: Oct 5 (200.0/15.0 = 13.33)
	Second half average efficiency: 13.33
	Efficiency improvement: 13.33 - 11.24 = 2.10 (rounded to 2 decimal places)
	Drivers not included:
	Carol Davis (driver_id = 3): Only has trips in first half (Mar, May)
	David Wilson (driver_id = 4): Only has trips in second half (Jul, Nov)
	Emma Brown (driver_id = 5): Only has trips in first half (Feb)
	The output table is ordered by efficiency improvement in descending order then by name in ascending order.

'''
from pyspark.sql.functions import month
from six import integer_types

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
driver_data = [
    ('1', 'Alice Johnson')
    ,('2', 'Bob Smith')
    ,('3', 'Carol Davis')
    ,('4', 'David Wilson')
    ,('5', 'Emma Brown')
]

driver_columns = ["driver_id", "driver_name"]

# convert list to data frame
driver_df = spark.createDataFrame(driver_data,driver_columns)

trips_data = [
    ('1', '1', '2023-02-15', '120.5', '10.2')
    , ('2', '1', '2023-03-20', '200.0', '16.5')
    , ('3', '1', '2023-08-10', '150.0', '11.0')
    , ('4', '1', '2023-09-25', '180.0', '12.5')
    , ('5', '2', '2023-01-10', '100.0', '9.0')
    , ('6', '2', '2023-04-15', '250.0', '22.0')
    , ('7', '2', '2023-10-05', '200.0', '15.0')
    , ('8', '3', '2023-03-12', '80.0', '8.5')
    , ('9', '3', '2023-05-18', '90.0', '9.2')
    , ('10', '4', '2023-07-22', '160.0', '12.8')
    , ('11', '4', '2023-11-30', '140.0', '11.0')
    , ('12', '5', '2023-02-28', '110.0', '11.5')
]

trips_columns = ["trip_id", "driver_id", "trip_date", "distance_km", "fuel_consumed"]

# convert list to data frame
trips_df = spark.createDataFrame(trips_data,trips_columns)
print()
print("==========Input Data=============")

driver_df.show()
trips_df.show()


print()
print("==========Expected output=============")

# #  # # # # # #### ================ Approach->1 : (DSL)
#
# df=(
#     (
#         (
#              trips_df.withColumn("efficiency", col("distance_km")/col("fuel_consumed"))
#                 .withColumn("half_year", when(month(col("trip_date")).between(1,6),1).otherwise(2).alias("half_year"))
#         ).groupby("driver_id","half_year").agg(avg("efficiency").alias("avg_effeciency"))
#     ).groupby("driver_id").agg(sum(when(col("half_year")==1,col("avg_effeciency"))
#                                    .otherwise(0)).alias("first_half_avg")
#                            ,sum(when(col("half_year")==2,col("avg_effeciency"))
#                                 .otherwise(0)).alias("second_half_avg"))
# ).where((col("first_half_avg")>0)
#         & (col("second_half_avg")>0)
#         & ((col("second_half_avg")-col("first_half_avg"))>0))\
# .join(driver_df,"driver_id","inner")\
# .select("driver_id","driver_name"
#         ,round(col("first_half_avg"),2).alias("first_half_avg")
#         ,round(col("second_half_avg"),2).alias("second_half_avg")
#         ,round((col("second_half_avg")-col("first_half_avg")),2).alias("efficiency_improvement"))\
# .orderBy(desc("efficiency_improvement"),asc("driver_name"))
# df.show()


# # # #### ================ Approach->2 : (SQL)
driver_df.createOrReplaceTempView("drivers")
trips_df.createOrReplaceTempView("trips")

sSQL="""
    WITH driver_effeciency AS (
        SELECT driver_id	
        , (distance_km*1.00 / fuel_consumed) AS efficiency	
        , CASE WHEN MONTH(trip_date) BETWEEN 1 AND 6 THEN 1 ELSE 2 END AS half_year	
        FROM trips 	
    ), driver_effeciency_avg AS (
        SELECT driver_id,half_year
        ,AVG(efficiency) avg_effeciency
        FROM driver_effeciency DE 
        GROUP BY driver_id,half_year
    ), final_result AS (
        SELECT driver_id
        , SUM(CASE WHEN half_year=1 THEN avg_effeciency ELSE 0 END) AS first_half_avg
        , SUM(CASE WHEN half_year=2 THEN avg_effeciency ELSE 0 END) AS second_half_avg
        FROM driver_effeciency_avg	
        GROUP BY driver_id
    )
    SELECT F.driver_id,D.driver_name
    ,ROUND(F.first_half_avg,2) AS first_half_avg
    ,ROUND(F.second_half_avg,2) AS second_half_avg
    ,ROUND(F.second_half_avg-F.first_half_avg,2) AS efficiency_improvement
    FROM final_result F
    INNER JOIN drivers D ON F.driver_id=D.driver_id
    WHERE first_half_avg>0 AND second_half_avg>0 AND ROUND(F.second_half_avg-F.first_half_avg,2)>0
    ORDER BY efficiency_improvement DESC, driver_name ASC
"""
df=spark.sql(sSQL)
df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()