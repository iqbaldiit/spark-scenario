# Source:
'''
	Table: app_events

	+------------------+----------+
	| Column Name      | Type     |
	+------------------+----------+
	| event_id         | int      |
	| user_id          | int      |
	| event_timestamp  | datetime |
	| event_type       | varchar  |
	| session_id       | varchar  |
	| event_value      | int      |
	+------------------+----------+
	event_id is the unique identifier for this table.
	event_type can be app_open, click, scroll, purchase, or app_close.
	session_id groups events within the same user session.
	event_value represents: for purchase - amount in dollars, for scroll - pixels scrolled, for others - NULL.
	Write a solution to identify zombie sessions, sessions where users appear active but show abnormal behavior patterns. A session is considered a zombie session if it meets ALL the following criteria:

	The session duration is more than 30 minutes.
	Has at least 5 scroll events.
	The click-to-scroll ratio is less than 0.20 .
	No purchases were made during the session.
	Return the result table ordered by scroll_count in descending order, then by session_id in ascending order.

	The result format is in the following example.



	Example:

	Input:

	app_events table:

	+----------+---------+---------------------+------------+------------+-------------+
	| event_id | user_id | event_timestamp     | event_type | session_id | event_value |
	+----------+---------+---------------------+------------+------------+-------------+
	| 1        | 201     | 2024-03-01 10:00:00 | app_open   | S001       | NULL        |
	| 2        | 201     | 2024-03-01 10:05:00 | scroll     | S001       | 500         |
	| 3        | 201     | 2024-03-01 10:10:00 | scroll     | S001       | 750         |
	| 4        | 201     | 2024-03-01 10:15:00 | scroll     | S001       | 600         |
	| 5        | 201     | 2024-03-01 10:20:00 | scroll     | S001       | 800         |
	| 6        | 201     | 2024-03-01 10:25:00 | scroll     | S001       | 550         |
	| 7        | 201     | 2024-03-01 10:30:00 | scroll     | S001       | 900         |
	| 8        | 201     | 2024-03-01 10:35:00 | app_close  | S001       | NULL        |
	| 9        | 202     | 2024-03-01 11:00:00 | app_open   | S002       | NULL        |
	| 10       | 202     | 2024-03-01 11:02:00 | click      | S002       | NULL        |
	| 11       | 202     | 2024-03-01 11:05:00 | scroll     | S002       | 400         |
	| 12       | 202     | 2024-03-01 11:08:00 | click      | S002       | NULL        |
	| 13       | 202     | 2024-03-01 11:10:00 | scroll     | S002       | 350         |
	| 14       | 202     | 2024-03-01 11:15:00 | purchase   | S002       | 50          |
	| 15       | 202     | 2024-03-01 11:20:00 | app_close  | S002       | NULL        |
	| 16       | 203     | 2024-03-01 12:00:00 | app_open   | S003       | NULL        |
	| 17       | 203     | 2024-03-01 12:10:00 | scroll     | S003       | 1000        |
	| 18       | 203     | 2024-03-01 12:20:00 | scroll     | S003       | 1200        |
	| 19       | 203     | 2024-03-01 12:25:00 | click      | S003       | NULL        |
	| 20       | 203     | 2024-03-01 12:30:00 | scroll     | S003       | 800         |
	| 21       | 203     | 2024-03-01 12:40:00 | scroll     | S003       | 900         |
	| 22       | 203     | 2024-03-01 12:50:00 | scroll     | S003       | 1100        |
	| 23       | 203     | 2024-03-01 13:00:00 | app_close  | S003       | NULL        |
	| 24       | 204     | 2024-03-01 14:00:00 | app_open   | S004       | NULL        |
	| 25       | 204     | 2024-03-01 14:05:00 | scroll     | S004       | 600         |
	| 26       | 204     | 2024-03-01 14:08:00 | scroll     | S004       | 700         |
	| 27       | 204     | 2024-03-01 14:10:00 | click      | S004       | NULL        |
	| 28       | 204     | 2024-03-01 14:12:00 | app_close  | S004       | NULL        |
	+----------+---------+---------------------+------------+------------+-------------+
	Output:

	+------------+---------+--------------------------+--------------+
	| session_id | user_id | session_duration_minutes | scroll_count |
	+------------+---------+--------------------------+--------------+
	| S001       | 201     | 35                       | 6            |
	+------------+---------+--------------------------+--------------+
	Explanation:

	Session S001 (User 201):
	Duration: 10:00:00 to 10:35:00 = 35 minutes (more than 30)
	Scroll events: 6 (at least 5)
	Click events: 0
	Click-to-scroll ratio: 0/6 = 0.00 (less than 0.20)
	Purchases: 0 (no purchases)
	S001 is a zombie session (meets all criteria)
	Session S002 (User 202):
	Duration: 11:00:00 to 11:20:00 = 20 minutes (less than 30)
	Has a purchase event
	S002 is not a zombie session
	Session S003 (User 203):
	Duration: 12:00:00 to 13:00:00 = 60 minutes (more than 30)
	Scroll events: 5 (at least 5)
	Click events: 1
	Click-to-scroll ratio: 1/5 = 0.20 (not less than 0.20)
	Purchases: 0 (no purchases)
	S003 is not a zombie session (click-to-scroll ratio equals 0.20, needs to be less)
	Session S004 (User 204):
	Duration: 14:00:00 to 14:12:00 = 12 minutes (less than 30)
	Scroll events: 2 (less than 5)
	S004  is not a zombie session
	The result table is ordered by scroll_count in descending order, then by session_id in ascending order.

'''
from pyspark.sql.functions import month, unix_timestamp
from six import integer_types

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    ('1', '201', '2024-03-01 10:00:00', 'app_open', 'S001', None)
    , ('2', '201', '2024-03-01 10:05:00', 'scroll', 'S001', '500')
    , ('3', '201', '2024-03-01 10:10:00', 'scroll', 'S001', '750')
    , ('4', '201', '2024-03-01 10:15:00', 'scroll', 'S001', '600')
    , ('5', '201', '2024-03-01 10:20:00', 'scroll', 'S001', '800')
    , ('6', '201', '2024-03-01 10:25:00', 'scroll', 'S001', '550')
    , ('7', '201', '2024-03-01 10:30:00', 'scroll', 'S001', '900')
    , ('8', '201', '2024-03-01 10:35:00', 'app_close', 'S001', None)
    , ('9', '202', '2024-03-01 11:00:00', 'app_open', 'S002', None)
    , ('10', '202', '2024-03-01 11:02:00', 'click', 'S002', None)
    , ('11', '202', '2024-03-01 11:05:00', 'scroll', 'S002', '400')
    , ('12', '202', '2024-03-01 11:08:00', 'click', 'S002', None)
    , ('13', '202', '2024-03-01 11:10:00', 'scroll', 'S002', '350')
    , ('14', '202', '2024-03-01 11:15:00', 'purchase', 'S002', '50')
    , ('15', '202', '2024-03-01 11:20:00', 'app_close', 'S002', None)
    , ('16', '203', '2024-03-01 12:00:00', 'app_open', 'S003', None)
    , ('17', '203', '2024-03-01 12:10:00', 'scroll', 'S003', '1000')
    , ('18', '203', '2024-03-01 12:20:00', 'scroll', 'S003', '1200')
    , ('19', '203', '2024-03-01 12:25:00', 'click', 'S003', None)
    , ('20', '203', '2024-03-01 12:30:00', 'scroll', 'S003', '800')
    , ('21', '203', '2024-03-01 12:40:00', 'scroll', 'S003', '900')
    , ('22', '203', '2024-03-01 12:50:00', 'scroll', 'S003', '1100')
    , ('23', '203', '2024-03-01 13:00:00', 'app_close', 'S003', None)
    , ('24', '204', '2024-03-01 14:00:00', 'app_open', 'S004', None)
    , ('25', '204', '2024-03-01 14:05:00', 'scroll', 'S004', '600')
    , ('26', '204', '2024-03-01 14:08:00', 'scroll', 'S004', '700')
    , ('27', '204', '2024-03-01 14:10:00', 'click', 'S004', None)
    , ('28', '204', '2024-03-01 14:12:00', 'app_close', 'S004', None)
]

columns = ["event_id","user_id","event_timestamp","event_type","session_id","event_value"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")
df.show()

print()
print("==========Expected output=============")

#  # # # # # #### ================ Approach->1 : (DSL)
df=(df.groupby("session_id","user_id").agg(
    min(col("event_timestamp")).alias("session_start")
    ,min(col("event_timestamp")).alias("session_end")
    ,sum(when(col("event_type")=="scroll",1).otherwise(0)).alias("scroll_count")
    ,sum(when(col("event_type")=="click",1).otherwise(0)).alias("click_count")
    ,sum(when(col("event_type")=="purchase",1).otherwise(0)).alias("purchase_count")
    )
)

#df=df.withColumn("duration", (unix_timestamp(col("session_end"))-unix_timestamp(col("session_start")))/60)

df.show()


# # # # #### ================ Approach->2 : (SQL)
# driver_df.createOrReplaceTempView("drivers")
# trips_df.createOrReplaceTempView("trips")
#
# sSQL="""
#     WITH driver_effeciency AS (
#         SELECT driver_id
#         , (distance_km*1.00 / fuel_consumed) AS efficiency
#         , CASE WHEN MONTH(trip_date) BETWEEN 1 AND 6 THEN 1 ELSE 2 END AS half_year
#         FROM trips
#     ), driver_effeciency_avg AS (
#         SELECT driver_id,half_year
#         ,AVG(efficiency) avg_effeciency
#         FROM driver_effeciency DE
#         GROUP BY driver_id,half_year
#     ), final_result AS (
#         SELECT driver_id
#         , SUM(CASE WHEN half_year=1 THEN avg_effeciency ELSE 0 END) AS first_half_avg
#         , SUM(CASE WHEN half_year=2 THEN avg_effeciency ELSE 0 END) AS second_half_avg
#         FROM driver_effeciency_avg
#         GROUP BY driver_id
#     )
#     SELECT F.driver_id,D.driver_name
#     ,ROUND(F.first_half_avg,2) AS first_half_avg
#     ,ROUND(F.second_half_avg,2) AS second_half_avg
#     ,ROUND(F.second_half_avg-F.first_half_avg,2) AS efficiency_improvement
#     FROM final_result F
#     INNER JOIN drivers D ON F.driver_id=D.driver_id
#     WHERE first_half_avg>0 AND second_half_avg>0 AND ROUND(F.second_half_avg-F.first_half_avg,2)>0
#     ORDER BY efficiency_improvement DESC, driver_name ASC
# """
# df=spark.sql(sSQL)
# df.show()

## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()