'''
    You are given a table "logins" with the with user_id  and  login_date Column
    Each row represents a day a user logged into the system. Your task is to identify all streaks of consecutive login days for each user.
    A login streak is defined as a sequence where a user logs in every day without missing any day

    Find a solution where Return all such streaks where the consecutive day count is greater than or equal to 2.

Input:
            +-------+----------+
            |user_id|login_date|
            +-------+----------+
            |      1|01-03-2025|
            |      1|02-03-2025|
            |      1|03-03-2025|
            |      1|05-03-2025|
            |      2|02-03-2025|
            |      2|03-03-2025|
            |      3|04-03-2025|
            |      3|05-03-2025|
            |      4|01-03-2025|
            |      4|02-03-2025|
            +-------+----------+

Expected Output:

            +-------+----------+----------+---------------------+
            |user_id|start_date|start_date|consecutive_day_count|
            +-------+----------+----------+---------------------+
            |      1|2025-03-01|2025-03-03|                    2|
            +-------+----------+----------+---------------------+


Solution Explanation : The problem stated that there is a login table with user login data, we need to find out
                       which user login each day and streaks more than 1. I am login today and tomorrow and each day.
                       in that way streak more than 1

Approach:
    1. check data type by print schema
    2. convert it if necessary
    3. there might have duplicate as the user login multiple time each day
    4. Find the next login date beside today
    5. remove the record where next date null as the login date already taken as next_date of previous login date
    6. make difference today and next day
    6. find start date, end date and consecutive day count
    7. filter more than 1
    8. print result.

'''


from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, '01-03-2025'), (1, '02-03-2025'), (1, '03-03-2025'), (1, '05-03-2025'),
    (2, '02-03-2025'), (2, '03-03-2025'),
    (3, '04-03-2025'), (3, '05-03-2025'),
    (4, '01-03-2025'), (4, '02-03-2025')
]

columns=['user_id', 'login_date']

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
df.printSchema()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (Using self join (DSL))

# Convert login_date string to date
df = df.withColumn("login_date", to_date(col("login_date"), "dd-MM-yyyy"))

# drop duplicate record
df = df.dropDuplicates(["user_id", "login_date"]).orderBy("user_id", "login_date")

# create a window for lead (for next consecutive date)
window=Window.partitionBy("user_id").orderBy("login_date")

# add a column with next date
df=df.withColumn("next_date",lead(col("login_date")).over(window))
#df.show()

#drop null where next date null
df=df.dropna()
#df.show()

# add a column date_diff for different the date
df=df.withColumn("date_diff",date_diff(col("next_date"),col("login_date")))
#df.show()

# filter the consecutive date
df=df.filter(col("date_diff")==1)
#df.show()

df=(df.groupBy("user_id").agg(min(col("login_date")).alias("start_date")
                             ,max(col("next_date")).alias("start_date")
                             ,sum(col("date_diff")).alias("consecutive_day_count")
                           )
    )

#df.show()
# find the use user who have more than 1 days streaks (equals 2 or more)
df=df.filter(col("consecutive_day_count")>1)
df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()