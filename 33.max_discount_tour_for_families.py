'''
Title: Maximum Discount Tours for Families

A travel agency offers discount tours to families, but each tour has specific size requirements.
Given two tables - one with family information and another with tour requirements

- write a query to find the family that can choose the maximum number of discount tours based on their size.

Input:
    +--------------------+--------------+-----------+
    |                  id|          name|family_size|
    +--------------------+--------------+-----------+
    |c00dac11bde74750b...|   Alex Thomas|          9|
    |eb6f2d3426694667a...|    Chris Gray|          2|
    |3f7b5b8e835d4e1c8...| Emily Johnson|          4|
    |9a345b079d9f4d3ca...| Michael Brown|          6|
    |e0a5f57516024de2a...|Jessica Wilson|          3|
    +--------------------+--------------+-----------+

    +--------------------+------------+--------+--------+
    |                  id|        name|min_size|max_size|
    +--------------------+------------+--------+--------+
    |023fd23615bd4ff4b...|     Bolivia|       2|       4|
    |be247f73de0f4b2d8...|Cook Islands|       4|       8|
    |3e85ab80a6f84ef3b...|      Brazil|       4|       7|
    |e571e164152c4f7c8...|   Australia|       5|       9|
    |f35a7bb7d44342f7a...|      Canada|       3|       5|
    |a1b5a4b5fc5f46f89...|       Japan|      10|      12|
    +--------------------+------------+--------+--------+

Expected Output:
    +-------------+-------------------+
    |         name|number_of_countries|
    +-------------+-------------------+
    |Emily Johnson|                  4|
    +-------------+-------------------+

Explanation:

Emily Johnson (family_size=4) can choose:
    Bolivia (min=2, max=4)
    Cook Islands (min=4, max=8)
    Brazil (min=4, max=7)
    Canada (min=3, max=5)
No other family can choose more tours than this

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
family_data = [
    ("c00dac11bde74750b...", "Alex Thomas", 9),
    ("eb6f2d3426694667a...", "Chris Gray", 2),
    ("3f7b5b8e835d4e1c8...", "Emily Johnson", 4),
    ("9a345b079d9f4d3ca...", "Michael Brown", 6),
    ("e0a5f57516024de2a...", "Jessica Wilson", 3)
]

family_columns = ["id", "name", "family_size"]

tour_data = [
    ("023fd23615bd4ff4b...", "Bolivia", 2, 4),
    ("be247f73de0f4b2d8...", "Cook Islands", 4, 8),
    ("3e85ab80a6f84ef3b...", "Brazil", 4, 7),
    ("e571e164152c4f7c8...", "Australia", 5, 9),
    ("f35a7bb7d44342f7a...", "Canada", 3, 5),
    ("a1b5a4b5fc5f46f89...", "Japan", 10, 12)
]

tour_columns=["id", "name", "min_size", "max_size"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df_family = spark.createDataFrame(family_data,family_columns)
df_tour = spark.createDataFrame(tour_data,tour_columns)

print()
print("==========Input Data=============")

df_family.show()
df_tour.show()


print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

df=(df_family.alias("F").crossJoin(df_tour.alias("T"))
    .where(col("F.family_size").between(col("T.min_size"),col("T.max_size")))
    .groupby("F.name")
    .agg(count("F.id").alias("number_of_countries"))
)

window=Window.orderBy(col("number_of_countries").desc())

df=(df.withColumn("rk", dense_rank().over(window))
    .where(col("rk")==1)
    .drop("rk")
)

df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df_family.createOrReplaceTempView("Family")
# df_tour.createOrReplaceTempView("Tour")
#
# sSQL="""
#     SELECT name,number_of_countries  FROM(
#         SELECT tab.*
#         ,DENSE_RANK() OVER(ORDER BY tab.number_of_countries DESC) AS rk
#         FROM(
#             SELECT F.name,COUNT(*) AS number_of_countries
#             FROM Family F
#             CROSS JOIN Tour T
#             WHERE F.family_size BETWEEN T.min_size AND T.max_size
#             GROUP BY F.name
#         )tab
#     )tab2 WHERE tab2.rk=1
# """
#
# df=spark.sql(sSQL)
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()