'''
    you are given a table name "employees" with the following column
    id, name, dept, salary

    please Write a PySpark solution to remove duplicate employee records based on the name column,
    keeping only the first occurrence of each name. The output should maintain all original columns
    and preserve the order of first occurrences.

input:

    +---+----+-----------+------+
    | id|name|       dept|salary|
    +---+----+-----------+------+
    |  1|Jhon|    Testing|  5000|
    |  2| Tim|Development|  6000|
    |  3|Jhon|Development|  5000|
    |  4| Sky| Prodcution|  8000|
    +---+----+-----------+------+

Expected output:
    +---+----+-----------+------+
    | id|name|       dept|salary|
    +---+----+-----------+------+
    |  1|Jhon|    Testing|  5000|
    |  2| Tim|Development|  6000|
    |  4| Sky| Prodcution|  8000|
    +---+----+-----------+------+

Solution Explanation: The problem stated that, we have to remove duplicate rows based on "name" column,
                    and we have to keep only the first row. for example, In the question "Jhon"
                    appear 2 times. one id is 1 and another id is 3. So we have to keep id 1.

Approach:
    1. user drop duplicate by name
    2. show
'''

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, "Jhon", "Testing", 5000),
    (2, "Tim", "Development", 6000),
    (3, "Jhon", "Development", 5000),
    (4, "Sky", "Prodcution", 8000)
]
columns=["id","name","dept","salary"]

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : ((DSL))
####df=df.orderBy(col("id").desc())
df=df.dropDuplicates(["name"]).orderBy(col("id").asc())
df.show()


# # # # # #### ================ Approach->2 : ((SQL))
# df.createOrReplaceTempView("tbl")
# sSQL="""
#     SELECT id,name,dept,salary FROM
#     (SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id ASC) row_id
#         FROM tbl) tab WHERE row_id=1 ORDER BY  id
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