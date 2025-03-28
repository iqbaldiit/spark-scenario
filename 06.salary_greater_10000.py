'''
    You are given a table name Employee containing emp_id, name and salary
    Write a solution where Employee salary greater than 10000 provide designation as manager else employee
'''

from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()

data = [
    ("1", "a", "10000"),
    ("2", "b", "5000"),
    ("3", "c", "15000"),
    ("4", "d", "25000"),
    ("5", "e", "50000"),
    ("6", "f", "7000")
]
columns = ["emp_id","name","salary"]

df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # #### ================ Approach->1 : (CASE WHEN (DSL))
df=df.withColumn("Designation", expr("CASE WHEN salary>10000 THEN 'Manager' ELSE 'Employee' END"))
df.show()

# # # # #### ================ Approach->2 : (CASE WHEN (SQL))
# df.createOrReplaceTempView("Employee")
# df=spark.sql("SELECT *, "
#              " CASE WHEN salary>10000 THEN 'Manager' ELSE 'Employee' END As Designation"
#              " FROM Employee")
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()