'''
Title: Clean and Load Valid Employee Records

You are given a CSV file containing employee information, but it also includes some corrupt/bad records.
Each valid record has exactly 3 fields:
emp_no, emp_name, and dep.

The corrupt records do not match the correct format (they have extra/missing fields, text corruption, etc.).

Write a function that reads the file into a Spark DataFrame, but filters out the bad records while reading itself (without using .filter() after reading).
Return a DataFrame containing only the valid records.

Constraints:
============
1. Do not use .filter() after loading the DataFrame.
2. You must handle corrupt/bad records during the reading phase itself.

There are three modes available when reading a file in Spark:

PERMISSIVE : This is the default mode. It attempts to parse all the rows in the file, and if it encounters any malformed
            data or parsing errors, it sets the problematic fields to null and adds a new column called _corrupt_record
            to store the entire problematic row as a string.

DROPMALFORMED : This mode drops the rows that contain malformed data or cannot be parsed according to the specified schema.
                It only includes the rows that can be successfully parsed.

FAILFAST : This mode throws an exception and fails immediately if it encounters any malformed data or parsing errors
            in the file. It does not process any further rows after the first encountered error.

You can specify the desired mode using the mode option when reading a file,
such as option("mode", "PERMISSIVE") or option("mode", "FAILFAST").
If the mode option is not explicitly set, it defaults to PERMISSIVE.


Expected Output :-
    +------+--------+----------+
    |emp_no|emp_name|       dep|
    +------+--------+----------+
    |   101| Murugan|HealthCare|
    |   102|  Kannan|   Finance|
    |   103|    Mani|        IT|
    |   104|   Pavan|        HR|
    +------+--------+----------+


'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

print()
print("==========Input Data=============")
df = spark.read.format("csv").option("header", "true") \
    .load("data/Scenerio25.csv")

df.show()

print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (drop corrupt lines during file reading)

df = (spark.read.format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .load("data/Scenerio25.csv"))

df.show()

# # # # # #### ================ Approach->2 : ( store bad lines separately)
#
# df = (spark.read.format("csv")
#       .option("header", "true")
#       .option("mode", "PERMISSIVE")
#       .load("data/Scenerio25.csv"))
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()

















