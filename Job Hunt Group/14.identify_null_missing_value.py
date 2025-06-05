'''
You are working on a data quality check in a data lake for the Indian government's digital identity initiative.
You are provided with a raw dataset that contains Aadhaar card registration details. Your task is to identify
null/missing values in each column of the dataset.

Task
- Load the above data into a PySpark DataFrame.
- Write PySpark code to count the number of nulls in each column.
- Your code should be dynamic — it should work for any DataFrame (not hardcoded).
- Show the final output in a DataFrame with two columns: column_name, null_count.


Sample data :
data = [
 ("1234567890", "Rajesh", "M", "Maharashtra", "Mumbai", "400001", "9876543210", "rajesh@gmail.com"),
 ("2345678901", "Priya", "F", "Karnataka", "Bengaluru", "560001", None, "priya@gmail.com"),
 ("3456789012", None, "M", "Delhi", "New Delhi", "110001", "9876501234", None),
 ("4567890123", "Anjali", "F", "Maharashtra", "Pune", None, None, "anjali@yahoo.com"),
 ("5678901234", "Rakesh", None, "Tamil Nadu", "Chennai", "600001", "9898989898", None),
 ("6789012345", "Kavita", "F", None, None, None, None, "kavita@rediff.com")
]

columns = ["aadhaar_id", "name", "gender", "state", "city", "pin_code", "phone_number", "email"]


Expected Output :
+------------+----------+
| column_name|null_count|
+------------+----------+
|  aadhaar_id|         0|
|        city|         1|
|       email|         2|
|      gender|         1|
|        name|         1|
|phone_number|         3|
|    pin_code|         2|
|       state|         1|
+------------+----------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
data = [
    ("1234567890", "Rajesh", "M", "Maharashtra", "Mumbai", "400001", "9876543210", "rajesh@gmail.com"),
    ("2345678901", "Priya", "F", "Karnataka", "Bengaluru", "560001", None, "priya@gmail.com"),
    ("3456789012", None, "M", "Delhi", "New Delhi", "110001", "9876501234", None),
    ("4567890123", "Anjali", "F", "Maharashtra", "Pune", None, None, "anjali@yahoo.com"),
    ("5678901234", "Rakesh", None, "Tamil Nadu", "Chennai", "600001", "9898989898", None),
    ("6789012345", "Kavita", "F", None, None, None, None, "kavita@rediff.com")
]

columns = ["aadhaar_id", "name", "gender", "state", "city", "pin_code", "phone_number", "email"]

# convert list to data frame
df = spark.createDataFrame(data,columns)

print()
print("==========Input Data=============")

df.show()

print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (DSL)

# Dynamic null count
null_counts=df.select([sum(when(col(c).isNull(),1).otherwise(0)).alias(c) for c in df.columns])

# Transform to desired output format
df=null_counts.toPandas().melt(var_name='column_name',value_name='null_count')

#convert to spark dataframe again
df=spark.createDataFrame(df)
df=df.orderBy('column_name')
df.show()


## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()