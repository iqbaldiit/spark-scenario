'''

You are given a table called Salary with the following columns:
sno,name,salary
You need to remove the first salary record
(i.e., deduplicate based on name, record sequence maintain by Sno).


Input:
    +---+--------+------+
    |Sno|    Name|Salary|
    +---+--------+------+
    |  1|    AMIT|  1500|
    |  2|    AMIT|  1500|
    |  3|  Baskar|  2500|
    |  4|    Raja|  2500|
    |  5|Mukuthan|  3500|
    +---+--------+------+

Expected output:
    +---+--------+------+
    |Sno|    Name|Salary|
    +---+--------+------+
    |  2|    AMIT|  1500|
    |  3|  Baskar|  2500|
    |  4|    Raja|  2500|
    |  5|Mukuthan|  3500|
    +---+--------+------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, "AMIT", 1500),
    (2, "AMIT", 1500),
    (3, "Baskar", 2500),
    (4, "Raja", 2500),
    (5, "Mukuthan", 3500),

]

columns = ["Sno", "Name", "Salary"]

# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

#create window partition by duplicates and order by Sno
window=Window.partitionBy("Name").orderBy(col("Sno").asc())
df=df.withColumn("row_id",row_number().over(window))
# find the row number within thw window
df_cnt=df.groupBy("Name").agg(count(col("Sno")).alias("cnt"))


df=df.join(df_cnt,["Name"],"inner")
df=(df.withColumn("is_remove",(col("row_id")==1) & (col("cnt")>1))
    .where(col("is_remove")==False)
    .orderBy(col("Sno").asc())
    .select("Sno","Name","Salary")
)

df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()