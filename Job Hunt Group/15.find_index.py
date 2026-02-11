

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# start timer to see execution time
start_timer()

#============ Data preparation===============
# Create DataFrame
data = [
    (1, "a", 23),
    (2, "B", None),
    (3, "C", 56),
    (4, None, None)
]
# expected :  0,2,3
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()

# # Add index (0-based)
#
df_with_index = df.withColumn("idx", monotonically_increasing_id())
df_with_index.show()

# Filter rows where name OR age is NULL
result = df_with_index.filter(
    (col("name").isNull() & col("age").isNull()) | (col("name").isNotNull() & col("age").isNotNull())
).select("idx")

df_with_index.show()
result.show()



## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()