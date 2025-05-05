from idlelib.replace import replace

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window





# Read CSV (adjust path as needed)
df = spark.read.csv("../data/csv.csv", header=True, inferSchema=True,quote='"',multiLine=True,escape='"')

df.show()


df=(df.select(
    split(
        regexp_replace("error_msg",'"','')
        ,",").alias("error_msg")
))


df=(df
    .withColumn("message",col("error_msg")[0])
    .withColumn("excess_info", regexp_replace(col("error_msg")[1],"excess:",""))
    .withColumn("client", regexp_replace(col("error_msg")[2],"client:",""))
    .withColumn("server", regexp_replace(col("error_msg")[3],"server:",""))
    .withColumn("request", regexp_replace(col("error_msg")[4],"request:",""))
    .withColumn("host", regexp_replace(col("error_msg")[5],"host:",""))
    .drop("error_msg")
)

df.show()