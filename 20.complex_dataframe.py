'''
you are given a json file. read the data and create complex dataframe (In Data folder scenario20.json)

Input :
    root
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createAt: string (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- dislikes: long (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- likeCount: long (nullable = true)
 |-- likes: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- name: string (nullable = true)
 |-- place: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- totalFeed: long (nullable = true)
 |-- url: string (nullable = true)
 |-- userAction: long (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)

Output:
root
 |-- code: long (nullable = true)
 |-- commentCount: long (nullable = true)
 |-- createdAt: string (nullable = true)
 |-- description: string (nullable = true)
 |-- feedsComment: string (nullable = true)
 |-- id: long (nullable = true)
 |-- imagePaths: string (nullable = true)
 |-- images: string (nullable = true)
 |-- isdeleted: boolean (nullable = true)
 |-- lat: long (nullable = true)
 |-- likeDislike: struct (nullable = false)
 |    |-- dislikes: long (nullable = true)
 |    |-- likes: long (nullable = true)
 |    |-- userAction: long (nullable = true)
 |-- lng: long (nullable = true)
 |-- location: string (nullable = true)
 |-- mediatype: long (nullable = true)
 |-- msg: string (nullable = true)
 |-- multiMedia: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- createAt: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- likeCount: long (nullable = true)
 |    |    |-- mediatype: long (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- place: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |-- name: string (nullable = true)
 |-- profilePicture: string (nullable = true)
 |-- title: string (nullable = true)
 |-- userId: long (nullable = true)
 |-- videoUrl: string (nullable = true)
 |-- totalFeed: long (nullable = true)

'''

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Read data from json file and convert to dataframe
df = spark.read.format("json").option("multiline", "true").load("data/scenario20.json")


print()
print("==========Input Data=============")

df.printSchema()
print()
print("==========Expected output=============")

df=(df.withColumn("likeDislike",struct(col("dislikes"),col("likes"),col("userAction")))
        .withColumn("multiMedia",array(struct(
            col("createAt"),col("description"),col("id"),col("likeCount")
            ,col("mediatype"),col("name"),col("place"),col("url")
)))).drop(col("dislikes"),col("likes"),col("userAction"),col("createAt"),col("description")
            ,col("id"),col("likeCount"),col("mediatype"),col("name"),col("place"),col("url"))

df.printSchema()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()