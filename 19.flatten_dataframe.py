'''
you are given a json file. read the data and flatten (In Data folder scenario19.json)

Input :
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
     |-- likeDislike: struct (nullable = true)
     |    |-- dislikes: long (nullable = true)
     |    |-- likes: long (nullable = true)
     |    |-- userAction: long (nullable = true)
     |-- lng: long (nullable = true)
     |-- location: string (nullable = true)
     |-- mediatype: long (nullable = true)
     |-- msg: string (nullable = true)
     |-- multiMedia: array (nullable = true)
     |    |-- element: struct (containsNull = true)
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
     |-- totalFeed: long (nullable = true)
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
     |-- lng: long (nullable = true)
     |-- location: string (nullable = true)
     |-- mediatype: long (nullable = true)
     |-- msg: string (nullable = true)
     |-- name: string (nullable = true)
     |-- profilePicture: string (nullable = true)
     |-- title: string (nullable = true)
     |-- totalFeed: long (nullable = true)
     |-- userId: long (nullable = true)
     |-- videoUrl: string (nullable = true)
     |-- dislikes: long (nullable = true)
     |-- likes: long (nullable = true)
     |-- userAction: long (nullable = true)
     |-- createAt: string (nullable = true)
     |-- media_description: string (nullable = true)
     |-- media_id: long (nullable = true)
     |-- media_likeCount: long (nullable = true)
     |-- media_mediatype: long (nullable = true)
     |-- media_name: string (nullable = true)
     |-- media_place: string (nullable = true)
     |-- media_url: string (nullable = true)

'''

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Read data from json file and convert to dataframe
df = spark.read.format("json").option("multiline", "true").load("data/scenario19.json")


print()
print("==========Input Data=============")

df.printSchema()
print()
print("==========Expected output=============")

df=(df.withColumn("dislikes",expr("likeDislike.dislikes"))
    .withColumn("likes",expr("likeDislike.likes"))
    .withColumn("dislikes",expr("likeDislike.dislikes"))
    .withColumn("userAction",expr("likeDislike.userAction"))

    .withColumn("media",explode("multiMedia")) # explode it because of array and converted to struct

    .withColumn("createAt",expr("media.createAt"))
    .withColumn("media_description",expr("media.description"))
    .withColumn("media_id",expr("media.id"))
    .withColumn("media_likeCount",expr("media.likeCount"))
    .withColumn("media_mediatype",expr("media.mediatype"))
    .withColumn("media_name",expr("media.name"))
    .withColumn("media_place",expr("media.place"))
    .withColumn("media_url",expr("media.url"))
).drop("likeDislike","multiMedia","media")

df.printSchema()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()