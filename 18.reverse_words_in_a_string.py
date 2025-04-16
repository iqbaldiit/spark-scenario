'''
Write a PySpark solution to reverse each word in a string while preserving the
original word order and whitespace.
Given a DataFrame with a single string column, you should transform it such that each word
in the string is reversed, while maintaining the original structure of the string.

Input:
    +------------------+
    |              word|
    +------------------+
    |The Social Dilemma|
    +------------------+

Output:

    +------------------+
    |      reverse word|
    +------------------+
    |ehT laicoS ammeliD|
    +------------------+
'''
from six import string_types

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Sample Data
data = [ ("The Social Dilemma",)]
columns=["word",]

# convert the worker list to data frame
df=spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))
#
# user defined function for keep sentence structure as it is
# def reverse_word(sString):
#     return " ".join([word[::-1] for word in sString.split(" ")])
#
# # register the function to use in dsl
# udf_reversed=udf(reverse_word,StringType())#
# df=df.withColumn("reverse word", udf_reversed(df.word))

df=(df.withColumn("word_ary", split(df.word," "))
    .withColumn("reversed_words", transform("word_ary", lambda x:reverse(x)))
    .withColumn("reverse word", array_join("reversed_words"," "))
).select("reverse word")
df.show()


# # # # # #### ================ Approach->2 : ((SQL))
# df.createOrReplaceTempView("tbl")
# sSQL="""
#     SELECT CONCAT_WS(' ',TRANSFORM (SPLIT(word,' '), t->REVERSE(t))) AS reverse_word
#     FROM tbl
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