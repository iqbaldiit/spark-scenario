'''
Title: Compare Two Tables and Identify Differences

You are given two tables:

1. Source Table with columns (id, name)
2. Target Table with columns (id1, name1)

Write a query to generate an output that contains:

id from either the source or target. A comment based on the following rules:

1. If an id exists in the source but not in the target, label it as 'new in source'.
2. If an id exists in the target but not in the source, label it as 'new in target'.
3. If an id exists in both, but name and name1 are different, label it as 'mismatch'.

Return the result ordered by id.

Input :-
        +---+----+
        | id|name|
        +---+----+
        |  1|   A|
        |  2|   B|
        |  3|   C|
        |  4|   D|
        +---+----+

        +---+-----+
        |id1|name1|
        +---+-----+
        |  1|    A|
        |  2|    B|
        |  4|    X|
        |  5|    F|
        +---+-----+

Expected Output :-
        +---+-------------+
        | id|      comment|
        +---+-------------+
        |  3|new in source|
        |  4|     mismatch|
        |  5|new in target|
        +---+-------------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
source_data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
target_data = [(1, "A"), (2, "B"), (4, "X"), (5, "F")]

source_columns = ["id", "name"]
target_columns = ["id1", "name1"]

# convert the worker list to data frame
df_source = spark.createDataFrame(source_data,source_columns)
df_target = spark.createDataFrame(target_data,target_columns)

print()
print("==========Input Data=============")

df_source.show()
df_target.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

df=(df_source.alias("S")
    .join(df_target.alias("T")
          , [(col("S.id")==col("T.id1"))]
          , "outer")
)

df=(df.withColumn("comment",
                      when((col("id").isNotNull() & col("id1").isNull()), "new in source")
                      .when((col("id").isNull() & col("id1").isNotNull()), "new in target")
                      .when((col("id").isNotNull() & col("id1").isNotNull()
                             & (col("name")!=col("name1"))), "mismatch")
                      .otherwise(None)
    ).where(col("comment").isNotNull())
    .select(coalesce("id","id1").alias("id"),"comment")
    .orderBy(col("id").asc())
)
df.show()

# # # # # # #### ================ Approach->2 : ((SQL))
#
# df_source.createOrReplaceTempView("Source")
# df_target.createOrReplaceTempView("Target")
#
# sSQL="""
#     SELECT COALESCE (S.id,T.id1) AS id
#         , CASE WHEN S.id IS NOT NULL AND T.id1 IS NULL THEN 'new in source'
#             WHEN S.id IS NULL AND T.id1 IS NOT NULL THEN 'new in target'
#             WHEN S.id IS NOT NULL AND T.id1 IS NOT NULL
#                 AND S.name <> T.name1 THEN 'mismatch'
#             ELSE NULL END AS comment
#     FROM Source S
#     FULL JOIN Target T ON S.id=T.id1
#     WHERE (S.id IS NULL OR T.id1 IS NULL OR s.name <> t.name1)
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

















