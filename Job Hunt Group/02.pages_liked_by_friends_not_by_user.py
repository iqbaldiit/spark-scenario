'''
    You are given two tables name "Friends" and "Likes"
    "Friends" table represent two column user_id and friend_id
    "Likes" table represent two tables user_id and page_id. Each row indicates that a user likes a particular page.

    Write an SQL query to find the user_id and page_id of pages that are:
        1. liked by at least one of their friends,
        2. but not liked by the user himself.

    Return the result sorted by user_id and then page_id.

Input:
    Friend:
            +-------+---------+
            |user_id|friend_id|
            +-------+---------+
            |      1|        2|
            |      1|        3|
            |      1|        4|
            |      2|        1|
            |      3|        1|
            +-------+---------+

    Like:
        +-------+-------+
        |user_id|page_id|
        +-------+-------+
        |      1|      A|
        |      1|      B|
        |      1|      C|
        |      2|      A|
        |      3|      A|
        +-------+-------+

Expected Output:

        +-------+-------+
        |user_id|page_id|
        +-------+-------+
        |      2|      B|
        |      2|      C|
        |      3|      B|
        |      3|      C|
        +-------+-------+


Solution Explanation : The problem stated that, simply, we have to find out our friend's likes those we didn't like.
                        For example--1 : (1) has friend (2). (2) has likes on (A) and also (1) has like on (A). So, (1) removed.
                                         (1) has friend (3). (3) has likes on (A) and also (1) has like on (A). So, (1) removed.
                                         (1) has friend (4). (4) has no likes. So, (1) again removed.
                                         finally user (1) will not in the result list

                        For Example--2:  (2) has friend (1). (1) has likes on (A,B,C) and also (2) has like on (A). So, (2,A) removed.
                                         finally (2,B) and (2,C) will be in the result list.

                        For Example--3: try yourself and make confident.

Approach:
    1. First find out friends page like by inner joining.
    2. Then, with the friend's like join likes as user's like on user_id and friend's page_id (left anti)
    3. select desired columns and print.

'''


from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
friend_data = [(1, 2), (1, 3), (1, 4), (2, 1), (3, 1)]
likes_data = [(1, 'A'), (1, 'B'), (1, 'C'), (2, 'A'), (3, 'A')]

friend_columns=['user_id', 'friend_id']
likes_column=['user_id', 'page_id']

# convert the worker list to data frame
friend_df=spark.createDataFrame(friend_data,friend_columns)
like_df=spark.createDataFrame(likes_data,likes_column)

print()
print("==========Input Data=============")

friend_df.show()
like_df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))

#get the friends like by joining
friends_like_df=(friend_df.alias("F").join(
                    like_df.alias("L"),col("F.friend_id")==col("L.user_id"), "inner")
                    .select(col("F.user_id"),col("L.page_id"))
)



#friends_like_df.show()

# now join the friends like with user likes to segregate likes those are not by user (self anti)
friend_not_user_df=(friends_like_df.alias("FL").join(like_df.alias("UL"),
                                     (col("FL.user_id") == col("UL.user_id")) &
                                     (col("FL.page_id") == col("UL.page_id")),
                                     "left_anti")  # keeps record those not match
)

friend_not_user_df.show()


# # # # # #### ================ Approach->2 : ((SQL))
# friend_df.createOrReplaceTempView("Friends")
# like_df.createOrReplaceTempView("Likes")
#
# sSQL = """
#     WITH friends_likes AS (
#         SELECT f.user_id, l.page_id
#         FROM Friends f
#         JOIN Likes l
#         ON f.friend_id = l.user_id
#     )
#     SELECT fl.user_id, fl.page_id
#     FROM friends_likes fl
#     LEFT ANTI JOIN likes ul
#     ON fl.user_id = ul.user_id AND fl.page_id = ul.page_id
# """
#
# df=spark.sql(sSQL)
# df.show()


# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()