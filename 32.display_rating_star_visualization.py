'''
Title: Display Food Ratings with Star Visualization

Given two tables name "food_items" and "ratings" containing food items and their ratings,
create a query that joins these tables and displays a visual star representation of the ratings.
It means
    1 star = *
    2 stars = **
    ...
    5 stars = *****

Input:

    food_items
    +-------+-------------------+
    |food_id|          food_item|
    +-------+-------------------+
    |      1|        Veg Biryani|
    |      2|     Veg Fried Rice|
    |      3|    Kaju Fried Rice|
    |      4|    Chicken Biryani|
    |      5|Chicken Dum Biryani|
    |      6|     Prawns Biryani|
    |      7|      Fish Birayani|
    +-------+-------------------+

    ratings
    +-------+------+
    |food_id|rating|
    +-------+------+
    |      1|     5|
    |      2|     3|
    |      3|     4|
    |      4|     4|
    |      5|     5|
    |      6|     4|
    |      7|     4|
    +-------+------+

Expected Output:

    +-------+-------------------+------+---------------+
    |food_id|          food_item|rating|stars(out of 5)|
    +-------+-------------------+------+---------------+
    |      1|        Veg Biryani|     5|          *****|
    |      2|     Veg Fried Rice|     3|            ***|
    |      3|    Kaju Fried Rice|     4|           ****|
    |      4|    Chicken Biryani|     4|           ****|
    |      5|Chicken Dum Biryani|     5|          *****|
    |      6|     Prawns Biryani|     4|           ****|
    |      7|      Fish Birayani|     4|           ****|
    +-------+-------------------+------+---------------+
'''
from pyspark.sql.functions import repeat

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
food_data = [
    (1, "Veg Biryani"), (2, "Veg Fried Rice"),
    (3, "Kaju Fried Rice"), (4, "Chicken Biryani"),
    (5, "Chicken Dum Biryani"), (6, "Prawns Biryani"),
    (7, "Fish Birayani")
]
food_columns = ["food_id", "food_item"]

ratings_data = [
    (1, 5), (2, 3), (3, 4),
    (4, 4), (5, 5), (6, 4),
    (7, 4)
]
rating_columns = ["food_id", "rating"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df_food = spark.createDataFrame(food_data,food_columns).cache()
df_rating = spark.createDataFrame(ratings_data,rating_columns).cache()


print()
print("==========Input Data=============")

df_food.show()
df_rating.show()


print()
print("==========Expected output=============")

# # # # # # #### ================ Approach->1 : (DSL)
#
# df= (df_food.join(df_rating,["food_id"],"inner")
#     .withColumn("stars(out of 5)", expr("REPEAT('*',rating)"))
#     .orderBy("food_id")
# )
#
# df.show()

# # # # # #### ================ Approach->2 : ((SQL))

df_food.createOrReplaceTempView("Food")
df_rating.createOrReplaceTempView("Rating")

sSQL="""
    SELECT F.*    
    ,REPEAT('*',R.rating) AS stars
    FROM Food F
    INNER JOIN Rating R ON F.food_id=R.food_id
    ORDER BY F.food_id
"""

df=spark.sql(sSQL)

df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()

