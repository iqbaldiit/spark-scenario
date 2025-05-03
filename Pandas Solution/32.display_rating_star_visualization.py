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
import pandas as pd

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
df_food = pd.DataFrame(food_data,columns=food_columns)
df_rating = pd.DataFrame(ratings_data,columns=rating_columns)


print()
print("==========Input Data=============")

print(df_food)
print(df_rating)


print()
print("==========Expected output=============")

df=pd.merge(df_food,df_rating,on="food_id",how="inner")
df["stars(out of 5)"]=df["rating"].apply(lambda x:x*'*')
df=df.sort_values(by="food_id")

print(df)












