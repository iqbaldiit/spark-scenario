'''
Title: Maximum Discount Tours for Families

A travel agency offers discount tours to families, but each tour has specific size requirements.
Given two tables - one with family information and another with tour requirements

- write a query to find the family that can choose the maximum number of discount tours based on their size.

Input:
    +--------------------+--------------+-----------+
    |                  id|          name|family_size|
    +--------------------+--------------+-----------+
    |c00dac11bde74750b...|   Alex Thomas|          9|
    |eb6f2d3426694667a...|    Chris Gray|          2|
    |3f7b5b8e835d4e1c8...| Emily Johnson|          4|
    |9a345b079d9f4d3ca...| Michael Brown|          6|
    |e0a5f57516024de2a...|Jessica Wilson|          3|
    +--------------------+--------------+-----------+

    +--------------------+------------+--------+--------+
    |                  id|        name|min_size|max_size|
    +--------------------+------------+--------+--------+
    |023fd23615bd4ff4b...|     Bolivia|       2|       4|
    |be247f73de0f4b2d8...|Cook Islands|       4|       8|
    |3e85ab80a6f84ef3b...|      Brazil|       4|       7|
    |e571e164152c4f7c8...|   Australia|       5|       9|
    |f35a7bb7d44342f7a...|      Canada|       3|       5|
    |a1b5a4b5fc5f46f89...|       Japan|      10|      12|
    +--------------------+------------+--------+--------+

Expected Output:
    +-------------+-------------------+
    |         name|number_of_countries|
    +-------------+-------------------+
    |Emily Johnson|                  4|
    +-------------+-------------------+

Explanation:

Emily Johnson (family_size=4) can choose:
    Bolivia (min=2, max=4)
    Cook Islands (min=4, max=8)
    Brazil (min=4, max=7)
    Canada (min=3, max=5)
No other family can choose more tours than this

'''

import pandas as pd

# Sample Data
family_data = [
    ("c00dac11bde74750b...", "Alex Thomas", 9),
    ("eb6f2d3426694667a...", "Chris Gray", 2),
    ("3f7b5b8e835d4e1c8...", "Emily Johnson", 4),
    ("9a345b079d9f4d3ca...", "Michael Brown", 6),
    ("e0a5f57516024de2a...", "Jessica Wilson", 3)
]

family_columns = ["id", "name", "family_size"]

tour_data = [
    ("023fd23615bd4ff4b...", "Bolivia", 2, 4),
    ("be247f73de0f4b2d8...", "Cook Islands", 4, 8),
    ("3e85ab80a6f84ef3b...", "Brazil", 4, 7),
    ("e571e164152c4f7c8...", "Australia", 5, 9),
    ("f35a7bb7d44342f7a...", "Canada", 3, 5),
    ("a1b5a4b5fc5f46f89...", "Japan", 10, 12)
]

tour_columns=["id", "name", "min_size", "max_size"]

# convert to data frame

df_family = pd.DataFrame(family_data,columns=family_columns)
df_tour = pd.DataFrame(tour_data,columns=tour_columns)

print()
print("==========Input Data=============")

print(df_family)
print(df_tour)


print()
print("==========Expected output=============")

df=pd.merge(df_family,df_tour,how="cross") # cross join to make relation each

# find the possible tour of each person
df=df[df["family_size"].between(df["min_size"],df["max_size"])]

# find the no of discount for each person
df=df.groupby("name_x")["name_y"].count().reset_index(name="number_of_countries")

# find the max number of discount tour
max_tour=df["number_of_countries"].max()

#find the person who get max number of discount.
df=df[df["number_of_countries"]==max_tour]

# reset the column
df.columns=["name","number_of_countries"]

print(df)
