'''
Title: Find Grandparent Relationships in Family Tree

Given a table representing parent-child relationships,
write a SQL query to find each child with their immediate parent and grandparent.

Input:
    +-----+------+
    |child|parent|
    +-----+------+
    |    A|    AA|
    |    B|    BB|
    |    C|    CC|
    |   AA|   AAA|
    |   BB|   BBB|
    |   CC|   CCC|
    +-----+------+

Expected Output:

    +-----+------+-----------+
    |child|parent|grandparent|
    +-----+------+-----------+
    |    A|    AA|        AAA|
    |    C|    CC|        CCC|
    |    B|    BB|        BBB|
    +-----+------+-----------+
'''

import pandas as pd

# Sample Data
data = [("A", "AA"), ("B", "BB"), ("C", "CC"),
        ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

columns = ["child", "parent"]

# convert to data frame

df = pd.DataFrame(data,columns=columns)


print()
print("==========Input Data=============")

print(df)


print()
print("==========Expected output=============")

df=(
        pd.merge(df,df,left_on="parent",right_on="child",how="inner",suffixes=("","_g"))
                .rename(columns={"parent_g":"grandparent"})
                [["child","parent","grandparent"]]
)
print(df)