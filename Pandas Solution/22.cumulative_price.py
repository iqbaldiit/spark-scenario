'''
Title: Cumulative Price by Product ID and Date

You are given a table "Sales" with the following schema:
pid, date and price

Each row represents the sale of a product (pid) on a specific date (date) for a certain price (price).
Write a query to generate a new column new_price which represents the cumulative sum of price for each pid, ordered by date.

Input:
    +---+------+-----+
    |pid|  date|price|
    +---+------+-----+
    |  1|26-May|  100|
    |  1|27-May|  200|
    |  1|28-May|  300|
    |  2|29-May|  400|
    |  3|30-May|  500|
    |  3|31-May|  600|
    +---+------+-----+

Expected Output:
    +---+------+-----+---------+
    |pid|  date|price|new_price|
    +---+------+-----+---------+
    |  1|26-May|  100|      100|
    |  1|27-May|  200|      300|
    |  1|28-May|  300|      600|
    |  2|29-May|  400|      400|
    |  3|30-May|  500|      500|
    |  3|31-May|  600|     1100|
    +---+------+-----+---------+
'''

import pandas as pd

# Sample Data
data = [
    (1, "26-May", 100),
    (1, "27-May", 200),
    (1, "28-May", 300),
    (2, "29-May", 400),
    (3, "30-May", 500),
    (3, "31-May", 600),
]

columns = ["pid", "date", "price"]

# convert the list to data frame
df=pd.DataFrame(data,columns=columns)
print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

df["new_price"]=df.sort_values("date").groupby("pid")["price"].cumsum()
print(df)