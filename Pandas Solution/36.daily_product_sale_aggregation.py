'''
Title: Group Products Sold by Date

Given a table of daily product sales, write a query to:
1. Group products sold on the same date into an array
2. Count the number of products sold each day (including duplicates)
3. Order the results by sell_date

Input:
    +------------+------------+
    | sell_date  | product    |
    +------------+------------+
    | 2020-05-30 | Headphone  |
    | 2020-06-01 | Pencil     |
    | 2020-06-02 | Mask       |
    | 2020-05-30 | Basketball |
    | 2020-06-01 | Book       |
    | 2020-06-02 | Mask       |
    | 2020-05-30 | T-Shirt    |
    +------------+------------+

Expected Output:

    +------------+---------------------------+-----------+
    | sell_date  | products                 | num_sold  |
    +------------+---------------------------+-----------+
    | 2020-05-30 | [T-Shirt, Basketball]    | 3         |
    | 2020-06-01 | [Pencil, Book]           | 2         |
    | 2020-06-02 | [Mask]                   | 1         |
    +------------+---------------------------+-----------+


Explanation:
    1. 2020-05-30: 3 products sold (T-Shirt, Basketball, Headphone)
    2. 2020-06-01: 2 products sold (Pencil, Book)
    3. 2020-06-02: 1 unique product sold (Mask appears twice but counted once)
    4. Products are sorted alphabetically in the array

'''

import pandas as pd

# Sample Data
data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Book"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

columns = ["sell_date", "product"]

# convert to data frame
df = pd.DataFrame(data,columns=columns)


print()
print("==========Input Data=============")

print(df)


print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : (DSL)

df=df.groupby("sell_date").agg(
    products=('product', lambda x: sorted(list(set(x))))
    ,num_sold=('product','nunique')
).reset_index()

print(df)
















