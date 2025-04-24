'''
Title: User Navigation Path

You are given a dataset that contains user web activity logs with userid and the page they visited,
ordered by their appearance in the dataset.

Write a query to group all visited pages in order into a list for each user (userid),
so you can track their complete navigation history.

Input :-
    +------+------------+
    |userid|        page|
    +------+------------+
    |     1|        home|
    |     1|    products|
    |     1|    checkout|
    |     1|confirmation|
    |     2|        home|
    |     2|    products|
    |     2|        cart|
    |     2|    checkout|
    |     2|confirmation|
    |     2|        home|
    |     2|    products|
    +------+------------+

Expected Output :-
    +------+--------------------------------------------------------------+
    |userid|pages                                                         |
    +------+--------------------------------------------------------------+
    |1     |[home, products, checkout, confirmation]                      |
    |2     |[home, products, cart, checkout, confirmation, home, products]|
    +------+--------------------------------------------------------------+

'''

import pandas as pd

# Sample Data
data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")]

columns = ["userid", "page"]

# convert dataframe
df = pd.DataFrame(data, columns=columns)
print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

df=df.groupby("userid")["page"].agg(list).reset_index(name="pages")
print(df)

















