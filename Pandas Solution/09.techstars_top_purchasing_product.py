'''
    Title: Top Purchasing Customers Per Store

    You are given a table "Sales" with the with store_id, customer_id   and  purchase_amt Column

    Write a SQL query to find the top purchasing customer(s) for each store based on their total purchase amount.
    If multiple customers have the same highest purchase amount for a store, include all of them.


Input:


Expected Output:


'''


import pandas as pd

# Sample Data
data = [
    (1, 101, 500), (1, 101, 300), (1, 102, 500),
    (2, 101, 1000), (2, 102, 900),
    (3, 101, 1000), (3, 102, 1000)
]

columns=["store_id", "customer_id", "purchase_amt"]

# convert list to data frame
df=pd.DataFrame(data,columns=columns)

print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

df=df.groupby(["store_id","customer_id"]).agg(
    total_purchase_amt=("purchase_amt","sum")
).reset_index()

df["rk"]=df.groupby("store_id")["total_purchase_amt"].rank(method="dense",ascending=False).astype(int)
df=df.query("rk==1").sort_values(["store_id","customer_id"])
print(df)
