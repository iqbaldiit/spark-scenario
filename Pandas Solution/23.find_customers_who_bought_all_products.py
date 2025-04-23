'''
Title: Find Customers Who Bought All Products

Question:

You are given two tables:

1. "CustomerProduct" that contains the columns customer_id and product_key, indicating which product each customer has purchased.
2. "Product" that contains all available product_keys in the store.

Write a query to return the IDs of customers who have purchased all the products listed in the Product table.
Return the result in any order.

input:
        +-----------+-----------+
        |customer_id|product_key|
        +-----------+-----------+
        |          1|          5|
        |          2|          6|
        |          3|          5|
        |          3|          6|
        |          1|          6|
        +-----------+-----------+
        +-----------+
        |product_key|
        +-----------+
        |          5|
        |          6|
        +-----------+

Expected Output:
        +-----------+
        |customer_id|
        +-----------+
        |          1|
        |          3|
        +-----------+

'''

import pandas as pd


# Sample Data
customer_data = [
    (1, 5),
    (2, 6),
    (3, 5),
    (3, 6),
    (1, 6),
]
product_data = [(5,), (6,)]

# Create DataFrames
df_customer = pd.DataFrame(customer_data, columns=["customer_id", "product_key"])
df_product = pd.DataFrame(product_data, columns=["product_key"])

print()
print("==========Input Data=============")

print(df_customer)
print(df_product)
print()
print("==========Expected output=============")

total_product=df_product["product_key"].count()
print(total_product)

df=(df_customer.groupby("customer_id")["product_key"]
                .nunique()
                .reset_index()
                .query("product_key==@total_product")
                [["customer_id"]]
    )

# df.columns=["customer_id","p_count"]
# df=df[["customer_id"]][df["p_count"]==total_product]

print(df)





















