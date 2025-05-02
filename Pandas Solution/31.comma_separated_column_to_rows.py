'''
Title: Transform Comma-Separated Columns into Rows

Given a DataFrame with multiple columns containing comma-separated values,
write a solution to transform these columns into rows while preserving their order and including an empty row.

Input:
    +----+-----+--------+-----------+
    |col1| col2|    col3|       col4|
    +----+-----+--------+-----------+
    |  m1|m1,m2|m1,m2,m3|m1,m2,m3,m4|
    +----+-----+--------+-----------+

Expected Output:

        +-----------+
        |        col|
        +-----------+
        |         m1|
        |      m1,m2|
        |   m1,m2,m3|
        |m1,m2,m3,m4|
        |           |
        +-----------+
'''

import pandas as pd


# Sample Data
data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

columns = ["col1", "col2", "col3", "col4"]

# convert to data frame
df = pd.DataFrame(data,columns=columns)


print()
print("==========Input Data=============")

print(df)

print()
print("==========Expected output=============")

df=pd.DataFrame({"col":df.values.flatten().tolist()})
print(df)

