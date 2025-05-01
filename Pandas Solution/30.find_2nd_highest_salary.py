'''
Title: Find 2nd highest salary

You are given two tables name
1. Employee (emp_id|name|dept_id| salary)
2. Department (dept_id1|dept_name)

Write a SQL query to extract second most salary for each department. The result is order by dept_name

Input:
        +------+----+-------+-------+
        |emp_id|name|dept_id| salary|
        +------+----+-------+-------+
        |     1|   A|      A|1000000|
        |     2|   B|      A|2500000|
        |     3|   C|      G| 500000|
        |     4|   D|      G| 800000|
        |     5|   E|      W|9000000|
        |     6|   F|      W|2000000|
        +------+----+-------+-------+

        +--------+---------+
        |dept_id1|dept_name|
        +--------+---------+
        |       A|    AZURE|
        |       G|      GCP|
        |       W|      AWS|
        +--------+---------+

Expected Output:

        +------+----+---------+-------+
        |emp_id|name|dept_name| salary|
        +------+----+---------+-------+
        |     1|   A|    AZURE|1000000|
        |     6|   F|      AWS|2000000|
        |     3|   C|      GCP| 500000|
        +------+----+---------+-------+
'''

import pandas as pd

# Sample Data
emp_data = [
        (1, 'A', 'A', 1000000),
        (2, 'B', 'A', 2500000),
        (3, 'C', 'G', 500000),
        (4, 'D', 'G', 800000),
        (5, 'E', 'W', 9000000),
        (6, 'F', 'W', 2000000)
]

emp_columns = ["emp_id", "name", "dept_id", "salary"]

dept_data = [('A', 'AZURE'), ('G', 'GCP'), ('W', 'AWS')]
dept_columns = ["dept_id1", "dept_name"]

# convert to data frame
emp_df = pd.DataFrame(emp_data,columns=emp_columns)
dept_df = pd.DataFrame(dept_data,columns=dept_columns)


print()
print("==========Input Data=============")

print (emp_df)
print (dept_df)


print()
print("==========Expected output=============")

emp_df["rk"]=emp_df.groupby("dept_id")["salary"].rank(method="dense",ascending=False).astype(int)
emp_df=emp_df[emp_df["rk"]==2].drop(columns=["rk"])

df=(pd.merge(emp_df,dept_df,left_on="dept_id",right_on="dept_id1", how="inner")
        [["emp_id","name","dept_name","salary"]]
)

print(emp_df)


















