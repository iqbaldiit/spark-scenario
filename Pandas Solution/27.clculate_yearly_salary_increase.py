'''
Title: Calculate Yearly Salary Increases

you are given a table name Employee with the following columns
emp_id, salary and year

Write a solution to calculate yearly salary increases for each employee based on their previous year's salary.
Given a table of employee salaries by year, create a new column that shows the salary increase from the previous year.
For the first year of data for each employee, the increase should be 0.

Input:
    +-----+------+----+
    |emp_id|salary|year|
    +-----+------+----+
    |    1| 60000|2018|
    |    1| 70000|2019|
    |    1| 80000|2020|
    |    2| 60000|2018|
    |    2| 65000|2019|
    |    2| 65000|2020|
    |    3| 60000|2018|
    |    3| 65000|2019|
    +-----+------+----+

Expected Output:
    +------+------+----+---------------+
    |emp_id|salary|year|increase_salary|
    +------+------+----+---------------+
    |     1| 60000|2018|              0|
    |     1| 70000|2019|          10000|
    |     1| 80000|2020|          10000|
    |     2| 60000|2018|              0|
    |     2| 65000|2019|           5000|
    |     2| 65000|2020|              0|
    |     3| 60000|2018|              0|
    |     3| 65000|2019|           5000|
    +------+------+----+---------------+
'''

import pandas as pd

# Sample Data
data = [
    (1, 60000, 2018), (1, 70000, 2019), (1, 80000, 2020),
    (2, 60000, 2018), (2, 65000, 2019), (2, 65000, 2020),
    (3, 60000, 2018), (3, 65000, 2019)
]

columns = ["emp_id", "salary", "year"]

# convert to data frame
df = pd.DataFrame(data,columns=columns)


print()
print("==========Input Data=============")

print(df)

print()
print("==========Expected output=============")

df["increase_salary"]=df.groupby("emp_id")["salary"].diff().fillna(0)
df["increase_salary"]=df["increase_salary"].astype(int)

print(df)
