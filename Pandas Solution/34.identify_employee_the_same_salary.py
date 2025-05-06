'''
Title: Identify Employees Sharing the Same Salary

Given a table of employee information with the following columns
worker_id, first_name, last_name, salary, joining_date, depart

write a query to find all employees who share the same salary
with at least one other employee in the company.

Input:
    +--------+---------+--------+------+-------------------+------+
    |worker_id|first_name|last_name|salary|     joining_date|depart|
    +--------+---------+--------+------+-------------------+------+
    |     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
    |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
    |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
    |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
    |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
    +--------+---------+--------+------+-------------------+------+

Expected Output:

    +--------+---------+--------+------+-------------------+------+
    |worker_id|first_name|last_name|salary|    joining_date|depart|
    +--------+---------+--------+------+-------------------+------+
    |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
    |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
    |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
    |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
    +--------+---------+--------+------+-------------------+------+


Explanation:
    1. Employees with worker_id 002 and 003 both earn 300000
    2. Employees with worker_id 004 and 005 both earn 500000
    3. Employee with worker_id 001 earns 100000 (unique salary) and is excluded

'''

import pandas as pd

# Sample Data
data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]

columns = ["worker_id", "first_name", "last_name", "salary", "joining_date", "depart"]

# convert to data frame
df = pd.DataFrame(data,columns=columns)


print()
print("==========Input Data=============")

print(df)


print()
print("==========Expected output=============")

df_same_salary=df.groupby("salary")["worker_id"].count()

df_same_salary=df_same_salary[df_same_salary>1].index

df=df[df["salary"].isin(df_same_salary)]

print (df)

