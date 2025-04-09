'''
    You are given a table called Emp_table containing information about employees, including
    their employee ID, name, and salary.

    Your task is to write a SQL query that returns all employee records with an additional column grade
    based on the following salary grading system:
    1. Salary greater than 10,000 → Grade 'A'
    2. Salary between 5,000 and 10,000 (inclusive) → Grade 'B'
    3. Salary less than 5,000 → Grade 'C'

    Input:
            +------+---------------+------+
            |emp_id|       emp_name|salary|
            +------+---------------+------+
            |     1|           Jhon|  4000|
            |     2|      Tim David| 12000|
            |     3|Json Bhrendroff|  7000|
            |     4|         Jordon|  8000|
            |     5|          Green| 14000|
            |     6|         Brewis|  6000|
            +------+---------------+------+

    Expected Output:
            +------+---------------+------+-----+
            |emp_id|       emp_name|salary|grade|
            +------+---------------+------+-----+
            |     1|           Jhon|  4000|    C|
            |     2|      Tim David| 12000|    A|
            |     3|Json Bhrendroff|  7000|    B|
            |     4|         Jordon|  8000|    B|
            |     5|          Green| 14000|    A|
            |     6|         Brewis|  6000|    B|
            +------+---------------+------+-----+

Solution Explanation: The problem stated very clearly that we have assign grade to employees as per their rules.

Approach:
    1. As per the requirement use case when statement in SQL and when-otherwise in dsl.
'''

import pandas as pd

data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]

columns = ["emp_id", "emp_name", "salary"]

# convert the worker list to data frame
df = pd.DataFrame(data=data, columns=columns)
print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 :
df["grade"]="C"
df.loc[df['salary'] > 10000, 'grade'] = 'A'
df.loc[df['salary'].between(5000, 10000, inclusive="both"), 'grade'] = 'B'
print(df)

# # # # #### ================ Approach->2 :
# def get_grade(salary):
#     if salary>10000:
#         return "A"
#     elif 5000 <=salary<= 10000:
#         return "B"
#     else:
#         return "C"
#
# df["grade"]=df["salary"].apply(get_grade)
# print(df)

# # # # # #### ================ Approach->3 (Using numpy--> recommended for large dataset) :
# import numpy as np
# conditions=[
#     df["salary"]>10000
#     ,df["salary"].between(5000,10000,inclusive="both")
#     ,df["salary"]<50000
# ]
# condition_result=["A","B","C"]
# df["grade"]=np.select(conditions,condition_result, default='C')
# print(df)

