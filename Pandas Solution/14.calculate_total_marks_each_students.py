'''
    Title: Calculate Total Marks for Each Student

    You are given a table name "student_marks" with the following column
    roll_no--> role number of each student
    , name--> Name of the student
    , telugu, english, maths, science, social --> all are marks in Integer value.

    Write a solution to calculate the total marks obtained by each student across all subjects.
    Return a table with the original columns and an additional column:
    total: The sum of marks from telugu, english, maths, science, and social.

Input:
    |roll_no|  name|telugu|english|maths|science|social|
    +------+------+------+-------+-----+-------+------+
    |203040|rajesh|    10|     20|   30|     40|    50|
    +------+------+------+-------+-----+-------+------+

Output:
    +------+------+------+-------+-----+-------+------+-----+
    |roll_no|  name|telugu|english|maths|science|social|total|
    +------+------+------+-------+-----+-------+------+-----+
    |203040|rajesh|    10|     20|   30|     40|    50|  150|
    +------+------+------+-------+-----+-------+------+-----+

'''

import pandas as pd
# Sample Data
data = {
    "rollno": [203040],
    "name": ["rajesh"],
    "telugu": [10],
    "english": [20],
    "maths": [30],
    "science": [40],
    "social": [50]
}

df = pd.DataFrame(data)

# Calculate total
#df["total"] = df[["telugu", "english", "maths", "science", "social"]].sum(axis=1)
df["total"] = df.iloc[:, 2:].sum(axis=1)

print(df)