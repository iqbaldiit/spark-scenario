'''
Title: Compare Two Tables and Identify Differences

You are given two tables:

1. Source Table with columns (id, name)
2. Target Table with columns (id1, name1)

Write a query to generate an output that contains:

id from either the source or target. A comment based on the following rules:

1. If an id exists in the source but not in the target, label it as 'new in source'.
2. If an id exists in the target but not in the source, label it as 'new in target'.
3. If an id exists in both, but name and name1 are different, label it as 'mismatch'.

Return the result ordered by id.

Input :-
        +---+----+
        | id|name|
        +---+----+
        |  1|   A|
        |  2|   B|
        |  3|   C|
        |  4|   D|
        +---+----+

        +---+-----+
        |id1|name1|
        +---+-----+
        |  1|    A|
        |  2|    B|
        |  4|    X|
        |  5|    F|
        +---+-----+

Expected Output :-
        +---+-------------+
        | id|      comment|
        +---+-------------+
        |  3|new in source|
        |  4|     mismatch|
        |  5|new in target|
        +---+-------------+

'''

import pandas as pd

# Sample Data
source_data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
target_data = [(1, "A"), (2, "B"), (4, "X"), (5, "F")]

source_columns = ["id", "name"]
target_columns = ["id1", "name1"]

# convert to data frame
df_source = pd.DataFrame(source_data,columns=source_columns)
df_target = pd.DataFrame(target_data,columns=target_columns)

print()
print("==========Input Data=============")

print(df_source)
print(df_target)
print()
print("==========Expected output=============")

df=pd.merge(df_source,df_target,left_on="id",right_on="id1",how="outer")
df["final_id"]=df["id"].combine_first(df["id1"])

def get_comment(oRow):
    if pd.isna(oRow["id"]):
        return "new in target"
    elif pd.isna(oRow["id1"]):
        return "new in source"
    elif oRow["name"]!=oRow["name1"]:
        return "mismatch"
    else:
        return None

df["comment"]=df.apply(get_comment,axis=1)
'''
    axis=1 means: apply the function row by row.
    axis=0 would mean: apply the function column by column.
'''
df=df[["final_id","comment"]].dropna().sort_values('final_id',ascending=True).reset_index(drop=True)
df.columns=["id","comment"]

print(df)






