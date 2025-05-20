'''
    Title: Monthly Transaction Summary by Country

    You are given a dataset containing transaction information with the following column
    id, country, state, amount,trans_date.

    Write a query for each month and country, return the following aggregated details:

        1. month (in format YYYY-MM)
        2. country
        3. trans_count: total number of transactions
        4. approved_count: number of approved transactions
        5. trans_total_amount: total transaction amount
        6. approved_total_amount: total amount of approved transactions

    Return the result ordered by month ascending, then country ascending.


Input:
    +---+-------+--------+------+----------+
    | id|country|   state|amount|trans_date|
    +---+-------+--------+------+----------+
    |121|     US|approved|  1000|2018-12-18|
    |122|     US|declined|  2000|2018-12-19|
    |123|     US|approved|  2000|2019-01-01|
    |124|     DE|approved|  2000|2019-01-07|
    +---+-------+--------+------+----------+

Expected Output:

    +-------+-------+-----------+--------------+------------------+---------------------+
    |  month|country|trans_count|approved_count|trans_total_amount|approved_total_amount|
    +-------+-------+-----------+--------------+------------------+---------------------+
    |2018-12|     US|          2|             1|              3000|                 1000|
    |2019-01|     DE|          1|             1|              2000|                 2000|
    |2019-01|     US|          1|             1|              2000|                 2000|
    +-------+-------+-----------+--------------+------------------+---------------------+


'''


import pandas as pd

# Sample Data
data = [
    (121, 'US', 'approved', 1000, '2018-12-18'),
    (122, 'US', 'declined', 2000, '2018-12-19'),
    (123, 'US', 'approved', 2000, '2019-01-01'),
    (124, 'DE', 'approved', 2000, '2019-01-07'),
]

columns=['id', 'country', 'state', 'amount', 'trans_date']

# convert list to data frame
df=pd.DataFrame(data,columns=columns)
print()
print("==========Input Data=============")

print(df)
print()
print("==========Expected output=============")

# # # # # #### ================ Approach->1 : ((DSL))
#convert the trans_date column to date time data type
df['trans_date']=pd.to_datetime(df['trans_date'])

#get month from date
df['month']=df['trans_date'].dt.strftime('%Y-%m')

df=df.groupby(['month','country']).agg(
    trans_count=('id','count')
    ,approved_count=('state', lambda x:(x=='approved').sum())
    ,trans_total_amount=('amount','sum')
    ,approved_sum=('amount',lambda x:x[df.loc[x.index,'state']=='approved'].sum())
).reset_index().sort_values(['month','country'])

print (df)

# get month and year extracting date



