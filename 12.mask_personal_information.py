'''
    Title: Mask Personal Information

    You are given a table named "contacts" that contains sensitive personal information of users
    with the following columns:
    1. email (string): The email address of the user.
    2. mobile (string): The mobile number of the user (10 digits).

    Write a solution in PySpark that returns a masked version of the table where:

    1. Emails are masked such that only the first and last two characters before @ are visible.
        All intermediate characters should be replaced with *.
    2. Mobile numbers are masked such that only the first two and last three digits are visible.
        The middle five digits should be replaced with *.
    Return the resulting masked table in the same schema.

    Inputs:
            +--------------------+----------+
            |               email|    mobile|
            +--------------------+----------+
            |Renuka1992@gmail.com|9856765434|
            |anbu.arasu@gmail.com|9844567788|
            |         a@gmail.com|9844567788|
            |     bc@hotmail.info|9844567788|
            +--------------------+----------+

    Outputs:
            +--------------------+------------------+
            |               email       |    mobile|
            +--------------------+------------------+
            |R**********92@gmail.com    |98*****434|
            |a**********su@gmail.com    |98*****788|
            |a************@gmail.com    |98*****788|
            |b***********c@hotmail.info |98*****788|
            +--------------------+-----------------+

Solution Explanation: The problem stated that, we have mask the email and mobile number as their given criteria.

Approach:
    1. For Email Masking (Assume emails are valid):
        1.1. split email by @
        1.2. take first character form 1st split part
        1.3. take last 2 character from 1st split part
        1.4. if the first split par length 1 the set ** instead of last 2
        1.5. if the first split par length 2 the set * and last 1 instead of last 2
        1.6. concat as their given logic
    2. For mobile Masking (Assume valid mobile number):
        2.1. take first 2 and last 3 character
        2.1. set * and concat
    3. show.


'''
from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()

data = [
    ("Renuka1992@gmail.com", "9856765434"),
    ("anbu.arasu@gmail.com", "9844567788"),
    ("a@gmail.com", "9844567788"),
    ("bc@hotmail.info", "9844567788"),
]

columns = ["email", "mobile"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : ((DSL))

df=(df.withColumn("email_prefix", split("email", "@")[0])
    .withColumn("email_domain", split("email", "@",)[1])
    .withColumn("mask_email",concat(
        col("email_prefix").substr(1,1)
        ,lit("*"*10)
        ,( when(length("email_prefix")==1,lit("**"))
           .when(length("email_prefix")==2,concat(lit("*"),col("email_prefix").substr(-1, 1)))
           .otherwise(col("email_prefix").substr(-2, 2))
         )

        ,lit("@")
        ,col("email_domain")
    ))
    .withColumn("mobile_mask",concat(
        col("mobile").substr(1,2)
        ,lit("*"*5)
        ,col("mobile").substr(-3,3)
    ))
).select(col("mask_email").alias("email"),col("mobile_mask").alias("mobile"))
#
df.show()

# # # # # #### ================ Approach->2 : ( (SQL))
# # create temp table
# df.createOrReplaceTempView("tbl")
#
# sQuery="""
#     SELECT  CONCAT(
#                     LEFT(SPLIT(email,'@')[0],1)
#                     ,'**********'
#                     ,CASE WHEN LEN(SPLIT(email,'@')[0])=1 THEN "**"
#                         WHEN LEN(SPLIT(email,'@')[0])=2 THEN CONCAT("*", RIGHT(SPLIT(email,'@')[0],1))
#                         ELSE RIGHT(SPLIT(email,'@')[0],2) END
#                     ,'@',SPLIT(email,'@')[1]
#                  ) AS email
#
#             ,CONCAT(
#                     LEFT(mobile,2)
#                     ,'*****'
#                     ,RIGHT(mobile,2)
#                 ) AS mobile
#     FROM tbl
# """
#
# df=spark.sql(sQuery)
#
# df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()
