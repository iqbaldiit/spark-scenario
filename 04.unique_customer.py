"""
You are given a table "Customers" containing customer information, including a unique cust_id, cust_name, and address.
A customer may have multiple addresses, some of which might be duplicates.

Write a SQL query to return a list of unique customer names along with a distinct set of addresses associated with each
customer, sorted in any order.

Input :-
        +------+-----------+-------+
        |cust_id|   cust_name|address|
        +------+-----------+-------+
        |     1|   Mark Ray|     AB|
        |     2|Peter Smith|     CD|
        |     1|   Mark Ray|     EF|
        |     2|Peter Smith|     GH|
        |     2|Peter Smith|     CD|
        |     3|       Kate|     IJ|
        +------+-----------+-------+

Expected Output :-

        +------+-----------+--------+
        |cust_id|   cust_name| address|
        +------+-----------+--------+
        |     1|   Mark Ray|[EF, AB]|
        |     2|Peter Smith|[CD, GH]|
        |     3|       Kate|    [IJ]|
        +------+-----------+--------+


Solution Explanation: The problem stated that, we have to find distinct customer with all his / her distinct address.

Approach-> : (Using aggregation)
    1. Group by customer
    2. use Collect set for  distinct address. [Note: collect_list combine with duplicates too]
    3. print

"""
from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()


data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]

columns = ["cust_id", "cust_name", "address"]

# convert the worker list to data frame
df = spark.createDataFrame(data, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (Using aggregation (DSL))

df=df.groupBy("cust_id","cust_name").agg(collect_set("address").alias("address")).orderBy("cust_id")
df.show()

# # # # #### ================ Approach->2 : (Using aggregation (SQL))
# # create temp table
# df.createOrReplaceTempView("Customers")
# df=spark.sql(""
#              " SELECT cust_id, cust_name, COLLECT_SET(address) AS address"
#              " FROM Customers GROUP BY cust_id, cust_name ORDER  BY cust_id"
# "")
#
# df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()