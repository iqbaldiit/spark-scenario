'''
Title: User Navigation Path

You are given a dataset that contains user web activity logs with userid and the page they visited,
ordered by their appearance in the dataset.

Write a query to group all visited pages in order into a list for each user (userid),
so you can track their complete navigation history.

Input :-
    +------+------------+
    |userid|        page|
    +------+------------+
    |     1|        home|
    |     1|    products|
    |     1|    checkout|
    |     1|confirmation|
    |     2|        home|
    |     2|    products|
    |     2|        cart|
    |     2|    checkout|
    |     2|confirmation|
    |     2|        home|
    |     2|    products|
    +------+------------+

Expected Output :-
    +------+--------------------------------------------------------------+
    |userid|pages                                                         |
    +------+--------------------------------------------------------------+
    |1     |[home, products, checkout, confirmation]                      |
    |2     |[home, products, cart, checkout, confirmation, home, products]|
    +------+--------------------------------------------------------------+

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")]

columns = ["userid", "page"]

# convert the worker list to data frame
df = spark.createDataFrame(data,columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # # #### ================ Approach->1 : (DSL)

# df=df.groupBy("userid").agg(collect_list("page").alias("pages"))
# df.show()

# # # # # #### ================ Approach->2 : ((SQL))

df.createOrReplaceTempView("tbl")
sSQL="""

    SELECT userid, collect_list(page) AS page 
    FROM tbl GROUP BY userid 

"""

df=spark.sql(sSQL)

df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()

















