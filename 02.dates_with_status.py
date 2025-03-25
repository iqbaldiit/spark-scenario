'''
    Problem: You are given a table with three columns: order_id,status_date , and status.
             Each row represents the status of an order on a specific date. Your task is to
             write a query to find all rows where the 'status' changes "ordered" to "dispatched",
             along with the corresponding  order_id and status_date.

    Input :-
            +-------+----------+----------+
            |order_id|status_date|    status|
            +-------+----------+----------+
            |      1|     1-Jan|   Ordered|
            |      1|     2-Jan|dispatched|
            |      1|     3-Jan|dispatched|
            |      1|     4-Jan|   Shipped|
            |      1|     5-Jan|   Shipped|
            |      1|     6-Jan| Delivered|
            |      2|     1-Jan|   Ordered|
            |      2|     2-Jan|dispatched|
            |      2|     3-Jan|   shipped|
            +-------+----------+----------+
    Expected Output :-
            +-------+----------+----------+
            |order_id|status_date|    status|
            +-------+----------+----------+
            |      1|     2-Jan|dispatched|
            |      1|     3-Jan|dispatched|
            |      2|     2-Jan|dispatched|
            +-------+----------+----------+

    Solution Explanation:
        1. We have to find all orders whose status="dispatched"
        2. Now check the Dispatched order's previous status is "Ordered"

    Approach--> : (Using window function)
        1. Use lag function to get the previous order status and add a column name "previous_status"
        2. Now check status="dispatched" and previous_status="ordered"


'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# ============ Data Preparation
oOrders = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped")
]
columns = ["order_id", "status_date", "status"]

# convert the worker list to data frame
df = spark.createDataFrame(oOrders, columns)
print()
print("==========Input Data=============")

df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : (Using window function)
# define the window
window=Window.partitionBy("order_id").orderBy("status_date")

# add a column name as previous_status
df=df.withColumn("previous_status", lag("status").over(window))

# filter as per requirements
df = df.filter((col("status") == "dispatched") & (col("previous_status") == "Ordered")) \
    .select("order_id", "status_date", "status")
df.show()


# # # # #### ================ Approach->2.0 : (Using SQL (Window))
# # create temp table
# df.createOrReplaceTempView("Orders")
# df=spark.sql("SELECT tab.order_id,tab.status_date, tab.status FROM ("
#              "SELECT *"
#              ", LAG(status) OVER (PARTITION BY order_id ORDER BY status_date) AS previous_status  "
#              " FROM Orders)tab WHERE tab.status='dispatched' AND tab.previous_status='Ordered'"
# "")
# df.show()


## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()