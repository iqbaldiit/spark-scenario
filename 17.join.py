'''
You are given 2 tables Employee and Address,
Write a solution to join them.
input:
    +------+-----+---+------+-------+
    |emp_id| name|age| state|country|
    +------+-----+---+------+-------+
    |     1|  Tim| 24|Kerala|  India|
    |     2|Asman| 26|Kerala|  India|
    +------+-----+---+------+-------+

    +------+-----+---+-------+
    |emp_id| name|age|address|
    +------+-----+---+-------+
    |     1|  Tim| 24|Comcity|
    |     2|Asman| 26|bimcity|
    +------+-----+---+-------+

Expected Output:
    +------+-----+---+------+-------+-------+
    |emp_id| name|age| state|country|address|
    +------+-----+---+------+-------+-------+
    |     1|  Tim| 24|Kerala|  India|Comcity|
    |     2|Asman| 26|Kerala|  India|bimcity|
    +------+-----+---+------+-------+-------+
'''

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# Sample Data
employee_data = [
    (1, "Tim", 24, "Kerala", "India"),
    (2, "Asman", 26, "Kerala", "India")
]
employee_columns=["emp_id", "name", "age", "state", "country"]

addresses_data = [
    (1, "Tim", 24, "Comcity"),
    (2, "Asman", 26, "bimcity")
]
address_columns= ["emp_id", "name", "age", "address"]

# convert the worker list to data frame
employee_df=spark.createDataFrame(employee_data,employee_columns)
addresses_df=spark.createDataFrame(addresses_data,address_columns)
print()
print("==========Input Data=============")

employee_df.show()
addresses_df.show()
print()
print("==========Expected output=============")

# # # #### ================ Approach->1 : ((DSL))

df=(employee_df.join(addresses_df,["emp_id"],"inner")
    .select(employee_df["emp_id"], employee_df["name"], employee_df["age"]
            , employee_df["state"], employee_df["country"],addresses_df["address"]))
df.show()


# # # # # # #### ================ Approach->2 : ((SQL))
# employee_df.createOrReplaceTempView("Employee")
# addresses_df.createOrReplaceTempView("Address")
# sSQL="""
#     SELECT E.emp_id ,E.name,E.age,E.state,E.country,A.address
#     FROM  Employee E
#     INNER JOIN Address A ON E.emp_id=A.emp_id
# """
#
# df=spark.sql(sSQL)
#
# df.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()