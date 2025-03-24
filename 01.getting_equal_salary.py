'''
Scenario: Query to get who are getting equal salary

input:
        +--------+---------+--------+------+-------------------+------+
        |workerid|firstname|lastname|salary|        joiningdate|depart|
        +--------+---------+--------+------+-------------------+------+
        |     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
        |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
        |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
        |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
        |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
        +--------+---------+--------+------+-------------------+------+

Expected Output :

        +--------+---------+--------+------+-------------------+------+
        |workerid|firstname|lastname|salary|        joiningdate|depart|
        +--------+---------+--------+------+-------------------+------+
        |     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
        |     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
        |     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
        |     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
        +--------+---------+--------+------+-------------------+------+

Solution Explanation:
    As per the question, multiple worker might have the same salary.

Approach->1: We have to count worker id for salary. if we get count more than 1, consider the expected result.
Approach->2: Self join

'''
from spark_session import *

#============ Data preparation===============
oWorkers = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR")
        ,("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin")
        ,("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR")
        ,("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin")
        ,("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]

columns = ["workerid","firstname","lastname","salary","joiningdate","depart"]

# convert the worker list to data frame
df = spark.createDataFrame(oWorkers,columns)
print()
print("==========Raw Data=============")

df.show()

# #### ================ Approach->1 (Recommended):
# Find salaries with more than one employee
salary_counts = df.groupBy("salary").agg(count("workerid").alias("num_workers"))

# Find the same salary
salaries_with_multiple_workers = salary_counts.filter("num_workers > 1").select("salary")

# join with the original data frame
oResult=df.join(salaries_with_multiple_workers,"salary","inner").select(
    "workerid","firstname","lastname","salary","joiningdate","depart"
)
oResult.show()


###### ================ Approach-->2: (Not recommended)

##Through SQL
# df.createOrReplaceTempView("worktab")
# spark.sql("select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.depart from worktab a, worktab b where a.salary=b.salary and a.workerid !=b.workerid").show()

##Through Spark DSL
#finaldf = df.alias("a").join(df.alias("b"), (col("a.salary") == col("b.salary")) & (col("a.workerid") != col("b.workerid")), "inner").select(col("a.workerid"), col("a.firstname"), col("a.lastname"), col("a.salary"), col("a.joiningdate"), col("a.depart")).show()


## to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
#input("Press Enter to exit...")
#######http://localhost:4040/jobs/



