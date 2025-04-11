'''
Title: Combine Employee Data with Valid Emails

You are given two tables:
    employees: Employee details containing columns (id, name, age, email)
    employees_salary: Employee details with additional salary column (id, name, age, email, salary)

Write a solution that combine this two tables keeping only those records who have valid email address. Valid email
address contain '@' for once not multiple time. there is no salary in the table employees, so add salary column
with default 1000 amount. If there is any duplicate keep one record (2nd table is the highest priority).
result should be ordered by id in ascending.

finally Write the final result to a target location, by partitioning on salary.

Input:
        +---+----+---+---------------+
        | id|name|age|          email|
        +---+----+---+---------------+
        |  1| abc| 31|  abc@gmail.com|
        |  2| def| 23|   defyahoo.com|
        |  3| xyz| 26|  xyz@gmail.com|
        |  4| qwe| 34|   qwegmail.com|
        |  5| iop| 24|  iop@gmail.com|
        | 15| lkj| 29|lkj@outlook.com|
        +---+----+---+---------------+
Employees[1] with employee_salary[1]

        +---+----+---+---------------+------+
        | id|name|age|          email|salary|
        +---+----+---+---------------+------+
        | 11| jkl| 22|  abc@gmail.com|  1000|
        | 12| vbn| 33|vbn@yah@@oo.com|  3000|
        | 13| wer| 27|            wer|  2000|
        | 14| zxc| 30|        zxc.com|  2000|
        | 15| lkj| 29|lkj@outlook.com|  2000|
        +---+----+---+---------------+------+

        1. Add a column name 'salary' in Employees table with default amount 10000
        2. Combine two table into a single table
        3. Keeps the record with valid email address
        4. Remove duplicate (id) and prioritize 2nd table
        5. Show()

Expected output:

    +---+----+---+---------------+------+
    | id|name|age|          email|salary|
    +---+----+---+---------------+------+
    | 11| jkl| 22|  abc@gmail.com|  1000|
    | 15| lkj| 29|lkj@outlook.com|  2000|
    |  1| abc| 31|  abc@gmail.com|  1000|
    |  3| xyz| 26|  xyz@gmail.com|  1000|
    |  5| iop| 24|  iop@gmail.com|  1000|
    | 15| lkj| 29|lkj@outlook.com|  1000|
    +---+----+---+---------------+------+

Solution Explanation: The problem stated that, we have to set default 1000 amount as salary in the first table and
                        then combine the two tables into a single table. We have to keep only those records who have valid email.
                        we have drop the duplicate by name also and write the solution to a target location.

Approach:
    1. Add column and set default value 1000 as salary in the employee table
    2. Combine two table using union by name
    3. filter valid email: email containing @ for once.
    4. drop duplicate. if record found from 2nd table, keep this record and remove first table's record
    5. save the data to target location.


Note:
    1. UNION removes duplicate rows from the combined result set,
       while UNION ALL includes all rows, including duplicates.

    2. In PySpark 2.0 and above, union() behaves like the old unionAll()
        — it includes duplicates. Both require the same schema and column order.
        If the schemas don’t match exactly (in order and number of columns),
        PySpark will throw an error.

    3. unionByName() solve the (no 2 problem). you don't need to maintain column order
        and also consider missing column. It matches by column name not column order.
        It allows missing column if allowMissingColumns=True is used.

'''

from spark_session import *
from pyspark.sql.functions import *

# start timer to see execution time
start_timer()

# Sample data for employees
employees_data = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com"),
    (15, "lkj", 29, "lkj@outlook.com")
]

# Sample data for employees_salary
employees_salary_data = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yah@@oo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]

columns_emp = ["id", "name", "age", "email"]
columns_salary = ["id", "name", "age", "email", "salary"]

# convert the data list to data frame
df_employees = spark.createDataFrame(employees_data, columns_emp)
df_employees_salary = spark.createDataFrame(employees_salary_data, columns_salary)

print()
print("==========Input Data=============")

df_employees.show()
df_employees_salary.show()

print()
print("==========Expected output=============")


# add a column name salary in employee df with default salary amount 1000
df_employees=df_employees.withColumn("salary",lit(1000))
#df_employees.show()

# combine the two dataset into single dataset
df_combine=df_employees_salary.unionByName(df_employees)
#df_combine.show()

# keeps only valid email that contain exactly 1 @ symbol
df=df_combine.filter(size(split(col("email"), "@")) == 2)
df.show()

# drop duplicates by name
df=df.dropDuplicates(["name"]).orderBy(col("id").asc())
df.show()

# # write the location in a target location partition by salary
df.write.mode("overwrite").format("csv").partitionBy("salary").save("D:/Library/BigData/aws/payroll/salary")

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()