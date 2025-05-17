'''
Given a pipe-delimited string containing multiple person records concatenated together,
write a Solution to parse the string and extract each individual record with their complete information.

Input:

    "Rahul|BE|8|Bigdata|9876543210|Ramesh|BTech|6|Java|8765432109|Rajesh|BE|9|Linux|7890654321"

Output:

    +------+------+----------+-------+----------+
    |  Name|Degree|Experience|  Skill|     Phone|
    +------+------+----------+-------+----------+
    | Rahul|    BE|         8|Bigdata|9876543210|
    |Ramesh| BTech|         6|   Java|8765432109|
    |Rajesh|    BE|         9|  Linux|7890654321|
    +------+------+----------+-------+----------+

Explanation:

        1. Each person record contains exactly 5 fields: Name, Degree, Experience, Skill, Phone
        2. The output should separate each complete record on a new line
        3. Maintain all original fields in their original order
        4. Preserve the exact field values without modification
        5. Handle any number of complete records in the input string

'''
from six import string_types

from spark_session import *
from pyspark.sql.functions import *


# start timer to see execution time
start_timer()

# input data
input_str = "Rahul|BE|8|Bigdata|9876543210|Ramesh|BTech|6|Java|8765432109|Rajesh|BE|9|Linux|7890654321"

print()
print("==========Input Data=============")

print(input_str)
print()
print("==========Expected output=============")

#split string by |
fields=input_str.split('|')
# group every 5 elements
data=[fields[i:i+5] for i in range(0,len(fields),5)]
columns=["Name","Degree","Experience","Skill","Phone"]

df=spark.createDataFrame(data,columns)
df.show()



# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()