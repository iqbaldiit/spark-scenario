'''
Title: Process Student Data with Missing Values

1. Create a new datafrane df1 with the given values
2. Count null entries in a datafarme
3. Remove null entries and the store the null entries in a new datafarme df2
4. Create a new dataframe df3 with the given values and join the two dataframes df1 & df2
5. Fill the null values with the mean age all of students
6. Filter the students who are 18 years above and older

'''

from spark_session import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start timer to see execution time
start_timer()

# Sample Data
data = [(1,'Jhon',17),(2,'Maria',20),(3,'Raj',None),(4,'Rachel',18)]

columns = ["id","name","age"]

# convert to data frame
#df = spark.createDataFrame(data,columns).cache()
df = spark.createDataFrame(data,columns).cache()


print()
print("==========Input Data=============")

df.show()


print()
print("==========Expected output=============")

# # # # # # #### ================ Approach->1 : (DSL)

# Count null entries in each column
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()


#Remove the row with null entires and store them in a new dataframe named df2
df2 = df.filter(col("age").isNull())
df2.show()

#create a new dataframe df3
data2 = [(1,'seatle',82),(2,'london',75),(3,'banglore',60),(4,'boston',90)]
columns2 = ["id","city","code"]

df3 = spark.createDataFrame(data2,columns2)
df3.show()

mergedf = df.join(df3, df["id"]==df3["id"],"inner").select(df["id"],"name","age","city","code")
mergedf.show()

#fill the null value with the mean age of students
#calculate the mean age
meanage = mergedf.select(round(mean("age"))).collect()[0][0]
print(meanage)

filldf = mergedf.na.fill({"age":meanage})
filldf.show()

#Get the students who are 18 years or older
filterdf = filldf.filter(col("age")>= 18)
filterdf.show()

# # to show DAG or query estimation plan un comment the following lines and go to the url to see spark UI
# input("Press Enter to exit...")
# ######http://localhost:4040/jobs/

# end timer to see execution time
end_timer()