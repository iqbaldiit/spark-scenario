import os
import sys
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#import pyspark.sql.functions as F  # Import all functions under alias F

# Set environment variables
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "hadoop/bin"
os.environ['JAVA_HOME'] = r'C:\Users\MSI\.jdks\corretto-1.8.0_442'

# Create Spark configuration
conf = (SparkConf()
        .setAppName("pyspark")
        .setMaster("local[*]")
        .set("spark.driver.host", "localhost")
        .set("spark.default.parallelism", "1"))

# Initialize Spark Context and Session
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# # Dynamically import all functions from pyspark.sql.functions
# globals().update({name: getattr(F, name) for name in dir(F) if not name.startswith("_")})

# Export everything dynamically
__all__ = ['spark', 'sc', 'start_timer', 'end_timer'] #+ [name for name in dir(F) if not name.startswith("_")]

# Timer functions
def start_timer():
        """Start the execution timer."""
        global start_time
        start_time = time.time()

def end_timer():
        """End the execution timer and print the total execution time."""
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Total execution time: {execution_time:.2f} seconds")
