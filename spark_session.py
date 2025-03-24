'''
This module is created to export all necessary libraries and other imports.
If you need to declare a library that needed most of the scenario, you can import here
and export it dynamically like sql functions
'''

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # Import all functions here
import pyspark.sql.functions as F  # Import all functions under alias F

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

# Dynamically get all functions from pyspark.sql.functions
globals().update({name: getattr(F, name) for name in dir(F) if not name.startswith("_")})

# Export everything dynamically
__all__ = ['spark', 'sc'] + [name for name in dir(F) if not name.startswith("_")]
