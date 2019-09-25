"""
			***** INSIGHT DATA ENGINEERING ****

Code to run a simple example transforming all my json files in S3 and processing them to a local database. 
Cohort: SEA '19C
Name: Harold Nikoue


"""

import sys
from operator import add
import json
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        # always need to provide one folder argument to the functions
        print("Usage: initial_spark_processing <folder>", file=sys.stderr)
        sys.exit(-1)
    folder_name = sys.argv[1]
    spark = SparkSession.builder.appName("Initial App").getOrCreate()
    sc = spark.sparkContext
    tweetDF = spark.read.json(folder_name)
    tweetDF.printSchema()
