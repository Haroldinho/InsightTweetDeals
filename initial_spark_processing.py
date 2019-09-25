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
from pyspark.sql import SQLContext

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Initial App").getOrCreate()
	sc = spark.sparkContext
	sqlcontext = SQLContext(sc)
	#textFiles = sc.wholeTextFiles("s3n://insighttwitterdeals/Twitter_dumps/bz2_folder/")
	#print(textFiles.collect())
#	tweetDF = sqlcontext.read.json("s3a://insighttwitterdeals/Twitter_Streams/*.json")
	tweetDF = sqlcontext.read.json("s3a://insighttwitterdeals/Twitter_dumps/bz2_folder/*/*/*/*")
	tweetDF.printSchema()
