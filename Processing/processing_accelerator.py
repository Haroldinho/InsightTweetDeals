"""
			***** INSIGHT DATA ENGINEERING ****

Code to transform input json.bz2 formats on S3 to parquet files on S3 
to speed up the processing fo the data.
Two operations are needed:
1) Load the bz2 json files in groups from S3
2) Create a schema for each grouping of files
3) Save the files as parquet 
filename: processing_accelerator.py 
Cohort: SEA '19C
Name: Harold Nikoue


"""
#!/usr/bin/python3
#-*- coding: utf-8 -*-
import sys
from operator import add
import json
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import lower, upper, col, column, instr
from spark_to_postgres_library import write_to_postgres 
import logging
from pyspark.sql.types import DoubleType, TimestampType, StringType, LongType, IntegerType, DoubleType


spark = SparkSession.builder.appName("Preprocessing App").getOrCreate()
sc = spark.sparkContext
logging.basicConfig(filename="json_bz2_to_parquet.log", level=logging.WARN,
                    filemode="w", format="%(asctime)s: %(process)d - %(levelname)s - %(message)s")
def save_all_files_to_parquet_format(sqlcontext):
	for m in range(6,11):
		logging.info("Reading files for the {}th month of 2018.".format(m))
		compressed_json_folder = "s3a://{bz2_bucket}/{month:02d}/*/*/*".format(bz2_bucket=os.environ["COMPRESSED_S3_FOLDER"], month=m)
		save_to_parquet(sqlcontext, compressed_json_folder, m)

def save_to_parquet(sqlcontext, json_folder, month):
	logging.info("Started reading json.bz2 files from {} on S3.".format(json_folder))
	folder_data = sqlcontext.read.json(json_folder)
	logging.info("Finished reading json.bz2 files from S3.")
	logging.info("Adding a fixed schema to the dataframe.")
	folder_data = reduce_dataframe_to_essential_fields(folder_data)
	logging.info("Writing new dataframe to Parquet.")
	folder_data.write.format("parquet").mode("overwrite").save("s3a://{s3_bucket}/Twitter_dumps/Parquet_folder/parquet_file_{month:02d}".format(s3_bucket=os.environ['S3_FOLDER'],month=month))


def reduce_dataframe_to_essential_fields(df):
	return df.select(col('created_at'),col('user.followers_count').alias('follower_count'), col('user.description').alias('description'), col('user.name').alias('username'),col('text'),col('timestamp_ms'),col('quote_count'),col('place.name').alias('location'),col('retweet_count'),col('quoted_status.retweeted').alias('retweeted'), col('favorite_count')) 



def normalize_df_schema(original_df):
	"""
		:param originaL_df: original dataframe with inferred schema
		:return new_df:	new dataframe with specified schema
	"""
	return original_df.withColumn("created_at", original_df["created_at"].cast(TimeStampType)).\
		withColumn("follower_count", original_df["follower_count"].cast(IntegerType)).\
		withColumn("description", original_df["description"].cast(StringType)).\
		withColumn("username", original_df["username"].cast(StringType)).\
		withColumn("text", original_df["text"].cast(StringType)).\
		withColumn("timestamp_ms", original_df["timestamp_ms"].cast(IntegerType)).\
		withColumn("quote_count", original_df["quote_count"].cast(IntegerType)).\
		withColumn("location", origina_df["location"].cast(StringType)).\
		withColumn("retweet_count", original_df["retweet_count"].cast(IntegerType))
	


if __name__ == "__main__":
	sqlcontext = SQLContext(sc)
	save_all_files_to_parquet_format(sqlcontext)

