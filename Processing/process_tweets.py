"""
			***** INSIGHT DATA ENGINEERING ****

Code to run the transformation of my json files on S3 and processing them to a local database. 
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
from pyspark.sql.functions import lower, upper, col, column, instr, regexp_extract
from spark_to_postgres_library import write_to_postgres 
import logging
from pyspark.sql.types import DoubleType, TimestampType, StringType, LongType, IntegerType, DoubleType


spark = SparkSession.builder.appName("Initial App").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')
logging.basicConfig(filename="spark_processing.log", level=logging.DEBUG,
	filemode="w", format="%(asctime)s: %(process)d - %(levelname)s - %(message)s")
# TODO: Make the following list obsolete
known_user_list = ['amazondeals', 'bookdealdaily', 'couponkid', 'dealnews', 'retailnenot', 'redplumeditor',
                'smartsourcecpns', 'starbucks', 'theflightdeal', 'videogamedeals', 'truecouponing', 'couponology',
                'krazycouponlady', 'couponwithtoni', 'moneysavingmom', 'coupons', 'couponcraving', 'dealspotr',
                'couponing4you', 'fatkiddeals', 'amazondeals', 'slickdeals', 'bookbub', 'sneakersteal','kicks deals',
                'dealnews', 'kicksdeals', 'retailmenot', 'dealsplus', 'smartsourcecpns', '9to5toys', 'DealBase.com',
                'kinjadeals', 'theflightdeal', 'cheaptweet', 'heyitsfree', 'freestuffrocks', 'survivingstores',
                'targetdeals', 'jetbluecheep', 'drecoverycoupon', 'ihartcoupons', 'every1lovescoup', 'er1ca',
                'savethedollar', 'silverlight00', 'yeswecouponinc', 'couppwning', 'savingaplenty', 'coupon divas',
                'accidentalsaver', 'dealsplus', 'kicksdeals','amazon.com deals', 'kinja deals', 'retailmenot.com',
		'walmart','target','kroger','publix','krazycouponlady','befrugal','best buy','walgreens','mrs. frugal find',
		'brad''s deals', 'rakuten', 'the coupon mom', 'best buy deals', 'the frugal girls', 'jcpenney', 'groupon',
		'savings.com', 'momswhosave.com', 'slickdeals', 'andrea deckard', 'andrea deckard', 'freecouponcodes']


if __name__ == "__main__":
	sqlcontext = SQLContext(sc)
#	tweet_df = sqlcontext.read.json("s3a://{stream_bucket}/*.json".format(stream_bucket=os.environ['STREAMING_S3_FOLDER']))
#	tweet_df = sqlcontext.read.json("s3a://{bz2_bucket}/07/*/*/*".format(bz2_bucket=os.environ['COMPRESSED_S3_FOLDER']))
	tweet_df = spark.read.format("parquet").load("s3a://{s3_bucket}/Twitter_dumps/Parquet_folder/parquet_file_0*/*".format(s3_bucket=os.environ['S3_FOLDER']))
#	tweet_df = sqlcontext.read.load("s3a://{s3_bucket}/Twitter_dumps/Parquet_folder/*".format(s3_bucket=os.environ['S3_FOLDER']))	
	#filtered_df = tweet_df.filter(tweet_df.user.followers_count > 50000)
	#logging.debug(filtered_df.head())
	#logging.debug(filtered_df.select('user.followers_count','timestamp_ms','user.name','text','user.description').show(20))
	
	# Select specific columns
# 	focused_tweet_table = tweet_df.select(col('created_at'),col('user.followers_count').alias('follower_count'), col('user.description').alias('description'), col('user.name').alias('username'),\
# col('text'),col('timestamp_ms'),col('quote_count'),col('place.name').alias('location'),col('retweet_count'),col('quoted_status.retweeted').alias('retweeted')) 

	focused_tweet_table = tweet_df
	# Filter columns with a price and from  a user in our knwon_user_list
	money_deals_df = focused_tweet_table.where(lower(col('username')).isin(known_user_list)).filter(col('text').rlike("\$( ?)\d")|lower(col('text')).like('%free%'))
	# money_deals_df = focused_tweet_table.where(lower(col('username')).isin(known_user_list))
	print("Found  {} Tweet entries with a $ amount from your list of users.".format(money_deals_df.count()))
#	money_deals_df.select('username','text').withColumn('price','.(\$\d+).').show(20,False)


	# Create ['Product', 'Price', 'Company'] columns
	end_df = money_deals_df.withColumn('Price', regexp_extract(col('text'), '(\$( ?)\d+(.\d+)?)', 1)).withColumn('url', regexp_extract(col('text'), '(http[a-zA-Z0-9_.-\/:]*)', 1))

	# display 15 entries
	end_df.select('created_at','text','Price','username','url').show(15)
	# Write to SQL
 	write_to_postgres(end_df, "price_table")


# ----------------- DEAD WORKING QUERIES
# 	limited_df = filtered_df.select('user.followers_count','timestamp_ms','user.name','text','user.description')
# 	contains_price_filter = instr(col("text"), "$") >= 1 
# 	limited_df.withColumn("text", contains_price_filter). where("name").select("followers_count","timestamp_ms","name","description")
# 	limited_df.printSchema()
#	filtered_df = tweet_df.filter(tweet_df.user.name.isin(known_user_list))
#	print(filtered_df.head())
