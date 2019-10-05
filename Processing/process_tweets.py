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
from spacy_ned_library import add_product_column_to_df, add_brand_column_to_df	

spark = SparkSession.builder.appName("Initial App").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.python.worker.reuse", "true")
sc = spark.sparkContext
sc.addPyFile('/home/ubuntu/InsightTweetDeals/Processing/spacy_ned_library.py')
sc.setLogLevel('WARN')
logging.basicConfig(filename="spark_processing.log", level=logging.DEBUG,
	filemode="w", format="%(asctime)s: %(process)d - %(levelname)s - %(message)s")
# TODO: Make the following list obsolete
known_user_list = ['amazondeals', 'bookdealdaily', 'couponkid', 'dealnews', 'retailnenot', 'redplumeditor', 'offers.com',
                'smartsourcecpns', 'starbucks', 'theflightdeal', 'videogamedeals', 'truecouponing', 'couponology', 'heels.com',
                'krazycouponlady', 'couponwithtoni', 'moneysavingmom', 'coupons', 'couponcraving', 'dealspotr', 'fandangonow',
                'couponing4you', 'fatkiddeals', 'amazondeals', 'slickdeals', 'bookbub', 'sneakersteal','kicks deals', 'coupon-smart.com',
                'dealnews', 'kicksdeals', 'retailmenot', 'dealsplus', 'smartsourcecpns', '9to5toys', 'DealBase.com', 'freecoupons.com',
                'kinjadeals', 'theflightdeal', 'cheaptweet', 'heyitsfree', 'freestuffrocks', 'survivingstores', 'best coupon online',
                'targetdeals', 'jetbluecheep', 'drecoverycoupon', 'ihartcoupons', 'every1lovescoup', 'er1ca','couponcabin',
                'savethedollar', 'silverlight00', 'yeswecouponinc', 'couppwning', 'savingaplenty', 'coupon divas', 'deal master',
                'accidentalsaver', 'dealsplus', 'kicksdeals','amazon.com deals', 'kinja deals', 'retailmenot.com','dealmaster',
		'walmart','target','kroger','publix','krazycouponlady','befrugal','best buy','walgreens','mrs. frugal find',
		'brad''s deals', 'rakuten', 'the coupon mom', 'best buy deals', 'the frugal girls', 'jcpenney', 'groupon',
		'savings.com', 'momswhosave.com', 'slickdeals', 'andrea deckard', 'andrea deckard', 'freecouponcodes',
		'kicks under cost', 'amazon deals', '9to5toys', '9to5mac.com', 'bgr.com']


if __name__ == "__main__":
	sqlcontext = SQLContext(sc)
#	tweet_df = sqlcontext.read.json("s3a://{bz2_bucket}/07/*/*/*".format(bz2_bucket=os.environ['COMPRESSED_S3_FOLDER']))
	tweet_df = spark.read.format("parquet").load("s3a://{s3_bucket}/Twitter_dumps/Parquet_folder/*/*".format(s3_bucket=os.environ['S3_FOLDER']))
	focused_tweet_table = tweet_df
	# Filter columns with a price and from  a user in our knwon_user_list
	money_deals_df = focused_tweet_table.where(lower(col('username')).isin(known_user_list)).filter(col('text').rlike("\$( ?)\d")|lower(col('text')).like('%free%'))
	# money_deals_df = focused_tweet_table.where(lower(col('username')).isin(known_user_list))
	print("Found  {} Tweet entries with a $ amount from your list of users.".format(money_deals_df.count()))
#	money_deals_df.select('username','text').withColumn('price','.(\$\d+).').show(20,False)


	# Create ['Product', 'Price', 'Company'] columns
	end_df = money_deals_df.withColumn('Price', regexp_extract(col('text'), '(\$( ?)\d+(.\d+)?)', 1)).withColumn('url', regexp_extract(col('text'), '(http[a-zA-Z0-9_.-\/:]*)', 1))
	end_df = add_product_column_to_df(end_df)
	end_df = add_brand_column_to_df(end_df)
	# display 15 entries
	end_df.select('created_at', 'text', 'Price', 'username', 'product', 'brand', 'url').show(15)
	# Write to SQL
 	#write_to_postgres(end_df, "price_table")


