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
from spark_to_postgres_library import write_to_postgres 

# hashtag_list = ['AmazonDeals', 'BookDealDaily', 'CouponKid', "DealNews", "RetailNeNot", "RedPlumEditor",
#                 "SmartSourceCpns", "Starbucks", "TheFlightDeal", "VideoGameDeals", "TrueCouponing",
#                 "KrazyCouponLady", "couponwithtoni", "MoneySavingMom", "Coupons", "CouponCraving", "dealspotr",
#                 "couponing4you", "FatKidDeals", "amazondeals", "slickdeals", "BookBub", "sneakersteal",
#                 "DealNews", "KicksDeals", "RetailMeNot", "DealsPlus", "SmartSourceCpns", "9to5toys",
#                 "KinjaDeals", "TheFlightDeal", "CheapTweet", "HeyItsFree", "FreeStuffROCKS", "survivingstores",
#                 "TargetDeals", "JetBlueCheep", "drecoverycoupon", "ihartcoupons", "every1lovescoup", "er1ca",
#                 "SaveTheDollar", "silverlight00", "yeswecouponinc", "coupPWNing", "SavingAplenty",
#                 "AccidentalSaver"]


if __name__ == "__main__":
# 	spark = SparkSession.builder.config('spark.driver.extraClassPath','/opt/spark/postgresql-42.2.8.jar').config('spark.driver.extraClassPath','/opt/spark/postgresql-42.2.8.jar').appName("Initial App").getOrCreate()
	spark = SparkSession.builder.appName("Initial App").getOrCreate()
	sc = spark.sparkContext
	sqlcontext = SQLContext(sc)
	#textFiles = sc.wholeTextFiles("s3n://insighttwitterdeals/Twitter_dumps/bz2_folder/")
	#print(textFiles.collect())
#	tweetdf = sqlcontext.read.json("s3a://insighttwitterdeals/Twitter_Streams/*.json")
	tweetdf = sqlcontext.read.json("s3a://insighttwitterdeals/Twitter_dumps/bz2_folder/06/07/00/*")
#	tweetdf.printSchema()
#	print("id_str ", tweetdf.user.id_str.dtypes)
#	filtered_df = tweetdf.filter(tweetdf.user.followers_count > 50000)

	filtered_df = tweetdf.filter('amazon' in tweetdf.quoted_status.user.name)
	print(filtered_df.head())
#	print("name ", tweetdf.user.name.dtypes)
	print(filtered_df.select('user.followers_count','timestamp_ms','user.name','text','user.description').show(20))
	limited_df = filtered_df.select('user.followers_count','timestamp_ms','user.name','text','user.description')
	limited_df.printSchema()
#	write_to_postgres(limited_df, "new_table")

#	filtered_df = tweetdf.filter(tweetdf.user.name.isin(hashtag_list))
#	print(filtered_df.head())
