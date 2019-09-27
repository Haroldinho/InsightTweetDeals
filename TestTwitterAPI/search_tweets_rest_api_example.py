"""
A new demonstration of the twitter request using only the REST API

gen_rule_payload pulls more tweets than the default sandbox (100), don't include dates and default to hourly counts when
using the count API
"""

from searchtweets import load_credentials, gen_rule_payload, collect_results
import json

dev_search_args = load_credentials(filename="~/.twitter_keys.yaml", yaml_key="search_tweets_30_day_dev",
                                   env_overwrite=False)
hashtag_list = ['@AmazonDeals', '@BookDealDaily', '@CouponKid', "@DealNews", "@RetailNeNot", "@RedPlumEditor",
                "SmartSourceCpns", "@Starbucks", "@TheFlightDeal", "@VideoGameDeals", "@TrueCouponing",
                "@KrazyCouponLady", "@couponwithtoni", "@MoneySavingMom", "@Coupons", "@CouponCraving", "@dealspotr",
                "couponing4you", "@FatKidDeals", "@amazondeals", "@slickdeals", "@BookBub", "@sneakersteal",
                "@DealNews", "@KicksDeals", "@RetailMeNot", "@DealsPlus", "@SmartSourceCpns", "@9to5toys",
                "@KinjaDeals", "@TheFlightDeal", "@CheapTweet", "@HeyItsFree", "@FreeStuffROCKS", "@survivingstores",
                "@TargetDeals", "@JetBlueCheep", "@drecoverycoupon", "@ihartcoupons", "@every1lovescoup", "@er1ca",
                "@SaveTheDollar", "@silverlight00", "@yeswecouponinc", "@coupPWNing", "@SavingAplenty",
                "@AccidentalSaver", "@Walmart", "@Target", "@BestBuy", "deals", "saved", "supersaving", "bargain",
                "lightningdeal", "freebies",  "free", "DealMaster"]

file_name_30_day = "api_30day_info.json"



# testing a sandbox account
power_track_rule = '@KinjaDeals'
rule = gen_rule_payload(power_track_rule, results_per_call=100)
# funny thing about the next call is that results_per_call does not matter
# count_rule = gen_rule_payload(power_track_rule, results_per_call=100, count_bucket='day')
# collect_results has options to cutoff search when hitting limits on the number of Tweets and API calls

tweets = collect_results(rule, max_results=100, result_stream_args=dev_search_args)
# tweet_counts = collect_results(count_rule, result_stream_args=dev_search_args)
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Get attributes of a single tweet for power track {}: ".format(power_track_rule))
print(tweets[-1].__dict__)
print(dir(tweets[int(len(tweets)/2)]))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
[print(tweet.created_at_datetime, tweet.all_text, end='\n\n') for tweet in tweets if '$' in tweet.text]
print("Number of tweets: {}".format(len(tweets)))
