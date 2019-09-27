"""
                    Insight Data Engineering SEA'19C
Project: Traffic Control of API source


"""

from private_twitter_authentication_config import SystemDesignerCredentials
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.api import API
import logging
import urllib3
import socket
import time
import json

# all debug will be recorded
# other information logging.INFO, logging.WARNING, logging.CRITICAL
# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename="twitter_connector_deals2.log", level=logging.DEBUG, filemode="w", format="%(asctime)s: %(process)d - %(levelname)s - %(message)s")
logging.debug("Logging into Twitter Stream API")
# General Authentication
config_instance = SystemDesignerCredentials()
consumer_key = config_instance.consumer_api_key
consumer_secret = config_instance.consumer_secret_api_key
access_token = config_instance.access_token
access_secret = config_instance.access_token_secret
TWEET_LIMIT_PER_FILE = 1000
# hashtag_list = "'@AmazonDeals', '@BookDealDaily', '@CouponKid', '@DealNews'," +
# "'@RetailNeNot', '@RedPlumEditor', 'SmartSourceCpns', '@Starbucks'," +
# "'@TheFlightDeal'," +
#         "'@VideoGameDeals', '@TrueCouponing', '@KrazyCouponLady'," +
#         "'@couponwithtoni', '@MoneySavingMom', '@Coupons', '@CouponCraving'," +
#         "'@dealspotr'," +
#         "'couponing4you', '@FatKidDeals', '@amazondeals', '@slickdeals'," +
#         "'@BookBub', '@sneakersteal', 'DealNews', '@KicksDeals'," +
#         "'@RetailMeNot', '@DealsPlus', '@SmartSourceCpns', '@9to5toys'," +
#         "'@KinjaDeals'," +
#         "'@CheapTweet',  '@HeyItsFree', '@FreeStuffROCKS'," +
#         "'@survivingstores'," +
#         "'@TargetDeals', '@JetBlueCheep', '@JetBlueCheap'," +
#         "'@drecoverycoupon'," +
#         "'@ihartcoupons', '@every1lovescoup', '@er1ca', '@SaveTheDollar'," +
#         "'@silverlight00'," +
#         "'@yeswecouponinc', '@coupPWNing', '@SavingAplenty'," +
#         "'@AccidentalSaver', '@Walmart', '@Target', '@BestBuy', 'deals'," +
#         "'saved', 'supersaving', 'bargain'," +
#         "'lightningdeal', 'freebies', 'free'"


def convert_status_to_json_dict(status):
    print(status)
    json_obj = {"id": status.id_str,
                "user_name": status.user.screen_name,
                "user_location": status.user.location,
                "user_description": status.user.description,
                "followers_count": status.user.followers_count,
                "text": status.text,
                "creation_time": status.created_at,
                "retweets": status.retweet_count,
                "is_retweet": status.retweeted}
    return json_obj


def convert_status_dict_to_json_dict(status):
    print(status)
    json_obj = {"id": status["id_str"],
                "user_name": status["user"]["screen_name"],
                "user_location": status["user"]["location"],
                "user_description": status["user"]["description"],
                "followers_count": status["user"]["followers_count"],
                "text": status["text"],
                "creation_time": status["created_at"],
                "retweets": status["retweet_count"],
                "is_retweet": status["retweeted"]}
    return json_obj


# Basic listener printing received tweets to stdout
class MySimpleStreamListener(StreamListener):
    def __init__(self, api=None):
        self.api = api or API()
        self.internal_list = []
        self.filename_data = './Data/json_dump'
        self.filename_status = './Data_Status/json_dump'
        self.num_files_written = 0

    def on_connect(self):
        logging.info("Successfully connected")
        pass

    def on_disconnect(self, notice):
        logging.warn("Disconnect notice: "+notice)
#        f.close()
        return

    def on_status(self, status):
        logging.info("Status " + status)
        logging.info(status)
        json_dict = convert_status_to_json_dict(status)
        if len(self.internal_list) > TWEET_LIMIT_PER_FILE:
            filename = self.filename_status + str(round(time.time())) + ".json"
            with open(filename, 'w+', encoding='utf-8') as output_file:
                json.dump(self.internal_list, output_file, indent=4)
                self.internal_list = []
                logging.info('-------Status Data Dumped--------')
            self.internal_list.append(json_dict)
            self.num_files_written += 1
        else:
            self.internal_list.append(json_dict)
        if self.num_files_written > 50:
            exit()
#        f.write()
#        logging.info("Current Status " + status)
        return

    def on_data(self, data):
        logging.info("Data " + data)
        status = json.loads(data)
        json_dict = convert_status_dict_to_json_dict(status)
        json_text = json.dumps(json_dict)
        print(json_text)
        if len(self.internal_list) > TWEET_LIMIT_PER_FILE:
            filename = self.filename_data + str(round(time.time())) + ".json"
            with open(filename, 'w+', encoding='utf-8') as output_file:
                json.dump(self.internal_list, output_file, indent=4)
                self.internal_list = []
                logging.info('------- Data Data Dumped--------')
            self.internal_list.append(json_dict)
            self.num_files_written += 1
        else:
            self.internal_list.append(json_dict)
        if self.num_files_written > 50:
            exit()
        return True

    def on_error(self, status):
        logging.error(status)
        # Exceeded connection attempt limits if 420
        if status == 420:
            # returning False is on_data disconnects the stream
            return False


def establish_connection():
    my_stream_listener = MySimpleStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    my_stream = Stream(auth, listener=my_stream_listener)

    # Filter Twitter Streams to capture data by keywords:
    my_stream.filter(track=['@AmazonDeals', '@BookDealDaily', '@CouponKid',
        "@DealNews", "@RetailNeNot", "@RedPlumEditor",
                        "SmartSourceCpns", "@Starbucks", "@TheFlightDeal",
                        "@VideoGameDeals", "@TrueCouponing",
                                        "@KrazyCouponLady", "@couponwithtoni",
                                        "@MoneySavingMom", "@Coupons",
                                        "@CouponCraving", "@dealspotr",
                                                        "couponing4you",
                                                        "@FatKidDeals",
                                                        "@amazondeals",
                                                        "@slickdeals",
                                                        "@BookBub",
                                                        "@sneakersteal",
                                                                        "@DealNews",
                                                                        "@KicksDeals",
                                                                        "@RetailMeNot",
                                                                        "@DealsPlus",
                                                                        "@SmartSourceCpns",
                                                                        "@9to5toys",
                                                                                        "@KinjaDeals",
                                                                                        "@TheFlightDeal",
                                                                                        "@CheapTweet",
                                                                                        "@HeyItsFree",
                                                                                        "@FreeStuffROCKS",
                                                                                        "@survivingstores",
                                                                                                        "@TargetDeals",
                                                                                                        "@JetBlueCheep",
                                                                                                        "@drecoverycoupon",
                                                                                                        "@ihartcoupons",
                                                                                                        "@every1lovescoup",
                                                                                                        "@er1ca",
                                                                                                                        "@SaveTheDollar",
                                                                                                                        "@silverlight00",
                                                                                                                        "@yeswecouponinc",
                                                                                                                        "@coupPWNing",
                                                                                                                        "@SavingAplenty",
                                                                                                                                        "@AccidentalSaver"])


if __name__ == '__main__':
    # This handles Twitter authentication and the
    # connection to Twitter Streaming API
    try:
        establish_connection()
    except (urllib3.exceptions.ReadTimeoutError, socket.timeout):
        logging.info("ATTN: Lost connection with Twitter Streaming Services")
        # Pause for 60 seconds and start back
        time.sleep(60)
        logging.info("Starting back")
        establish_connection()




