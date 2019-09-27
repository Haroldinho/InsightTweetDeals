"""
                    Insight Data Engineering SEA'19C
Project: Traffic Control of API source
Test of RESTful API capabilities
Sunday 9/15/2019
"""

from private_twitter_authentication_config import SystemDesignerCredentials
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import logging
import pprint

logging.basicConfig(level=logging.WARNING, format="%(asctime)s: %(process)d - %(levelname)s - %(message)s")
logging.debug("Logging into Twitter REST API")

# General Authentication
config_instance = SystemDesignerCredentials()
consumer_key = config_instance.consumer_api_key
consumer_secret = config_instance.consumer_secret_api_key
access_token = config_instance.access_token
access_secret = config_instance.access_token_secret


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
try:
    api.verify_credentials()
except Exception as e:
    logging.error("Error creating API")
    raise e
logging.info("API created")
# # Get my public tweet
# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     pprint.pprint(tweet)


# Or get specific tweets

# # Need to exchange public token for access token
# try:
#     redirect_url = auth.get_authorization_url()
# except tweepy.TweepError:
#     logging.error('Failed to get request token.')

# Search Tweets
for tweet in api.search(q="Data Engineering", lang="en", rpp=10):
    print(f"{tweet.user.name}:{tweet.text}")