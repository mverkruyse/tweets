import tweepy as tw
import pandas as pd
import extract_params

def get_tweets():

    print "Step 1 Started: Beginning Tweet Extraction from Twitter"

    params = extract_params.get_params()

    # Get your secret keys from the Twitter API
    # (https://developer.twitter.com/en/apps)
    consumer_key=params['get_tweets']['consumer_key']
    consumer_secret=params['get_tweets']['consumer_secret']
    access_token=params['get_tweets']['access_token']
    access_token_secret=params['get_tweets']['access_token_secret']

    # Create an authorized connection to the Twitter API
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    # Define the search term and thedate range variables
    search_words = params['get_tweets']['search_words']
    date_since = params['get_tweets']['date_since']
    date_until = params['get_tweets']['date_until']

    # Remove retweets
    new_search = search_words + " -filter:retweets"

    # Collect tweets (number based on the int places in items(x))
    tweets = tw.Cursor(api.search, q=new_search, lang="en", since=date_since, until=date_until, tweet_mode="extended").items(500)

    print "Tweets pulled down from Twitter"

    # Extract username, location, place, tweet text, timestamp from each tweet object 
    # (https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object)
    tweet_details = [[tweet.user.screen_name, tweet.user.location, tweet.place, tweet.full_text, tweet.created_at] for tweet in tweets]

    # Iterates through each tweet
    for record in tweet_details:
        # Records all have tweet.place, but if there isnt a geography tagged, then tweet.place is empty
        # If the record isnt empty, then we extract the lat/long, and then set the place field to the full place name
        # If the record is empty, then we do nothing and leave it empty
        record.append("")
        if record[2] is not None:
            lat = record[2].bounding_box.coordinates[0][0][1]
            lng = record[2].bounding_box.coordinates[0][0][0]
            lat_lng = str(lat) + ", " + str(lng)
            record[5] = lat_lng
            record[2] = record[2].full_name


    # Converts the tweet records to a pandas dataframe, to be converted to a .csv
    tweet_text = pd.DataFrame(data=tweet_details, 
                                columns=['user', "location", "place", 'tweet', "datetime", 'lat_lng'])

    # Exports the dataframe to a .csv as a checkpoint
    export_csv = tweet_text.to_csv (r'../output/export_dataframe.csv', index=None, header=True, encoding='utf-8')

    print "Step 1 Completed: Tweets Extracted from Twitter"
