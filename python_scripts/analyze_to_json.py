from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.cloud import automl_v1beta1
from google.cloud.automl_v1beta1.proto import service_pb2
from time import sleep
import six
import pandas as pd
import json
import extract_params
import os


# Extracts entities mentioned in tweets (people, places, things, etc...)
def analyze_entities(content, client, itr=0):

    # Outputs how many times analyze_entities has been run
    print "Entity Call #" + str(itr + 1)

    # Decodes to utf-8 if it is a binary type
    if isinstance(content, six.binary_type):
        content = content.decode('utf-8')

    # creates the document object required for entity extraction
    type_ = enums.Document.Type.PLAIN_TEXT
    document = {'type': type_, 'content': content, 'language':'en'}

    #extracts entities
    response = client.analyze_entities(document)

    persons_temp = []
    places_temp = []
    things_temp = []

    # Fills a list for people, places, things
    # From (https://cloud.google.com/natural-language/docs/reference/rest/v1/Entity#type_1)
    for entity in response.entities:
        if entity.type == 1:
            persons_temp.append(entity.name)
        elif entity.type == 2:
            places_temp.append(entity.name)
        elif entity.type == 11 or entity.type == 12:
            pass
        else:
            things_temp.append(entity.name)

    # To keep under quota cap of 1000 calls per 100 seconds 
    sleep(0.11)

    # Returns the people, places, and things
    return([persons_temp, places_temp, things_temp])


# Analyzes the sentiment of a tweet based on a custom AutoML model
def analyze_sentiment_automl(content, location_id, project_id, model_id, client, itr=0):

    # Outputs the number of times the model has been called
    print "AutoML Call #" + str(itr + 1)    
    
    # Converts the tweet to utf-8 if it is in binary format
    if isinstance(content, six.binary_type):
        content = content.decode('utf-8')
  
    # Creates the payload and requests the sentiment results
    name = 'projects/{}/locations/{}/models/{}'.format(project_id,location_id, model_id)
    payload = {'text_snippet': {'content': content, 'mime_type': 'text/plain' }}
    params = {}
    request = client.predict(name, payload, params)

    # To keep under quota cap of 600 calls per 60 seconds
    sleep(0.11)
    
    # Returns the sentiment results
    return request


# Analyzes the sentiment of a tweet based on the standard Google NLP Model
def analyze_sentiment(content, client, itr=0):

    # Outputs the number of times the model has been called
    print "Sentiment Call #" + str(itr + 1)

    # Converts the tweet to utf-8 if it is in binary format
    if isinstance(content, six.binary_type):
        content = content.decode('utf-8')

    # Creates the document required for analysis
    type_ = enums.Document.Type.PLAIN_TEXT
    document = {'type': type_, 'content': content, 'language': 'en'}

    # Returns the sentiment results
    response = client.analyze_sentiment(document)
    sentiment = response.document_sentiment
    
    # To keep under the quota cap of 1000 calls per 10 minutes
    sleep(0.1)
    
    # Returns the sentiment results
    return([sentiment.score, sentiment.magnitude])

# Creates a directory if it does not already exist
def create_folders(dirName):
    if not os.path.exists(dirName):
        os.mkdir(dirName)
    return None

# Runs the functions defined above to extract entities/sentiment for a set of tweets
def analyze_tweets():
    
    print "Step 2 Started: Analysis/Extraction on Tweets"
    
    # Creates folders for json documents to be placed if they do not already exist
    create_folders("../output/json")
    create_folders("../output/json/raw")
    create_folders("../output/json/processed")

    # Gets parameters from the pipeline json document
    params = extract_params.get_params()

    # Creates connection clients for entity extraction and sentiment analysis
    entity_client = language_v1.LanguageServiceClient()
    sentiment_client = automl_v1beta1.PredictionServiceClient()

    # Gets the tweets from the intermediary CSV from get_tweets.py
    filepath = '../output/export_dataframe.csv'
    tweet_records = pd.read_csv(filepath, engine="python", error_bad_lines=False)

    sentiments = []
    sentiment_labels = []    
    
    # Runs AutoML Sentiment Analysis if the pipeline json specified an AutoML model as "yes"
    if params['analyze']['automl_project'].lower() == "yes":
        automl_project_id = params['analyze']['automl_project_id']
        automl_model_id = params['analyze']['automl_model_id']
        automl_location_id = params['analyze']['automl_location_id']
        tweets_with_sentiment = [analyze_sentiment_automl(tweet, automl_project_id, automl_location_id, automl_model_id, sentiment_client, itr) for itr,tweet in enumerate(tweet_records['tweet'])]
        
        for tweet in tweets_with_sentiment:
        #workaround for AutoML sentiment = 0 not showing up in text_sentiment.sentiment payload
            if str(tweet.payload[0].text_sentiment) == "":
                sentiments.append("0")
                sentiment_labels.append("Negative")
            else:
                sentiments.append(tweet.payload[0].text_sentiment.sentiment)
                if tweet.payload[0].text_sentiment.sentiment == 1:
                    sentiment_labels.append("Neutral")
                else:
                    sentiment_labels.append("Positive")
    # Otherwise, standard NLP sentiment analysis is applied
    else:
        tweets_with_sentiment = [analyze_sentiment(tweet, entity_client, itr) for itr,tweet in enumerate(tweet_records['tweet'])]
        for tweet in tweets_with_sentiment:
            sentiments.append(tweet[0])
            if tweet[0] == 0:
                sentiment_labels.append("Neutral")
            elif tweet[0] < 0:
                sentiment_labels.append("Negative")
            else:
                sentiment_labels.append("Positive")


    tweet_records['sentiment'] = sentiments
    tweet_records['sentiment_label'] = sentiment_labels

    # Entities are extracted for the tweets
    tweets_with_entities = [analyze_entities(tweet, entity_client, itr) for itr, tweet in enumerate(tweet_records['tweet'])]

    people = []
    locations = []
    things = []

    # Entities are filled as lists (since each tweet can have 0 to many people/places/things)
    for tweet in tweets_with_entities:
        people.append(tweet[0])
        locations.append(tweet[1])
        things.append(tweet[2])

    tweet_records['people'] = people
    tweet_records['locations'] = locations
    tweet_records['things'] = things

    # JSON document created for each tweet (so that records can have many people/places/things)
    for index, row in tweet_records.iterrows():
        json_people = row['people']
        json_places = row['locations']
        json_things = row['things']
        tweet_user = row['user']
        tweet_text = row['tweet']
        tweet_bio_location = row['location']
        tweet_tagged_location = row['place']
        tweet_sentiment = row['sentiment']
        tweet_sentiment_label = row['sentiment_label']
        tweet_geocoord = row['lat_lng']
        tweet_datetime = row['datetime']
        tweet_keywords_flag = False

        key_words = []

        for i in params['analyze']['key_words']:
            key_words.append(i.encode('ascii', 'ignore'))

        if any(word in tweet_text for word in key_words):
            tweet_keywords_flag = True

        record = {
                "user": tweet_user,
                "tweet": tweet_text,
                "bio_location": tweet_bio_location,
                "tweet_tagged_location": tweet_tagged_location,
                "sentiment_score": tweet_sentiment,
                "sentiment_label": tweet_sentiment_label,
                "tweet_datetime": tweet_datetime,
                "tweet_tagged_geocoord": tweet_geocoord,
                "tweet_keywords_flag": tweet_keywords_flag
                }

        dic = {
                "people": json_people,
                "places": json_places,
                "things": json_things
              }

        json_dict = {}
        data = []

        for k, v in dic.iteritems():
            tmp_dict = {}
            tmp_dict[k] = [{"instance": i} for i in v]
            data.append(tmp_dict)

        json_dict["record_data"] = record 
        json_dict["entities"] = data

        file_id = str(tweet_datetime) + str(tweet_user)

        file_name = "../output/json/raw/sentiment_analyzed" + file_id  + ".json"

        with open(file_name, "w") as outfile:
            json.dump(json_dict, outfile, indent=4, sort_keys=True)

    print "Step 2 Completed: Analysis/Extraction on Tweets"
