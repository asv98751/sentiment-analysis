from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
from confluent_kafka import Consumer
from confluent_kafka import Producer

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob
import spacy
import nltk
import string
import json
import os
import pandas as pd

# Get the directory where the script is located
script_directory = os.path.dirname(os.path.abspath(__file__))

# Specify the relative path to the download directory within the script's directory
download_dir = os.path.join(script_directory, 'nltk_data')

# Append the download directory to nltk.data.path
nltk.data.path.append(download_dir)

# Download NLTK stopwords data
nltk.download('stopwords', download_dir=download_dir)

nltk.download('punkt', download_dir=download_dir)

# Load spaCy English model
nlp = spacy.load('en_core_web_sm')


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    print("Running spark application")
    return conf

props = read_ccloud_config("./client.properties")
props["group.id"] = "python-group-1"
props["auto.offset.reset"] = "earliest"

consumer = Consumer(props)
consumer.subscribe(["sample"])

producer = Producer(props)


def preprocess_text(text):
    # Lowercase
    text = text.lower()

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Tokenization
    tokens = word_tokenize(text)

    # Remove stop words
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]

    return tokens

# Sentiment analysis
def get_sentiment(text):
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    return 1 if polarity > 0 else -1 if polarity < 0 else 0


# Competitor extraction
def extract_competitors(text):
    doc = nlp(text)
    competitors = [ent.text for ent in doc.ents if ent.label_ == 'ORG']
    return competitors

def getfield(text):
    return text

def process_and_save(df):
    
    df['tokens'] = df['Review'].apply(preprocess_text)
    df['sentiment'] = df['Review'].apply(get_sentiment)
    df['competitors'] = df['Review'].apply(extract_competitors)
    df['id'] = df['id'].apply(getfield)
    # df['timestamp'] = df['timestamp'].apply(getfield)
    
    result_df = df[['Review', 'sentiment', 'competitors', 'id']]
    
    extract_json = result_df.to_json(orient='records', lines=True)

    jsonString = json.dumps(extract_json)

    # message_value = json_str.encode('utf-8')

    
    publish_sentiment(extract_json)


# publishes the data back to analysis stream..
def publish_sentiment(result_json):
    producer.produce("processed_tweets", key='tweetsentiment', value=result_json)
    producer.flush()
    

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("KafkaSparkApplication").getOrCreate()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                # Decode message key and value from bytes to string
                key = msg.key().decode('utf-8') if msg.key() else None
                val = msg.value().decode('utf-8') if msg.value() else None

                # Parse the JSON message
                try:
                    json_data = json.loads(val)
                    
                    if json_data['Review'] is not None:
                    
                        array =  []
                        array.append(json_data)
                        
                        df = pd.DataFrame(array)
                        
                        
                        process_and_save(df)
                        
                    else :
                        continue
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
