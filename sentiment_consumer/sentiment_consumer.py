from datetime import datetime
import json
from elasticsearch import Elasticsearch
import pymongo
import json
from io import StringIO
from datetime import datetime
from confluent_kafka import Consumer

ELASTIC_URL = "https://site:33cec238f197ade0bf08eaa4cf1c5736@oin-us-east-1.searchly.com:443"
es = Elasticsearch([ELASTIC_URL],
                    use_ssl=True,
                    verify_certs=False
                )
es_index = 'tweet-sentiments'

myClient = pymongo.MongoClient("mongodb+srv://DFSUSER:DFSUMBC@dfs.2iuyfir.mongodb.net")
mydb = myClient['DFS']
tweets_collection = mydb['tweets-sentiment']


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


props = read_ccloud_config("client.properties")
props["group.id"] = "sentiment"
props["auto.offset.reset"] = "earliest"

consumer = Consumer(props)
consumer.subscribe(["processed_tweets"])


def save_to_es(json_):
    es.update(index=es_index, doc_type='_doc', id=json_['id'], body={'doc': json_, 'doc_as_upsert': True})

def save_to_mongo(json_):
    json_['_id'] = json_['id']
    tweets_collection.update_one({'_id': json_['id']}, {'$set': json_}, upsert=True)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            # Decode message key and value from bytes to string
            key = msg.key().decode('utf-8') if msg.key() else None
            val = msg.value().decode('utf-8') if msg.value() else None
            # Parse the JSON message
            try:
                io = StringIO(val)
                json_data = json.loads(val)

                current_datetime = datetime.now()

                # Format datetime as a string in ISO 8601 format
                formatted_timestamp = current_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

                # Parse the JSON-formatted string to a dictionary
                # Modify the dictionary
                json_data['timestamp'] = formatted_timestamp
                # json2 = json.loads(json_data)
                print(json_data)
                save_to_es(json_data)
                save_to_mongo(json_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
