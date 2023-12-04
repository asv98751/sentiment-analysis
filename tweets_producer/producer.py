from datetime import datetime
from confluent_kafka import Producer
import pandas as pd
import json
import time
import random

csv_path_file = 'tweets_sub.csv'
chunk_size = 1

csv_reader = pd.read_csv(csv_path_file, chunksize=chunk_size)


# Function to read Confluent Cloud configuration
def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


# Function to generate random interval within a 5-minute windo
def get_random_interval():
    return 0


# Initialize Kafka producer
producer = Producer(read_ccloud_config("client.properties"))

#Iterate over chunks
for i, chunk in enumerate(csv_reader):
    # Extract data from the chunk
    row = chunk.iloc[0]  # Access the first row in the chunk
    review_text = row['Review']
    id = row['id']
    sno = row['sno']

    data = {
        'sno': str(sno),
        'Review': review_text,
        'id': id,
        # 'timestamp': datetime.now() # Add a timestamp for each item
    }
    
    # data['timestamp'] = str(data['timestamp'])

    print(data)

    # Produce message to Kafka topic in JSON format
    producer.produce("sample", key=id, value=json.dumps(data))
    producer.flush()
    # print(data)

    # Introduce a random interval within the 5-minute window
    time.sleep(get_random_interval())