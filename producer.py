from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import numpy as np
import time
import sys

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             x.encode('ISO-8859-1'))
    path = sys.argv[1]
    current_tweets = 0
    current_numbers = np.random.randint(30, 50)
    with open(path, encoding='ISO-8859-1') as f:
        start = time.time()
        for line in f:
            if current_tweets == current_numbers:
                time_dif = time.time() - start
                if time_dif < 1:
                    time.sleep(1 - time_dif)
                start = time.time()
                current_numbers = np.random.randint(30, 50)
                current_tweets = 0
            line = line.strip().split(',')

            producer.send('tweets', value=dumps(
                {
                    "account": line[4],
                    "date": datetime.now().isoformat(),
                    "message": line[5]
                }
            ))
            current_tweets += 1

