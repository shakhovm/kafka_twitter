from kafka import KafkaConsumer
import json
from cloud_storage_saver import save_json_to_cloud
from queries import Queries
import pandas as pd
from datetime import datetime
import sys


if __name__ == "__main__":
    consumer = KafkaConsumer(
        'tweets',
        value_deserializer=lambda x: json.loads(x.decode('ISO-8859-1')),
        auto_offset_reset='earliest',
        group_id='group01',
        enable_auto_commit=True
    )
    t = datetime.now()
    print("Application Began!")
    data = []
    for msg in consumer:
        data.append({
            "message": msg.value['message'],
            "account": msg.value['account'],
            "date": datetime.fromisoformat(msg.value['date'])
        })
        if t < data[-1]['date']:
            break
    df = pd.DataFrame.from_dict(data)
    q = Queries(df, t, int(sys.argv[1]))
    filename = sys.argv[3]
    dct = q.to_json()
    if sys.argv[2] == "--google-cloud":
        save_json_to_cloud(filename, dct, sys.argv[4])
    elif sys.argv[2] == "--local-file":
        with open(filename, 'w', encoding='ISO-8859-1') as f:
            json.dump(dct, f, indent=4)

