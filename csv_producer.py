import pandas as pd
from confluent_kafka import Producer
import json
import time
from pipeline import logger

def create_kafka_producer():
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'client.id': 'csv-producer',
        'acks': 'all'
    }
    return Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def produce_from_csv(topic):
    file_path = 'query.csv'
    producer = create_kafka_producer()
    df = pd.read_csv(file_path)

    logger.info(f"Loaded {len(df)} rows from {file_path}")

    # Correct rename mapping (old_name: new_name)
    rename_map = {
        'id': 'feature_id','magType': 'magtype','dmin': 'dmin','types': 'product_type',
        'title': 'title','gap': 'gap','type': 'type',
        'rms': 'rms','nst': 'nst','ids': 'ids','sources': 'sources',
        'net': 'net','code': 'code','sig': 'sig','tsunami': 'tsunami',
        'alert': 'alert','status': 'status','cdi': 'cdi','mmi': 'mmi',
        'detail': 'detail','felt': 'felt','tz': 'tz',
        'url': 'url','updated': 'updated','time': 'time','mag': 'mag',
        'depth': 'depth','longitude': 'longitude','latitude': 'latitude'
    }

    # Apply renaming
    df.rename(columns=rename_map, inplace=True)

    for _, row in df.iterrows():
        message = row.to_json()
        producer.produce(topic=topic, value=message, key=str(row['feature_id']), callback=delivery_report)
        # Optional delay for better streaming behavior
        time.sleep(0.3)

    producer.flush()
    logger.info(f"Finished producing {len(df)} records from CSV to Kafka topic '{topic}'")

if __name__ == "__main__":
    produce_from_csv(topic='earthquake_data')
