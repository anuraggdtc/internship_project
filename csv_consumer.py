import json
from confluent_kafka import Consumer
import pandas as pd
from sqlalchemy import create_engine
from pipeline import logger
import time

def create_kafka_consumer(topic):
    config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'earthquake-consumer',
        'auto.offset.reset': 'earliest',  # Consume from start
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer

def store_in_postgres(df):
    if df.empty:
        logger.info("No records to insert into PostgreSQL")
        return
    db_connection_str = 'postgresql://root:root@localhost:5432/db'
    engine = create_engine(db_connection_str)
    try:
        df.to_sql('consumertable', engine, if_exists='append', index=False)
        logger.info(f"Inserted {len(df)} records into PostgreSQL (consumertable)")
    except Exception as e:
        logger.error(f"PostgreSQL insert error: {e}")

def consume_to_postgres(consumer, sleep_seconds=3):
    while True:
        msg = consumer.poll(10.0)
        if msg is None:
            print("No message yet.")
            time.sleep(sleep_seconds)
            continue
        if msg.error():
            
            logger.error(f"Consumer error: {msg.error()}")
            time.sleep(sleep_seconds)
            continue
        try:
            consumed_data = json.loads(msg.value().decode('utf-8'))
            df = pd.DataFrame([consumed_data])
            store_in_postgres(df)
            logger.info(f"Consumed and inserted record with feature_id: {consumed_data.get('feature_id')}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    consumer_obj = create_kafka_consumer('earthquake_data')
    consume_to_postgres(consumer_obj)
