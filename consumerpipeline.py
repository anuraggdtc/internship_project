import json
from confluent_kafka import Consumer, KafkaException
import pandas as pd
from pipeline import logger
from sqlalchemy import create_engine
from time import sleep

def create_kafka_consumer(topic):
    config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'earthquake-consumer',
    'auto.offset.reset': 'latest',
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer

def msg_to_df(cons, dataframe):
    while True:
        msg = cons.poll(10.0) # Poll for a longer timeout to wait for messages
        # err ye empty
        if msg is None:
            print("No message till now.")
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # main message
        try:
            consumed_data = json.loads(msg.value().decode('utf-8'))
            #print(f"mila: {consumed_data}")
            new_data_df = pd.DataFrame([consumed_data])
            dataframe = pd.concat([dataframe, new_data_df], ignore_index=True)
            #print(dataframe) 
            store_in_postgres(new_data_df)
            sleep(3)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            sleep(2)
            continue
    
def store_in_postgres(df):
    if df.empty:
        logger.info("No records to insert into PostgreSQL")
        # return nahi toh gives error and stops 
        return
    db_connection_str = 'postgresql://root:root@localhost:5432/db'
    engine = create_engine(db_connection_str)
    try:
        df.to_sql('consumertable', engine, if_exists='append', index=False)
        logger.info(f"Inserted {len(df)} records into PostgreSQL")
    except Exception as e: # why is new data not showing? (fixd)
        logger.error(f"PostgreSQL insert error: {e}")

if __name__ == "__main__":
    consumer_obj = create_kafka_consumer(topic='earthquake_data')
    dff = pd.DataFrame()
    df = msg_to_df(consumer_obj, dff)
