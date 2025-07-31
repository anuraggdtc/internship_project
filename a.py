from confluent_kafka import Consumer, KafkaException
import logging
from sqlalchemy import create_engine
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_kafka_consumer():
    # this will give me the conf
    conf = {
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'}
    return Consumer(conf)

def store_in_postgres(df):
    # same as producerpipeline
    if df.empty:
        logger.info("No records to insert into PostgreSQL")
        return
    db_connection_str = 'postgresql://root:root@localhost:5432/db'
    engine = create_engine(db_connection_str)
    try:#just changed the table
        df.to_sql('consumertable', engine, if_exists='append', index=False)
        logger.info(f"Inserted {len(df)} records into PostgreSQL")
    except Exception as e:
        logger.error(f"PostgreSQL insert error: {e}")

def consume_data(consumer,mytopic):
    consumer.subscribe([mytopic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0) # Poll for messages with a 1-second timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                else:
                    print(f"Consumer error: {msg.error()}")
            else:
                # Process the message
                consumed_data = msg.value().decode('utf-8')
                print(consumed_data) # Print the consumed data
                return consumed_data # Return the consumed data

    except KeyboardInterrupt:
        pass

def main():
    # get the conf
    consumer_obj = create_kafka_consumer()
    topic = 'earthquake_data'
    consumed_data = consume_data(consumer_obj,topic)
    # print(consumed_data) # Removed this print as it's now inside consume_data


if __name__ == "__main__":
    main()