
import pandas as pd
import requests
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine
from confluent_kafka import Producer
import logging

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# set ko global so all loop mein seen
existing_feature_ids = set()

def process_to_df(data):
    rows = []
    for feature in data.get('features', []):
        if 'geometry' in feature and 'properties' in feature:
            geometry = feature['geometry']
            properties = feature['properties']
            if geometry.get('type') == 'Point' and isinstance(geometry['coordinates'], list):
                row = {
                    'feature_id': feature.get('id'),'feature_type': feature.get('type'),
                    'longitude': geometry['coordinates'][0],
                    'latitude': geometry['coordinates'][1],
                    'depth': geometry['coordinates'][2],
                    'mag': properties.get('mag'),'place': properties.get('place'),
                    'time': pd.to_datetime(properties.get('time'), unit='ms') if properties.get('time') else None,
                    'updated': pd.to_datetime(properties.get('updated'), unit='ms') if properties.get('updated') else None,
                    'tz': properties.get('tz'),'url': properties.get('url'),
                    'detail': properties.get('detail'),'felt': properties.get('felt'),
                    'cdi': properties.get('cdi'),'mmi': properties.get('mmi'),
                    'alert': properties.get('alert'),'status': properties.get('status'),
                    'tsunami': properties.get('tsunami'),'sig': properties.get('sig'),
                    'net': properties.get('net'),'code': properties.get('code'),
                    'ids': properties.get('ids'),'sources': properties.get('sources'),
                    'product_type': properties.get('types'),'nst': properties.get('nst'),
                    'dmin': properties.get('dmin'),'rms': properties.get('rms'),
                    'gap': properties.get('gap'),'magtype': properties.get('magType'),
                    'type': properties.get('type'),'title': properties.get('title')
                }
                rows.append(row)
    df = pd.DataFrame(rows)
    logger.info(f"Created DataFrame with {len(df)} records")
    return df

def check_existing_ids(df):
    # upper wala call kiya
    global existing_feature_ids
    # if empty then ignore
    if df.empty:
        return []
    # or else ietrate through feature_id and check new value is unique 
    return [fid for fid in df['feature_id'] if fid in existing_feature_ids]

def get_data(last_endtime, is_initial_fetch=True):
    global existing_feature_ids
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    limit = 5000
    all_dfs = []

    # Set time window to one day
    start_time = last_endtime.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1) - timedelta(seconds=1)  # End of the day (23:59:59)
    current_time = datetime.utcnow()

    if end_time > current_time: # if 2nd pointer goes out of bound bring it back
        end_time = current_time

    offset = 1
    while True:
        logger.info(f"Fetching data for {start_time.strftime('%Y-%m-%d')} with offset {offset}")
        try:
            response = requests.get(url, params={
                'format': 'geojson',
                'eventtype': 'earthquake',
                'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
                'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
                'minmagnitude': 2,'maxmagnitude': 10,
                'limit': limit,'offset': offset,'orderby': 'time-asc'
            }, timeout=30)

            response.raise_for_status()
            data = response.json()
            features = data.get('features', [])
            logger.info(f"Fetched {len(features)} events for {start_time.strftime('%Y-%m-%d')} with offset {offset}")

            df = process_to_df(data)
            if not df.empty:
                existing_ids = check_existing_ids(df)
                unique_df = df[~df['feature_id'].isin(existing_ids)]
                existing_feature_ids.update(unique_df['feature_id'].tolist())
                all_dfs.append(unique_df)
                logger.info(f"Filtered to {len(unique_df)} unique records for this batch")

            # Check for pagination
            total_count = data.get('metadata', {}).get('count', 0)
            if len(features) < limit:
                break  # No data for this day
            offset += limit

        except requests.RequestException as e:
            logger.error(f"API request failed for {start_time.strftime('%Y-%m-%d')}: {e}")
            break

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total unique records for {start_time.strftime('%Y-%m-%d')}: {len(final_df)}")
        return final_df, end_time + timedelta(seconds=1), False
    else:
        logger.info(f"No new data for {start_time.strftime('%Y-%m-%d')}")
        return pd.DataFrame(), end_time + timedelta(seconds=1), False
#kafka prodcuer codde conf only
def create_kafka_producer():
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'client.id': 'earthquake-producer',
        'acks': 'all',
        'retries': 5,
        'delivery.timeout.ms': 10000
    }
    return Producer(conf)
# check why kafka message not going, abhi topics main dekh raha hai.
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')

def store_in_postgres(df):
    if df.empty:
        logger.info("No records to insert into PostgreSQL")
        # return nahi toh gives error and stops 
        return
    db_connection_str = 'postgresql://root:root@localhost:5432/db'
    engine = create_engine(db_connection_str)
    try:
        df.to_sql('mytable', engine, if_exists='append', index=False)
        logger.info(f"Inserted {len(df)} records into PostgreSQL")
    except Exception as e: # why is new data not showing? (fixd)
        logger.error(f"PostgreSQL insert error: {e}")

def produce_to_kafka(df, producer):
    if df.empty:
        logger.info("No records to send to Kafka")
        return
    # want to put my rows, so go through each row and convert row to json and feed kafka
    for _, row in df.iterrows():
        message = row.to_json()
        producer.produce(
            topic='earthquake_data',
            value=message,
            key=str(row['feature_id']),
            callback=delivery_report
        )
    # close() wouldn't stop for messages to be sent
    producer.flush()
    logger.info(f"Produced {len(df)} records to Kafka topic earthquake_data")

def main():
    producer = create_kafka_producer() # got the conf
    last_endtime = datetime.strptime('2000-01-01T00:00:00+00:00', '%Y-%m-%dT%H:%M:%S+00:00')
    is_initial_fetch = True
    while True:
        df, current_endtime, is_initial_fetch = get_data(last_endtime, is_initial_fetch)# unwrapped here
        if not df.empty:
            store_in_postgres(df)
            produce_to_kafka(df, producer) # producer is the conf
        last_endtime = current_endtime
        if is_initial_fetch:
            sleep(2)  # Short break
        else:
            logger.info(f"Waiting 30 seconds for next poll at {datetime.utcnow()}")
            sleep(3)  # Poll every 1/2 minutes for real-time data

if __name__ == '__main__':
    main()