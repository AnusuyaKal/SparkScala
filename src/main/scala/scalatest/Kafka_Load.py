from confluent_kafka import Producer, Consumer
import json
import time
import random
import happybase

# Kafka configurations
kafka_config = {
    'bootstrap.servers': '<kafka_broker>',
    'group.id': 'my_group',
}

# HBase configurations
hbase_host = '<hbase_host>'
hbase_table_name = 'my_table'

# Function to produce data to Kafka topic
def produce_data():
    producer = Producer(kafka_config)
    while True:
        records = [{'value': {'id': i, 'data': random.randint(1, 100)}} for i in range(1000)]
        for record in records:
            producer.produce('<kafka_topic>', json.dumps(record))
        producer.flush()
        time.sleep(5)

# Function to consume data from Kafka topic and store in HBase
def consume_and_store_data():
    connection = happybase.Connection(hbase_host)
    table = connection.table(hbase_table_name)
    consumer = Consumer(kafka_config)
    consumer.subscribe(['<kafka_topic>'])
    while True:
        msg = consumer.poll(timeout=10)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            row_key = str(data['id'])
            table.put(row_key, {'cf:data': str(data['data'])})
            print("Data stored in HBase:", data)
        except Exception as e:
            print("Error processing message:", e)

# Run producer and consumer concurrently
if __name__ == "__main__":
    import threading
    producer_thread = threading.Thread(target=produce_data)
    consumer_thread = threading.Thread(target=consume_and_store_data)
    producer_thread.start()
    consumer_thread.start()
