# A Consumer microservice that tracks orders from the Broker

from confluent_kafka import Consumer
import os
import json

kafka_broker = os.environ.get('KAFKA_BROKER') # 'localhost:9092'
print(kafka_broker)
# Check if the environment variable is set
if not kafka_broker:
    raise ValueError("KAFKA_BROKER environment variable is not set.")

consumer_config = {
    'bootstrap.servers': kafka_broker,
    # identifies the consumer group a consumer belongs to
    'group.id': 'order-tracker',
    # tells the consumer what to do if it can't find which message it last read
    # occurs if no initial offset or current offset does not exist anymore
    'auto.offset.reset': "earliest"
                   }

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])
print("Consumer is running and subscribed to orders topic")

try:
    while True:
        # poll() asks the broker for new messages in the subscribed topics
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(order)
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")

except KeyboardInterrupt:
    # handle graceful stopping of consumer
    # helps avoid consumer leak, where messages are treated as read from the broker, 
    # but the consumer did not properly handle them
    print("Stopping consumer") 

finally:
    # ensure that resources are properly released
    # i.e. offsets are committed, consumer's partitions assignments revoked
    consumer.close()