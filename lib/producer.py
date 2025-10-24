from confluent_kafka import Producer
import os
import json
import uuid
import time

print("test")
# print("test2. were changes detected?")

# while True:
#     print("test")
#     time.sleep(2)

# configure where the producer can talk to the Kafka server
# bootstrap.servers is the initial host starting point

kafka_broker = os.environ.get('KAFKA_BROKER') # 'localhost:9092'
print(kafka_broker)
# Check if the environment variable is set
if not kafka_broker:
    raise ValueError("KAFKA_BROKER environment variable is not set.")

producer_config = {'bootstrap.servers': kafka_broker}

producer = Producer(producer_config)

# callback after producing an order
def delivery_report(err, msg):
    if err:
        print(f"Delivery report error: {err}")
    else:
        # message returned in bytes
        print(f"Delivery {msg.value().decode('utf-8')}")

        print(f"Delivered to topic: {msg.topic()}, partition {msg.partition()}, at offset {msg.offset()}")

        # print the object
        print(dir(msg))

# example order
order = {
    "order_id": str(uuid.uuid4()),
    "user": "test_user",
    "item": "calculator",
    "quantity": 2 
}

# need to convert json to Kafka format, which is bytes
value = json.dumps(order).encode("utf-8")

# save to the orders topic. If it doesn't exist yet, kafka creates it
producer.produce(
    topic="orders", 
    value=value,
    callback=delivery_report
    )

# clean the kafka message buffer. 
# Especially important in case of system failure
# because it will guarantee messages in the buffer go though
producer.flush()