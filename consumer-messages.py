from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'wikimedia-data',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_offset_reset='earliest'
)
print(consumer)
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
