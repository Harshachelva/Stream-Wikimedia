from kafka import KafkaProducer
from sseclient import SSEClient

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Set up Wikimedia stream
url = 'https://stream.wikimedia.org/v2/stream/recentchange'
client = SSEClient(url)

for event in client:
    producer.send('wikimedia-recent-changes', value=event.data.encode('utf-8'))
    producer.flush()
