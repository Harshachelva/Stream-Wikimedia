from kafka import KafkaConsumer
import mysql.connector

# Set up Kafka consumer
consumer = KafkaConsumer(
    'wikimedia-recent-changes',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mygroup'
)

# Connect to MySQL
conn = mysql.connector.connect(
    user='yourusername',
    password='yourpassword',
    host='localhost',
    database='wikimedia'
)
cursor = conn.cursor()

for message in consumer:
    data = message.value.decode('utf-8')
    query = "INSERT INTO changes (data) VALUES (%s)"
    cursor.execute(query, (data,))
    conn.commit()

cursor.close()
conn.close()
