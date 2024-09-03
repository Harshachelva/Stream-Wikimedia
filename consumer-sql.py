import json
from kafka import KafkaConsumer
import mysql.connector
from kafka.errors import NoBrokersAvailable

def create_kafka_consumer(bootstrap_server, topic_name):
    try:
        consumer = KafkaConsumer(topic_name,
                                 bootstrap_servers=bootstrap_server,
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        print('Kafka consumer connected!')
        return consumer
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

def connect_mysql(host, user, password, database):
    try:
        cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
        cursor = cnx.cursor()
        print('Connected to MySQL database!')
        return cnx, cursor
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

def insert_batch_into_mysql(cursor, cnx, events):
    add_event = ("INSERT INTO events "
                 "(id, domain, namespace, title, timestamp, user_name, user_type, new_length) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

    data_events = [(event['id'], event['domain'], event['namespace'], event['title'],
                    event['timestamp'], event['user_name'], event['user_type'],
                    event['new_length']) for event in events]
    
    cursor.executemany(add_event, data_events)
    cnx.commit()

if __name__ == "__main__":
    bootstrap_server = 'localhost:9092'
    topic_name = 'wikipedia-events'
    mysql_host = 'localhost'
    mysql_user = 'root'
    mysql_password = 'rootpassword'
    mysql_db = 'wikimedia'

    cnx, cursor = connect_mysql(mysql_host, mysql_user, mysql_password, mysql_db)
    consumer = create_kafka_consumer(bootstrap_server, topic_name)

    batch_size = 10
    message_batch = []

    for message in consumer:
        # Assuming the message value is a list of events
        events = message.value

        if isinstance(events, list):
            message_batch.extend(events)

            if len(message_batch) >= batch_size:
                insert_batch_into_mysql(cursor, cnx, message_batch[:batch_size])
                message_batch = message_batch[batch_size:]

    if message_batch:
        insert_batch_into_mysql(cursor, cnx, message_batch)

    cursor.close()
    cnx.close()
    print('Consumer closed and database connection closed.')
