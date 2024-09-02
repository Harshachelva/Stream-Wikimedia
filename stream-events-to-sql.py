import json
import argparse
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import mysql.connector

# Function to create a Kafka producer
def create_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)

# Function to create a Kafka consumer
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

# Function to construct event data
def construct_event(event_data, user_types):
    try:
        event_data['namespace'] = namespace_dict[event_data['namespace']]
    except KeyError:
        event_data['namespace'] = 'unknown'

    user_type = user_types[event_data['bot']]

    event = {
        "id": event_data['id'],
        "domain": event_data['meta']['domain'],
        "namespace": event_data['namespace'],
        "title": event_data['title'],
        "timestamp": event_data['meta']['dt'],
        "user_name": event_data['user'],
        "user_type": user_type,
        "old_length": event_data['length']['old'],
        "new_length": event_data['length']['new']
    }

    return event

# Function to initialize namespaces
def init_namespaces():
    namespace_dict = {
        -2: 'Media', -1: 'Special', 0: 'main namespace',
        1: 'Talk', 2: 'User', 3: 'User Talk',
        4: 'Wikipedia', 5: 'Wikipedia Talk', 6: 'File', 7: 'File Talk',
        8: 'MediaWiki', 9: 'MediaWiki Talk', 10: 'Template', 11: 'Template Talk',
        12: 'Help', 13: 'Help Talk', 14: 'Category', 15: 'Category Talk',
        100: 'Portal', 101: 'Portal Talk', 108: 'Book', 109: 'Book Talk',
        118: 'Draft', 119: 'Draft Talk', 446: 'Education Program', 447: 'Education Program Talk',
        710: 'TimedText', 711: 'TimedText Talk', 828: 'Module', 829: 'Module Talk',
        2300: 'Gadget', 2301: 'Gadget Talk', 2302: 'Gadget definition', 2303: 'Gadget definition Talk'
    }
    return namespace_dict

# Function to parse command line arguments
def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia-events', help='Destination topic name', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int, default=1000)
    parser.add_argument('--mysql_host', default='localhost', help='MySQL host', type=str)
    parser.add_argument('--mysql_user', default='harsha', help='MySQL username', type=str)
    parser.add_argument('--mysql_password', default='chelva', help='MySQL password', type=str)
    parser.add_argument('--mysql_db', default='wikimedia', help='MySQL database name', type=str)

    return parser.parse_args()

# Function to connect to MySQL
def connect_mysql(host, user, password, database):
    try:
        cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
        cursor = cnx.cursor()
        print('Connected to MySQL database!')
        return cnx, cursor
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

# Function to insert data into MySQL
def insert_into_mysql(cursor, cnx, event):
    add_event = ("INSERT INTO events "
                 "(id, domain, namespace, title, timestamp, user_name, user_type, old_length, new_length) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    data_event = (
        event['id'], event['domain'], event['namespace'], event['title'],
        event['timestamp'], event['user_name'], event['user_type'],
        event['old_length'], event['new_length']
    )
    cursor.execute(add_event, data_event)
    cnx.commit()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_command_line_arguments()

    # Init producer
    producer = create_kafka_producer(args.bootstrap_server)

    # Init dictionary of namespaces
    namespace_dict = init_namespaces()

    # Used to parse user type
    user_types = {True: 'bot', False: 'human'}

    # Consume websocket
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    print('Messages are being published to Kafka topic')
    messages_count = 0
    
    # Produce to Kafka
    for event in EventSource(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                if event_data["type"] == 'edit':
                    event_to_send = construct_event(event_data, user_types)
                    producer.send(args.topic_name, value=event_to_send)
                    messages_count += 1
        
        if messages_count >= args.events_to_produce:
            print(f'Producer will be killed as {args.events_to_produce} events were produced')

    # Init MySQL connection
    cnx, cursor = connect_mysql(args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_db)

    # Consume from Kafka and insert into MySQL
    consumer = create_kafka_consumer(args.bootstrap_server, args.topic_name)
    
    for message in consumer:
        event = message.value
        insert_into_mysql(cursor, cnx, event)
    
    # Close MySQL connection
    cursor.close()
    cnx.close()
