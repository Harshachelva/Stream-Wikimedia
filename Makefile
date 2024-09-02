.PHONY: all create-topic list-topics delete-topic start-producer start-consumer

# Variables
KAFKA_CONTAINER = kafka-container
TOPIC_NAME = wikimedia-data
PARTITIONS = 1
REPLICATION_FACTOR = 1

# Default target
all: create-topic

# Target to create Kafka topic
create-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --create --topic $(TOPIC_NAME) --bootstrap-server localhost:9092 --partitions $(PARTITIONS) --replication-factor $(REPLICATION_FACTOR)
	echo "Kafka topic $(TOPIC_NAME) created with $(PARTITIONS) partitions and $(REPLICATION_FACTOR) replication factor."

# Target to list Kafka topics
list-topics:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --list --bootstrap-server localhost:9092

# Target to delete Kafka topic
delete-topic:
	@if [ -z "$(topic)" ]; then \
		echo "Error: No topic name provided. Use 'make delete-topic topic=<topic_name>'."; \
		exit 1; \
	fi
	docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --delete --topic $(topic) --bootstrap-server localhost:9092
	echo "Kafka topic $(topic) deleted."

view-messages:
	docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikipedia-events --from-beginning


# Target to start Kafka producer
start-producer:
	python producer.py

# Target to start Kafka consumer
start-consumer:
	python consumer.py
