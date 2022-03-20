
# exec into kafka container
docker exec -it kafka /bin/sh

# cd into kafka bin files
cd opt/kafka_2.13-2.8.1/bin

# list all kafka topics
kafka-topics.sh --zookeeper zookeeper:2181 --list

# create new Kafka topic
kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic ethhourly # replace ethhourly with desired topic name

# use console consumer to read messages from topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ethhourly --from-beginning
