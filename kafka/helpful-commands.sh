
# create new Kafka topic
kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 13 \
    --topic ethhourly
