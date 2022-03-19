import requests, time, json
from datetime import datetime
import schedule
from kafka import KafkaProducer

from config import AV_API_KEY, KAFKA_HOST, KAFKA_PORT, KAFKA_TOPICS
from scripts.eth_hourly import eth_hourly, eth_hourly_full

'''
ETH-Hourly message format:
{
    "format": "hour", "train" (hour is a single point, initial is the full data used for training)
    "data": <Time-series message>
}

Time-series message format:
{
    "datetime", "open", "high", "low", "close", "volume"
}
'''

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
    value_serializer=serializer
)

def producer_send_logging(topic, value):
    producer.send(topic, value)
    print("sending message to producer: " + str(value))

schedule.every(1).minutes.do(
    producer_send_logging, 
    topic = KAFKA_TOPICS['eth-hourly'], 
    value = {
        "format": "hour",
        "data": eth_hourly(AV_API_KEY)
    }
)

# schedule.every().hour.do(
#     producer_send_logging, 
#     topic = KAFKA_TOPICS['eth-hourly'], 
#     value = {
#         "format": "hour",
#         "data": eth_hourly(AV_API_KEY)
#     }
# )

load_initial_eth_hourly = False

print("Starting -------------")

if __name__ == "__main__":
    if load_initial_eth_hourly:
        message = eth_hourly_full(AV_API_KEY)
        producer.send(
            KAFKA_TOPICS['eth-hourly'], 
            {
                "format": "train",
                "data": message
            }
        )
        print("sent initial training data to kafka: " + str(message))

    while True:
        schedule.run_pending()
