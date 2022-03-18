import requests, time, json, schedule
from datetime import datetime
from kafka import KafkaProducer

from config import AV_API_KEY, KAFKA_HOST, KAFKA_PORT, KAFKA_TOPICS
from scripts.eth_hourly import eth_hourly

'''
ETH-Hourly message format:
{
    "format": "hour", "initial" (hour is a single point, initial is the full data used for training)
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

load_initial_eth_hourly = False

if __name__ == "__main__":
    if load_initial_eth_hourly:
        producer.send(
            KAFKA_TOPICS['eth-hourly'], 
            {
                "format": "hour",
                "data": eth_hourly(AV_API_KEY)
            }
        )

    schedule.every(1).hours.do(
        producer.send, 
        topic = KAFKA_TOPICS['eth-hourly'], 
        value = {
            "format": "hour",
            "data": eth_hourly(AV_API_KEY)
        }
    )
