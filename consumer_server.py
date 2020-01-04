import logging

import confluent_kafka
from confluent_kafka import Consumer
import asyncio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

BROKER_PROPERTIES = {
    'bootstrap.servers': "PLAINTEXT://localhost:9092",
    'group.id': 'client_consumer_jh',
    'auto.offset.reset': 'earliest',
}

TOPIC_NAME = 'org.sf.crime.stats'

class KafkaConsumer:
    def __init__(self, topic_name, poll_timeout=1.0):
        self.topic_name = topic_name
        self.poll_timeout = poll_timeout

        # Create the Consumer
        self.consumer = Consumer(BROKER_PROPERTIES)

        # subscribe to the topic
        self.consumer.subscribe(topics=[self.topic_name])

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            message = self.consumer.poll(self.poll_timeout)
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"""\nCONSUMED MESSAGE FROM TOPIC {self.topic_name}:\n{message.value()}\n""")

            await asyncio.sleep(1.0)
            

    def close(self):
        """Cleans up any open kafka consumers"""
        if self.consumer is not None:    
            self.consumer.close()
            logger.info("consumer closed successfully")

def run_consumer_server():
    kc = KafkaConsumer(topic_name=TOPIC_NAME)
    try:
        asyncio.run(kc.consume())
    except KeyboardInterrupt as e:
        logger.info("shutting down consumer")
        kc.close()

if __name__ == "__main__":
    run_consumer_server()