import socket
from pyais import decode
import time
import json
import os
from logger import define_logger
from dotenv import load_dotenv
from pyais.stream import IterMessages
from producer_consumer import producer, consumer
load_dotenv()

AIS_HOST = os.getenv("AIS_HOST")
AIS_PORT = os.getenv("AIS_PORT")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logger = define_logger()

class AISReceiver():
    def __init__(self, ais_host, ais_port, kafka_bootstrap, kafka_topic, logger):
        self.ais_host = ais_host
        self.ais_port = ais_port
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.logger = logger
    
    @staticmethod
    def send_message_by_producer(producer, topic, data):
        print("---------"*100)
        logger.debug(f"🚀 Отправляю данные: {data}")
        producer.produce(topic, value=data)
        producer.flush()
        
    @staticmethod
    def pull_message_by_consumer(consumer):
        print("---------"*100)
        msg = consumer.poll(timeout=1) 
        if msg is None:
            logger.info(f"Пока не получил данные")
        else:
            mess = msg.value().decode("utf-8")
            logger.info(f"📩 Получаю данные: {mess}")
            
            messages = [line.encode() for line in mess.split() if line]

            with IterMessages(messages) as s:
                for msg in s:
                    if msg.tag_block is not None:
                        msg.tag_block.init()
                    logger.info(f"📩 Их декодированная версия: {msg.decode()}")

        return msg

    def receive_data(self, producer, consumer, topic):
        buffer = []
        consumer.subscribe([topic])
        try:
            with socket.create_connection((self.ais_host, self.ais_port), timeout=5) as sock:
                logger.info("✅ Подключено к AIS-стриму")
                while True:
                    data = sock.recv(1024).decode("utf-8")
                    buffer.append(data)
                    # Отправка в топик
                    self.send_message_by_producer(producer, topic, data)
                    # Получение из топика
                    message = self.pull_message_by_consumer(consumer)
                    
        except Exception as e:
            logger.error(f"❌ Обрыв связи с AIS-стримом: {e}")

if __name__ == "__main__":
    receiver = AISReceiver(AIS_HOST, AIS_PORT, KAFKA_BOOTSTRAP, KAFKA_TOPIC, logger)
    receiver.receive_data(producer, consumer, "KYSTVERKET")

