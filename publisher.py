from typing import Dict
import pika
import json
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Lê as variáveis
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "data_queue")
RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "data_exchange")


class RabbitmqPublisher:
    def __init__(self):
        self.__host = RABBITMQ_HOST
        self.__port = RABBITMQ_PORT
        self.__username = RABBITMQ_USER
        self.__password = RABBITMQ_PASS
        self.__exchange = RABBIT_EXCHANGE
        self.__routing_key = ""
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()
        return channel

    def send_message(self, body: Dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routing_key,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
    

rabbitmq_publisher = RabbitmqPublisher()
rabbitmq_publisher.send_message({"message": "Mensagem de teste"})