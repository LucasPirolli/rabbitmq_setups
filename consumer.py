import pika
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


class RabbitmqConsumer:
    def __init__(self, callback):
        self.__host = RABBITMQ_HOST
        self.__port = RABBITMQ_PORT
        self.__username = RABBITMQ_USER
        self.__password = RABBITMQ_PASS
        self.__queue = RABBITMQ_QUEUE
        self.__callback = callback
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

        # Cria um canal e declara a fila
        channel = pika.BlockingConnection(connection_parameters).channel()
        channel.queue_declare(queue=self.__queue, durable=True)

        # Começa a consumir mensagens
        channel.basic_consume(
            queue=self.__queue,
            on_message_callback=self.__callback,
            auto_ack=True
        )

        return channel

    def start_consuming(self):
        print(f"Aguardando mensagens em '{self.__queue}'...")
        self.__channel.start_consuming()

def callback(ch, method, properties, body):
    print("Corpo:", body)

rabbitmq_consumer = RabbitmqConsumer(callback)
rabbitmq_consumer.start_consuming()