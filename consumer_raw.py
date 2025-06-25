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


def callback(ch, method, properties, body):
    print("Corpo:", body)


# Conexão com o RabbitMQ
connection_parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=pika.PlainCredentials(
        username=RABBITMQ_USER,
        password=RABBITMQ_PASS
    )
)

# Cria um canal e declara a fila
channel = pika.BlockingConnection(connection_parameters).channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# Começa a consumir mensagens
channel.basic_consume(
    queue=RABBITMQ_QUEUE,
    on_message_callback=callback,
    auto_ack=True
)

print(f"Aguardando mensagens em '{RABBITMQ_QUEUE}'...")
channel.start_consuming()
