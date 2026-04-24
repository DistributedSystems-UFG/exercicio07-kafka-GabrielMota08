from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in = sys.argv[1]
    topic_out = sys.argv[2]
except:
    print('Uso: python3 consumer_processor <topic_in> <topic_out>')
    exit(1)

consumer = KafkaConsumer(
    topic_in,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

print(f'Processing from {topic_in} → {topic_out}')

for msg in consumer:
    original = msg.value.decode()

    processed = ' '.join(original)

    print(f'Original:  {original}')
    print(f'Processado: {processed}')
    print('---')

    producer.send(topic_out, value=processed.encode())

producer.flush()