from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'locations',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Waiting for messages...")

for message in consumer:
    data = message.value
    print("Received:", data)

    # Aquí luego puedes guardar en PostgreSQL