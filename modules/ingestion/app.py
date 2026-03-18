from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_location():
    data = {
        "user_id": 1,
        "latitude": 40.4168,
        "longitude": -3.7038
    }

    producer.send('locations', data)
    print("Sent:", data)

if __name__ == "__main__":
    while True:
        send_location()
        time.sleep(5)