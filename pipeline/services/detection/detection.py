from kafka import KafkaConsumer, KafkaProducer
import os, json

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
consumer = KafkaConsumer(
    "frames", bootstrap_servers=KAFKA_BOOTSTRAP, group_id="detector"
)
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

for msg in consumer:
    frame = json.loads(msg.value)
    # TODO: replace with real model inference
    detections = [{"bbox": [0,0,100,100], "confidence": 0.9, "class": "lion"}]
    producer.send("detections", json.dumps(detections).encode("utf-8"))
