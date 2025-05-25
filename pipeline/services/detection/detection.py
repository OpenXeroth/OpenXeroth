from kafka import KafkaConsumer, KafkaProducer
import os
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = "frames"
OUTPUT_TOPIC = "detections"
CONSUMER_GROUP_ID = "detector"

def main():
    print(f"Detection service: Starting consumer for topic '{INPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}' with group ID '{CONSUMER_GROUP_ID}'")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        # auto_offset_reset='earliest', # Optional: depending on desired behavior
        # consumer_timeout_ms=1000 # Optional: to prevent blocking indefinitely if no messages
    )
    
    print(f"Detection service: Starting producer for topic '{OUTPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}'")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Detection service: Waiting for messages...")
    for msg in consumer:
        try:
            frame = json.loads(msg.value.decode('utf-8'))
            print(f"Detection service: Received frame: {frame}")
            
            # TODO: replace with real model inference
            detections = [{"bbox": [0,0,100,100], "confidence": 0.9, "class": "lion"}]
            print(f"Detection service: Sending detections: {detections}")
            
            producer.send(OUTPUT_TOPIC, detections)
            producer.flush() # Ensure messages are sent
        except json.JSONDecodeError as e:
            print(f"Detection service: Error decoding JSON: {e}. Message: {msg.value}")
        except Exception as e:
            print(f"Detection service: An error occurred: {e}")

if __name__ == "__main__":
    main()
