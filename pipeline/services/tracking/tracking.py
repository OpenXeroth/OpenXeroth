from kafka import KafkaConsumer, KafkaProducer
import psycopg2 # For future Postgres integration
import os
import json

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = "detections"
OUTPUT_TOPIC = "tracks"
CONSUMER_GROUP_ID = "tracker"

# PostgreSQL Configuration (optional, for future use)
POSTGRES_DB = os.getenv("POSTGRES_DB", "openxeroth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ox")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "oxpass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

def main():
    print(f"Tracking service: Starting consumer for topic '{INPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}' with group ID '{CONSUMER_GROUP_ID}'")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        # auto_offset_reset='earliest', # Optional
        # consumer_timeout_ms=1000 # Optional
    )

    print(f"Tracking service: Starting producer for topic '{OUTPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}'")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Example of connecting to Postgres, though not actively used in this stub's main loop
    # pg_conn = None
    # try:
    #     print(f"Tracking service: Attempting to connect to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_DB}")
    #     pg_conn = psycopg2.connect(
    #         dbname=POSTGRES_DB,
    #         user=POSTGRES_USER,
    #         password=POSTGRES_PASSWORD,
    #         host=POSTGRES_HOST
    #     )
    #     cur = pg_conn.cursor()
    #     print("Tracking service: Successfully connected to PostgreSQL!")
    #     cur.close()
    #     # pg_conn.close() # Decide if connection should be persistent or per-message
    # except Exception as e:
    #     print(f"Tracking service: Error connecting to PostgreSQL: {e}")

    print("Tracking service: Waiting for messages...")
    for msg in consumer:
        try:
            detections = msg.value # Already deserialized by KafkaConsumer
            print(f"Tracking service: Received detections: {detections}")
            
            # TODO: replace with real tracking logic
            tracks = [{"track_id": 1, "bbox": det["bbox"], "class": det["class"]} for det in detections]
            print(f"Tracking service: Sending tracks: {tracks}")
            
            producer.send(OUTPUT_TOPIC, tracks)
            producer.flush() # Ensure messages are sent
        # It's good practice to catch specific exceptions, e.g., json.JSONDecodeError
        # if deserialization was done manually in the loop.
        # Since it's in KafkaConsumer, it might raise KafkaError for deserialization issues.
        except Exception as e:
            print(f"Tracking service: An error occurred: {e}")
            # Consider how to handle errors: skip message, retry, log, etc.
            
    # if pg_conn:
    #     pg_conn.close()
    #     print("Tracking service: PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
