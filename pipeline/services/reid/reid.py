from kafka import KafkaConsumer, KafkaProducer
import psycopg2
import os
import json

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = "tracks"
OUTPUT_TOPIC = "identifications" # Optional: if ReID produces to Kafka
CONSUMER_GROUP_ID = "reid"

# PostgreSQL Configuration
POSTGRES_DB = os.getenv("POSTGRES_DB", "openxeroth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ox")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "oxpass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

def main():
    print(f"ReID service: Starting consumer for topic '{INPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}' with group ID '{CONSUMER_GROUP_ID}'")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Optional: Producer for sending re-identification results
    print(f"ReID service: Starting producer for topic '{OUTPUT_TOPIC}' on servers '{KAFKA_BOOTSTRAP_SERVERS}'")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    pg_conn = None
    try:
        print(f"ReID service: Attempting to connect to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_DB}")
        pg_conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST
        )
        print("ReID service: Successfully connected to PostgreSQL!")
        # Placeholder: maybe create a table if it doesn't exist
        # with pg_conn.cursor() as cur:
        #     cur.execute("""
        #     CREATE TABLE IF NOT EXISTS identifications (
        #         track_id INTEGER PRIMARY KEY,
        #         identity VARCHAR(255)
        #     );
        #     """)
        #     pg_conn.commit()
    except Exception as e:
        print(f"ReID service: Error connecting to PostgreSQL: {e}")
        # pg_conn will remain None

    print("ReID service: Waiting for messages...")
    for msg in consumer:
        try:
            tracks = msg.value # Already deserialized
            print(f"ReID service: Received tracks: {tracks}")
            
            identifications = []
            if pg_conn: # Only proceed if DB connection was successful
                try:
                    with pg_conn.cursor() as cur:
                        for track in tracks:
                            identity = f"person_{track['track_id']}" # Example ReID
                            print(f"ReID service: Processing track {track['track_id']}, assigned identity {identity}")
                            
                            # Example: Insert or update identity in Postgres
                            # cur.execute(
                            #     "INSERT INTO identifications (track_id, identity) VALUES (%s, %s) ON CONFLICT (track_id) DO UPDATE SET identity = %s",
                            #     (track['track_id'], identity, identity)
                            # )
                            identifications.append({"track_id": track["track_id"], "identity": identity})
                        # pg_conn.commit() # Commit after processing a batch
                except Exception as e:
                    print(f"ReID service: Error during database operation: {e}")
                    # If DB operation fails, we might still want to output something or handle it
                    # For now, it will fall through and potentially send an empty or partially filled 'identifications' list
            else:
                print("ReID service: No PostgreSQL connection, skipping database interaction.")
                # Fallback if no DB: just pass through or generate dummy data
                for track in tracks: # Ensure tracks is defined even if pg_conn is None early on
                    identifications.append({"track_id": track["track_id"], "identity": "unknown_due_to_db_error"})

            # Optional: Produce identifications to Kafka
            if identifications:
                producer.send(OUTPUT_TOPIC, identifications)
                producer.flush()
                print(f"ReID service: Sent identifications: {identifications}")
        except Exception as e:
            print(f"ReID service: An error occurred in message loop: {e}")

    if pg_conn:
        pg_conn.close()
        print("ReID service: PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
