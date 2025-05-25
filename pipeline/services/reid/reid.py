from kafka import KafkaConsumer, KafkaProducer
import psycopg2
import os, json

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_DB = os.getenv("POSTGRES_DB", "openxeroth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ox")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "oxpass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

consumer = KafkaConsumer(
    "tracks", bootstrap_servers=KAFKA_BOOTSTRAP, group_id="reid"
)
# Optional: Producer for sending re-identification results
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

try:
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST
    )
    cur = conn.cursor()
    print("ReID service connected to PostgreSQL!")
    # Placeholder: maybe create a table if it doesn't exist
    # cur.execute("""
    # CREATE TABLE IF NOT EXISTS identifications (
    #     track_id INTEGER PRIMARY KEY,
    #     identity VARCHAR(255)
    # );
    # """)
    # conn.commit()
    cur.close()
    # conn.close() # Keep connection open for the loop or manage connections per message
except Exception as e:
    print(f"ReID service: Error connecting to PostgreSQL: {e}")
    conn = None # Ensure conn is None if connection failed

for msg in consumer:
    tracks = json.loads(msg.value)
    # TODO: replace with real ReID logic (feature extraction, matching)
    # and database interaction
    
    identifications = []
    if conn: # Only proceed if DB connection was successful
        try:
            cur = conn.cursor()
            for track in tracks:
                # Example: assign a placeholder identity
                identity = f"person_{track['track_id']}"
                print(f"ReID service: Processing track {track['track_id']}, assigned identity {identity}")
                
                # Example: Insert or update identity in Postgres
                # cur.execute(
                #     "INSERT INTO identifications (track_id, identity) VALUES (%s, %s) ON CONFLICT (track_id) DO UPDATE SET identity = %s",
                #     (track['track_id'], identity, identity)
                # )
                
                identifications.append({"track_id": track["track_id"], "identity": identity})
            # conn.commit() # Commit after processing a batch of messages or individual messages
            cur.close()
        except Exception as e:
            print(f"ReID service: Error during database operation: {e}")
            # Optionally, re-attempt connection or handle error
    
    # Optional: Produce identifications to Kafka
    if identifications:
        producer.send("identifications", json.dumps(identifications).encode("utf-8"))
        print(f"ReID service sent identifications: {identifications}")

# Close the connection when the consumer is done (e.g., on script termination)
if conn:
    conn.close()
    print("ReID service disconnected from PostgreSQL.")
