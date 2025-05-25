from kafka import KafkaConsumer, KafkaProducer
import psycopg2 # For future Postgres integration
import os, json

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_DB = os.getenv("POSTGRES_DB", "openxeroth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ox")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "oxpass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

consumer = KafkaConsumer(
    "detections", bootstrap_servers=KAFKA_BOOTSTRAP, group_id="tracker"
)
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

# Example of connecting to Postgres, though not used in this stub
# try:
#     conn = psycopg2.connect(
#         dbname=POSTGRES_DB,
#         user=POSTGRES_USER,
#         password=POSTGRES_PASSWORD,
#         host=POSTGRES_HOST
#     )
#     cur = conn.cursor()
#     print("Tracking service connected to PostgreSQL!")
#     cur.close()
#     conn.close()
# except Exception as e:
#     print(f"Tracking service: Error connecting to PostgreSQL: {e}")


for msg in consumer:
    detections = json.loads(msg.value)
    # TODO: replace with real tracking logic
    tracks = [{"track_id": 1, "bbox": det["bbox"], "class": det["class"]} for det in detections]
    producer.send("tracks", json.dumps(tracks).encode("utf-8"))
    print(f"Tracking service processed detections: {detections}, sent tracks: {tracks}")
