import json
import time
from typing import Dict, Any, List

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch


# -----------------------------
# Configuration
# -----------------------------
KAFKA_TOPIC = "transactions"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

POSTGRES_CONFIG = {
    "dbname": "compliance",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
}

BATCH_SIZE = 100          # reduces DB I/O overhead
DURATION_THRESHOLD = 5000 # ms
HIGH_RISK_ACTIONS = {"login_failed", "password_reset", "suspicious_transfer"}


# -----------------------------
# Anomaly Detection Logic
# -----------------------------
class AnomalyDetector:
    """
    Optimized rule-based anomaly detector.
    Logic preserved, execution optimized.
    """

    __slots__ = ("duration_threshold", "high_risk_actions")

    def __init__(self):
        self.duration_threshold = DURATION_THRESHOLD
        self.high_risk_actions = HIGH_RISK_ACTIONS

    def is_anomalous(self, event: Dict[str, Any]) -> bool:
        """
        Fast rule evaluation with early exits.
        """
        if event.get("is_high_risk"):
            return True

        if event.get("action") in self.high_risk_actions:
            return True

        if event.get("duration_ms", 0) > self.duration_threshold:
            return True

        return False


# -----------------------------
# Database Writer
# -----------------------------
class PostgresWriter:
    """
    Optimized batch writer to PostgreSQL.
    """

    def __init__(self):
        self.conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.cursor = self.conn.cursor()

    def insert_anomalies(self, records: List[Dict[str, Any]]):
        if not records:
            return

        query = """
            INSERT INTO anomalies (
                user_id,
                action,
                page,
                duration_ms,
                device_type,
                timestamp,
                referrer,
                session_id
            )
            VALUES (
                %(user_id)s,
                %(action)s,
                %(page)s,
                %(duration_ms)s,
                %(device_type)s,
                %(timestamp)s,
                %(referrer)s,
                %(session_id)s
            );
        """

        execute_batch(self.cursor, query, records, page_size=BATCH_SIZE)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


# -----------------------------
# Kafka Consumer Loop
# -----------------------------
def run_detector():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    detector = AnomalyDetector()
    writer = PostgresWriter()

    anomaly_buffer = []
    start_time = time.time()

    try:
        for message in consumer:
            event = message.value

            if detector.is_anomalous(event):
                anomaly_buffer.append(event)

            if len(anomaly_buffer) >= BATCH_SIZE:
                writer.insert_anomalies(anomaly_buffer)
                anomaly_buffer.clear()

    except KeyboardInterrupt:
        pass

    finally:
        # Flush remaining anomalies
        if anomaly_buffer:
            writer.insert_anomalies(anomaly_buffer)

        writer.close()
        consumer.close()

        elapsed = time.time() - start_time
        print(f"Detector stopped. Runtime: {elapsed:.2f}s")


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run_detector()