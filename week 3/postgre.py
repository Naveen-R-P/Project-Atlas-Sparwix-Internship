import psycopg2
from psycopg2.extras import execute_batch


class PostgresAnomalyWriter:
    def __init__(self, db_config: dict, batch_size: int = 100):
        self.batch_size = batch_size
        self.conn = psycopg2.connect(**db_config)
        self.cursor = self.conn.cursor()

    def insert(self, records: list):
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
        )
        """

        execute_batch(self.cursor, query, records, page_size=self.batch_size)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()