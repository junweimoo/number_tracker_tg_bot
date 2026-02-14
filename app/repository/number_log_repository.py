class NumberLogRepository:
    def __init__(self, db):
        self.db = db

    def get_insert_query(self):
        return """
        INSERT INTO number_logs (chat_id, thread_id, user_id, user_name, ts, number)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

    def get_raw_stats_query(self):
        return """
        SELECT count(*), sum(number)
        FROM number_logs
        WHERE user_id = %s AND chat_id = %s
        """

    def get_recent_logs_for_number(self, chat_id, number, limit=3):
        query = """
        SELECT user_name, ts
        FROM number_logs
        WHERE chat_id = %s AND number = %s
        ORDER BY ts DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (chat_id, number, limit))

    def get_hourly_counts(self, chat_id, start_time, user_id=None):
        if user_id:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND user_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return self.db.fetch_all(query, (chat_id, user_id, start_time))
        else:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return self.db.fetch_all(query, (chat_id, start_time))
