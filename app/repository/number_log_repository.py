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