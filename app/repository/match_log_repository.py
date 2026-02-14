class MatchLogRepository:
    def __init__(self, db):
        self.db = db

    def get_insert_query(self):
        return """
        INSERT INTO match_logs (
            chat_id, thread_id, 
            user_id_1, user_name_1, 
            user_id_2, user_name_2, 
            ts, match_type, 
            number_1, number_2
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    def get_upsert_match_counts_query(self):
        return """
        INSERT INTO match_counts (chat_id, thread_id, user_id_1, user_id_2, match_type, count)
        VALUES (%s, %s, %s, %s, %s, 1)
        ON CONFLICT (chat_id, user_id_1, user_id_2, match_type) 
        DO UPDATE SET count = match_counts.count + 1
        """