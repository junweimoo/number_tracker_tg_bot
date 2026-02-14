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

    def get_top_matches(self, user_id, chat_id, limit=3):
        query = """
        SELECT 
            CASE 
                WHEN user_id_1 = %s THEN user_id_2 
                ELSE user_id_1 
            END as matched_user_id,
            sum(count) as total_matches
        FROM match_counts
        WHERE chat_id = %s AND (user_id_1 = %s OR user_id_2 = %s)
        GROUP BY matched_user_id
        ORDER BY total_matches DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (user_id, chat_id, user_id, user_id, limit))

    def get_top_matched_pairs(self, chat_id, limit=3):
        query = """
        SELECT user_id_1, user_id_2, sum(count) as total_matches
        FROM match_counts
        WHERE chat_id = %s
        GROUP BY user_id_1, user_id_2
        ORDER BY total_matches DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (chat_id, limit))

    def get_top_matched_pairs_daily(self, chat_id, date, timezone_str, limit=3):
        query = """
        SELECT 
            LEAST(user_id_1, user_id_2) as u1, 
            GREATEST(user_id_1, user_id_2) as u2, 
            count(*) as total_matches
        FROM match_logs
        WHERE chat_id = %s AND date(ts AT TIME ZONE 'UTC' AT TIME ZONE %s) = %s
        GROUP BY u1, u2
        ORDER BY total_matches DESC
        LIMIT %s
        """
        return self.db.fetch_all(query, (chat_id, timezone_str, date, limit))
