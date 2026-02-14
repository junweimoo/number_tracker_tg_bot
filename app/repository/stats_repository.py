class StatsRepository:
    def __init__(self, db):
        self.db = db

    def get_upsert_counts_query(self):
        return """
        INSERT INTO user_number_counts (user_id, chat_id, number, count)
        VALUES (%s, %s, %s, 1)
        ON CONFLICT (user_id, chat_id, number) 
        DO UPDATE SET count = user_number_counts.count + 1
        """

    def get_upsert_daily_counts_query(self):
        return """
        INSERT INTO user_daily_number_counts (user_id, chat_id, log_date, number, count)
        VALUES (%s, %s, %s, %s, 1)
        ON CONFLICT (user_id, chat_id, log_date, number) 
        DO UPDATE SET count = user_daily_number_counts.count + 1
        """

    def get_user_stats(self, user_id, chat_id):
        query = """
        SELECT sum(count), sum(number * count)
        FROM user_number_counts
        WHERE user_id = %s AND chat_id = %s
        """
        return self.db.fetch_one(query, (user_id, chat_id))

    def get_daily_average(self, chat_id, date):
        query = """
        SELECT sum(number * count), sum(count)
        FROM user_daily_number_counts
        WHERE chat_id = %s AND log_date = %s
        """
        result = self.db.fetch_one(query, (chat_id, date))
        if result and result[1] and result[1] > 0:
            return result[0] / result[1]
        return None
