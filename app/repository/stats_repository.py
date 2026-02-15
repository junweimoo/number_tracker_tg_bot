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

    async def get_user_stats(self, user_id, chat_id):
        query = """
        SELECT sum(count), sum(number * count)
        FROM user_number_counts
        WHERE user_id = %s AND chat_id = %s
        """
        return await self.db.fetch_one(query, (user_id, chat_id))

    async def get_daily_average(self, chat_id, date):
        query = """
        SELECT sum(number * count), sum(count)
        FROM user_daily_number_counts
        WHERE chat_id = %s AND log_date = %s
        """
        result = await self.db.fetch_one(query, (chat_id, date))
        if result and result[1] and result[1] > 0:
            return result[0] / result[1]
        return None

    async def get_specific_number_counts(self, user_id, chat_id, numbers):
        query = """
        SELECT number, count
        FROM user_number_counts
        WHERE user_id = %s AND chat_id = %s AND number = ANY(%s)
        """
        return await self.db.fetch_all(query, (user_id, chat_id, numbers))

    async def get_most_frequent_numbers(self, user_id, chat_id):
        query = """
        WITH max_count AS (
            SELECT MAX(count) as max_c
            FROM user_number_counts
            WHERE user_id = %s AND chat_id = %s
        )
        SELECT number, count
        FROM user_number_counts, max_count
        WHERE user_id = %s AND chat_id = %s AND count = max_c
        """
        return await self.db.fetch_all(query, (user_id, chat_id, user_id, chat_id))

    async def get_top_users_by_count(self, chat_id, limit=3):
        query = """
        SELECT user_id, sum(count) as total_count
        FROM user_number_counts
        WHERE chat_id = %s
        GROUP BY user_id
        ORDER BY total_count DESC
        LIMIT %s
        """
        return await self.db.fetch_all(query, (chat_id, limit))

    async def get_user_total_counts(self, chat_id):
        query = """
        SELECT user_id, sum(count) as total_count
        FROM user_number_counts
        WHERE chat_id = %s
        GROUP BY user_id
        """
        return await self.db.fetch_all(query, (chat_id,))

    async def get_top_users_by_count_daily(self, chat_id, date, limit=3):
        query = """
        SELECT user_id, sum(count) as total_count
        FROM user_daily_number_counts
        WHERE chat_id = %s AND log_date = %s
        GROUP BY user_id
        ORDER BY total_count DESC
        LIMIT %s
        """
        return await self.db.fetch_all(query, (chat_id, date, limit))

    async def get_top_user_for_number(self, chat_id, number):
        query = """
        SELECT user_id, count
        FROM user_number_counts
        WHERE chat_id = %s AND number = %s
        ORDER BY count DESC
        LIMIT 1
        """
        return await self.db.fetch_one(query, (chat_id, number))

    async def get_top_user_for_number_daily(self, chat_id, number, date):
        query = """
        SELECT user_id, count
        FROM user_daily_number_counts
        WHERE chat_id = %s AND number = %s AND log_date = %s
        ORDER BY count DESC
        LIMIT 1
        """
        return await self.db.fetch_one(query, (chat_id, number, date))

    async def get_all_number_counts(self, chat_id, user_id=None):
        if user_id:
            query = """
            SELECT number, count
            FROM user_number_counts
            WHERE chat_id = %s AND user_id = %s
            ORDER BY number
            """
            return await self.db.fetch_all(query, (chat_id, user_id))
        else:
            query = """
            SELECT number, sum(count)
            FROM user_number_counts
            WHERE chat_id = %s
            GROUP BY number
            ORDER BY number
            """
            return await self.db.fetch_all(query, (chat_id,))

    def get_all_number_counts_query(self, chat_id, user_id=None):
        if user_id:
            query = """
            SELECT number, count
            FROM user_number_counts
            WHERE chat_id = %s AND user_id = %s
            ORDER BY number
            """
            return query, (chat_id, user_id)
        else:
            query = """
            SELECT number, sum(count)
            FROM user_number_counts
            WHERE chat_id = %s
            GROUP BY number
            ORDER BY number
            """
            return query, (chat_id,)

    async def get_number_counts_since(self, chat_id, start_date, user_id=None):
        if user_id:
            query = """
            SELECT number, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND user_id = %s AND log_date >= %s
            GROUP BY number
            ORDER BY number
            """
            return await self.db.fetch_all(query, (chat_id, user_id, start_date))
        else:
            query = """
            SELECT number, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND log_date >= %s
            GROUP BY number
            ORDER BY number
            """
            return await self.db.fetch_all(query, (chat_id, start_date))

    def get_number_counts_since_query(self, chat_id, start_date, user_id=None):
        if user_id:
            query = """
            SELECT number, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND user_id = %s AND log_date >= %s
            GROUP BY number
            ORDER BY number
            """
            return query, (chat_id, user_id, start_date)
        else:
            query = """
            SELECT number, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND log_date >= %s
            GROUP BY number
            ORDER BY number
            """
            return query, (chat_id, start_date)

    async def get_daily_counts(self, chat_id, start_date, user_id=None):
        if user_id:
            query = """
            SELECT log_date, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND user_id = %s AND log_date >= %s
            GROUP BY log_date
            ORDER BY log_date
            """
            return await self.db.fetch_all(query, (chat_id, user_id, start_date))
        else:
            query = """
            SELECT log_date, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND log_date >= %s
            GROUP BY log_date
            ORDER BY log_date
            """
            return await self.db.fetch_all(query, (chat_id, start_date))

    def get_daily_counts_query(self, chat_id, start_date, user_id=None):
        if user_id:
            query = """
            SELECT log_date, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND user_id = %s AND log_date >= %s
            GROUP BY log_date
            ORDER BY log_date
            """
            return query, (chat_id, user_id, start_date)
        else:
            query = """
            SELECT log_date, sum(count)
            FROM user_daily_number_counts
            WHERE chat_id = %s AND log_date >= %s
            GROUP BY log_date
            ORDER BY log_date
            """
            return query, (chat_id, start_date)
