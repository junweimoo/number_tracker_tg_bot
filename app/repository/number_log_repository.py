class NumberLogRepository:
    """
    Repository for managing and retrieving raw number log entries from the database.
    """
    def __init__(self, db):
        """
        Initializes the NumberLogRepository.

        Args:
            db: The database connection.
        """
        self.db = db

    def get_insert_query(self):
        """
        Returns the SQL query to insert a new number log entry.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO number_logs (chat_id, thread_id, user_id, user_name, ts, number)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

    def get_raw_stats_query(self):
        """
        Returns the SQL query to fetch raw statistics (count and sum) for a user in a chat.

        Returns:
            str: The SQL query string.
        """
        return """
        SELECT count(*), sum(number)
        FROM number_logs
        WHERE user_id = %s AND chat_id = %s
        """

    async def get_recent_logs_for_number(self, chat_id, number, limit=3):
        """
        Fetches the most recent logs for a specific number in a chat.

        Args:
            chat_id (int): The ID of the chat.
            number (int): The number to check.
            limit (int): The maximum number of logs to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (user_name, ts).
        """
        query = """
        SELECT user_name, ts
        FROM number_logs
        WHERE chat_id = %s AND number = %s
        ORDER BY ts DESC
        LIMIT %s
        """
        return await self.db.fetch_all(query, (chat_id, number, limit))

    async def get_hourly_counts(self, chat_id, start_time, user_id=None):
        """
        Fetches the count of numbers logged per hour since a specific time.

        Args:
            chat_id (int): The ID of the chat.
            start_time (datetime): The start time.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            list: A list of tuples containing (bucket, count).
        """
        if user_id:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND user_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return await self.db.fetch_all(query, (chat_id, user_id, start_time))
        else:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return await self.db.fetch_all(query, (chat_id, start_time))

    def get_hourly_counts_query(self, chat_id, start_time, user_id=None):
        """
        Returns the SQL query and parameters to fetch hourly log counts.

        Args:
            chat_id (int): The ID of the chat.
            start_time (datetime): The start time.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            tuple: (query_string, parameters)
        """
        if user_id:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND user_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return query, (chat_id, user_id, start_time)
        else:
            query = """
            SELECT time_bucket('1 hour', ts) AS bucket, count(*)
            FROM number_logs
            WHERE chat_id = %s AND ts >= %s
            GROUP BY bucket
            ORDER BY bucket
            """
            return query, (chat_id, start_time)

    async def get_all_logs(self):
        """
        Fetches all number log entries from the database.

        Returns:
            list: A list of tuples containing all log columns.
        """
        query = """
        SELECT id, chat_id, thread_id, user_id, user_name, ts, number
        FROM number_logs
        ORDER BY ts ASC
        """
        return await self.db.fetch_all(query)
