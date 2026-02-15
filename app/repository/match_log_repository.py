class MatchLogRepository:
    """
    Repository for managing and retrieving match logs and match counts from the database.
    """
    def __init__(self, db):
        """
        Initializes the MatchLogRepository.

        Args:
            db: The database connection.
        """
        self.db = db

    def get_insert_query(self):
        """
        Returns the SQL query to insert a new match log entry.

        Returns:
            str: The SQL query string.
        """
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
        """
        Returns the SQL query to upsert the count of matches between two users.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO match_counts (chat_id, thread_id, user_id_1, user_id_2, match_type, count)
        VALUES (%s, %s, %s, %s, %s, 1)
        ON CONFLICT (chat_id, user_id_1, user_id_2, match_type) 
        DO UPDATE SET count = match_counts.count + 1
        """

    async def get_top_matches(self, user_id, chat_id, limit=3):
        """
        Fetches the top users matched with a specific user in a chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.
            limit (int): The maximum number of matches to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (matched_user_id, total_matches).
        """
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
        return await self.db.fetch_all(query, (user_id, chat_id, user_id, user_id, limit))

    async def get_top_matched_pairs(self, chat_id, limit=3):
        """
        Fetches the top matched pairs of users in a chat.

        Args:
            chat_id (int): The ID of the chat.
            limit (int): The maximum number of pairs to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (user_id_1, user_id_2, total_matches).
        """
        query = """
        SELECT user_id_1, user_id_2, sum(count) as total_matches
        FROM match_counts
        WHERE chat_id = %s
        GROUP BY user_id_1, user_id_2
        ORDER BY total_matches DESC
        LIMIT %s
        """
        return await self.db.fetch_all(query, (chat_id, limit))

    async def get_all_matched_pairs(self, chat_id, user_id=None):
        """
        Fetches all matched pairs in a chat, optionally filtered by a specific user.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            list: A list of tuples containing (user_id_1, user_id_2, total_matches).
        """
        if user_id:
            query = """
            SELECT user_id_1, user_id_2, sum(count) as total_matches
            FROM match_counts
            WHERE chat_id = %s AND (user_id_1 = %s OR user_id_2 = %s)
            GROUP BY user_id_1, user_id_2
            """
            return await self.db.fetch_all(query, (chat_id, user_id, user_id))
        else:
            query = """
            SELECT user_id_1, user_id_2, sum(count) as total_matches
            FROM match_counts
            WHERE chat_id = %s
            GROUP BY user_id_1, user_id_2
            """
            return await self.db.fetch_all(query, (chat_id,))

    async def get_top_matched_pairs_daily(self, chat_id, date, timezone_str, limit=3):
        """
        Fetches the top matched pairs in a chat for a specific date.

        Args:
            chat_id (int): The ID of the chat.
            date (date): The date to check.
            timezone_str (str): The timezone offset string (e.g., '+08').
            limit (int): The maximum number of pairs to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (u1, u2, total_matches).
        """
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
        return await self.db.fetch_all(query, (chat_id, timezone_str, date, limit))
