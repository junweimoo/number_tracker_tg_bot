class StatsRepository:
    """
    Repository for managing and retrieving statistical data from the database.
    """
    def __init__(self, db):
        """
        Initializes the StatsRepository.

        Args:
            db: The database connection.
        """
        self.db = db

    def get_upsert_counts_query(self):
        """
        Returns the SQL query to upsert the total count of a specific number logged by a user.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_number_counts (user_id, chat_id, number, count)
        VALUES (%s, %s, %s, 1)
        ON CONFLICT (user_id, chat_id, number) 
        DO UPDATE SET count = user_number_counts.count + 1
        """

    def get_upsert_daily_counts_query(self):
        """
        Returns the SQL query to upsert the daily count of a specific number logged by a user.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_daily_number_counts (user_id, chat_id, log_date, number, count)
        VALUES (%s, %s, %s, %s, 1)
        ON CONFLICT (user_id, chat_id, log_date, number) 
        DO UPDATE SET count = user_daily_number_counts.count + 1
        """

    async def get_user_stats(self, user_id, chat_id):
        """
        Fetches the total count and sum of numbers logged by a specific user.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            tuple: (total_count, total_sum)
        """
        query = """
        SELECT sum(count), sum(number * count)
        FROM user_number_counts
        WHERE user_id = %s AND chat_id = %s
        """
        return await self.db.fetch_one(query, (user_id, chat_id))

    async def get_daily_average(self, chat_id, date):
        """
        Calculates the average of all numbers logged in a chat on a specific date.

        Args:
            chat_id (int): The ID of the chat.
            date (date): The date to calculate the average for.

        Returns:
            float: The average value, or None if no logs exist.
        """
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
        """
        Fetches the counts for a list of specific numbers logged by a user.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.
            numbers (list): A list of integers representing the numbers to check.

        Returns:
            list: A list of tuples containing (number, count).
        """
        query = """
        SELECT number, count
        FROM user_number_counts
        WHERE user_id = %s AND chat_id = %s AND number = ANY(%s)
        """
        return await self.db.fetch_all(query, (user_id, chat_id, numbers))

    async def get_most_frequent_numbers(self, user_id, chat_id):
        """
        Fetches the number(s) logged most frequently by a user.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            list: A list of tuples containing (number, count).
        """
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
        """
        Fetches the top users in a chat based on their total number of logs.

        Args:
            chat_id (int): The ID of the chat.
            limit (int): The maximum number of users to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (user_id, total_count).
        """
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
        """
        Fetches the total log counts for all users in a chat.

        Args:
            chat_id (int): The ID of the chat.

        Returns:
            list: A list of tuples containing (user_id, total_count).
        """
        query = """
        SELECT user_id, sum(count) as total_count
        FROM user_number_counts
        WHERE chat_id = %s
        GROUP BY user_id
        """
        return await self.db.fetch_all(query, (chat_id,))

    async def get_top_users_by_count_daily(self, chat_id, date, limit=3):
        """
        Fetches the top users in a chat based on their log count for a specific date.

        Args:
            chat_id (int): The ID of the chat.
            date (date): The date to check.
            limit (int): The maximum number of users to return. Defaults to 3.

        Returns:
            list: A list of tuples containing (user_id, total_count).
        """
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
        """
        Fetches the user who has logged a specific number the most times in a chat.

        Args:
            chat_id (int): The ID of the chat.
            number (int): The number to check.

        Returns:
            tuple: (user_id, count)
        """
        query = """
        SELECT user_id, count
        FROM user_number_counts
        WHERE chat_id = %s AND number = %s
        ORDER BY count DESC
        LIMIT 1
        """
        return await self.db.fetch_one(query, (chat_id, number))

    async def get_top_user_for_number_daily(self, chat_id, number, date):
        """
        Fetches the user who has logged a specific number the most times on a specific date.

        Args:
            chat_id (int): The ID of the chat.
            number (int): The number to check.
            date (date): The date to check.

        Returns:
            tuple: (user_id, count)
        """
        query = """
        SELECT user_id, count
        FROM user_daily_number_counts
        WHERE chat_id = %s AND number = %s AND log_date = %s
        ORDER BY count DESC
        LIMIT 1
        """
        return await self.db.fetch_one(query, (chat_id, number, date))

    async def get_all_number_counts(self, chat_id, user_id=None):
        """
        Fetches the counts for all numbers logged in a chat, optionally filtered by user.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            list: A list of tuples containing (number, count).
        """
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
        """
        Returns the SQL query and parameters to fetch all number counts.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            tuple: (query_string, parameters)
        """
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
        """
        Fetches number counts logged since a specific date.

        Args:
            chat_id (int): The ID of the chat.
            start_date (date): The start date.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            list: A list of tuples containing (number, count).
        """
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
        """
        Returns the SQL query and parameters to fetch number counts since a specific date.

        Args:
            chat_id (int): The ID of the chat.
            start_date (date): The start date.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            tuple: (query_string, parameters)
        """
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
        """
        Fetches the total log counts per day since a specific date.

        Args:
            chat_id (int): The ID of the chat.
            start_date (date): The start date.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            list: A list of tuples containing (log_date, total_count).
        """
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
        """
        Returns the SQL query and parameters to fetch daily log counts.

        Args:
            chat_id (int): The ID of the chat.
            start_date (date): The start date.
            user_id (int, optional): The ID of the user. Defaults to None.

        Returns:
            tuple: (query_string, parameters)
        """
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
