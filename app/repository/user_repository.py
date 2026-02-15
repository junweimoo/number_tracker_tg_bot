class UserRepository:
    """
    Repository for managing user data in the database.
    """
    def __init__(self, db):
        """
        Initializes the UserRepository.

        Args:
            db: The database connection.
        """
        self.db = db

    def get_update_streak_query(self):
        """
        Returns the SQL query to update a user's attendance streak.
        
        Logic:
        - If last_login_date is today: do nothing (keep streak same)
        - If last_login_date is yesterday: increment streak, update date
        - If last_login_date is older (or null): reset streak to 1, update date
        
        Expected Params Order: 
        [user_id, chat_id, today_date, today_date, yesterday_date, today_date]

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_data (user_id, chat_id, current_streak, last_login_date, user_name)
        VALUES (%s, %s, 1, %s, 'Unknown')
        ON CONFLICT (user_id, chat_id) DO UPDATE
        SET 
            current_streak = CASE 
                WHEN user_data.last_login_date = %s THEN user_data.current_streak
                WHEN user_data.last_login_date = %s THEN user_data.current_streak + 1
                ELSE 1
            END,
            last_login_date = %s
        """

    def get_fetch_streak_query(self):
        """
        Returns the SQL query to fetch a user's current streak.

        Returns:
            str: The SQL query string.
        """
        return "SELECT current_streak FROM user_data WHERE user_id = %s AND chat_id = %s"

    def get_last_login_date_query(self):
        """
        Returns the SQL query to fetch a user's last login date.

        Returns:
            str: The SQL query string.
        """
        return "SELECT last_login_date FROM user_data WHERE user_id = %s AND chat_id = %s"

    def get_upsert_user_bitmap_query(self):
        """
        Returns the SQL query to upsert user data and update their collected numbers bitmap.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_data (user_id, chat_id, user_name, numbers_bitmap)
        VALUES (%s, %s, %s, set_bit(repeat('0', 128)::bit(128), %s, 1))
        ON CONFLICT (user_id, chat_id) DO UPDATE
        SET 
            user_name = EXCLUDED.user_name,
            numbers_bitmap = set_bit(user_data.numbers_bitmap, %s, 1)
        """

    def get_upsert_user_bitmap_with_achievements_query(self):
        """
        Returns the SQL query to upsert user data, update bitmap, and append new achievements.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_data (user_id, chat_id, user_name, numbers_bitmap, achievements)
        VALUES (%s, %s, %s, set_bit(repeat('0', 128)::bit(128), %s, 1), %s)
        ON CONFLICT (user_id, chat_id) DO UPDATE
        SET 
            user_name = EXCLUDED.user_name,
            numbers_bitmap = set_bit(user_data.numbers_bitmap, %s, 1),
            achievements = CASE 
                WHEN user_data.achievements IS NULL OR user_data.achievements = '' THEN EXCLUDED.achievements
                ELSE user_data.achievements || ',' || EXCLUDED.achievements
            END
        """

    def get_all_users_query(self):
        """
        Returns the SQL query to fetch all user data.

        Returns:
            str: The SQL query string.
        """
        return """
        SELECT 
            id, chat_id, thread_id, user_id, user_name, user_handle, 
            numbers_bitmap, last_login_date, current_streak, achievements, extend_info
        FROM user_data
        """

    async def get_all_users_in_chat(self, chat_id):
        """
        Fetches all users belonging to a specific chat.

        Args:
            chat_id (int): The ID of the chat.

        Returns:
            list: A list of tuples containing user_id and user_name.
        """
        query = "SELECT user_id, user_name FROM user_data WHERE chat_id = %s"
        return await self.db.fetch_all(query, (chat_id,))

    async def get_user_name(self, user_id, chat_id):
        """
        Fetches the name of a specific user in a chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            str: The user's name, or "Unknown" if not found.
        """
        query = "SELECT user_name FROM user_data WHERE user_id = %s AND chat_id = %s"
        result = await self.db.fetch_one(query, (user_id, chat_id))
        return result[0] if result else "Unknown"

    def get_user_name_query(self, user_id, chat_id):
        """
        Returns the SQL query and parameters to fetch a user's name.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            tuple: (query_string, parameters)
        """
        query = "SELECT user_name FROM user_data WHERE user_id = %s AND chat_id = %s"
        return query, (user_id, chat_id)
