class UserRepository:
    def __init__(self, db):
        self.db = db

    def get_update_streak_query(self):
        """
        Updates the user's streak and last_log_date in user_data.
        Logic:
        - If last_login_date is today: do nothing (keep streak same)
        - If last_login_date is yesterday: increment streak, update date
        - If last_login_date is older (or null): reset streak to 1, update date
        
        Expected Params Order: 
        [user_id, chat_id, today_date, today_date, yesterday_date, today_date]
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
        return "SELECT current_streak FROM user_data WHERE user_id = %s AND chat_id = %s"

    def get_last_login_date_query(self):
        return "SELECT last_login_date FROM user_data WHERE user_id = %s AND chat_id = %s"

    def get_upsert_user_bitmap_query(self):
        """
        Upserts the user_data row and updates the numbers_bitmap.
        The bitmap is updated by OR-ing the existing bitmap with a new bitmask.
        The new bitmask has a 1 at the position corresponding to the number (0-100).
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
        Upserts the user_data row and updates the numbers_bitmap and achievements.
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
        return """
        SELECT 
            id, chat_id, thread_id, user_id, user_name, user_handle, 
            numbers_bitmap, last_login_date, current_streak, achievements, extend_info
        FROM user_data
        """

    async def get_all_users_in_chat(self, chat_id):
        query = "SELECT user_id, user_name FROM user_data WHERE chat_id = %s"
        return await self.db.fetch_all(query, (chat_id,))

    async def get_user_name(self, user_id, chat_id):
        query = "SELECT user_name FROM user_data WHERE user_id = %s AND chat_id = %s"
        result = await self.db.fetch_one(query, (user_id, chat_id))
        return result[0] if result else "Unknown"

    def get_user_name_query(self, user_id, chat_id):
        query = "SELECT user_name FROM user_data WHERE user_id = %s AND chat_id = %s"
        return query, (user_id, chat_id)
