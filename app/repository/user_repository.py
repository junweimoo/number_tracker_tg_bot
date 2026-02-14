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