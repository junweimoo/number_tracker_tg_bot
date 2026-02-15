class AttendanceRepository:
    def __init__(self, db):
        self.db = db

    def get_insert_query(self):
        return """
        INSERT INTO user_attendance (user_id, chat_id, log_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, chat_id, log_date) DO NOTHING
        """

    async def get_recent_attendance(self, user_id, chat_id, limit=365):
        query = """
        SELECT log_date 
        FROM user_attendance 
        WHERE user_id = %s AND chat_id = %s 
        ORDER BY log_date DESC 
        LIMIT %s
        """
        return await self.db.fetch_all(query, (user_id, chat_id, limit))