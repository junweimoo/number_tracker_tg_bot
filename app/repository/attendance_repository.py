class AttendanceRepository:
    """
    Repository for managing user attendance records in the database.
    """
    def __init__(self, db):
        """
        Initializes the AttendanceRepository.

        Args:
            db: The database connection.
        """
        self.db = db

    def get_insert_query(self):
        """
        Returns the SQL query to insert a new attendance record.

        Returns:
            str: The SQL query string.
        """
        return """
        INSERT INTO user_attendance (user_id, chat_id, log_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, chat_id, log_date) DO NOTHING
        """

    async def get_recent_attendance(self, user_id, chat_id, limit=365):
        """
        Fetches recent attendance dates for a specific user in a chat.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.
            limit (int): The maximum number of records to return. Defaults to 365.

        Returns:
            list: A list of tuples containing log_date.
        """
        query = """
        SELECT log_date 
        FROM user_attendance 
        WHERE user_id = %s AND chat_id = %s 
        ORDER BY log_date DESC 
        LIMIT %s
        """
        return await self.db.fetch_all(query, (user_id, chat_id, limit))
