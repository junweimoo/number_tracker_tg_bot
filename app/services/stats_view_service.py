import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class StatsViewService:
    def __init__(self, db, config, repositories, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.stats_repo = repositories['stats']
        self.attendance_repo = repositories['attendance']

    def set_bot(self, bot):
        self.bot = bot

    async def get_stats_summary(self, message):
        user_id = message.user_id
        chat_id = message.chat_id
        first_name = message.first_name

        try:
            # 1. Fetch Stats (Count & Average)
            result = self.stats_repo.get_user_stats(user_id, chat_id)
            
            if result and result[0] is not None and result[0] > 0:
                count = result[0]
                total_sum = result[1]
                average = round(total_sum / count, 2)
                response = f"Stats for {first_name} in this chat:\nTotal numbers: {count}\nAverage: {average}"
            else:
                response = f"No numbers recorded for {first_name} in this chat yet."

            # 2. Calculate Streak Dynamically
            attendance_rows = self.attendance_repo.get_recent_attendance(user_id, chat_id)
            
            current_streak = 0
            if attendance_rows:
                sgt_timezone = timezone(timedelta(hours=8))
                today = datetime.now(sgt_timezone).date()
                yesterday = today - timedelta(days=1)
                
                dates = [row[0] for row in attendance_rows]
                
                # Check if the streak is active (must have logged today or yesterday)
                if dates[0] == today or dates[0] == yesterday:
                    current_streak = 1
                    expected_date = dates[0] - timedelta(days=1)
                    
                    for i in range(1, len(dates)):
                        if dates[i] == expected_date:
                            current_streak += 1
                            expected_date -= timedelta(days=1)
                        else:
                            break
                else:
                    current_streak = 0

            if current_streak > 0:
                response += f"\nCurrent Streak: {current_streak} days 🔥"

            return response

        except Exception as e:
            logger.error(f"Error calculating stats summary: {e}", exc_info=True)
            return "An error occurred while fetching your stats."
