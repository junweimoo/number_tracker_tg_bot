import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DailyStatsTask:
    def __init__(self, bot, stats_repository, chat_id):
        self.bot = bot
        self.stats_repository = stats_repository
        self.chat_id = chat_id

    async def run_midnight_stats(self):
        try:
            logger.info(f"Running daily midnight stats for chat {self.chat_id}...")
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_date = yesterday.date()

            avg_number = self.stats_repository.get_daily_average(self.chat_id, yesterday_date)
            
            if avg_number is not None:
                message = f"📅 Daily Stats for {yesterday_date}:\nAverage Number: {avg_number:.2f}"
                await self.bot.send_message(self.chat_id, message)
            else:
                logger.info(f"No stats found for chat {self.chat_id} on {yesterday_date}")
                await self.bot.send_message(self.chat_id, "test")
                
        except Exception as e:
            logger.error(f"Error running DailyStatsTask: {e}", exc_info=True)

    async def run_midday_stats(self):
        try:
            logger.info(f"Running daily midday stats for chat {self.chat_id}...")
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_date = yesterday.date()

            avg_number = self.stats_repository.get_daily_average(self.chat_id, yesterday_date)

            if avg_number is not None:
                message = f"📅 Daily Stats for {yesterday_date}:\nAverage Number: {avg_number:.2f}"
                await self.bot.send_message(self.chat_id, message)
            else:
                logger.info(f"No stats found for chat {self.chat_id} on {yesterday_date}")
                await self.bot.send_message(self.chat_id, "test")

        except Exception as e:
            logger.error(f"Error running DailyStatsTask: {e}", exc_info=True)