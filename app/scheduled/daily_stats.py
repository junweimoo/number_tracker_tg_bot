import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DailyStatsTask:
    """
    Scheduled task for generating and sending daily statistics to chats.
    """
    def __init__(self, bot, stats_repository, chat_id, visualization_service, config):
        """
        Initializes the DailyStatsTask.

        Args:
            bot: The bot instance.
            stats_repository: The stats repository.
            chat_id (int): The ID of the chat to send stats to.
            visualization_service: The visualization service.
            config: Configuration object.
        """
        self.bot = bot
        self.stats_repository = stats_repository
        self.chat_id = chat_id
        self.visualization_service = visualization_service
        self.config = config

    async def run_midnight_stats(self):
        """
        Generates and sends statistics for the previous day at midnight.
        Includes a bar chart of numbers logged and an activity time series.
        """
        try:
            logger.info(f"Running daily midnight stats for chat {self.chat_id}...")
            yesterday = datetime.now() - timedelta(days=1)
            yesterday_date = yesterday.date()

            # 1. Visualization of numbers obtained from the previous day
            viz_buf = await self.visualization_service.generate_number_count_visualization(
                self.chat_id, start_date=yesterday_date
            )
            if viz_buf:
                await self.bot.send_photo(self.chat_id, viz_buf, caption=f"Numbers logged on {yesterday_date}")

            # 2. Timeseries plot of numbers logged from the previous 24 hours
            ts_viz_buf = await self.visualization_service.generate_time_series_visualization(
                self.chat_id, hourly_buckets=True, buckets=24
            )
            if ts_viz_buf:
                await self.bot.send_photo(self.chat_id, ts_viz_buf, caption=f"Activity in the past 24 hours")

            # 3. Configurable message
            await self.bot.send_message(self.chat_id, self.config.new_day_message)
                
        except Exception as e:
            logger.error(f"Error running DailyStatsTask: {e}", exc_info=True)

    async def run_midday_stats(self):
        """
        Generates and sends statistics for the current day at midday.
        Includes a bar chart of numbers logged so far today.
        """
        try:
            logger.info(f"Running daily midday stats for chat {self.chat_id}...")
            today_date = datetime.now().date()

            # Visualization of numbers obtained today
            viz_buf = await self.visualization_service.generate_number_count_visualization(
                self.chat_id, start_date=today_date
            )

            if viz_buf:
                await self.bot.send_photo(self.chat_id, viz_buf, caption=f"Numbers logged today ({today_date})")
            else:
                logger.info(f"No stats found for chat {self.chat_id} on {today_date}")

        except Exception as e:
            logger.error(f"Error running DailyStatsTask: {e}", exc_info=True)
