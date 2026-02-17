import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class DailyStatsTask:
    """
    Scheduled task for generating and sending daily statistics to chats.
    """
    def __init__(self, bot, stats_repository, chat_id, visualization_service, stats_view_service, admin_service, config):
        """
        Initializes the DailyStatsTask.

        Args:
            bot: The bot instance.
            stats_repository: The stats repository.
            chat_id (int): The ID of the chat to send stats to.
            visualization_service: The visualization service.
            stats_view_service: The stats view service.
            config: Configuration object.
        """
        self.bot = bot
        self.stats_repository = stats_repository
        self.chat_id = chat_id
        self.visualization_service = visualization_service
        self.stats_view_service = stats_view_service
        self.admin_service = admin_service
        self.config = config

    async def run_midnight_stats(self):
        """
        Generates and sends statistics for the previous day at midnight.
        Includes a bar chart of numbers logged and an activity time series.
        """
        try:
            logger.info(f"Running daily midnight stats for chat {self.chat_id}...")
            tz = timezone(timedelta(hours=self.config.timezone_gmt))
            today_date = datetime.now(tz).date()
            yesterday_date = today_date - timedelta(days=1)

            # 1. Visualization of numbers obtained from the previous day
            viz_buf = await self.visualization_service.generate_number_count_visualization_grid(
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

            # 3. Leaderboard
            leaderboard_text = await self.stats_view_service.get_leaderboard(self.chat_id, stats_date=yesterday_date)
            if leaderboard_text:
                await self.bot.send_html(self.chat_id, leaderboard_text)

            # 4. Configurable message
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
            tz = timezone(timedelta(hours=self.config.timezone_gmt))
            today_date = datetime.now(tz).date()

            # 1. Visualization of numbers obtained today
            viz_buf = await self.visualization_service.generate_number_count_visualization_grid(
                self.chat_id, start_date=today_date
            )

            if viz_buf:
                await self.bot.send_photo(self.chat_id, viz_buf, caption=f"Numbers logged today ({today_date})")
            else:
                logger.info(f"No stats found for chat {self.chat_id} on {today_date}")

            # 2. Numbers remaining for all users in chat
            nums_remaining_text = await self.stats_view_service.get_user_nums_remaining_in_chat(self.chat_id)
            if nums_remaining_text:
                await self.bot.send_html(self.chat_id, nums_remaining_text)

        except Exception as e:
            logger.error(f"Error running DailyStatsTask: {e}", exc_info=True)
