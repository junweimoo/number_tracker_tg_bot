import logging
from scheduled.daily_stats import DailyStatsTask

logger = logging.getLogger(__name__)

class AdminService:
    def __init__(self, bot, stats_repository, visualization_service, config):
        self.bot = bot
        self.stats_repository = stats_repository
        self.visualization_service = visualization_service
        self.config = config

    async def invoke_job(self, chat_id, job_name):
        logger.info(f"Invoking job {job_name} for chat {chat_id}")
        task = DailyStatsTask(
            self.bot,
            self.stats_repository,
            chat_id,
            self.visualization_service,
            self.config
        )

        if job_name == 'midnight_stats':
            await task.run_midnight_stats()
        elif job_name == 'midday_stats':
            await task.run_midday_stats()
        else:
            raise ValueError(f"Unknown job name: {job_name}")
