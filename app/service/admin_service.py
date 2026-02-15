import logging
import csv
import io
import os
from datetime import datetime
from scheduled.daily_stats import DailyStatsTask
from database.database_schema import SchemaManager

logger = logging.getLogger(__name__)

class SimulatedMessage:
    def __init__(self, chat_id, thread_id, user_id, user_name, ts, message_id=0):
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.user_id = user_id
        self.message_id = message_id
        self.date = int(ts.timestamp())

        self.first_name = user_name
        self.last_name = None
        self.username = None

class AdminService:
    def __init__(self, bot, repositories, visualization_service, number_log_service, config, db=None):
        self.bot = bot
        self.repositories = repositories
        self.stats_repository = repositories['stats']
        self.number_log_repository = repositories['number_log']
        self.visualization_service = visualization_service
        self.number_log_service = number_log_service
        self.config = config
        self.db = db
        self.schema_manager = SchemaManager(db) if db else None

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

    async def export_number_logs(self, file_path):
        logger.info(f"Exporting number logs to {file_path}")
        logs = await self.number_log_repository.get_all_logs()
        
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'chat_id', 'thread_id', 'user_id', 'user_name', 'ts', 'number'])
            for log in logs:
                writer.writerow(log)
        
        logger.info(f"Exported {len(logs)} logs.")
        return len(logs)

    async def import_number_logs(self, file_path, clear_db=False):
        logger.info(f"Importing number logs from {file_path}, clear_db={clear_db}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Import file not found: {file_path}")

        if clear_db and self.schema_manager:
            self.schema_manager.clear_db()
            self.number_log_service.user_info_cache = {}
            self.number_log_service.user_log_cache = {}
            self.number_log_service.chat_log_cache = {}
            self.number_log_service.streak_info_cache = {}
            logger.info(f"Cleared database.")

        count = 0
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = datetime.fromisoformat(row['ts'])
                message = SimulatedMessage(
                    chat_id=int(row['chat_id']),
                    thread_id=int(row['thread_id']) if row['thread_id'] else None,
                    user_id=int(row['user_id']),
                    user_name=row['user_name'],
                    ts=ts
                )
                number = int(row['number'])
                
                # We bypass the bot's feedback during import by temporarily setting bot to None
                original_bot = self.number_log_service.bot
                self.number_log_service.bot = None
                try:
                    await self.number_log_service.process_number(message, number)
                    count += 1
                finally:
                    self.number_log_service.bot = original_bot
                    
        logger.info(f"Imported {count} logs.")
        return count
