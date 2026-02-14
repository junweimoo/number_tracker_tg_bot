import asyncio
import os
from datetime import datetime, timedelta, timezone
from bot import TelegramBot
from config import Config
from database.database_core import Database
from database.database_schema import SchemaManager
from handlers import (
    leaderboard_handler, start_handler, echo_handler, number_parser_handler, stats_handler, leaderboard_handler)
from service.number_log_service import NumberLogService
from service.stats_view_service import StatsViewService
from repository.number_log_repository import NumberLogRepository
from repository.attendance_repository import AttendanceRepository
from repository.stats_repository import StatsRepository
from repository.match_log_repository import MatchLogRepository
from repository.user_repository import UserRepository
from utils.transaction_queue import TransactionQueue
from utils.scheduler import Scheduler
from scheduled.daily_stats import DailyStatsTask

async def main():
    # Initialize env variables
    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set.")
        exit(1)

    # Initialize Configs
    try:
        config = Config('config.json')
    except Exception as e:
        print(f"Error loading config: {e}")
        exit(1)
    tz = timezone(timedelta(hours=config.timezone_gmt))

    # Initialize Database
    try:
        db = Database()
        schema_manager = SchemaManager(db)
        schema_manager.init_db()
    except Exception as e:
        print(f"Error initializing database: {e}")
        exit(1)

    # Initialize Transaction Queue
    transaction_queue = TransactionQueue(db)
    transaction_queue.start_worker()

    # Initialize Repositories
    repositories = {
        'number_log': NumberLogRepository(db),
        'attendance': AttendanceRepository(db),
        'stats': StatsRepository(db),
        'match_log': MatchLogRepository(db),
        'user': UserRepository(db)
    }

    # Initialize Services
    number_log_service = NumberLogService(db, config, repositories, transaction_queue)
    stats_view_service = StatsViewService(db, config, repositories)

    # Initialize Scheduler
    scheduler = Scheduler()

    # Initialize Bot
    context = {
        'config': config,
        'db': db,
        'number_log_service': number_log_service,
        'stats_view_service': stats_view_service,
        'scheduler': scheduler
    }
    bot = TelegramBot(TOKEN, context=context)
    number_log_service.set_bot(bot)
    stats_view_service.set_bot(bot)

    # Register Handlers
    bot.register_command_handler('/start', start_handler)
    bot.register_command_handler('/stats', stats_handler)
    bot.register_command_handler('/leaderboard', leaderboard_handler)
    bot.register_message_handler(number_parser_handler)

    # Register Tasks
    stats_chat_ids = getattr(config, 'daily_stats_chat_ids', None)
    for chat_id in stats_chat_ids:
        daily_stats_task = DailyStatsTask(bot, repositories['stats'], chat_id)
        scheduler.register_recurring_job(daily_stats_task.run_midnight_stats, 0, 0, 0, tz)
        scheduler.register_recurring_job(daily_stats_task.run_midday_stats, 13, 0, 0, tz)

    # Start Scheduler
    scheduler.start_worker()

    try:
        await bot.start_polling()
    except asyncio.CancelledError:
        print("Bot stopped.")
    finally:
        await scheduler.stop_worker()
        await transaction_queue.stop_worker()
        db.close_all_connections()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass