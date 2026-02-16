import asyncio
import os
from datetime import datetime, timedelta, timezone
from bot import TelegramBot
from config import Config
from database.database_core import Database
from database.database_schema import SchemaManager
from handlers import (
    leaderboard_handler, start_handler, echo_handler, number_parser_handler, stats_handler,
    my_remaining_nums_handler,
    visualize_group_time_series_handler, visualize_group_num_counts_handler, visualize_group_num_counts_grid_handler,
    visualize_my_time_series_handler, visualize_my_num_counts_handler, visualize_my_num_counts_grid_handler,
    visualize_chat_match_graph_handler, visualize_my_match_graph_handler, visualize_personal_profile_handler,
    invoke_job_handler, export_handler, import_handler)
from scheduled.daily_backup import DailyBackupTask
from service.number_log_service import NumberLogService
from service.stats_view_service import StatsViewService
from service.visualization_service import VisualizationService
from service.admin_service import AdminService
from repository.number_log_repository import NumberLogRepository
from repository.attendance_repository import AttendanceRepository
from repository.stats_repository import StatsRepository
from repository.match_log_repository import MatchLogRepository
from repository.user_repository import UserRepository
from utils.transaction_queue import TransactionQueue
from utils.scheduler import Scheduler
from scheduled.daily_stats import DailyStatsTask

async def main():
    """
    The main entry point for the application.
    Initializes configurations, database, services, repositories, and starts the bot.
    """
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
    visualization_service = VisualizationService(db, config, repositories)
    visualization_service.set_stats_view_service(stats_view_service)

    # Initialize Scheduler
    scheduler = Scheduler()

    # Initialize Bot
    context = {
        'config': config,
        'db': db,
        'number_log_service': number_log_service,
        'stats_view_service': stats_view_service,
        'visualization_service': visualization_service,
        'scheduler': scheduler
    }
    bot = TelegramBot(TOKEN, context=context)
    number_log_service.set_bot(bot)
    stats_view_service.set_bot(bot)
    visualization_service.set_bot(bot)

    # Initialize Admin Service
    admin_service = AdminService(bot, repositories, visualization_service,
                                 number_log_service, stats_view_service, config, db)
    context['admin_service'] = admin_service

    # Register Handlers

    # Basic stats
    bot.register_command_handler('/stats', stats_handler)
    bot.register_command_handler('/leaderboard', leaderboard_handler)
    bot.register_command_handler('/myremainingnums', my_remaining_nums_handler)
    bot.register_command_handler('/myprofile', visualize_personal_profile_handler)

    # Group visualizations
    bot.register_command_handler('/chatcounthist', visualize_group_num_counts_handler)
    bot.register_command_handler('/chatcountgrid', visualize_group_num_counts_grid_handler)
    bot.register_command_handler('/chattimeseries', visualize_group_time_series_handler)
    bot.register_command_handler('/chatmatchgraph', visualize_chat_match_graph_handler)
    
    # Personal visualizations
    bot.register_command_handler('/mycounthist', visualize_my_num_counts_handler)
    bot.register_command_handler('/mycountgrid', visualize_my_num_counts_grid_handler)
    bot.register_command_handler('/mytimeseries', visualize_my_time_series_handler)
    bot.register_command_handler('/mymatchgraph', visualize_my_match_graph_handler)

    # Admin commands
    bot.register_command_handler('/invokejob', invoke_job_handler)
    bot.register_command_handler('/export', export_handler)
    bot.register_command_handler('/import', import_handler)
    bot.register_message_handler(number_parser_handler)

    # Register Tasks
    jobs_config = config.scheduled_jobs

    daily_backup_task = DailyBackupTask(admin_service, config)

    daily_backup_config = jobs_config.get("daily_backup")
    scheduler.register_recurring_job(daily_backup_task.run_daily_backup(),
                                     int(daily_backup_config.get("h")),
                                     int(daily_backup_config.get("m")),
                                     int(daily_backup_config.get("s")),
                                     tz)

    stats_chat_ids = getattr(config, 'daily_stats_chat_ids', None)
    for chat_id in stats_chat_ids:
        daily_stats_task = DailyStatsTask(bot, repositories['stats'], chat_id,
                                          visualization_service, stats_view_service, admin_service, config)

        midnight_stats_config = jobs_config.get("midnight_stats")
        scheduler.register_recurring_job(daily_stats_task.run_midnight_stats,
                                         int(midnight_stats_config.get("h")),
                                         int(midnight_stats_config.get("m")),
                                         int(midnight_stats_config.get("s")),
                                         tz)

        midday_stats_config = jobs_config.get("midday_stats")
        scheduler.register_recurring_job(daily_stats_task.run_midday_stats,
                                         int(midday_stats_config.get("h")),
                                         int(midday_stats_config.get("m")),
                                         int(midday_stats_config.get("s")),
                                         tz)

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
