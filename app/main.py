import asyncio
import os
from bot import TelegramBot
from config import Config
from database.database_core import Database
from database.database_schema import SchemaManager
from handlers import start_handler, echo_handler, number_parser_handler, stats_handler
from services.number_log_service import NumberLogService
from services.stats_view_service import StatsViewService
from repository.number_log_repository import NumberLogRepository
from repository.attendance_repository import AttendanceRepository
from repository.stats_repository import StatsRepository

if __name__ == '__main__':
    # Initialize env variables
    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set.")
        exit(1)

    # Initialize configs
    try:
        config = Config('config.json')
    except Exception as e:
        print(f"Error loading config: {e}")
        exit(1)

    # Initialize database
    try:
        db = Database()
        schema_manager = SchemaManager(db)
        schema_manager.init_db()
    except Exception as e:
        print(f"Error initializing database: {e}")
        exit(1)

    # Initialize Repositories
    repositories = {
        'number_log': NumberLogRepository(db),
        'attendance': AttendanceRepository(db),
        'stats': StatsRepository(db)
    }

    # Initialize Services
    number_log_service = NumberLogService(db, config, repositories)
    stats_view_service = StatsViewService(db, config, repositories)

    context = {
        'config': config,
        'db': db,
        'number_log_service': number_log_service,
        'stats_view_service': stats_view_service
    }
    
    bot = TelegramBot(TOKEN, context=context)

    number_log_service.set_bot(bot)
    stats_view_service.set_bot(bot)

    # Register handlers
    bot.register_command_handler('/start', start_handler)
    bot.register_command_handler('/stats', stats_handler)

    bot.register_message_handler(number_parser_handler)
    bot.register_message_handler(echo_handler)

    # Start the bot
    try:
        asyncio.run(bot.start_polling())
    except KeyboardInterrupt:
        print("Bot stopped.")
    finally:
        db.close_all_connections()