import asyncio
import os
from bot import TelegramBot
from config import Config
from handlers import start_handler, echo_handler, number_parser_handler

if __name__ == '__main__':
    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set.")
        exit(1)

    try:
        config = Config('config.json')
    except Exception as e:
        print(f"Error loading config: {e}")
        exit(1)

    context = {'config': config}
    bot = TelegramBot(TOKEN, context=context)
    
    bot.register_command_handler('/start', start_handler)

    bot.register_message_handler(number_parser_handler)
    # bot.register_message_handler(echo_handler)
    
    try:
        asyncio.run(bot.start_polling())
    except KeyboardInterrupt:
        print("Bot stopped.")