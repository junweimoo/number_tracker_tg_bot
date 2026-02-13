import asyncio
import os
from bot import TelegramBot
from handlers import start_handler, echo_handler

if __name__ == '__main__':
    TOKEN = os.environ.get("BOT_TOKEN")
    
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set.")
        exit(1)
    
    bot = TelegramBot(TOKEN)
    
    bot.register_command_handler('/start', start_handler)
    bot.register_message_handler(echo_handler)
    
    try:
        asyncio.run(bot.start_polling())
    except KeyboardInterrupt:
        print("Bot stopped.")