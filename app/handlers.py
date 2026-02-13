import re
import logging

logger = logging.getLogger(__name__)

async def start_handler(message, ctx):
    bot = ctx['bot']
    await bot.send_message(message.chat_id, "Hello! I am your bot.")

async def echo_handler(message, ctx):
    bot = ctx['bot']
    await bot.send_message(message.chat_id, f"You said: {message.text}")

async def number_parser_handler(message, ctx):
    bot = ctx['bot']
    config = ctx['config']
    
    regex_pattern = config.message_regex
    if not regex_pattern:
        return

    match = re.search(regex_pattern, message.text)
    if match:
        try:
            number = int(match.group(1))
            if 0 <= number <= 100:
                logger.info(f"Parsed number {number} from message: {message.text}")

                await bot.set_message_reaction(message.chat_id, message.message_id, '👍')
                
                reply_template = config.reply_message
                if reply_template:
                    reply_text = reply_template.format(number=number)
                    await bot.send_reply(message.chat_id, message.message_id, reply_text)
        except (ValueError, IndexError):
            pass