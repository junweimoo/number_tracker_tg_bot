async def start_handler(message, ctx):
    bot = ctx['bot']
    await bot.send_message(message.chat_id, "Hello! I am your bot.")

async def echo_handler(message, ctx):
    bot = ctx['bot']
    await bot.send_message(message.chat_id, f"You said: {message.text}")