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
    config = ctx['config']

    if str(message.chat_id) not in config.tracked_chat_ids:
        return
    
    regex_pattern = config.message_regex
    if not regex_pattern:
        return

    match = re.search(regex_pattern, message.text)
    if match:
        try:
            number = int(match.group(1))
            if 0 <= number <= 100:
                logger.info(f"Parsed number {number} from message: {message.text}")

                service = ctx['number_log_service']

                lock_mgr = ctx['lock_manager']
                lock = await lock_mgr.get_lock(message.chat_id)

                async with lock:
                    await service.process_number(message, number)

        except (ValueError, IndexError):
            pass

async def stats_handler(message, ctx):
    bot = ctx['bot']

    service = ctx['stats_view_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        response = await service.get_user_stats_summary(message)
    
    await bot.send_message(message.chat_id, response)

async def leaderboard_handler(message, ctx):
    bot = ctx['bot']

    service = ctx['stats_view_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        response = await service.get_leaderboard(message.chat_id)

    await bot.send_message(message.chat_id, response)

async def visualize_num_counts_handler(message, ctx):
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        image_buf = service.generate_number_count_visualization(message.chat_id)
    
    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Number Frequency Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for visualization.")


async def visualize_time_series_handler(message, ctx):
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        image_buf = service.generate_time_series_visualization(message.chat_id)

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Time Series Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for visualization.")

async def invoke_job_handler(message, ctx):
    bot = ctx['bot']
    config = ctx['config']
    
    # Check if user is developer
    if str(message.user_id) not in config.developer_user_ids:
        await bot.send_message(message.chat_id, "You are not authorized to use this command.")
        return

    args = message.text.split()
    if len(args) < 2:
        await bot.send_message(message.chat_id, "Usage: /invokejob {job_name}")
        return

    job_name = args[1]
    service = ctx['admin_service']

    try:
        await service.invoke_job(message.chat_id, job_name)
    except ValueError as e:
        await bot.send_message(message.chat_id, str(e))
    except Exception as e:
        logger.error(f"Error invoking job {job_name}: {e}", exc_info=True)
        await bot.send_message(message.chat_id, f"Error invoking job {job_name}.")