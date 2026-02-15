import re
import logging
import os
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def _get_start_date(message_text, config):
    """
    Determines the start date for filtering data based on the message text.

    Args:
        message_text (str): The text of the message.
        config: Configuration object.

    Returns:
        date: The start date, or None if 'alltime' is specified.
    """
    if "alltime" in message_text:
        return None
    
    tz = timezone(timedelta(hours=config.timezone_gmt))
    return datetime.now(tz).date()

async def start_handler(message, ctx):
    """
    Handles the /start command.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    await bot.send_message(message.chat_id, "Hello! I am your bot.")

async def echo_handler(message, ctx):
    """
    Echoes back the user's message.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    await bot.send_message(message.chat_id, f"You said: {message.text}")

async def number_parser_handler(message, ctx):
    """
    Parses numbers from incoming messages and processes them if they match the configured regex.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
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
    """
    Handles the /stats command, providing a summary of the user's statistics.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']

    service = ctx['stats_view_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        response = await service.get_user_stats_summary(message)

        duration = time.perf_counter() - start_time
        logger.info(f"Fetched user stats in {duration:.6f}s")
    
    await bot.send_html(message.chat_id, response)

async def leaderboard_handler(message, ctx):
    """
    Handles the /leaderboard command, providing the chat's leaderboard.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']

    service = ctx['stats_view_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        response = await service.get_leaderboard(message.chat_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Fetched leaderboard in {duration:.6f}s")

    await bot.send_html(message.chat_id, response)

async def my_remaining_nums_handler(message, ctx):
    """
    Handles the /myremainingnums command, providing a list of remaining numbers for users in the chat.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['stats_view_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        response = await service.get_user_nums_remaining_in_chat(message.chat_id, message.user_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Fetched chat remaining numbers in {duration:.6f}s")

    await bot.send_html(message.chat_id, response)

async def visualize_group_num_counts_handler(message, ctx):
    """
    Handles the /chatcounthist command, sending a bar chart of number frequencies for the chat.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']
    config = ctx['config']

    start_date = _get_start_date(message.text, config)

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_number_count_visualization(message.chat_id, start_date=start_date)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated number count histogram in {duration:.6f}s")
    
    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Number Frequency Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for visualization.")

async def visualize_my_num_counts_handler(message, ctx):
    """
    Handles the /mycounthist command, sending a bar chart of number frequencies for the user.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']
    config = ctx['config']

    start_date = _get_start_date(message.text, config)

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_number_count_visualization(message.chat_id, user_id=message.user_id, start_date=start_date)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated number count histogram in {duration:.6f}s")
    
    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Your Number Frequency Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for your visualization.")

async def visualize_group_num_counts_grid_handler(message, ctx):
    """
    Handles the /chatcountgrid command, sending a grid visualization of number frequencies for the chat.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']
    config = ctx['config']

    start_date = _get_start_date(message.text, config)

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_number_count_visualization_grid(message.chat_id, start_date=start_date)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated number count grid in {duration:.6f}s")
    
    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Number Frequency Grid Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for visualization.")

async def visualize_my_num_counts_grid_handler(message, ctx):
    """
    Handles the /mycountgrid command, sending a grid visualization of number frequencies for the user.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']
    config = ctx['config']

    start_date = _get_start_date(message.text, config)

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_number_count_visualization_grid(message.chat_id, user_id=message.user_id, start_date=start_date)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated number count grid in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Your Number Frequency Grid Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for your visualization.")

async def visualize_group_time_series_handler(message, ctx):
    """
    Handles the /chattimeseries command, sending a time series visualization of chat activity.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_time_series_visualization(message.chat_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated time series graph in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Time Series Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for visualization.")

async def visualize_my_time_series_handler(message, ctx):
    """
    Handles the /mytimeseries command, sending a time series visualization of the user's activity.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_time_series_visualization(message.chat_id, user_id=message.user_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated time series graph in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Your Time Series Visualization")
    else:
        await bot.send_message(message.chat_id, "No data available for your visualization.")

async def visualize_chat_match_graph_handler(message, ctx):
    """
    Handles the /chatmatchgraph command, sending a network graph of user matches in the chat.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_match_graph_visualization(message.chat_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated match graph in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Chat Match Graph Visualization")
    else:
        await bot.send_message(message.chat_id, "No match data available for visualization.")

async def visualize_my_match_graph_handler(message, ctx):
    """
    Handles the /mymatchgraph command, sending a network graph of matches for the user.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.generate_match_graph_visualization(message.chat_id, user_id=message.user_id)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated match graph in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption="Your Match Graph Visualization")
    else:
        await bot.send_message(message.chat_id, "No match data available for your visualization.")

async def visualize_personal_profile_handler(message, ctx):
    """
    Handles the /myprofile command, sending a comprehensive personal statistics dashboard.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    service = ctx['visualization_service']

    lock_mgr = ctx['lock_manager']
    lock = await lock_mgr.get_lock(message.chat_id)

    async with lock:
        start_time = time.perf_counter()

        image_buf = await service.personal_stats_visualization(message.chat_id, message.user_id, message.first_name)

        duration = time.perf_counter() - start_time
        logger.info(f"Generated profile in {duration:.6f}s")

    if image_buf:
        await bot.send_photo(message.chat_id, image_buf, caption=f"Personal Profile for {message.first_name}")
    else:
        await bot.send_message(message.chat_id, "No data available for your profile.")

async def invoke_job_handler(message, ctx):
    """
    Handles the /invokejob command for administrative tasks.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
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

async def export_handler(message, ctx):
    """
    Handles the /export command for exporting number logs.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    config = ctx['config']
    
    if str(message.user_id) not in config.developer_user_ids:
        await bot.send_message(message.chat_id, "You are not authorized to use this command.")
        return

    service = ctx['admin_service']
    file_path = "number_logs_export.csv"
    
    try:
        count = await service.export_number_logs(file_path)
        await bot.send_message(message.chat_id, f"Exported {count} logs to {file_path}")
    except Exception as e:
        logger.error(f"Error exporting logs: {e}", exc_info=True)
        await bot.send_message(message.chat_id, f"Error exporting logs: {e}")

async def import_handler(message, ctx):
    """
    Handles the /import command for importing number logs.

    Args:
        message: The message object.
        ctx (dict): The context dictionary.
    """
    bot = ctx['bot']
    config = ctx['config']
    
    if str(message.user_id) not in config.developer_user_ids:
        await bot.send_message(message.chat_id, "You are not authorized to use this command.")
        return

    args = message.text.split()
    clear_db = "--clear" in args
    
    service = ctx['admin_service']
    file_path = "number_logs_export.csv"
    
    if not os.path.exists(file_path):
        await bot.send_message(message.chat_id, f"Import file not found: {file_path}")
        return

    try:
        await bot.send_message(message.chat_id, f"Starting import (clear_db={clear_db})...")

        start_time = time.perf_counter()
        count = await service.import_number_logs(file_path, clear_db=clear_db)
        duration = time.perf_counter() - start_time

        await bot.send_message(message.chat_id, f"Successfully imported {count} logs in {duration:.4f}s.")
    except Exception as e:
        logger.error(f"Error importing logs: {e}", exc_info=True)
        await bot.send_message(message.chat_id, f"Error importing logs: {e}")
