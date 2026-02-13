import re
import logging
from datetime import datetime, timezone, timedelta

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
    db = ctx['db']
    
    regex_pattern = config.message_regex
    if not regex_pattern:
        return

    match = re.search(regex_pattern, message.text)
    if match:
        try:
            number = int(match.group(1))
            if 0 <= number <= 100:
                logger.info(f"Parsed number {number} from message: {message.text}")

                try:
                    if message.date:
                        ts = datetime.fromtimestamp(message.date, tz=timezone.utc)
                    else:
                        ts = datetime.now(timezone.utc)

                    # Calculate Singapore Time (GMT+8)
                    sgt_timezone = timezone(timedelta(hours=8))
                    ts_sgt = ts.astimezone(sgt_timezone)
                    today_date = ts_sgt.date()

                    thread_id = message.thread_id
                    user_name = message.first_name
                    if message.last_name:
                        user_name += f" {message.last_name}"
                    if not user_name:
                        user_name = "Unknown"

                    # Prepare queries for atomic transaction
                    
                    # 1. Insert into number_logs
                    insert_log_query = """
                    INSERT INTO number_logs (chat_id, thread_id, user_id, user_name, ts, number)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    log_params = (
                        message.chat_id,
                        thread_id,
                        message.user_id,
                        user_name,
                        ts,
                        number
                    )

                    # 2. Insert into user_attendance (idempotent)
                    insert_attendance_query = """
                    INSERT INTO user_attendance (user_id, chat_id, log_date)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, chat_id, log_date) DO NOTHING
                    """
                    attendance_params = (
                        message.user_id,
                        message.chat_id,
                        today_date
                    )

                    # 3. Upsert into user_number_counts
                    upsert_counts_query = """
                    INSERT INTO user_number_counts (user_id, chat_id, number, count)
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (user_id, chat_id, number) 
                    DO UPDATE SET count = user_number_counts.count + 1
                    """
                    counts_params = (
                        message.user_id,
                        message.chat_id,
                        number
                    )

                    # Execute all in a single transaction
                    db.execute_transaction([
                        (insert_log_query, log_params),
                        (insert_attendance_query, attendance_params),
                        (upsert_counts_query, counts_params)
                    ])
                    
                    logger.info(f"Logged number {number}, attendance, and count for user {message.user_id}.")

                except Exception as e:
                    logger.error(f"Failed to log number/attendance to database: {e}")

                await bot.set_message_reaction(message.chat_id, message.message_id, '👍')

                reply_template = config.reply_message
                if reply_template:
                    reply_text = reply_template.format(number=number)
                    await bot.send_reply(message.chat_id, message.message_id, reply_text)

        except (ValueError, IndexError):
            pass

async def stats_handler(message, ctx):
    bot = ctx['bot']
    db = ctx['db']
    
    user_id = message.user_id
    chat_id = message.chat_id

    # 1. Fetch Stats (Count & Average) using user_number_counts
    query = """
    SELECT sum(count), sum(number * count)
    FROM user_number_counts
    WHERE user_id = %s AND chat_id = %s
    """
    
    try:
        result = db.fetch_one(query, (user_id, chat_id))
        
        if result and result[0] is not None and result[0] > 0:
            count = result[0]
            total_sum = result[1]
            average = round(total_sum / count, 2)
            response = f"Stats for {message.first_name} in this chat:\nTotal numbers: {count}\nAverage: {average}"
        else:
            response = f"No numbers recorded for {message.first_name} in this chat yet."
        
        # 2. Calculate Streak Dynamically
        attendance_query = """
        SELECT log_date 
        FROM user_attendance 
        WHERE user_id = %s AND chat_id = %s 
        ORDER BY log_date DESC 
        LIMIT 365
        """
        attendance_rows = db.fetch_all(attendance_query, (user_id, chat_id))
        
        current_streak = 0
        if attendance_rows:
            sgt_timezone = timezone(timedelta(hours=8))
            today = datetime.now(sgt_timezone).date()
            yesterday = today - timedelta(days=1)
            
            dates = [row[0] for row in attendance_rows]
            
            if dates[0] == today or dates[0] == yesterday:
                current_streak = 1
                expected_date = dates[0] - timedelta(days=1)
                
                for i in range(1, len(dates)):
                    if dates[i] == expected_date:
                        current_streak += 1
                        expected_date -= timedelta(days=1)
                    else:
                        break
            else:
                current_streak = 0

        if current_streak > 0:
            response += f"\nCurrent Streak: {current_streak} days 🔥"

        await bot.send_message(message.chat_id, response)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        await bot.send_message(message.chat_id, "An error occurred while fetching your stats.")