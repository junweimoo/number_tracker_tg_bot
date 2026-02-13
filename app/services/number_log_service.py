import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class NumberLogService:
    def __init__(self, db, config, repositories, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.number_log_repo = repositories['number_log']
        self.attendance_repo = repositories['attendance']
        self.stats_repo = repositories['stats']
        self.cache = {}

    def set_bot(self, bot):
        self.bot = bot

    async def process_number(self, message, number):
        try:
            # 1. Timestamp Calculation
            if message.date:
                ts = datetime.fromtimestamp(message.date, tz=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Calculate Singapore Time (GMT+8) for attendance
            sgt_timezone = timezone(timedelta(hours=8))
            ts_sgt = ts.astimezone(sgt_timezone)
            today_date = ts_sgt.date()

            # 2. User Name Logic
            user_name = message.first_name
            if message.last_name:
                user_name += f" {message.last_name}"
            if not user_name:
                user_name = "Unknown"

            # 3. Prepare Queries for Atomic Transaction
            
            # A. Insert into number_logs
            insert_log_query = self.number_log_repo.get_insert_query()
            log_params = (
                message.chat_id,
                message.thread_id,
                message.user_id,
                user_name,
                ts,
                number
            )

            # B. Insert into user_attendance (idempotent)
            insert_attendance_query = self.attendance_repo.get_insert_query()
            attendance_params = (
                message.user_id,
                message.chat_id,
                today_date
            )

            # C. Upsert into user_number_counts
            upsert_counts_query = self.stats_repo.get_upsert_counts_query()
            counts_params = (
                message.user_id,
                message.chat_id,
                number
            )

            # 4. Execute Transaction
            self.db.execute_transaction([
                (insert_log_query, log_params),
                (insert_attendance_query, attendance_params),
                (upsert_counts_query, counts_params)
            ])
            
            logger.info(f"Logged number {number}, attendance, and count for user {message.user_id}.")
            
            # 5. Send Feedback (Reaction & Reply)
            if self.bot:
                await self.bot.set_message_reaction(message.chat_id, message.message_id, '👍')
                
                reply_template = self.config.reply_message
                if reply_template:
                    reply_text = reply_template.format(number=number)
                    await self.bot.send_reply(message.chat_id, message.message_id, reply_text)

        except Exception as e:
            logger.error(f"Failed to process number log: {e}", exc_info=True)
