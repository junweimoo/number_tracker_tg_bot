import logging
from datetime import datetime, timezone, timedelta
from collections import deque
from service.matches import (
    MatchContext,
    SameNumberMatchStrategy,
    SelfSameNumberMatchStrategy,
    ReverseNumberMatchStrategy,
    SelfReverseNumberMatchStrategy,
    SumTargetMatchStrategy,
    SelfSumTargetMatchStrategy)

logger = logging.getLogger(__name__)

class StreakInfo:
    def __init__(self, matches=0, hits=0):
        self.matches = matches
        self.hits = hits

    @property
    def total(self):
        return self.matches + self.hits

class UserInfo:
    def __init__(self, fullname, handle):
        self.fullname = fullname
        self.handle = handle

class CacheData:
    def __init__(self, user_info_cache, user_log_cache, chat_log_cache):
        self.user_info_cache = user_info_cache
        self.user_log_cache = user_log_cache
        self.chat_log_cache = chat_log_cache

class NumberLogService:
    def __init__(self, db, config, repositories, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.number_log_repo = repositories['number_log']
        self.attendance_repo = repositories['attendance']
        self.stats_repo = repositories['stats']
        self.match_log_repo = repositories['match_log']

        # Cache: user_id -> UserInfo
        self.user_info_cache = {}
        
        # Cache: (user_id, chat_id) -> deque of (number, timestamp, message_id), max length = 10
        self.user_log_cache = {}
        
        # Cache: chat_id -> deque of (user_id, number, timestamp, message_id), max length = 10
        self.chat_log_cache = {}
        
        # Cache: chat_id -> StreakInfo
        self.streak_info_cache = {}

        # Initialize Match Strategies
        self.match_strategies = [
            SameNumberMatchStrategy(),
            SelfSameNumberMatchStrategy(),
            ReverseNumberMatchStrategy(),
            SelfReverseNumberMatchStrategy(),
            SumTargetMatchStrategy(100),
            SelfSumTargetMatchStrategy(100),
        ]

    def set_bot(self, bot):
        self.bot = bot

    def duplicate_check(self, message, number, ts):
        if message.user_id in self.config.developer_user_ids:
            return False
        user_log_cache_key = (message.user_id, message.chat_id)
        if user_log_cache_key in self.user_log_cache:
            history = self.user_log_cache[user_log_cache_key]
            if history:
                last_number, last_ts, last_msg_id = history[-1]
                time_diff = (ts - last_ts).total_seconds()
                if last_number == number and time_diff < 60:
                    logger.info(
                        f"Ignored duplicate number {number} for user {message.user_id} "
                        f"(last logged {time_diff:.1f}s ago).")
                    return True
        return False

    async def process_number(self, message, number):
        try:
            # Timestamp Calculation
            if message.date:
                ts = datetime.fromtimestamp(message.date, tz=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Duplicate Check (Debounce)
            if self.duplicate_check(message, number, ts):
                return

            # Calculate Singapore Time (GMT+8) for attendance
            sgt_timezone = timezone(timedelta(hours=8))
            ts_sgt = ts.astimezone(sgt_timezone)
            today_date = ts_sgt.date()

            # User Name Logic
            user_name = message.first_name
            if message.last_name:
                user_name += f" {message.last_name}"
            if not user_name:
                user_name = "Unknown"
            
            # Update User Info Cache (Before processing matches so we have current user info)
            self.user_info_cache[message.user_id] = UserInfo(user_name, message.username)

            # --- Match Logic ---
            # Prepare Cache Data Object
            cache_data = CacheData(
                self.user_info_cache,
                self.user_log_cache,
                self.chat_log_cache
            )

            match_context = MatchContext()
            is_any_match = False

            for strategy in self.match_strategies:
                if strategy.has_conflict(match_context):
                    continue
                result = strategy.check(message, number, cache_data)
                if result:
                    match_context.add_match(
                        result.match_type,
                        message.user_id,
                        result.matched_user_id,
                        result.matched_number,
                        result.matched_message_id,
                        result.reply_text
                    )
                    is_any_match = True
                    logger.info(f"Match detected! {result.reply_text} - User {message.user_id} matched user {result.matched_user_id} in chat {message.chat_id}.")
            # ---

            # --- Streak Logic ---
            chat_id = message.chat_id
            current_matches = len(match_context.matches)
            current_hits = 0 # Placeholder for now
            
            if (current_matches + current_hits) > 0:
                if chat_id not in self.streak_info_cache:
                    self.streak_info_cache[chat_id] = StreakInfo()
                
                self.streak_info_cache[chat_id].matches += current_matches
                self.streak_info_cache[chat_id].hits += current_hits
            else:
                self.streak_info_cache[chat_id] = StreakInfo(0, 0)

            streak_total = self.streak_info_cache[chat_id].total
            # ---

            # --- Transaction Logic ---
            transaction_queries = []
            
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
            transaction_queries.append((insert_log_query, log_params))

            # B. Insert into user_attendance (idempotent)
            insert_attendance_query = self.attendance_repo.get_insert_query()
            attendance_params = (
                message.user_id,
                message.chat_id,
                today_date
            )
            transaction_queries.append((insert_attendance_query, attendance_params))

            # C. Upsert into user_number_counts
            upsert_counts_query = self.stats_repo.get_upsert_counts_query()
            counts_params = (
                message.user_id,
                message.chat_id,
                number
            )
            transaction_queries.append((upsert_counts_query, counts_params))

            # D. Upsert into user_daily_number_counts
            upsert_daily_counts_query = self.stats_repo.get_upsert_daily_counts_query()
            daily_counts_params = (
                message.user_id,
                message.chat_id,
                today_date,
                number
            )
            transaction_queries.append((upsert_daily_counts_query, daily_counts_params))

            # E. Insert into match_logs
            if is_any_match:
                insert_match_query = self.match_log_repo.get_insert_query()
                upsert_match_counts_query = self.match_log_repo.get_upsert_match_counts_query()
                
                for match in match_context.matches:
                    match_type, msg_user_id, matched_user_id, matched_number, matched_msg_id, _ = match

                    user1_info = self.user_info_cache.get(msg_user_id)
                    user1_name = user1_info.fullname if user1_info else "Unknown"
                    
                    user2_info = self.user_info_cache.get(matched_user_id)
                    user2_name = user2_info.fullname if user2_info else "Unknown"

                    # Log the match
                    match_params = (
                        message.chat_id,
                        message.thread_id,
                        msg_user_id,
                        user1_name,
                        matched_user_id,
                        user2_name,
                        ts,
                        match_type.value,
                        number,
                        matched_number
                    )
                    transaction_queries.append((insert_match_query, match_params))
                    
                    # Upsert match counts
                    match_counts_params = (
                        message.chat_id,
                        message.thread_id,
                        msg_user_id,
                        matched_user_id,
                        match_type.value
                    )
                    transaction_queries.append((upsert_match_counts_query, match_counts_params))

            # Execute Transaction
            self.db.execute_transaction(transaction_queries)
            # ---

            # --- Update Caches ---
            # Update User Cache
            user_log_cache_key = (message.user_id, message.chat_id)
            if user_log_cache_key not in self.user_log_cache:
                self.user_log_cache[user_log_cache_key] = deque(maxlen=10)
            self.user_log_cache[user_log_cache_key].append((number, ts, message.message_id))
            
            # Update Chat Cache
            chat_log_cache_key = message.chat_id
            if chat_log_cache_key not in self.chat_log_cache:
                self.chat_log_cache[chat_log_cache_key] = deque(maxlen=10)
            self.chat_log_cache[chat_log_cache_key].append((message.user_id, number, ts, message.message_id))
            # ---
            
            logger.info(f"Logged number {number}, attendance, and count for user {message.user_id}.")
            
            # Send Feedback (Reaction & Reply)
            if self.bot:
                reaction = '🔥' if is_any_match else '👍'
                await self.bot.set_message_reaction(message.chat_id, message.message_id, reaction)
                
                # Send generic detection reply
                reply_template = self.config.reply_message
                if reply_template:
                    reply_text = reply_template.format(number=number)
                    await self.bot.send_reply(message.chat_id, message.message_id, reply_text)
                
                # Send specific match replies to the matched message IDs
                for match in match_context.matches:
                    _, _, _, _, matched_msg_id, match_reply_text = match
                    if matched_msg_id:
                        await self.bot.send_reply(message.chat_id, matched_msg_id, match_reply_text)
                
                # Send Streak message if total >= 2
                if streak_total >= 2:
                    streak_msg = f"Streak: {streak_total}!"
                    await self.bot.send_message(message.chat_id, streak_msg)
            # ---

        except Exception as e:
            logger.error(f"Failed to process number log: {e}", exc_info=True)
