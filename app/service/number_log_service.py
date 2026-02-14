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
from service.hits import HitContext, HitSpecificNumberStrategy, HitType

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
        self.user_repo = repositories['user']

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
        
        # Initialize Hit Strategies from Config
        self.hit_strategies = []
        hit_numbers = self.config.hit_numbers
        for number_str, details in hit_numbers.items():
            try:
                target_number = int(number_str)
                reply_text = details.get('reply')
                reaction = details.get('reaction')
                self.hit_strategies.append(
                    HitSpecificNumberStrategy(target_number, reply_text, reaction, self.number_log_repo)
                )
            except ValueError:
                logger.warning(f"Invalid hit number in config: {number_str}")

    def set_bot(self, bot):
        self.bot = bot

    def _duplicate_check(self, message, number, ts):
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

    async def _mark_user_attendance(self, message, today_date, yesterday_date):
        """
        Updates the user's streak and attendance if this is their first log of the day.
        Sends a reply with the updated streak info.
        """
        # A. Update user_data
        update_streak_query = self.user_repo.get_update_streak_query()
        streak_params = (
            message.user_id,
            message.chat_id,
            today_date,
            today_date,
            yesterday_date,
            today_date
        )

        # B. Insert into user_attendance (idempotent)
        insert_attendance_query = self.attendance_repo.get_insert_query()
        attendance_params = (
            message.user_id,
            message.chat_id,
            today_date
        )

        try:
            # Execute streak update and attendance insert in a single transaction
            self.db.execute_transaction([
                (update_streak_query, streak_params),
                (insert_attendance_query, attendance_params)
            ])
            
            # Fetch updated streak and attendance history for reply
            # We need to fetch the current streak from user_data
            streak_query = self.user_repo.get_fetch_streak_query()
            streak_result = self.db.fetch_one(streak_query, (message.user_id, message.chat_id))
            current_user_streak = streak_result[0] if streak_result else 0
            
            # Fetch last 7 days attendance
            attendance_rows = self.attendance_repo.get_recent_attendance(message.user_id, message.chat_id, limit=7)
            attendance_dates = [row[0] for row in attendance_rows]
            
            # Format attendance string
            attendance_str = ""
            for i in range(6, -1, -1):
                check_date = today_date - timedelta(days=i)
                if check_date in attendance_dates:
                    attendance_str += "✅ "
                else:
                    attendance_str += "⬜ "
            
            streak_reply = f"Your Streak: {current_user_streak} days 🔥\n{attendance_str}"
            await self.bot.send_reply(message.chat_id, message.message_id, streak_reply)

        except Exception as e:
            logger.error(f"Failed to update user streak/attendance: {e}")

    async def process_number(self, message, number):
        try:
            # Timestamp Calculation
            if message.date:
                ts = datetime.fromtimestamp(message.date, tz=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Duplicate Check (Debounce)
            if self._duplicate_check(message, number, ts):
                return

            # Calculate Singapore Time (GMT+8) for attendance
            sgt_timezone = timezone(timedelta(hours=8))
            ts_sgt = ts.astimezone(sgt_timezone)
            today_date = ts_sgt.date()
            yesterday_date = today_date - timedelta(days=1)

            # User Name Logic
            user_name = message.first_name
            if message.last_name:
                user_name += f" {message.last_name}"
            if not user_name:
                user_name = "Unknown"

            # Update User Info Cache (Before processing matches so we have current user info)
            self.user_info_cache[message.user_id] = UserInfo(user_name, message.username)

            # Prepare Cache Data Object
            cache_data = CacheData(
                self.user_info_cache,
                self.user_log_cache,
                self.chat_log_cache
            )

            # --- Hit Logic ---
            hit_context = HitContext()
            is_any_hit = False

            for strategy in self.hit_strategies:
                result = strategy.check(message, number, cache_data)
                if result:
                    hit_context.add_hit(result.hit_type, result.hit_number, result.reply_text, result.react_emoji)
                    is_any_hit = True
                    logger.info(f"Hit detected! - User {message.user_id} in chat {message.chat_id}.")
            # ---

            # --- Match Logic ---
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
                    logger.info(f"Match detected! - User {message.user_id} matched user {result.matched_user_id} in chat {message.chat_id}.")
            # ---

            # --- Streak Logic ---
            chat_id = message.chat_id
            current_matches = len(match_context.matches)
            current_hits = len(hit_context.hits)

            if (current_matches + current_hits) > 0:
                if chat_id not in self.streak_info_cache:
                    self.streak_info_cache[chat_id] = StreakInfo()
                self.streak_info_cache[chat_id].matches += current_matches
                self.streak_info_cache[chat_id].hits += current_hits
            else:
                self.streak_info_cache[chat_id] = StreakInfo(0, 0)

            streak_total = self.streak_info_cache[chat_id].total
            # ---

            # --- Update User Streak ---
            user_log_cache_key = (message.user_id, message.chat_id)
            last_login_date = None
            if user_log_cache_key in self.user_log_cache:
                history = self.user_log_cache[user_log_cache_key]
                if history:
                    _, last_ts, _ = history[-1]
                    last_ts_sgt = last_ts.astimezone(sgt_timezone)
                    last_login_date = last_ts_sgt.date()

            should_mark_attendance = False
            if last_login_date == today_date:
                should_mark_attendance = False
            else:
                last_login_query = self.user_repo.get_last_login_date_query()
                last_login_result = self.db.fetch_one(last_login_query, (message.user_id, message.chat_id))
                db_last_login_date = last_login_result[0] if last_login_result else None
                if db_last_login_date != today_date:
                    should_mark_attendance = True

            if should_mark_attendance:
                await self._mark_user_attendance(message, today_date, yesterday_date)
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

            # F. Update user_data bitmap
            upsert_bitmap_query = self.user_repo.get_upsert_user_bitmap_query()
            bitmap_params = (
                message.user_id,
                message.chat_id,
                user_name,
                number,
                number
            )
            transaction_queries.append((upsert_bitmap_query, bitmap_params))

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
                # Send hit replies
                hit_reaction = None
                for hit in hit_context.hits:
                    _, _, _, current_reaction = hit
                    if current_reaction:
                        hit_reaction = current_reaction
                if hit_reaction:
                    await self.bot.set_message_reaction(message.chat_id, message.message_id, hit_reaction)
                for hit in hit_context.hits:
                    _, _, hit_reply_text, _ = hit
                    if hit_reply_text:
                        await self.bot.send_reply(message.chat_id, message.message_id, hit_reply_text)

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
