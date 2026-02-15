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
    SelfSumTargetMatchStrategy,
    ABCSumMatchStrategy,
    DoubleMatchStrategy,
    SelfDoubleMatchStrategy,
    HalfMatchStrategy,
    SelfHalfMatchStrategy,
    StepMatchStrategy,
    SelfStepMatchStrategy,
    SquareMatchStrategy,
    SelfSquareMatchStrategy,
    SqrtMatchStrategy,
    SelfSqrtMatchStrategy,
    ArithmeticProgressionMatchStrategy,
    GeometricProgressionMatchStrategy,
    DigitSumMatchStrategy)
from service.hits import HitContext, HitSpecificNumberStrategy, HitCloseNumberStrategy

logger = logging.getLogger(__name__)

class StreakInfo:
    def __init__(self, matches=0, hits=0):
        self.matches = matches
        self.hits = hits

    @property
    def total(self):
        return self.matches + self.hits

class UserInfo:
    def __init__(self, chat_id, thread_id, user_id, user_name, user_handle, numbers_bitmap=0,
                 last_login_date=None, current_streak=0, extend_info=None):
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.user_id = user_id
        self.user_name = user_name
        self.user_handle = user_handle
        self.numbers_bitmap = numbers_bitmap
        self.last_login_date = last_login_date
        self.current_streak = current_streak
        self.extend_info = extend_info

class CacheData:
    def __init__(self, user_info_cache, user_log_cache, chat_log_cache):
        self.user_info_cache = user_info_cache
        self.user_log_cache = user_log_cache
        self.chat_log_cache = chat_log_cache

class NumberLogService:
    def __init__(self, db, config, repositories, transaction_queue=None, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.number_log_repo = repositories['number_log']
        self.attendance_repo = repositories['attendance']
        self.stats_repo = repositories['stats']
        self.match_log_repo = repositories['match_log']
        self.user_repo = repositories['user']
        
        self.transaction_queue = transaction_queue

        # Cache: (user_id, chat_id) -> UserInfo
        self.user_info_cache = {}
        
        # Cache: (user_id, chat_id) -> deque of (number, timestamp, message_id), max length = 10
        self.user_log_cache = {}
        
        # Cache: chat_id -> deque of (user_id, number, timestamp, message_id), max length = 10
        self.chat_log_cache = {}
        
        # Cache: chat_id -> StreakInfo
        self.streak_info_cache = {}

        # Initialize Match Strategies
        self.match_strategies = [
            SameNumberMatchStrategy(self.config),
            SelfSameNumberMatchStrategy(self.config),
            ReverseNumberMatchStrategy(self.config),
            SelfReverseNumberMatchStrategy(self.config),
            SumTargetMatchStrategy(100, self.config),
            SelfSumTargetMatchStrategy(100, self.config),
            ABCSumMatchStrategy(self.config),
            DoubleMatchStrategy(self.config),
            SelfDoubleMatchStrategy(self.config),
            HalfMatchStrategy(self.config),
            SelfHalfMatchStrategy(self.config),
            # StepMatchStrategy(self.config),
            # SelfStepMatchStrategy(self.config),
            SquareMatchStrategy(self.config),
            SelfSquareMatchStrategy(self.config),
            SqrtMatchStrategy(self.config),
            SelfSqrtMatchStrategy(self.config),
            ArithmeticProgressionMatchStrategy(self.config),
            GeometricProgressionMatchStrategy(self.config),
            DigitSumMatchStrategy(self.config)
        ]
        
        # Initialize Hit Strategies from Config
        self.hit_strategies = []
        hit_numbers = self.config.hit_numbers
        for number_str, _ in hit_numbers.items():
            try:
                target_number = int(number_str)
                self.hit_strategies.append(
                    HitSpecificNumberStrategy(target_number, self.number_log_repo, self.config)
                )
            except ValueError:
                logger.warning(f"Invalid hit number in config: {number_str}")
        close_numbers = self.config.close_numbers
        for number_str, _ in close_numbers.items():
            try:
                target_number = int(number_str)
                self.hit_strategies.append(
                    HitCloseNumberStrategy(target_number, self.config)
                )
            except ValueError:
                logger.warning(f"Invalid hit number in config: {number_str}")

        # Precache user data from database
        self._precache_user_data()

    def set_bot(self, bot):
        self.bot = bot

    def _precache_user_data(self):
        """Pre-caches user info from the database."""
        try:
            query = self.user_repo.get_all_users_query()
            users = self.db.fetch_all(query)
            if users:
                for user in users:
                    # Unpack all columns
                    _, chat_id, thread_id, user_id, user_name, user_handle, numbers_bitmap, last_login_date, current_streak, extend_info = user
                    
                    # Convert numbers_bitmap to int
                    if isinstance(numbers_bitmap, str):
                        try:
                            numbers_bitmap = int(numbers_bitmap, 2)
                        except ValueError:
                            numbers_bitmap = 0
                    elif not isinstance(numbers_bitmap, int):
                        numbers_bitmap = 0

                    self.user_info_cache[(user_id, chat_id)] = UserInfo(
                        chat_id, thread_id, user_id, user_name, user_handle,
                        numbers_bitmap, last_login_date, current_streak, extend_info
                    )
                logger.info(f"Pre-cached {len(users)} users.")
        except Exception as e:
            logger.error(f"Failed to precache user data: {e}")

    def _duplicate_check(self, message, number, ts):
        if str(message.user_id) in self.config.developer_user_ids:
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

    async def _mark_user_attendance(self, message, today_date, yesterday_date, user_name):
        """
        Updates the user's streak and attendance if this is their first log of the day.
        Sends a reply with the updated streak info.
        """
        # A. Update user_data (idempotent)
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

            streak_reply = "\n".join(self.config.attendance_replies)
            streak_reply = streak_reply.format(name=user_name, attendance=attendance_str, streak=current_user_streak)
            await self.bot.send_reply(message.chat_id, message.message_id, streak_reply)

        except Exception as e:
            logger.error(f"Failed to update user streak/attendance: {e}")

    def _update_user_info_cache(self, message, user_name):
        user_info_cache_key = (message.user_id, message.chat_id)
        if user_info_cache_key in self.user_info_cache:
            user_info = self.user_info_cache[user_info_cache_key]
            user_info.user_name = user_name
            user_info.user_handle = message.username
        else:
            self.user_info_cache[user_info_cache_key] = UserInfo(
                message.chat_id,
                message.thread_id,
                message.user_id,
                user_name,
                message.username
            )

    def _update_user_bitmap_cache(self, message, number):
        user_info_cache_key = (message.user_id, message.chat_id)
        numbers_bitmap = self.user_info_cache[user_info_cache_key].numbers_bitmap
        current_mask = (1 << (127 - number))
        if not numbers_bitmap & current_mask:
            numbers_bitmap |= current_mask
            self.user_info_cache[user_info_cache_key].numbers_bitmap = numbers_bitmap
            mask = 1 << 127
            missing_numbers = []
            for i in range(101):
                if not mask & numbers_bitmap:
                    missing_numbers.append(i)
                mask >>= 1
            return missing_numbers
        return None

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
            sgt_timezone = timezone(timedelta(hours=self.config.timezone_gmt))
            ts_sgt = ts.astimezone(sgt_timezone)
            today_date = ts_sgt.date()
            yesterday_date = today_date - timedelta(days=1)

            # User Name Logic
            user_name = message.first_name
            if message.last_name:
                user_name += f" {message.last_name}"
            if not user_name:
                user_name = "Unknown"

            # Update User Info Cache name and handle
            self._update_user_info_cache(message, user_name)

            # Update User Info Cache numbers bitmap
            remaining_numbers = self._update_user_bitmap_cache(message, number)

            # Prepare Cache Data Object
            cache_data = CacheData(
                self.user_info_cache,
                self.user_log_cache,
                self.chat_log_cache
            )

            # --- Hit Logic ---
            hit_context = HitContext()

            for strategy in self.hit_strategies:
                result = strategy.check(message, number, cache_data)
                if result:
                    hit_context.add_hit(result.hit_type, result.hit_number, result.reply_text,
                                        result.react_emoji, result.forward_chat_ids, result.streak_counted)
                    logger.info(f"Hit detected! - User {message.user_id} in chat {message.chat_id}.")
            # ---

            # --- Match Logic ---
            match_context = MatchContext()
            is_any_match = False

            for strategy in self.match_strategies:
                if strategy.has_conflict(match_context):
                    continue
                results = strategy.check(message, number, cache_data)
                for result in results:
                    match_context.add_match(
                        result.match_type,
                        message.user_id,
                        result.matched_user_id,
                        result.matched_number,
                        result.matched_message_id,
                        result.reply_text
                    )
                    is_any_match = True
                    logger.info(f"Match detected! - User {message.user_id} matched user "
                                f"{result.matched_user_id} in chat {message.chat_id}.")
            # ---

            # --- Streak Logic ---
            chat_id = message.chat_id
            current_matches = len(match_context.matches)
            current_hits = len([hit for hit in hit_context.hits if hit[5]])

            if (current_matches + current_hits) > 0:
                if chat_id not in self.streak_info_cache:
                    self.streak_info_cache[chat_id] = StreakInfo()
                self.streak_info_cache[chat_id].matches += current_matches
                self.streak_info_cache[chat_id].hits += current_hits
            else:
                self.streak_info_cache[chat_id] = StreakInfo(0, 0)

            streak_total = self.streak_info_cache[chat_id].total
            # ---

            # --- Update User Attendance ---
            user_info = self.user_info_cache[(message.user_id, message.chat_id)]
            last_login_date = user_info.last_login_date

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
                await self._mark_user_attendance(message, today_date, yesterday_date, user_name)
                user_info.last_login_date = today_date
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

                    smaller_user_id, bigger_user_id = (msg_user_id, matched_user_id) \
                        if msg_user_id < matched_user_id \
                        else (matched_user_id, msg_user_id)

                    user1_info = self.user_info_cache.get((smaller_user_id, message.chat_id))
                    user1_name = user1_info.user_name if user1_info else "Unknown"

                    user2_info = self.user_info_cache.get((bigger_user_id, message.chat_id))
                    user2_name = user2_info.user_name if user2_info else "Unknown"

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
            if self.transaction_queue:
                await self.transaction_queue.submit(transaction_queries)
            else:
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

            # Log Success
            logger.info(f"Logged number {number}, attendance, and count for user {message.user_id}.")

            # --- Send Feedback (Reaction & Reply) ---
            if self.bot:
                # Send last hit reaction
                hit_reaction = None
                for hit in hit_context.hits:
                    _, _, _, current_reaction, _, _ = hit
                    if current_reaction:
                        hit_reaction = current_reaction
                if hit_reaction:
                    await self.bot.set_message_reaction(message.chat_id, message.message_id, hit_reaction)

                # Send hit replies and forward message
                for hit in hit_context.hits:
                    _, _, hit_reply_text, _, forward_chat_ids, _ = hit
                    if hit_reply_text:
                        await self.bot.send_reply(message.chat_id, message.message_id, hit_reply_text)
                    for forward_chat_id in forward_chat_ids:
                        await self.bot.forward_message(message.chat_id, message.message_id, forward_chat_id)

                # Send match replies to the matched message IDs
                for match in match_context.matches:
                    _, _, _, _, matched_msg_id, match_reply_text = match
                    if matched_msg_id:
                        await self.bot.send_reply(message.chat_id, matched_msg_id, match_reply_text)

            # Send numbers obtained message if new number
            if remaining_numbers is not None:
                if len(remaining_numbers) == 0:
                    await self.bot.send_reply(message.chat_id, message.message_id, "All numbers collected!")
                else:
                    remaining_numbers_reply = (f"New number! {len(remaining_numbers)} "
                                               f"number{'s' if len(remaining_numbers) > 1 else ''} left to collect")
                    if len(remaining_numbers) < 10:
                        remaining_numbers_reply += f": {', '.join(map(str, remaining_numbers))}"
                    await self.bot.send_reply(message.chat_id, message.message_id, remaining_numbers_reply)

            # Send Streak message if total >= 2
            if streak_total >= 2:
                streak_msg = f"{streak_total}-Streak!\n"
                streak_msg += '🔥' * streak_total
                await self.bot.send_message(message.chat_id, streak_msg)
            # ---

        except Exception as e:
            logger.error(f"Failed to process number log: {e}", exc_info=True)