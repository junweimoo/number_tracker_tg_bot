import asyncio
import logging
import time
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
from service.achievements import (
    AchievementContext,
    AchievementType,
    ObtainAllNumbersAchievementStrategy,
    GetSpecificNumberAchievementStrategy
)

logger = logging.getLogger(__name__)

class StreakInfo:
    """
    Represents streak information for a chat, including matches and hits.
    """
    def __init__(self, matches=0, hits=0):
        """
        Initializes StreakInfo.

        Args:
            matches (int): Number of matches in the current streak.
            hits (int): Number of hits in the current streak.
        """
        self.matches = matches
        self.hits = hits

    @property
    def total(self):
        """
        Calculates the total streak count.

        Returns:
            int: The sum of matches and hits.
        """
        return self.matches + self.hits

class UserInfo:
    """
    Represents cached information about a user.
    """
    def __init__(self, chat_id, thread_id, user_id, user_name, user_handle, numbers_bitmap=0,
                 last_login_date=None, current_streak=0, achievements=None, extend_info=None):
        """
        Initializes UserInfo.

        Args:
            chat_id (int): The ID of the chat.
            thread_id (int): The ID of the thread.
            user_id (int): The ID of the user.
            user_name (str): The name of the user.
            user_handle (str): The handle/username of the user.
            numbers_bitmap (int): A bitmap representing numbers collected by the user.
            last_login_date (date): The date of the user's last login/activity.
            current_streak (int): The user's current attendance streak.
            achievements (str): A comma-separated string of achievement IDs.
            extend_info (str): Additional extended information.
        """
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.user_id = user_id
        self.user_name = user_name
        self.user_handle = user_handle
        self.numbers_bitmap = numbers_bitmap
        self.last_login_date = last_login_date
        self.current_streak = current_streak
        self.achievements = achievements
        self.extend_info = extend_info

class CacheData:
    """
    A container for various caches used during number processing.
    """
    def __init__(self, user_info_cache, user_log_cache, chat_log_cache):
        """
        Initializes CacheData.

        Args:
            user_info_cache (dict): Cache of UserInfo objects.
            user_log_cache (dict): Cache of recent numbers logged by users.
            chat_log_cache (dict): Cache of recent numbers logged in chats.
        """
        self.user_info_cache = user_info_cache
        self.user_log_cache = user_log_cache
        self.chat_log_cache = chat_log_cache

class NumberLogService:
    """
    Service responsible for processing logged numbers, handling matches, hits, streaks, and achievements.
    """
    def __init__(self, db, config, repositories, transaction_queue=None, bot=None):
        """
        Initializes the NumberLogService.

        Args:
            db: The database connection.
            config: Configuration object.
            repositories (dict): Dictionary of repository instances.
            transaction_queue: Optional queue for batching database transactions.
            bot: Optional bot instance.
        """
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

        # Initialize Achievement Strategies
        self.achievement_strategies = [
            ObtainAllNumbersAchievementStrategy(self.config, self.user_repo, self.db),
            GetSpecificNumberAchievementStrategy(0, AchievementType.GET_NUMBER_0, self.config, self.user_repo, self.db),
            GetSpecificNumberAchievementStrategy(69, AchievementType.GET_NUMBER_69, self.config, self.user_repo, self.db),
            GetSpecificNumberAchievementStrategy(88, AchievementType.GET_NUMBER_88, self.config, self.user_repo, self.db),
            GetSpecificNumberAchievementStrategy(100, AchievementType.GET_NUMBER_100, self.config, self.user_repo, self.db)
        ]

        # Precache user data from database
        self._precache_user_data()

    def set_bot(self, bot):
        """
        Sets the bot instance.

        Args:
            bot: The bot instance.
        """
        self.bot = bot

    def _precache_user_data(self):
        """Pre-caches user info from the database."""
        try:
            query = self.user_repo.get_all_users_query()
            users = self.db._fetch_all_sync(query)
            if users:
                for user in users:
                    # Unpack all columns
                    (_, chat_id, thread_id, user_id, user_name, user_handle, numbers_bitmap, last_login_date,
                     current_streak, achievements, extend_info) = user
                    
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
                        numbers_bitmap, last_login_date, current_streak, achievements, extend_info
                    )
                logger.info(f"Pre-cached {len(users)} users.")
        except Exception as e:
            logger.error(f"Failed to precache user data: {e}")

    def _duplicate_check(self, message, number, ts):
        """
        Checks if the logged number is a duplicate within a short timeframe.

        Args:
            message: The message object.
            number (int): The logged number.
            ts (datetime): The timestamp of the log.

        Returns:
            bool: True if it's a duplicate, False otherwise.
        """
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

    async def _mark_user_attendance(self, message, today_date, yesterday_date, user_name, current_streak):
        """
        Updates the user's streak and attendance if this is their first log of the day.
        Sends a reply with the updated streak info.

        Args:
            message: The message object.
            today_date (date): Today's date.
            yesterday_date (date): Yesterday's date.
            user_name (str): The name of the user.
            current_streak (int): The user's current streak.

        Returns:
            tuple: (streak_reply_text, actual_new_streak)
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
            await self.db.execute_transaction([
                (update_streak_query, streak_params),
                (insert_attendance_query, attendance_params)
            ])

            # Fetch last 7 days attendance
            attendance_rows = await self.attendance_repo.get_recent_attendance(message.user_id, message.chat_id, limit=7)
            attendance_dates = [row[0] for row in attendance_rows]
            
            # Ensure today is included in the visual grid even if the fetch happened before the insert was fully committed
            if today_date not in attendance_dates:
                attendance_dates.append(today_date)

            # Format attendance string
            attendance_str = ""
            for i in range(6, -1, -1):
                check_date = today_date - timedelta(days=i)
                if check_date in attendance_dates:
                    attendance_str += "✅ "
                else:
                    attendance_str += "⬜ "

            # Calculate new streak for display
            streak_query = self.user_repo.get_fetch_streak_query()
            streak_result = await self.db.fetch_one(streak_query, (message.user_id, message.chat_id))
            actual_new_streak = streak_result[0] if streak_result else 1

            if actual_new_streak > 1:
                streak_reply = "\n".join(self.config.attendance_replies)
                streak_reply = streak_reply.format(name=user_name, attendance=attendance_str, streak=actual_new_streak)
            else:
                filtered_replies = [line for line in self.config.attendance_replies if "{streak}" not in line]
                streak_reply = "\n".join(filtered_replies)
                streak_reply = streak_reply.format(name=user_name, attendance=attendance_str)

            return streak_reply, actual_new_streak

        except Exception as e:
            logger.error(f"Failed to update user streak/attendance: {e}")
            return None, current_streak

    def _update_user_info_cache(self, message, user_name):
        """
        Updates the user info cache with the latest user details.

        Args:
            message: The message object.
            user_name (str): The name of the user.
        """
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

    def _calculate_remaining_numbers(self, numbers_bitmap, number):
        """
        Calculates the remaining numbers to be collected and the updated bitmap.

        Args:
            numbers_bitmap (int): The current bitmap of collected numbers.
            number (int): The newly logged number.

        Returns:
            tuple: (list of missing numbers, updated bitmap) or (None, original bitmap) if number already collected.
        """
        current_mask = (1 << (127 - number))
        if not numbers_bitmap & current_mask:
            new_bitmap = numbers_bitmap | current_mask
            mask = 1 << 127
            missing_numbers = []
            for i in range(101):
                if not mask & new_bitmap:
                    missing_numbers.append(i)
                mask >>= 1
            return missing_numbers, new_bitmap
        return None, numbers_bitmap

    async def process_number(self, message, number, is_import=False):
        """
        Processes a logged number: checks for duplicates, updates attendance, 
        checks for hits/matches/achievements, and updates the database and caches.

        Args:
            message: The message object containing the logged number.
            number (int): The number being logged.
            is_import: Whether this is an import
        """
        try:
            start_time = time.perf_counter()

            # Timestamp Calculation
            if message.date:
                ts = datetime.fromtimestamp(message.date, tz=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Bot Whitelist Check
            if is_import or str(message.user_id) in self.config.developer_user_ids:
                pass
            elif message.via_bot:
                bot_username = message.via_bot.get('username')
                if bot_username not in self.config.whitelisted_bot_names:
                    logger.info(f"Ignored number {number} from non-whitelisted bot: {bot_username}")
                    return
            else:
                logger.info(f"Ignored number {number} not sent via bot")
                return

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

            # Get current user info from cache
            user_info_cache_key = (message.user_id, message.chat_id)
            user_info = self.user_info_cache.get(user_info_cache_key)
            if not user_info:
                user_info = UserInfo(
                    message.chat_id,
                    message.thread_id,
                    message.user_id,
                    user_name,
                    message.username
                )
            
            # Calculate new bitmap and remaining numbers (don't update cache yet)
            remaining_numbers, new_bitmap = self._calculate_remaining_numbers(user_info.numbers_bitmap, number)

            # Prepare Cache Data Object for strategies
            cache_data = CacheData(
                self.user_info_cache,
                self.user_log_cache,
                self.chat_log_cache
            )

            additional_replies = []

            # --- Hit Logic ---
            hit_context = HitContext()

            for strategy in self.hit_strategies:
                result = await strategy.check(message, number, cache_data)
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
            last_login_date = user_info.last_login_date
            new_streak = user_info.current_streak

            should_mark_attendance = False
            if last_login_date == today_date:
                should_mark_attendance = False
            else:
                last_login_query = self.user_repo.get_last_login_date_query()
                last_login_result = await self.db.fetch_one(last_login_query, (message.user_id, message.chat_id))
                db_last_login_date = last_login_result[0] if last_login_result else None
                if db_last_login_date != today_date:
                    should_mark_attendance = True


            if should_mark_attendance:
                reply_str, new_streak = await self._mark_user_attendance(
                    message, today_date, yesterday_date, user_name, user_info.current_streak
                )
                if reply_str:
                    additional_replies.append(reply_str)
            # ---

            # --- Achievement Logic ---
            achievement_context = AchievementContext()
            for strategy in self.achievement_strategies:
                result = await strategy.check(message, number, cache_data, remaining_numbers)
                if result:
                    achievement_context.add_achievement(result.achievement_type, result.reply_text)
                    logger.info(f"Achievement unlocked! - {result.achievement_type.name} for user {message.user_id}.")
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

                    user1_id, user2_id = (msg_user_id, matched_user_id) \
                        if msg_user_id < matched_user_id \
                        else (matched_user_id, msg_user_id)

                    user1_info = self.user_info_cache.get((user1_id, message.chat_id))
                    user1_name = user1_info.user_name if user1_info else "Unknown"

                    user2_info = self.user_info_cache.get((user2_id, message.chat_id))
                    user2_name = user2_info.user_name if user2_info else "Unknown"

                    # Log the match
                    match_params = (
                        message.chat_id,
                        message.thread_id,
                        user1_id,
                        user1_name,
                        user2_id,
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
                        user1_id,
                        user2_id,
                        match_type.value
                    )
                    transaction_queries.append((upsert_match_counts_query, match_counts_params))

            # F. Update user_data (bitmap and achievements)
            new_achievements_str = None
            if achievement_context.achievements:
                new_achievements_str = ",".join([a[0].value for a in achievement_context.achievements])
                upsert_bitmap_query = self.user_repo.get_upsert_user_bitmap_with_achievements_query()
                bitmap_params = (
                    message.user_id,
                    message.chat_id,
                    user_name,
                    number,
                    new_achievements_str,
                    number
                )
            else:
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
                await self.db.execute_transaction(transaction_queries)
            # ---

            # --- Update Caches (Only after successful transaction) ---
            # Update User Info Cache
            user_info.user_name = user_name
            user_info.user_handle = message.username
            user_info.numbers_bitmap = new_bitmap
            user_info.current_streak = new_streak
            if should_mark_attendance:
                user_info.last_login_date = today_date
            if new_achievements_str:
                if user_info.achievements:
                    user_info.achievements += "," + new_achievements_str
                else:
                    user_info.achievements = new_achievements_str
            self.user_info_cache[user_info_cache_key] = user_info

            # Update User Log Cache
            if user_info_cache_key not in self.user_log_cache:
                self.user_log_cache[user_info_cache_key] = deque(maxlen=10)
            self.user_log_cache[user_info_cache_key].append((number, ts, message.message_id))

            # Update Chat Log Cache
            chat_log_cache_key = message.chat_id
            if chat_log_cache_key not in self.chat_log_cache:
                self.chat_log_cache[chat_log_cache_key] = deque(maxlen=10)
            self.chat_log_cache[chat_log_cache_key].append((message.user_id, number, ts, message.message_id))
            # ---

            # Log Success
            duration = time.perf_counter() - start_time
            logger.info(f"Logged number {number}, attendance, and count for user {message.user_id} in {duration:.6f}s")

            # --- Send Feedback (Reaction & Reply) ---
            if self.bot and not is_import and str(message.chat_id) not in self.config.silent_chat_ids:
                tasks = []

                # Send last hit reaction
                hit_reaction = None
                for hit in hit_context.hits:
                    _, _, _, current_reaction, _, _ = hit
                    if current_reaction:
                        hit_reaction = current_reaction
                if hit_reaction:
                    tasks.append(self.bot.set_message_reaction(message.chat_id, message.message_id, hit_reaction))

                # Send hit replies and forward message
                for hit in hit_context.hits:
                    _, _, hit_reply_text, _, forward_chat_ids, _ = hit
                    if hit_reply_text:
                        tasks.append(self.bot.send_reply(message.chat_id, message.message_id, hit_reply_text))
                    for forward_chat_id, forward_thread_id in forward_chat_ids:
                        tasks.append(self.bot.forward_message(message.chat_id, message.message_id, forward_chat_id, forward_thread_id))

                # Send match replies to the matched message IDs
                for match in match_context.matches:
                    _, _, _, _, matched_msg_id, match_reply_text = match
                    if matched_msg_id:
                        tasks.append(self.bot.send_reply(message.chat_id, matched_msg_id, match_reply_text))

                # Send numbers obtained message if new number
                if remaining_numbers is not None:
                    if len(remaining_numbers) == 0:
                        tasks.append(self.bot.send_reply(message.chat_id, message.message_id, "All numbers collected!"))
                    else:
                        remaining_numbers_reply = (f"New number! {len(remaining_numbers)} "
                                                   f"number{'s' if len(remaining_numbers) > 1 else ''} left to collect")
                        if len(remaining_numbers) < 10:
                            remaining_numbers_reply += f": {', '.join(map(str, remaining_numbers))}"
                        tasks.append(self.bot.send_reply(message.chat_id, message.message_id, remaining_numbers_reply))

                if tasks:
                    await asyncio.gather(*tasks)


                # Send Streak message if total >= 2
                if streak_total >= 2:
                    streak_msg = f"{streak_total}-Streak!\n"
                    streak_msg += '🔥' * streak_total
                    await self.bot.send_message(message.chat_id, streak_msg)

                # Send achievement replies
                for achievement in achievement_context.achievements:
                    _, achievement_text = achievement
                    if achievement_text:
                        achievement_reply_text = "👏 Achievement Unlocked: " + achievement_text
                        await self.bot.send_reply(message.chat_id, message.message_id, achievement_reply_text)

                # Send additional replies
                for reply in additional_replies:
                    await self.bot.send_reply(message.chat_id, message.message_id, reply)
            # ---

        except Exception as e:
            logger.error(f"Failed to process number log: {e}", exc_info=True)
