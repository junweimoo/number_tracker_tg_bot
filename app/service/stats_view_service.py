import logging
import asyncio
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class StatsViewService:
    """
    Service responsible for generating text-based statistics views and leaderboards.
    """
    def __init__(self, db, config, repositories, bot=None):
        """
        Initializes the StatsViewService.

        Args:
            db: The database connection.
            config: Configuration object.
            repositories (dict): Dictionary of repository instances.
            bot: Optional bot instance.
        """
        self.db = db
        self.config = config
        self.bot = bot
        self.stats_repo = repositories['stats']
        self.attendance_repo = repositories['attendance']
        self.match_log_repo = repositories['match_log']
        self.user_repo = repositories['user']

    def set_bot(self, bot):
        """
        Sets the bot instance.

        Args:
            bot: The bot instance.
        """
        self.bot = bot

    async def get_user_achievements_emojis(self, user_id, chat_id):
        """
        Fetches and formats the user's achievements as a string of emojis.

        Args:
            user_id (int): The ID of the user.
            chat_id (int): The ID of the chat.

        Returns:
            str: A string containing achievement emojis.
        """
        try:
            query = "SELECT achievements FROM user_data WHERE user_id = %s AND chat_id = %s"
            result = await self.db.fetch_one(query, (user_id, chat_id))
            if result and result[0]:
                achievement_ids = result[0].split(',')
                achievement_emojis = []
                config_data = self.config.achievement_text
                
                for aid in achievement_ids:
                    data = config_data.get(aid)
                    if data:
                        emoji = data.get('emoji')
                        if emoji:
                            achievement_emojis.append(emoji)
                
                if achievement_emojis:
                    return " ".join(achievement_emojis)
            return ""
        except Exception as e:
            logger.error(f"Error fetching user achievements: {e}")
            return ""

    async def get_user_stats_summary(self, message):
        """
        Generates a summary of statistics for a specific user.

        Args:
            message: The message object containing user and chat information.

        Returns:
            str: A formatted string containing the user's statistics summary.
        """
        user_id = message.user_id
        chat_id = message.chat_id
        first_name = message.first_name

        try:
            hit_numbers = sorted([int(n) for n in self.config.hit_numbers.keys()])
            streak_query = self.user_repo.get_fetch_streak_query()

            # Prepare all coroutines for parallel execution
            tasks = [
                self.stats_repo.get_user_stats(user_id, chat_id),
                self.stats_repo.get_specific_number_counts(user_id, chat_id, hit_numbers),
                self.stats_repo.get_most_frequent_numbers(user_id, chat_id),
                self.match_log_repo.get_top_matches(user_id, chat_id, limit=3),
                self.db.fetch_one(streak_query, (user_id, chat_id)),
                self.get_user_achievements_emojis(user_id, chat_id),
                self.user_repo.get_all_users_in_chat(chat_id)
            ]

            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks)

            # Unpack results
            user_stats_result = results[0]
            specific_counts_raw = results[1]
            most_frequent_results = results[2]
            top_matches = results[3]
            streak_result = results[4]
            achievements_str = results[5]
            all_users = results[6]

            user_map = {uid: name for uid, name in all_users}

            # 1. Total count of numbers logged & 2. Overall mean & Unique count
            count = 0
            average = 0
            unique_count = 0
            if user_stats_result and user_stats_result[0] is not None and user_stats_result[0] > 0:
                count = user_stats_result[0]
                total_sum = user_stats_result[1]
                unique_count = user_stats_result[2]
                average = round(total_sum / count, 4)

            # 3. Count of specific numbers from config
            specific_counts_dict = {num: cnt for num, cnt in specific_counts_raw}
            counts_list = [f"{num} (Count: {specific_counts_dict.get(num, 0)})" for num in hit_numbers]
            counts_str = "\n".join(counts_list) if counts_list else "_No numbers recorded yet._"

            # 4. Number with the highest count
            most_frequent_str = "N/A"
            if most_frequent_results:
                numbers = [str(row[0]) for row in most_frequent_results]
                freq_count = most_frequent_results[0][1]
                most_frequent_str = f"{', '.join(numbers)} (Count: {freq_count})"

            # 5. Top 3 matched users
            top_matches_str = "None"
            if top_matches:
                match_names = []
                for match_user_id, match_count in top_matches:
                    match_name = user_map.get(match_user_id, "Unknown")
                    match_names.append(f"{match_name} ({match_count})")
                top_matches_str = "\n".join(match_names)

            # 6. Attendance streak
            current_streak = streak_result[0] if streak_result else 0

            # Format the response using config
            stats_reply = "\n".join(self.config.stats_replies)
            return stats_reply.format(
                name=f"{first_name}",
                count=f"{count}",
                average=f"{average}",
                unique_count=f"{unique_count}",
                counts=counts_str,
                most_frequent=most_frequent_str,
                top_matches=top_matches_str,
                streak=f"{current_streak}",
                achievements=achievements_str
            )

        except Exception as e:
            logger.error(f"Error calculating stats summary: {e}", exc_info=True)
            return "An error occurred while fetching your stats."

    async def get_leaderboard(self, chat_id):
        """
        Generates a leaderboard for the chat, including all-time and daily statistics.

        Args:
            chat_id (int): The ID of the chat.

        Returns:
            str: A formatted string containing the leaderboard.
        """
        try:
            # Determine today's date based on config timezone
            tz_offset = self.config.timezone_gmt
            tz = timezone(timedelta(hours=tz_offset))
            tz_str = f"{'+' if tz_offset >= 0 else '-'}{abs(tz_offset):02}"
            today = datetime.now(tz).date()
            special_numbers = self.config.hit_numbers.keys()

            # Prepare all coroutines for parallel execution
            tasks = [
                self.user_repo.get_all_users_in_chat(chat_id),
                self.stats_repo.get_top_users_by_count(chat_id, limit=3),
                self.match_log_repo.get_top_matched_pairs(chat_id, limit=3),
                self.stats_repo.get_top_users_by_count_daily(chat_id, today, limit=3),
                self.match_log_repo.get_top_matched_pairs_daily(chat_id, today, tz_str, limit=3)
            ]
            
            # Add special number tasks
            for num in special_numbers:
                tasks.append(self.stats_repo.get_top_user_for_number(chat_id, num))
            for num in special_numbers:
                tasks.append(self.stats_repo.get_top_user_for_number_daily(chat_id, num, today))

            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks)

            # Unpack results
            all_users = results[0]
            top_users = results[1]
            top_pairs = results[2]
            top_users_daily = results[3]
            top_pairs_daily = results[4]
            
            special_stats_results = results[5:5+len(special_numbers)]
            special_stats_daily_results = results[5+len(special_numbers):]

            user_map = {uid: name for uid, name in all_users}
            replies = self.config.leaderboard_replies
            response_parts = [replies.get("header", "🏆 Leaderboard 🏆")]

            # --- All Time Section ---
            response_parts.append(replies.get("all_time_section", "\n--- All Time ---"))

            # 1. Top 3 users with highest counts
            if top_users:
                response_parts.append(replies.get("top_loggers_title", "Top Loggers:"))
                for idx, (uid, count) in enumerate(top_users, 1):
                    name = user_map.get(uid, "Unknown")
                    response_parts.append(f"{idx}. {name}: {count}")
            
            # 2. Top 3 matched pairs
            if top_pairs:
                response_parts.append(replies.get("top_matched_pairs_title", "\nTop Matched Pairs:"))
                for idx, (u1, u2, count) in enumerate(top_pairs, 1):
                    name1 = user_map.get(u1, "Unknown")
                    name2 = user_map.get(u2, "Unknown")
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers
            special_stats = []
            for i, num in enumerate(special_numbers):
                top_user = special_stats_results[i]
                if top_user:
                    name = user_map.get(top_user[0], "Unknown")
                    special_stats.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats:
                response_parts.append(replies.get("special_numbers_kings_title", "\nSpecial Numbers Kings:"))
                response_parts.append("\n".join(special_stats))

            # --- Daily Section ---
            daily_section_header = replies.get("daily_section", "\n--- Today ({today}) ---").format(today=today)
            response_parts.append(daily_section_header)

            # 1. Top 3 users with highest counts (Daily)
            if top_users_daily:
                response_parts.append(replies.get("top_loggers_title", "Top Loggers:"))
                for idx, (uid, count) in enumerate(top_users_daily, 1):
                    name = user_map.get(uid, "Unknown")
                    response_parts.append(f"{idx}. {name}: {count}")
            else:
                response_parts.append(replies.get("no_logs_today", "No logs today."))

            # 2. Top 3 matched pairs (Daily)
            if top_pairs_daily:
                response_parts.append(replies.get("top_matched_pairs_title", "\nTop Matched Pairs:"))
                for idx, (u1, u2, count) in enumerate(top_pairs_daily, 1):
                    name1 = user_map.get(u1, "Unknown")
                    name2 = user_map.get(u2, "Unknown")
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers (Daily)
            special_stats_daily = []
            for i, num in enumerate(special_numbers):
                top_user = special_stats_daily_results[i]
                if top_user:
                    name = user_map.get(top_user[0], "Unknown")
                    special_stats_daily.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats_daily:
                response_parts.append(replies.get("special_numbers_kings_title", "\nSpecial Numbers:"))
                response_parts.append("\n".join(special_stats_daily))

            return "\n".join(response_parts)

        except Exception as e:
            logger.error(f"Error generating leaderboard: {e}", exc_info=True)
            return "An error occurred while generating the leaderboard."

    async def get_user_nums_remaining_in_chat(self, chat_id, user_id=None):
        """
        Queries the counts of numbers remaining for all users in the chat using the numbers_bitmap column
        and returns a formatted text.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of a specific user. Defaults to None.

        Returns:
            str: A formatted string containing the remaining numbers for users.
        """
        try:
            users_data = await self.user_repo.get_users_with_bitmap(chat_id, user_id)
            if not users_data:
                return "No user data found."

            completed_users = []
            remaining_users = {}

            for user_name, numbers_bitmap in users_data:
                if not numbers_bitmap:
                    remaining_count = 101
                    missing_numbers = list(range(101))
                else:
                    # Convert bitmap string to integer
                    bitmap_int = int(numbers_bitmap, 2)
                    
                    # Check numbers 0-100
                    missing_numbers = []
                    mask = 1 << 127
                    for i in range(101):
                        if not mask & bitmap_int:
                            missing_numbers.append(i)
                        mask >>= 1
                    
                    remaining_count = len(missing_numbers)

                if remaining_count == 0:
                    completed_users.append(user_name)
                else:
                    if remaining_count not in remaining_users:
                        remaining_users[remaining_count] = []
                    
                    user_entry = user_name
                    if user_id or remaining_count < 10:
                        missing_str = ", ".join(map(str, missing_numbers))
                        user_entry += f" ({missing_str})"
                    
                    remaining_users[remaining_count].append(user_entry)

            response_parts = [self.config.numbers_remaining_board.get("top_header")]

            if completed_users:
                response_parts.append(self.config.numbers_remaining_board.get("complete_header"))
                for user in sorted(completed_users):
                    response_parts.append(f"- {user}")

            sorted_counts = sorted(remaining_users.keys())
            for count in sorted_counts:
                response_parts.append(self.config.numbers_remaining_board.get("others_header").format(count=count))
                for user_entry in sorted(remaining_users[count]):
                    response_parts.append(f"- {user_entry}")

            return "\n".join(response_parts)

        except Exception as e:
            logger.error(f"Error generating remaining numbers view: {e}", exc_info=True)
            return "An error occurred while fetching remaining numbers."
