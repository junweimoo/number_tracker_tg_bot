import logging
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
            # 1. Total count of numbers logged & 2. Overall mean
            count = 0
            average = 0
            result = await self.stats_repo.get_user_stats(user_id, chat_id)
            if result and result[0] is not None and result[0] > 0:
                count = result[0]
                total_sum = result[1]
                average = round(total_sum / count, 4)

            # 3. Count of specific numbers: 0, 88, 100
            specific_numbers = [0, 88, 100]
            specific_counts = await self.stats_repo.get_specific_number_counts(user_id, chat_id, specific_numbers)
            counts_str = "No numbers recorded yet."
            if specific_counts:
                counts_str = ", ".join([f"{num}: {cnt}" for num, cnt in specific_counts])

            # 4. Number with the highest count
            most_frequent_str = "N/A"
            most_frequent_results = await self.stats_repo.get_most_frequent_numbers(user_id, chat_id)
            if most_frequent_results:
                numbers = [str(row[0]) for row in most_frequent_results]
                freq_count = most_frequent_results[0][1]
                most_frequent_str = f"{', '.join(numbers)} (Count: {freq_count})"

            # 5. Top 3 matched users
            top_matches_str = "None"
            top_matches = await self.match_log_repo.get_top_matches(user_id, chat_id, limit=3)
            if top_matches:
                match_names = []
                for match_user_id, match_count in top_matches:
                    match_name = await self.user_repo.get_user_name(match_user_id, chat_id)
                    match_names.append(f"{match_name} ({match_count})")
                top_matches_str = ", ".join(match_names)

            # 6. Attendance streak
            streak_query = self.user_repo.get_fetch_streak_query()
            streak_result = await self.db.fetch_one(streak_query, (user_id, chat_id))
            current_streak = streak_result[0] if streak_result else 0

            # 7. Achievements (Emojis only)
            achievements_str = await self.get_user_achievements_emojis(user_id, chat_id)

            # Format the response using config
            stats_reply = "\n".join(self.config.stats_replies)
            return stats_reply.format(
                name=first_name,
                count=count,
                average=average,
                counts=counts_str,
                most_frequent=most_frequent_str,
                top_matches=top_matches_str,
                streak=current_streak,
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
            replies = self.config.leaderboard_replies
            response_parts = [replies.get("header", "🏆 Leaderboard 🏆")]
            
            # Determine today's date based on config timezone
            tz_offset = self.config.timezone_gmt
            tz = timezone(timedelta(hours=tz_offset))
            tz_str = f"{'+' if tz_offset >= 0 else '-'}{abs(tz_offset):02}"
            today = datetime.now(tz).date()

            # --- All Time Section ---
            response_parts.append(replies.get("all_time_section", "\n--- All Time ---"))

            # 1. Top 3 users with highest counts
            top_users = await self.stats_repo.get_top_users_by_count(chat_id, limit=3)
            if top_users:
                response_parts.append(replies.get("top_loggers_title", "Top Loggers:"))
                for idx, (uid, count) in enumerate(top_users, 1):
                    name = await self.user_repo.get_user_name(uid, chat_id)
                    response_parts.append(f"{idx}. {name}: {count}")
            
            # 2. Top 3 matched pairs
            top_pairs = await self.match_log_repo.get_top_matched_pairs(chat_id, limit=3)
            if top_pairs:
                response_parts.append(replies.get("top_matched_pairs_title", "\nTop Matched Pairs:"))
                for idx, (u1, u2, count) in enumerate(top_pairs, 1):
                    name1 = await self.user_repo.get_user_name(u1, chat_id)
                    name2 = await self.user_repo.get_user_name(u2, chat_id)
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers
            special_numbers = [0, 88, 100]
            special_stats = []
            for num in special_numbers:
                top_user = await self.stats_repo.get_top_user_for_number(chat_id, num)
                if top_user:
                    name = await self.user_repo.get_user_name(top_user[0], chat_id)
                    special_stats.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats:
                response_parts.append(replies.get("special_numbers_kings_title", "\nSpecial Numbers Kings:"))
                response_parts.append(", ".join(special_stats))

            # --- Daily Section ---
            daily_section_header = replies.get("daily_section", "\n--- Today ({today}) ---").format(today=today)
            response_parts.append(daily_section_header)

            # 1. Top 3 users with highest counts (Daily)
            top_users_daily = await self.stats_repo.get_top_users_by_count_daily(chat_id, today, limit=3)
            if top_users_daily:
                response_parts.append(replies.get("top_loggers_title", "Top Loggers:"))
                for idx, (uid, count) in enumerate(top_users_daily, 1):
                    name = await self.user_repo.get_user_name(uid, chat_id)
                    response_parts.append(f"{idx}. {name}: {count}")
            else:
                response_parts.append(replies.get("no_logs_today", "No logs today."))

            # 2. Top 3 matched pairs (Daily)
            top_pairs_daily = await self.match_log_repo.get_top_matched_pairs_daily(chat_id, today, tz_str, limit=3)
            if top_pairs_daily:
                response_parts.append(replies.get("top_matched_pairs_title", "\nTop Matched Pairs:"))
                for idx, (u1, u2, count) in enumerate(top_pairs_daily, 1):
                    name1 = await self.user_repo.get_user_name(u1, chat_id)
                    name2 = await self.user_repo.get_user_name(u2, chat_id)
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers (Daily)
            special_stats_daily = []
            for num in special_numbers:
                top_user = await self.stats_repo.get_top_user_for_number_daily(chat_id, num, today)
                if top_user:
                    name = await self.user_repo.get_user_name(top_user[0], chat_id)
                    special_stats_daily.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats_daily:
                response_parts.append(replies.get("special_numbers_kings_title", "\nSpecial Numbers Kings:"))
                response_parts.append(", ".join(special_stats_daily))

            return "\n".join(response_parts)

        except Exception as e:
            logger.error(f"Error generating leaderboard: {e}", exc_info=True)
            return "An error occurred while generating the leaderboard."
