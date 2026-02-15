import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class StatsViewService:
    def __init__(self, db, config, repositories, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.stats_repo = repositories['stats']
        self.attendance_repo = repositories['attendance']
        self.match_log_repo = repositories['match_log']
        self.user_repo = repositories['user']

    def set_bot(self, bot):
        self.bot = bot

    async def get_user_achievements_emojis(self, user_id, chat_id):
        """Fetches and formats the user's achievements."""
        try:
            query = "SELECT achievements FROM user_data WHERE user_id = %s AND chat_id = %s"
            result = self.db.fetch_one(query, (user_id, chat_id))
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
        user_id = message.user_id
        chat_id = message.chat_id
        first_name = message.first_name

        try:
            response_parts = [f"Stats for {first_name} in this chat:"]

            # 1. Total count of numbers logged & 2. Overall mean
            result = self.stats_repo.get_user_stats(user_id, chat_id)
            if result and result[0] is not None and result[0] > 0:
                count = result[0]
                total_sum = result[1]
                average = round(total_sum / count, 4)
                response_parts.append(f"Total numbers: {count}")
                response_parts.append(f"Average: {average}")
            else:
                response_parts.append("No numbers recorded yet.")

            # 3. Count of specific numbers: 0, 88, 100
            specific_numbers = [0, 88, 100]
            specific_counts = self.stats_repo.get_specific_number_counts(user_id, chat_id, specific_numbers)
            if specific_counts:
                counts_str = ", ".join([f"{num}: {cnt}" for num, cnt in specific_counts])
                response_parts.append(f"Counts: {counts_str}")

            # 4. Number with the highest count
            most_frequent = self.stats_repo.get_most_frequent_number(user_id, chat_id)
            if most_frequent:
                response_parts.append(f"Most frequent: {most_frequent[0]} (Count: {most_frequent[1]})")

            # 5. Top 3 matched users
            top_matches = self.match_log_repo.get_top_matches(user_id, chat_id, limit=3)
            if top_matches:
                match_names = []
                for match_user_id, match_count in top_matches:
                    match_name = self.user_repo.get_user_name(match_user_id, chat_id)
                    match_names.append(f"{match_name} ({match_count})")
                response_parts.append(f"Top matches: {', '.join(match_names)}")

            # 6. Attendance streak
            streak_query = self.user_repo.get_fetch_streak_query()
            streak_result = self.db.fetch_one(streak_query, (user_id, chat_id))
            current_streak = streak_result[0] if streak_result else 0
            
            if current_streak > 0:
                response_parts.append(f"Current Streak: {current_streak} days 🔥")

            # 7. Achievements (Emojis only)
            achievements_str = await self.get_user_achievements_emojis(user_id, chat_id)
            if achievements_str:
                response_parts.append(f"\nAchievements: {achievements_str}")

            return "\n".join(response_parts)

        except Exception as e:
            logger.error(f"Error calculating stats summary: {e}", exc_info=True)
            return "An error occurred while fetching your stats."

    async def get_leaderboard(self, chat_id):
        try:
            response_parts = ["🏆 Leaderboard 🏆"]
            
            # Determine today's date based on config timezone
            tz_offset = self.config.timezone_gmt
            tz = timezone(timedelta(hours=tz_offset))
            # We need a timezone string for PostgreSQL, e.g., 'Asia/Singapore' or offset '+08'
            # Assuming config.timezone_gmt is an integer like 8.
            # Constructing a fixed offset string for Postgres: e.g. "+08"
            tz_str = f"{'+' if tz_offset >= 0 else '-'}{abs(tz_offset):02}"
            today = datetime.now(tz).date()

            # --- All Time Section ---
            response_parts.append("\n--- All Time ---")

            # 1. Top 3 users with highest counts
            top_users = self.stats_repo.get_top_users_by_count(chat_id, limit=3)
            if top_users:
                response_parts.append("Top Loggers:")
                for idx, (uid, count) in enumerate(top_users, 1):
                    name = self.user_repo.get_user_name(uid, chat_id)
                    response_parts.append(f"{idx}. {name}: {count}")
            
            # 2. Top 3 matched pairs
            top_pairs = self.match_log_repo.get_top_matched_pairs(chat_id, limit=3)
            if top_pairs:
                response_parts.append("\nTop Matched Pairs:")
                for idx, (u1, u2, count) in enumerate(top_pairs, 1):
                    name1 = self.user_repo.get_user_name(u1, chat_id)
                    name2 = self.user_repo.get_user_name(u2, chat_id)
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers
            special_numbers = [0, 88, 100]
            special_stats = []
            for num in special_numbers:
                top_user = self.stats_repo.get_top_user_for_number(chat_id, num)
                if top_user:
                    name = self.user_repo.get_user_name(top_user[0], chat_id)
                    special_stats.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats:
                response_parts.append("\nSpecial Numbers Kings:")
                response_parts.append(", ".join(special_stats))

            # --- Daily Section ---
            response_parts.append(f"\n--- Today ({today}) ---")

            # 1. Top 3 users with highest counts (Daily)
            top_users_daily = self.stats_repo.get_top_users_by_count_daily(chat_id, today, limit=3)
            if top_users_daily:
                response_parts.append("Top Loggers:")
                for idx, (uid, count) in enumerate(top_users_daily, 1):
                    name = self.user_repo.get_user_name(uid, chat_id)
                    response_parts.append(f"{idx}. {name}: {count}")
            else:
                response_parts.append("No logs today.")

            # 2. Top 3 matched pairs (Daily)
            top_pairs_daily = self.match_log_repo.get_top_matched_pairs_daily(chat_id, today, tz_str, limit=3)
            if top_pairs_daily:
                response_parts.append("\nTop Matched Pairs:")
                for idx, (u1, u2, count) in enumerate(top_pairs_daily, 1):
                    name1 = self.user_repo.get_user_name(u1, chat_id)
                    name2 = self.user_repo.get_user_name(u2, chat_id)
                    response_parts.append(f"{idx}. {name1} & {name2}: {count}")

            # 3. Top 1 user for special numbers (Daily)
            special_stats_daily = []
            for num in special_numbers:
                top_user = self.stats_repo.get_top_user_for_number_daily(chat_id, num, today)
                if top_user:
                    name = self.user_repo.get_user_name(top_user[0], chat_id)
                    special_stats_daily.append(f"{num}: {name} ({top_user[1]})")
            
            if special_stats_daily:
                response_parts.append("\nSpecial Numbers Kings:")
                response_parts.append(", ".join(special_stats_daily))

            return "\n".join(response_parts)

        except Exception as e:
            logger.error(f"Error generating leaderboard: {e}", exc_info=True)
            return "An error occurred while generating the leaderboard."
