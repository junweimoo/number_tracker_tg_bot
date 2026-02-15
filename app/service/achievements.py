from abc import ABC, abstractmethod
from enum import Enum

class AchievementType(Enum):
    """
    Enumeration of different achievement types.
    """
    ALL_NUMBERS = "ALL_NUMBERS"
    GET_NUMBER_0 = "GET_NUMBER_0"
    GET_NUMBER_88 = "GET_NUMBER_88"
    GET_NUMBER_100 = "GET_NUMBER_100"

class AchievementResult:
    """
    Represents the result of an achievement check.
    """
    def __init__(self, achievement_type, reply_text):
        """
        Initializes an AchievementResult.

        Args:
            achievement_type (AchievementType): The type of achievement unlocked.
            reply_text (str): The text to reply with.
        """
        self.achievement_type = achievement_type
        self.reply_text = reply_text

class AchievementContext:
    """
    Maintains the context of achievements unlocked during a single number processing.
    """
    def __init__(self):
        """
        Initializes AchievementContext.
        """
        # List of (achievement_type, reply_text)
        self.achievements = []
        # Set of achievement_types
        self.types = set()

    def add_achievement(self, achievement_type, reply_text):
        """
        Adds an unlocked achievement to the context.

        Args:
            achievement_type (AchievementType): The type of achievement.
            reply_text (str): The text to reply with.
        """
        self.achievements.append((achievement_type, reply_text))
        self.types.add(achievement_type)

class AchievementStrategy(ABC):
    """
    Abstract base class for achievement unlocking strategies.
    """
    def __init__(self, config, user_repo, db):
        """
        Initializes AchievementStrategy.

        Args:
            config: Configuration object.
            user_repo: Repository for user data.
            db: Database connection.
        """
        self.config = config
        self.user_repo = user_repo
        self.db = db

    @abstractmethod
    async def check(self, message, number, cache_data, remaining_numbers) -> AchievementResult:
        """
        Checks if the logged number unlocks an achievement.

        Args:
            message: The message object.
            number (int): The logged number.
            cache_data: Cached data.
            remaining_numbers (list): List of numbers the user hasn't collected yet.

        Returns:
            AchievementResult: The result if an achievement is unlocked, else None.
        """
        pass

    async def need_update(self, message, cache_data, achievement_type):
        """
        Checks if the user already has the achievement.

        Args:
            message: The message object.
            cache_data: Cached data.
            achievement_type (AchievementType): The achievement to check.

        Returns:
            bool: True if the user needs the achievement, False if they already have it.
        """
        user_info = cache_data.user_info_cache.get((message.user_id, message.chat_id))
        if user_info and user_info.achievements:
            if achievement_type.value in user_info.achievements.split(','):
                return False
        
        # Fallback to DB
        query = "SELECT achievements FROM user_data WHERE user_id = %s AND chat_id = %s"
        result = await self.db.fetch_one(query, (message.user_id, message.chat_id))
        if result and result[0]:
            achievements_list = result[0].split(',')
            if achievement_type.value in achievements_list:
                if user_info:
                    user_info.achievements = result[0]
                return False
        
        return True

    def get_achievement_reply(self, achievement_type):
        """
        Formats the achievement unlock message based on configuration.

        Args:
            achievement_type (AchievementType): The unlocked achievement.

        Returns:
            str: The formatted reply text.
        """
        config_data = self.config.achievement_text.get(achievement_type.value)
        if config_data:
            text = config_data.get('text', f"Achievement Unlocked: {achievement_type.name}")
            emoji = config_data.get('emoji', '🏆')
            return f"Achievement Unlocked: {text} {emoji}"
        return f"Achievement Unlocked: {achievement_type.name} 🏆"

class ObtainAllNumbersAchievementStrategy(AchievementStrategy):
    """
    Strategy for unlocking an achievement when all numbers (0-100) are collected.
    """
    async def check(self, message, number, cache_data, remaining_numbers):
        """
        Checks if the user has collected all numbers.
        """
        if remaining_numbers is not None and len(remaining_numbers) == 0:
            if await self.need_update(message, cache_data, AchievementType.ALL_NUMBERS):
                reply_text = self.get_achievement_reply(AchievementType.ALL_NUMBERS)
                return AchievementResult(AchievementType.ALL_NUMBERS, reply_text)
        return None

class GetSpecificNumberAchievementStrategy(AchievementStrategy):
    """
    Strategy for unlocking an achievement when a specific target number is logged.
    """
    def __init__(self, target_number, achievement_type, config, user_repo, db):
        """
        Initializes GetSpecificNumberAchievementStrategy.

        Args:
            target_number (int): The number to trigger on.
            achievement_type (AchievementType): The achievement to unlock.
            config: Configuration object.
            user_repo: User repository.
            db: Database connection.
        """
        super().__init__(config, user_repo, db)
        self.target_number = target_number
        self.achievement_type = achievement_type

    async def check(self, message, number, cache_data, remaining_numbers):
        """
        Checks if the logged number matches the target number.
        """
        if number == self.target_number:
            if await self.need_update(message, cache_data, self.achievement_type):
                reply_text = self.get_achievement_reply(self.achievement_type)
                return AchievementResult(self.achievement_type, reply_text)
        return None
