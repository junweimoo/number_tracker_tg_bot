from abc import ABC, abstractmethod
from enum import Enum

class AchievementType(Enum):
    ALL_NUMBERS = "ALL_NUMBERS"
    GET_NUMBER_0 = "GET_NUMBER_0"
    GET_NUMBER_88 = "GET_NUMBER_88"
    GET_NUMBER_100 = "GET_NUMBER_100"

class AchievementResult:
    def __init__(self, achievement_type, reply_text):
        self.achievement_type = achievement_type
        self.reply_text = reply_text

class AchievementContext:
    def __init__(self):
        # List of (achievement_type, reply_text)
        self.achievements = []
        # Set of achievement_types
        self.types = set()

    def add_achievement(self, achievement_type, reply_text):
        self.achievements.append((achievement_type, reply_text))
        self.types.add(achievement_type)

class AchievementStrategy(ABC):
    def __init__(self, config, user_repo, db):
        self.config = config
        self.user_repo = user_repo
        self.db = db

    @abstractmethod
    def check(self, message, number, cache_data, remaining_numbers) -> AchievementResult:
        pass

    def need_update(self, message, cache_data, achievement_type):
        user_info = cache_data.user_info_cache.get((message.user_id, message.chat_id))
        if user_info and user_info.achievements:
            if achievement_type.value in user_info.achievements.split(','):
                return False
        
        # Fallback to DB
        query = "SELECT achievements FROM user_data WHERE user_id = %s AND chat_id = %s"
        result = self.db.fetch_one(query, (message.user_id, message.chat_id))
        if result and result[0]:
            achievements_list = result[0].split(',')
            if achievement_type.value in achievements_list:
                if user_info:
                    user_info.achievements = result[0]
                return False
        
        return True

    def get_achievement_reply(self, achievement_type):
        config_data = self.config.achievement_text.get(achievement_type.value)
        if config_data:
            text = config_data.get('text', f"Achievement Unlocked: {achievement_type.name}")
            emoji = config_data.get('emoji', '🏆')
            return f"Achievement Unlocked: {text} {emoji}"
        return f"Achievement Unlocked: {achievement_type.name} 🏆"

class ObtainAllNumbersAchievementStrategy(AchievementStrategy):
    def check(self, message, number, cache_data, remaining_numbers):
        if remaining_numbers is not None and len(remaining_numbers) == 0:
            if self.need_update(message, cache_data, AchievementType.ALL_NUMBERS):
                reply_text = self.get_achievement_reply(AchievementType.ALL_NUMBERS)
                return AchievementResult(AchievementType.ALL_NUMBERS, reply_text)
        return None

class GetSpecificNumberAchievementStrategy(AchievementStrategy):
    def __init__(self, target_number, achievement_type, config, user_repo, db):
        super().__init__(config, user_repo, db)
        self.target_number = target_number
        self.achievement_type = achievement_type

    def check(self, message, number, cache_data, remaining_numbers):
        if number == self.target_number:
            if self.need_update(message, cache_data, self.achievement_type):
                reply_text = self.get_achievement_reply(self.achievement_type)
                return AchievementResult(self.achievement_type, reply_text)
        return None
