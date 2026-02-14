from abc import ABC, abstractmethod
from enum import Enum
from datetime import timezone, timedelta

class HitType(Enum):
    SPECIFIC_NUMBER = "SPECIFIC_NUMBER"

class HitResult:
    def __init__(self, hit_type, hit_number, reply_text, react_emoji=None):
        self.hit_type = hit_type
        self.hit_number = hit_number
        self.reply_text = reply_text
        self.react_emoji = react_emoji

class HitContext:
    def __init__(self):
        # Set of (HitType, hit_number, reply_text, react_emoji)
        self.hits = set()

    def add_hit(self, hit_type, hit_number, reply_text, react_emoji=None):
        self.hits.add((hit_type, hit_number, reply_text, react_emoji))

class HitStrategy(ABC):
    @abstractmethod
    def check(self, message, number, cache_data) -> HitResult:
        pass

class HitSpecificNumberStrategy(HitStrategy):
    def __init__(self, target_number, reply_text=None, react_emoji=None, number_log_repo=None):
        self.target_number = target_number
        self.reply_text = reply_text if reply_text else f"Hit {target_number}!"
        self.react_emoji = react_emoji
        self.number_log_repo = number_log_repo

    def check(self, message, number, cache_data):
        if number == self.target_number:
            reply = self.reply_text
            
            if self.number_log_repo:
                recent_logs = self.number_log_repo.get_recent_logs_for_number(message.chat_id, number, limit=3)
                if recent_logs:
                    reply += "\n\nPrevious hits:"
                    sgt_timezone = timezone(timedelta(hours=8))
                    for user_name, ts in recent_logs:
                        ts_sgt = ts.astimezone(sgt_timezone)
                        time_str = ts_sgt.strftime("%m-%d %H:%M:%S")
                        reply += f"\n- {user_name} at {time_str}"

            return HitResult(
                HitType.SPECIFIC_NUMBER,
                number,
                reply,
                self.react_emoji
            )
        return None