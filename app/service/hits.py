from abc import ABC, abstractmethod
from enum import Enum
from datetime import timezone, timedelta

class HitType(Enum):
    TARGET_NUMBER = "TARGET_NUMBER"
    CLOSE_NUMBER = "CLOSE_NUMBER"

class HitResult:
    def __init__(self, hit_type, hit_number, reply_text, react_emoji=None, forward_chat_ids=None, streak_counted = True):
        self.hit_type = hit_type
        self.hit_number = hit_number
        self.reply_text = reply_text
        self.react_emoji = react_emoji
        self.forward_chat_ids = forward_chat_ids
        self.streak_counted = streak_counted

class HitContext:
    def __init__(self):
        # List of (HitType, hit_number, reply_text, react_emoji, forward_chat_ids, streak_counted)
        self.hits = []
        # Set of HitType
        self.types = set()

    def add_hit(self, hit_type, hit_number, reply_text, react_emoji=None, forward_chat_ids=None, streak_counted=True):
        self.hits.append((hit_type, hit_number, reply_text, react_emoji, forward_chat_ids, streak_counted))
        self.types.add(hit_type)

class HitStrategy(ABC):
    @abstractmethod
    def check(self, message, number, cache_data) -> HitResult:
        pass

class HitSpecificNumberStrategy(HitStrategy):
    def __init__(self, target_number, number_log_repo, config):
        self.target_number = target_number
        self.number_log_repo = number_log_repo
        self.config = config

        details = config.hit_numbers.get(str(target_number))
        self.reply_text = details.get('reply')
        self.react_emoji = details.get('reaction')
        self.forwarding_chat_ids = config.forwarding_chat_ids

    def check(self, message, number, cache_data):
        if number == self.target_number:
            reply = self.reply_text

            recent_logs = self.number_log_repo.get_recent_logs_for_number(message.chat_id, number, limit=3)
            if recent_logs:
                reply += f"\nLast 3 gets for {self.target_number}:"
                sgt_timezone = timezone(timedelta(hours=8))
                for user_name, ts in recent_logs:
                    ts_sgt = ts.astimezone(sgt_timezone)
                    time_str = ts_sgt.strftime("%m-%d %H:%M:%S")
                    reply += f"\n- {user_name} at {time_str}"

            return HitResult(
                HitType.TARGET_NUMBER,
                number,
                reply,
                self.react_emoji,
                self.forwarding_chat_ids.get(str(message.chat_id), []),
                True
            )
        return None

class HitCloseNumberStrategy(HitStrategy):
    def __init__(self, target_number, config):
        self.target_number = target_number
        self.config = config

        details = config.close_numbers.get(str(target_number))
        self.react_emoji = details.get('reaction')

    def check(self, message, number, cache_data):
        if number == self.target_number:
             return HitResult(
                HitType.CLOSE_NUMBER,
                number,
                None,
                self.react_emoji,
                [],
                 False
             )
        return None