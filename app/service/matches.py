from abc import ABC, abstractmethod
from enum import Enum

class MatchType(Enum):
    SAME_NUMBER_LAST = "SAME_NUMBER_LAST"
    SAME_NUMBER_LAST_SELF = "SAME_NUMBER_LAST_SELF"
    REVERSE_NUMBER_LAST = "REVERSE_NUMBER_LAST"
    REVERSE_NUMBER_LAST_SELF = "REVERSE_NUMBER_LAST_SELF"
    SUM_TARGET = "SUM_TARGET"
    SUM_TARGET_SELF = "SUM_TARGET_SELF"

class MatchContext:
    def __init__(self):
        # Set of (MatchType, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text)
        self.matches = []
        # Set of MatchType
        self.types = set()

    def add_match(self, match_type, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text):
        self.matches.append((match_type, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text))
        self.types.add(match_type)

    def has_conflict(self, match_type, message_user_id, matched_user_id):
        # Check if there is already a match of this type for these users
        for m in self.matches:
            if m[0] == match_type and m[1] == message_user_id and m[2] == matched_user_id:
                return True
        return False

class MatchResult:
    def __init__(self, match_type, matched_user_id, matched_number, matched_message_id, reply_text):
        self.match_type = match_type
        self.matched_user_id = matched_user_id
        self.matched_number = matched_number
        self.matched_message_id = matched_message_id
        self.reply_text = reply_text

class MatchStrategy(ABC):
    @abstractmethod
    def check(self, message, number, cache_data) -> list[MatchResult]:
        pass

    @abstractmethod
    def has_conflict(self, match_context) -> bool:
        pass

class SameNumberMatchStrategy(MatchStrategy):
    def __init__(self, config):
        self.config = config

    def check(self, message, number, cache_data):
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_number == number and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.SAME_NUMBER_LAST.name, "Match!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name)
                return [MatchResult(
                    MatchType.SAME_NUMBER_LAST,
                    last_user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.SAME_NUMBER_LAST_SELF in match_context.types

class SelfSameNumberMatchStrategy(MatchStrategy):
    def __init__(self, config):
        self.config = config

    def check(self, message, number, cache_data):
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if last_number == number:
                config_str = self.config.match_replies.get(MatchType.SAME_NUMBER_LAST_SELF.name, "Match!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name)
                return [MatchResult(
                    MatchType.SAME_NUMBER_LAST_SELF,
                    message.user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.SAME_NUMBER_LAST in match_context.types

class ReverseNumberMatchStrategy(MatchStrategy):
    def __init__(self, config):
        self.config = config

    def check(self, message, number, cache_data):
        if not (10 <= number <= 99):
            return []
        
        tens = number // 10
        ones = number % 10
        if tens == ones:
            return []
            
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            reverse_number = (ones * 10) + tens
            if last_number == reverse_number and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.REVERSE_NUMBER_LAST.name, "Reverse!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name)
                return [MatchResult(
                    MatchType.REVERSE_NUMBER_LAST,
                    last_user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.REVERSE_NUMBER_LAST_SELF in match_context.types

class SelfReverseNumberMatchStrategy(MatchStrategy):
    def __init__(self, config):
        self.config = config

    def check(self, message, number, cache_data):
        if not (10 <= number <= 99):
            return []

        tens = number // 10
        ones = number % 10
        if tens == ones:
            return []

        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            reverse_number = (ones * 10) + tens
            if last_number == reverse_number:
                config_str = self.config.match_replies.get(MatchType.REVERSE_NUMBER_LAST_SELF.name, "Reverse!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name)
                return [MatchResult(
                    MatchType.REVERSE_NUMBER_LAST_SELF,
                    message.user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.REVERSE_NUMBER_LAST in match_context.types

class SumTargetMatchStrategy(MatchStrategy):
    def __init__(self, target, config):
        self.target = target
        self.config = config

    def check(self, message, number, cache_data):
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if (last_number + number) == self.target and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.SUM_TARGET.name, "Sum {target}!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, target=self.target)
                return [MatchResult(
                    MatchType.SUM_TARGET,
                    last_user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.SUM_TARGET_SELF in match_context.types

class SelfSumTargetMatchStrategy(MatchStrategy):
    def __init__(self, target, config):
        self.target = target
        self.config = config

    def check(self, message, number, cache_data):
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if (last_number + number) == self.target:
                config_str = self.config.match_replies.get(MatchType.SUM_TARGET_SELF.name, "Sum {target}!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, target=self.target)
                return [MatchResult(
                    MatchType.SUM_TARGET_SELF,
                    message.user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        return MatchType.SUM_TARGET in match_context.types