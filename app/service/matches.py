from abc import ABC, abstractmethod
from enum import Enum

class MatchType(Enum):
    """
    Enumeration of different types of matches between logged numbers.
    """
    SAME_NUMBER_LAST = "SAME_NUMBER_LAST"
    SAME_NUMBER_LAST_SELF = "SAME_NUMBER_LAST_SELF"
    REVERSE_NUMBER_LAST = "REVERSE_NUMBER_LAST"
    REVERSE_NUMBER_LAST_SELF = "REVERSE_NUMBER_LAST_SELF"
    SUM_TARGET = "SUM_TARGET"
    SUM_TARGET_SELF = "SUM_TARGET_SELF"
    ABC_SUM = "ABC_SUM"
    DOUBLE_LAST = "DOUBLE_LAST"
    DOUBLE_LAST_SELF = "DOUBLE_LAST_SELF"
    HALF_LAST = "HALF_LAST"
    HALF_LAST_SELF = "HALF_LAST_SELF"
    STEP_UP = "STEP_UP"
    STEP_UP_SELF = "STEP_UP_SELF"
    STEP_DOWN = "STEP_DOWN"
    STEP_DOWN_SELF = "STEP_DOWN_SELF"
    SQUARE_LAST = "SQUARE_LAST"
    SQUARE_LAST_SELF = "SQUARE_LAST_SELF"
    SQRT_LAST = "SQRT_LAST"
    SQRT_LAST_SELF = "SQRT_LAST_SELF"
    ARITHMETIC_PROGRESSION = "ARITHMETIC_PROGRESSION"
    GEOMETRIC_PROGRESSION = "GEOMETRIC_PROGRESSION"
    DIGIT_SUM = "DIGIT_SUM"

class MatchContext:
    """
    Maintains the context of matches detected during a single number processing.
    """
    def __init__(self):
        """
        Initializes MatchContext.
        """
        # Set of (MatchType, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text)
        self.matches = []
        # Set of MatchType
        self.types = set()

    def add_match(self, match_type, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text):
        """
        Adds a detected match to the context.

        Args:
            match_type (MatchType): The type of match.
            message_user_id (int): The ID of the user who logged the current number.
            matched_user_id (int): The ID of the user who logged the matched number.
            matched_number (int): The matched number.
            matched_message_id (int): The ID of the message containing the matched number.
            reply_text (str): The text to reply with.
        """
        self.matches.append((match_type, message_user_id, matched_user_id, matched_number, matched_message_id, reply_text))
        self.types.add(match_type)

    def has_conflict(self, match_type, message_user_id, matched_user_id):
        """
        Checks if a match of the same type already exists for the same users in this context.

        Args:
            match_type (MatchType): The type of match.
            message_user_id (int): The ID of the current user.
            matched_user_id (int): The ID of the matched user.

        Returns:
            bool: True if a conflict exists, False otherwise.
        """
        # Check if there is already a match of this type for these users
        for m in self.matches:
            if m[0] == match_type and m[1] == message_user_id and m[2] == matched_user_id:
                return True
        return False

class MatchResult:
    """
    Represents the result of a match check.
    """
    def __init__(self, match_type, matched_user_id, matched_number, matched_message_id, reply_text):
        """
        Initializes a MatchResult.

        Args:
            match_type (MatchType): The type of match.
            matched_user_id (int): The ID of the matched user.
            matched_number (int): The matched number.
            matched_message_id (int): The ID of the matched message.
            reply_text (str): The text to reply with.
        """
        self.match_type = match_type
        self.matched_user_id = matched_user_id
        self.matched_number = matched_number
        self.matched_message_id = matched_message_id
        self.reply_text = reply_text

class MatchStrategy(ABC):
    """
    Abstract base class for match detection strategies.
    """
    @abstractmethod
    def check(self, message, number, cache_data) -> list[MatchResult]:
        """
        Checks if the logged number triggers any matches.

        Args:
            message: The message object.
            number (int): The logged number.
            cache_data: Cached data for lookups.

        Returns:
            list[MatchResult]: A list of match results.
        """
        pass

    @abstractmethod
    def has_conflict(self, match_context) -> bool:
        """
        Checks if this strategy conflicts with already detected matches.

        Args:
            match_context (MatchContext): The current match context.

        Returns:
            bool: True if a conflict exists, False otherwise.
        """
        pass

class SameNumberMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the same as the last number logged in the chat by a different user.
    """
    def __init__(self, config):
        """
        Initializes SameNumberMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a same-number match in the chat log.
        """
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
        """
        Conflicts with self-same-number matches.
        """
        return MatchType.SAME_NUMBER_LAST_SELF in match_context.types

class SelfSameNumberMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the same as the last number logged by the same user.
    """
    def __init__(self, config):
        """
        Initializes SelfSameNumberMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a same-number match in the user's own log.
        """
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
        """
        Conflicts with same-number matches from other users.
        """
        return MatchType.SAME_NUMBER_LAST in match_context.types

class ReverseNumberMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the reverse of the last number logged in the chat.
    """
    def __init__(self, config):
        """
        Initializes ReverseNumberMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a reverse-number match in the chat log.
        """
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
        """
        Conflicts with self-reverse-number matches.
        """
        return MatchType.REVERSE_NUMBER_LAST_SELF in match_context.types

class SelfReverseNumberMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the reverse of the last number logged by the same user.
    """
    def __init__(self, config):
        """
        Initializes SelfReverseNumberMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a reverse-number match in the user's own log.
        """
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
        """
        Conflicts with reverse-number matches from other users.
        """
        return MatchType.REVERSE_NUMBER_LAST in match_context.types

class SumTargetMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the sum of the current number and the last number in the chat equals a target.
    """
    def __init__(self, target, config):
        """
        Initializes SumTargetMatchStrategy.

        Args:
            target (int): The target sum.
            config: Configuration object.
        """
        self.target = target
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a sum-target match in the chat log.
        """
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
        """
        Conflicts with self-sum-target matches.
        """
        return MatchType.SUM_TARGET_SELF in match_context.types

class SelfSumTargetMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the sum of the current number and the user's last number equals a target.
    """
    def __init__(self, target, config):
        """
        Initializes SelfSumTargetMatchStrategy.

        Args:
            target (int): The target sum.
            config: Configuration object.
        """
        self.target = target
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a sum-target match in the user's own log.
        """
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
        """
        Conflicts with sum-target matches from other users.
        """
        return MatchType.SUM_TARGET in match_context.types

class ABCSumMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the sum of any two recent numbers in the chat.
    """
    def __init__(self, config):
        """
        Initializes ABCSumMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for an ABC sum match in the recent chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if not chat_log or len(chat_log) < 2:
            return []

        seen = {}
        # Iterate backwards to find the most recent pair among the last 5 numbers.
        for i in range(len(chat_log) - 1, max(-1, len(chat_log) - 6), -1):
            user_id, val, ts, msg_id = chat_log[i]
            complement = number - val
            if complement in seen:
                # Found a pair (entry_a, entry_b) such that entry_a.val + entry_b.val == number
                entry_a = seen[complement] # newer
                entry_b = chat_log[i] # older
                
                config_str = self.config.match_replies.get(MatchType.ABC_SUM.name)
                
                u1 = cache_data.user_info_cache.get((entry_b[0], message.chat_id))
                u2 = cache_data.user_info_cache.get((entry_a[0], message.chat_id))
                u3 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                
                u1_name = u1.user_name if u1 else "Unknown"
                u2_name = u2.user_name if u2 else "Unknown"
                u3_name = u3.user_name if u3 else "Unknown"
                
                reply_str = config_str.format(n1=entry_b[1], n2=entry_a[1], n3=number,
                                              u1=u1_name, u2=u2_name, u3=u3_name, number=number)
                
                return [
                    MatchResult(MatchType.ABC_SUM, entry_b[0], entry_b[1], entry_b[3], reply_str),
                    MatchResult(MatchType.ABC_SUM, entry_a[0], entry_a[1], entry_a[3], reply_str)
                ]
            seen[val] = chat_log[i]
            
        return []

    def has_conflict(self, match_context) -> bool:
        """
        No conflicts for ABC sum matches.
        """
        return False

class DoubleMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is double the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes DoubleMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a double-number match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_number * 2 == number and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.DOUBLE_LAST.name, "Double!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, number=last_number)
                return [MatchResult(
                    MatchType.DOUBLE_LAST,
                    last_user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with self-double matches.
        """
        return MatchType.DOUBLE_LAST_SELF in match_context.types

class SelfDoubleMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is double the user's last number.
    """
    def __init__(self, config):
        """
        Initializes SelfDoubleMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a double-number match in the user's own log.
        """
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if last_number * 2 == number:
                config_str = self.config.match_replies.get(MatchType.DOUBLE_LAST_SELF.name, "Double!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, number=last_number)
                return [MatchResult(
                    MatchType.DOUBLE_LAST_SELF,
                    message.user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with double matches from other users.
        """
        return MatchType.DOUBLE_LAST in match_context.types

class HalfMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is half the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes HalfMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a half-number match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_number == number * 2 and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.HALF_LAST.name, "Half!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, number=last_number)
                return [MatchResult(
                    MatchType.HALF_LAST,
                    last_user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with self-half matches.
        """
        return MatchType.HALF_LAST_SELF in match_context.types

class SelfHalfMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is half the user's last number.
    """
    def __init__(self, config):
        """
        Initializes SelfHalfMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a half-number match in the user's own log.
        """
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if last_number == number * 2:
                config_str = self.config.match_replies.get(MatchType.HALF_LAST_SELF.name, "Half!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, number=last_number)
                return [MatchResult(
                    MatchType.HALF_LAST_SELF,
                    message.user_id,
                    last_number,
                    last_msg_id,
                    reply_str
                )]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with half matches from other users.
        """
        return MatchType.HALF_LAST in match_context.types

class StepMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is one more or one less than the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes StepMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a step match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_user_id != message.user_id:
                if last_number + 1 == number:
                    match_type = MatchType.STEP_UP
                elif last_number - 1 == number:
                    match_type = MatchType.STEP_DOWN
                else:
                    return []
                
                config_str = self.config.match_replies.get(match_type.name, "Step!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, last=last_number, current=number)
                return [MatchResult(match_type, last_user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with self-step matches.
        """
        return MatchType.STEP_UP_SELF in match_context.types or MatchType.STEP_DOWN_SELF in match_context.types

class SelfStepMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is one more or one less than the user's last number.
    """
    def __init__(self, config):
        """
        Initializes SelfStepMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a step match in the user's own log.
        """
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if last_number + 1 == number:
                match_type = MatchType.STEP_UP_SELF
            elif last_number - 1 == number:
                match_type = MatchType.STEP_DOWN_SELF
            else:
                return []

            config_str = self.config.match_replies.get(match_type.name, "Step!")
            u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
            reply_str = config_str.format(u1=u1.user_name, last=last_number, current=number)
            return [MatchResult(match_type, message.user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with step matches from other users.
        """
        return MatchType.STEP_UP in match_context.types or MatchType.STEP_DOWN in match_context.types

class SquareMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the square of the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes SquareMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a square match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_number ** 2 == number and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.SQUARE_LAST.name, "Square!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, last=last_number, current=number)
                return [MatchResult(MatchType.SQUARE_LAST, last_user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with self-square matches.
        """
        return MatchType.SQUARE_LAST_SELF in match_context.types

class SelfSquareMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the square of the user's last number.
    """
    def __init__(self, config):
        """
        Initializes SelfSquareMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a square match in the user's own log.
        """
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if last_number ** 2 == number:
                config_str = self.config.match_replies.get(MatchType.SQUARE_LAST_SELF.name, "Square!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, last=last_number, current=number)
                return [MatchResult(MatchType.SQUARE_LAST_SELF, message.user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with square matches from other users.
        """
        return MatchType.SQUARE_LAST in match_context.types

class SqrtMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the square root of the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes SqrtMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a square root match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if number ** 2 == last_number and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.SQRT_LAST.name, "Square Root!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, last=last_number, current=number)
                return [MatchResult(MatchType.SQRT_LAST, last_user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with self-square-root matches.
        """
        return MatchType.SQRT_LAST_SELF in match_context.types

class SelfSqrtMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number is the square root of the user's last number.
    """
    def __init__(self, config):
        """
        Initializes SelfSqrtMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a square root match in the user's own log.
        """
        user_log = cache_data.user_log_cache.get((message.user_id, message.chat_id))
        if user_log:
            last_number, last_ts, last_msg_id = user_log[-1]
            if number ** 2 == last_number:
                config_str = self.config.match_replies.get(MatchType.SQRT_LAST_SELF.name, "Square Root!")
                u1 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, last=last_number, current=number)
                return [MatchResult(MatchType.SQRT_LAST_SELF, message.user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        Conflicts with square root matches from other users.
        """
        return MatchType.SQRT_LAST in match_context.types

class ArithmeticProgressionMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the last three numbers in the chat form an arithmetic progression.
    """
    def __init__(self, config):
        """
        Initializes ArithmeticProgressionMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for an arithmetic progression in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if not chat_log or len(chat_log) < 2:
            return []
        
        u1_id, n1, ts1, m1 = chat_log[-2]
        u2_id, n2, ts2, m2 = chat_log[-1]
        
        if n2 - n1 == number - n2 and n2 - n1 != 0 and abs(n2 - n1) > 1:
            config_str = self.config.match_replies.get(MatchType.ARITHMETIC_PROGRESSION.name, "Sequence!")
            user1 = cache_data.user_info_cache.get((u1_id, message.chat_id))
            user2 = cache_data.user_info_cache.get((u2_id, message.chat_id))
            user3 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
            u1_name = user1.user_name if user1 else "Unknown"
            u2_name = user2.user_name if user2 else "Unknown"
            u3_name = user3.user_name if user3 else "Unknown"
            reply_str = config_str.format(u1=u1_name, u2=u2_name, u3=u3_name, n1=n1, n2=n2, n3=number, diff=n2-n1)
            return [
                MatchResult(MatchType.ARITHMETIC_PROGRESSION, u1_id, n1, m1, reply_str),
                MatchResult(MatchType.ARITHMETIC_PROGRESSION, u2_id, n2, m2, reply_str)
            ]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        No conflicts for arithmetic progression matches.
        """
        return False

class GeometricProgressionMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the last three numbers in the chat form a geometric progression.
    """
    def __init__(self, config):
        """
        Initializes GeometricProgressionMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def check(self, message, number, cache_data):
        """
        Checks for a geometric progression in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if not chat_log or len(chat_log) < 2:
            return []
        
        u1_id, n1, ts1, m1 = chat_log[-2]
        u2_id, n2, ts2, m2 = chat_log[-1]
        
        if n1 != 0 and n2 != 0 and n1 != n2 and n2 * n2 == n1 * number:
            config_str = self.config.match_replies.get(MatchType.GEOMETRIC_PROGRESSION.name, "Geometric Sequence!")
            user1 = cache_data.user_info_cache.get((u1_id, message.chat_id))
            user2 = cache_data.user_info_cache.get((u2_id, message.chat_id))
            user3 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
            u1_name = user1.user_name if user1 else "Unknown"
            u2_name = user2.user_name if user2 else "Unknown"
            u3_name = user3.user_name if user3 else "Unknown"
            reply_str = config_str.format(u1=u1_name, u2=u2_name, u3=u3_name, n1=n1, n2=n2, n3=number, ratio=n2/n1)
            return [
                MatchResult(MatchType.GEOMETRIC_PROGRESSION, u1_id, n1, m1, reply_str),
                MatchResult(MatchType.GEOMETRIC_PROGRESSION, u2_id, n2, m2, reply_str)
            ]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        No conflicts for geometric progression matches.
        """
        return False

class DigitSumMatchStrategy(MatchStrategy):
    """
    Strategy for detecting if the current number has the same digit sum as the last number in the chat.
    """
    def __init__(self, config):
        """
        Initializes DigitSumMatchStrategy.

        Args:
            config: Configuration object.
        """
        self.config = config

    def _sum_digits(self, n):
        """
        Calculates the sum of digits of a number.
        """
        return sum(int(d) for d in str(abs(n)))

    def check(self, message, number, cache_data):
        """
        Checks for a digit sum match in the chat log.
        """
        chat_log = cache_data.chat_log_cache.get(message.chat_id)
        if chat_log:
            last_user_id, last_number, last_ts, last_msg_id = chat_log[-1]
            if last_number != number and self._sum_digits(last_number) == self._sum_digits(number) and last_user_id != message.user_id:
                config_str = self.config.match_replies.get(MatchType.DIGIT_SUM.name, "Digit Sum Match!")
                u1 = cache_data.user_info_cache.get((last_user_id, message.chat_id))
                u2 = cache_data.user_info_cache.get((message.user_id, message.chat_id))
                reply_str = config_str.format(u1=u1.user_name, u2=u2.user_name, last=last_number, current=number, sum=self._sum_digits(number))
                return [MatchResult(MatchType.DIGIT_SUM, last_user_id, last_number, last_msg_id, reply_str)]
        return []

    def has_conflict(self, match_context) -> bool:
        """
        No conflicts for digit sum matches.
        """
        return False
