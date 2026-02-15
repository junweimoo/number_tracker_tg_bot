from abc import ABC, abstractmethod
from enum import Enum
from datetime import timezone, timedelta

class HitType(Enum):
    """
    Enumeration of different types of 'hits' (special numbers).
    """
    TARGET_NUMBER = "TARGET_NUMBER"
    CLOSE_NUMBER = "CLOSE_NUMBER"

class HitResult:
    """
    Represents the result of a hit check.
    """
    def __init__(self, hit_type, hit_number, reply_text, react_emoji=None, forward_chat_ids=None, streak_counted = True):
        """
        Initializes a HitResult.

        Args:
            hit_type (HitType): The type of hit.
            hit_number (int): The number that triggered the hit.
            reply_text (str): The text to reply with.
            react_emoji (str, optional): The emoji to react with.
            forward_chat_ids (list, optional): List of chat IDs to forward the message to.
            streak_counted (bool): Whether this hit counts towards a streak. Defaults to True.
        """
        self.hit_type = hit_type
        self.hit_number = hit_number
        self.reply_text = reply_text
        self.react_emoji = react_emoji
        self.forward_chat_ids = forward_chat_ids
        self.streak_counted = streak_counted

class HitContext:
    """
    Maintains the context of hits detected during a single number processing.
    """
    def __init__(self):
        """
        Initializes HitContext.
        """
        # List of (HitType, hit_number, reply_text, react_emoji, forward_chat_ids, streak_counted)
        self.hits = []
        # Set of HitType
        self.types = set()

    def add_hit(self, hit_type, hit_number, reply_text, react_emoji=None, forward_chat_ids=None, streak_counted=True):
        """
        Adds a detected hit to the context.

        Args:
            hit_type (HitType): The type of hit.
            hit_number (int): The number that triggered the hit.
            reply_text (str): The text to reply with.
            react_emoji (str, optional): The emoji to react with.
            forward_chat_ids (list, optional): List of chat IDs to forward the message to.
            streak_counted (bool): Whether this hit counts towards a streak. Defaults to True.
        """
        self.hits.append((hit_type, hit_number, reply_text, react_emoji, forward_chat_ids, streak_counted))
        self.types.add(hit_type)

class HitStrategy(ABC):
    """
    Abstract base class for hit detection strategies.
    """
    @abstractmethod
    async def check(self, message, number, cache_data) -> HitResult:
        """
        Checks if the logged number triggers a hit.

        Args:
            message: The message object.
            number (int): The logged number.
            cache_data: Cached data for lookups.

        Returns:
            HitResult: The result of the hit check, or None if no hit.
        """
        pass

class HitSpecificNumberStrategy(HitStrategy):
    """
    Strategy for detecting hits on specific target numbers.
    """
    def __init__(self, target_number, number_log_repo, config):
        """
        Initializes HitSpecificNumberStrategy.

        Args:
            target_number (int): The number to trigger on.
            number_log_repo: Repository for number logs.
            config: Configuration object.
        """
        self.target_number = target_number
        self.number_log_repo = number_log_repo
        self.config = config

        details = config.hit_numbers.get(str(target_number))
        self.reply_text = details.get('reply')
        self.react_emoji = details.get('reaction')
        self.forwarding_chat_ids = config.forwarding_chat_ids

    async def check(self, message, number, cache_data):
        """
        Checks if the number matches the target number and fetches recent logs for it.

        Args:
            message: The message object.
            number (int): The logged number.
            cache_data: Cached data.

        Returns:
            HitResult: The hit result if matched, else None.
        """
        if number == self.target_number:
            reply = self.reply_text

            recent_logs = await self.number_log_repo.get_recent_logs_for_number(message.chat_id, number, limit=3)
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
    """
    Strategy for detecting hits on 'close' numbers (e.g., near misses).
    """
    def __init__(self, target_number, config):
        """
        Initializes HitCloseNumberStrategy.

        Args:
            target_number (int): The number to trigger on.
            config: Configuration object.
        """
        self.target_number = target_number
        self.config = config

        details = config.close_numbers.get(str(target_number))
        self.react_emoji = details.get('reaction')

    async def check(self, message, number, cache_data):
        """
        Checks if the number matches the target close number.

        Args:
            message: The message object.
            number (int): The logged number.
            cache_data: Cached data.

        Returns:
            HitResult: The hit result if matched, else None.
        """
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
