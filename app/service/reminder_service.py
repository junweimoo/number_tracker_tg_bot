import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class ReminderService:
    def __init__(self, db, repositories, config, bot=None):
        self.db = db
        self.user_repository = repositories['user']
        self.config = config
        self.bot = bot

    def set_bot(self, bot):
        """
        Sets the bot instance.

        Args:
            bot: The bot instance.
        """
        self.bot = bot

    async def get_users_reminder(self, chat_id, thread_id=None):
        """
        Fetches users who last logged in yesterday and constructs a reminder message.

        Args:
            chat_id (int): The ID of the chat.
            thread_id (int, optional): The ID of the thread. Defaults to None.

        Returns:
            str: A reminder message, or None if no users are found.
        """
        tz = timezone(timedelta(hours=self.config.timezone_gmt))
        yesterday = (datetime.now(tz) - timedelta(days=1)).date()

        users = await self.user_repository.get_users_with_last_login(yesterday, chat_id)

        if not users:
            return None

        user_mentions = []
        for user_name, user_handle in users:
            if user_handle:
                user_mentions.append(f"@{user_handle}")
            else:
                user_mentions.append(user_name)

        message_template = self.config.reminder_message
        message = message_template.format(users_string=", ".join(user_mentions))
        return message
