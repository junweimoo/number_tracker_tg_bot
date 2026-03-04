import logging

logger = logging.getLogger(__name__)

class DailyReminderTask:
    def __init__(self, bot, chat_id, reminder_service, config):
        self.bot = bot
        self.chat_id = chat_id
        self.reminder_service = reminder_service
        self.config = config

    async def run_reminder(self):
        try:
            logger.info(f"Running DailyReminder for chat {self.chat_id}")
            reminder_msg = await self.reminder_service.get_users_reminder(self.chat_id)
            if reminder_msg:
                await self.bot.send_html(self.chat_id, reminder_msg)
                logger.info(f"Reminder sent to chat {self.chat_id}")
            else:
                logger.info(f"No users to remind in chat {self.chat_id}")
        except Exception as e:
            logger.error(f"Error running DailyReminder: {e}", exc_info=True)
