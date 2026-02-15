import asyncio
import json
import logging
import urllib.request
import urllib.parse
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from utils.lock_manager import ChatLockManager

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class Message:
    """
    Represents a Telegram message with relevant fields extracted from the update.
    """
    def __init__(self, update):
        """
        Initializes a Message object from a Telegram update dictionary.

        Args:
            update (dict): The update dictionary from Telegram API.
        """
        self.update_id = update.get('update_id')

        message_data = update.get('message', {})
        self.message_id = message_data.get('message_id')
        self.date = message_data.get('date')
        self.text = message_data.get('text', '')
        
        chat = message_data.get('chat', {})
        self.chat_id = chat.get('id')
        self.chat_type = chat.get('type')
        self.chat_title = chat.get('title')
        self.thread_id = message_data.get('message_thread_id')
        
        from_user = message_data.get('from', {})
        self.user_id = from_user.get('id')
        self.username = from_user.get('username')
        self.first_name = from_user.get('first_name')
        self.last_name = from_user.get('last_name')

    def __repr__(self):
        return f"<Message chat_id={self.chat_id} user_id={self.user_id} text='{self.text}'>"

class TelegramBot:
    """
    A simple Telegram bot implementation using polling.
    """
    def __init__(self, token, context=None):
        """
        Initializes the TelegramBot.

        Args:
            token (str): The Telegram bot token.
            context (dict, optional): Context dictionary to share data between handlers.
        """
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}/"
        self.command_handlers = {}
        self.message_handlers = []
        self.offset = 0
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.context = context if context is not None else {}
        self.context['bot'] = self
        self.context['lock_manager'] = ChatLockManager()

    def register_command_handler(self, command, handler):
        """
        Registers a handler for a specific command (e.g., '/start').

        Args:
            command (str): The command string.
            handler (callable): The async function to handle the command.
        """
        self.command_handlers[command] = handler

    def register_message_handler(self, handler):
        """
        Registers a handler for generic text messages.

        Args:
            handler (callable): The async function to handle messages.
        """
        self.message_handlers.append(handler)

    async def _make_request(self, method, params=None, json_data=None):
        """
        Makes an asynchronous request to the Telegram API.

        Args:
            method (str): The API method name.
            params (dict, optional): Query parameters.
            json_data (dict, optional): JSON body data.

        Returns:
            dict: The parsed JSON response.
        """
        url = self.base_url + method
        data = None
        headers = {}
        
        if json_data:
            data = json.dumps(json_data).encode('utf-8')
            headers = {'Content-Type': 'application/json'}
        elif params:
            data = urllib.parse.urlencode(params).encode('utf-8')

        # Create the request object (POST if data is provided)
        req = urllib.request.Request(url, data=data, headers=headers)

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(self.executor, self._perform_request, req)
        return json.loads(response)

    def _perform_request(self, req):
        """
        Performs the blocking network request.

        Args:
            req (urllib.request.Request): The request object.

        Returns:
            str: The response body as a string.
        """
        with urllib.request.urlopen(req, timeout=40) as response:
            return response.read().decode('utf-8')

    async def start_polling(self):
        """Starts the bot polling loop to receive updates."""
        logger.info("Bot started polling...")
        while True:
            try:
                updates = await self._make_request('getUpdates', {'offset': self.offset, 'timeout': 30})
                if updates.get('ok'):
                    for update in updates['result']:
                        asyncio.create_task(self._dispatch(update))
                        self.offset = update['update_id'] + 1
            except Exception as e:
                logger.error(f"Error polling: {e}")
                await asyncio.sleep(5)

    async def _dispatch(self, update):
        """
        Dispatches the update to the appropriate handler.

        Args:
            update (dict): The update dictionary from Telegram.
        """
        if 'message' not in update:
            return

        message = Message(update)

        logger.info(f"Received update: "
                    f"chat_id={message.chat_id}[{message.chat_title}], "
                    f"user_id={message.user_id}[{message.username}], "
                    f"text='{message.text}'")

        text = message.text

        if text.startswith('/'):
            command = text.split()[0]
            if command in self.command_handlers:
                try:
                    start_time = time.perf_counter()
                    await self.command_handlers[command](message, self.context)
                    duration = time.perf_counter() - start_time
                    logger.info(f"Handler {self.command_handlers[command].__name__} - execution took {duration:.6f} seconds.")
                except Exception as e:
                    logger.error(f"Error in command handler '{command}': {e}", exc_info=True)
        else:
            for handler in self.message_handlers:
                try:
                    start_time = time.perf_counter()
                    await handler(message, self.context)
                    duration = time.perf_counter() - start_time
                    logger.info(f"Handler {handler.__name__} - execution took {duration:.6f} seconds.")
                except Exception as e:
                    logger.error(f"Error in message handler: {e}", exc_info=True)

    async def send_message(self, chat_id, text):
        """
        Sends a message to a chat.

        Args:
            chat_id (int): The ID of the chat.
            text (str): The message text.
        """
        await self._make_request('sendMessage', {'chat_id': chat_id, 'text': text})

    async def send_reply(self, chat_id, message_id, text):
        """
        Sends a reply to a specific message.

        Args:
            chat_id (int): The ID of the chat.
            message_id (int): The ID of the message to reply to.
            text (str): The message text.
        """
        params = {
            'chat_id': chat_id,
            'text': text,
            'reply_parameters': json.dumps({'message_id': message_id})
        }
        await self._make_request('sendMessage', params)

    async def set_message_reaction(self, chat_id, message_id, emoji):
        """
        Sets a reaction on a message with a given emoji.

        Args:
            chat_id (int): The ID of the chat.
            message_id (int): The ID of the message.
            emoji (str): The emoji to react with.
        """
        json_data = {
            'chat_id': chat_id,
            'message_id': message_id,
            'reaction': [{'type': 'emoji', 'emoji': emoji}]
        }
        await self._make_request('setMessageReaction', json_data=json_data)

    async def forward_message(self, from_chat_id, message_id, to_chat_id):
        """
        Forwards a message from one chat to another.

        Args:
            from_chat_id (int): The ID of the source chat.
            message_id (int): The ID of the message to forward.
            to_chat_id (int): The ID of the destination chat.
        """
        params = {
            'chat_id': to_chat_id,
            'from_chat_id': from_chat_id,
            'message_id': message_id
        }
        await self._make_request('forwardMessage', params)

    async def send_photo(self, chat_id, photo_buffer, caption=None):
        """
        Sends a photo to a chat.

        Args:
            chat_id (int): The ID of the chat.
            photo_buffer (io.BytesIO): The buffer containing the photo data.
            caption (str, optional): The caption for the photo.
        """
        url = self.base_url + 'sendPhoto'
        data = {'chat_id': str(chat_id)}
        if caption:
            data['caption'] = caption
        
        files = {'photo': ('chart.png', photo_buffer, 'image/png')}
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, lambda: requests.post(url, data=data, files=files))
