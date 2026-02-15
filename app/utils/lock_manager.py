import asyncio
from weakref import WeakValueDictionary

class ChatLockManager:
    """
    Manages asynchronous locks for different chats to ensure thread-safe operations per chat.
    """
    def __init__(self):
        """
        Initializes the ChatLockManager.
        """
        self.locks = WeakValueDictionary()
        self._global_lock = asyncio.Lock()

    async def get_lock(self, chat_id):
        """
        Retrieves or creates an asyncio.Lock for a specific chat_id.

        Args:
            chat_id (int): The ID of the chat.

        Returns:
            asyncio.Lock: The lock associated with the chat.
        """
        async with self._global_lock:
            lock = self.locks.get(chat_id)

            if lock is None:
                lock = asyncio.Lock()
                self.locks[chat_id] = lock

            return lock
