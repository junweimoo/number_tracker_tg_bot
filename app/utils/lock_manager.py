import asyncio
from weakref import WeakValueDictionary

class ChatLockManager:
    def __init__(self):
        self.locks = WeakValueDictionary()
        self._global_lock = asyncio.Lock()

    async def get_lock(self, chat_id):
        async with self._global_lock:
            lock = self.locks.get(chat_id)

            if lock is None:
                lock = asyncio.Lock()
                self.locks[chat_id] = lock

            return lock
