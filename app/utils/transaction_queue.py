import asyncio
import logging

logger = logging.getLogger(__name__)

class TransactionQueue:
    """
    A queue-based system for processing database transactions sequentially in the background.
    """
    def __init__(self, db, max_size=1000):
        """
        Initializes the TransactionQueue.

        Args:
            db: The database connection.
            max_size (int): Maximum number of transactions to hold in the queue. Defaults to 1000.
        """
        self.queue = asyncio.Queue(maxsize=max_size)
        self.db = db
        self._worker_task = None

    def start_worker(self):
        """Starts the background worker task."""
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("Transaction worker started.")

    async def stop_worker(self):
        """Stops the background worker task gracefully, waiting for pending transactions."""
        if self._worker_task:
            await self.queue.join()  # Wait for all tasks to be processed
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            logger.info("Transaction worker stopped.")

    async def submit(self, queries):
        """
        Submits a list of queries (a transaction) to the queue.
        Blocks if the queue is full (backpressure).

        Args:
            queries (list): A list of (query_string, parameters) tuples.
        """
        await self.queue.put(queries)

    async def _worker(self):
        """The main worker loop that processes transactions sequentially from the queue."""
        while True:
            try:
                queries = await self.queue.get()
                
                try:
                    await self.db.execute_transaction(queries)
                    
                except Exception as e:
                    logger.error(f"Transaction failed: {e}", exc_info=True)
                    # In a future iteration, retry logic would go here.
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)
