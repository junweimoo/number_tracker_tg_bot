import asyncio
import logging

logger = logging.getLogger(__name__)

class TransactionQueue:
    """
    A queue-based system for processing database transactions sequentially in the background.
    """
    def __init__(self, db, max_size=5000, batch_size=200):
        """
        Initializes the TransactionQueue.

        Args:
            db: The database connection.
            max_size (int): Maximum number of transactions to hold in the queue.
            batch_size (int): Maximum number of transactions to group into a single DB commit.
        """
        self.queue = asyncio.Queue(maxsize=max_size)
        self.db = db
        self.batch_size = batch_size
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
                # Wait for the first item
                first_transaction = await self.queue.get()
                
                batched_queries = []
                batched_queries.extend(first_transaction)
                
                transactions_count = 1
                
                # Try to fetch more transactions to fill the batch
                while transactions_count < self.batch_size:
                    try:
                        # Use get_nowait to avoid blocking if queue is empty
                        next_transaction = self.queue.get_nowait()
                        batched_queries.extend(next_transaction)
                        transactions_count += 1
                    except asyncio.QueueEmpty:
                        break
                
                # Execute the batched queries
                try:
                    if batched_queries:
                        # logger.warning(f"{transactions_count}")
                        await self.db.execute_transaction(batched_queries)
                except Exception as e:
                    logger.error(f"Transaction batch failed: {e}", exc_info=True)
                finally:
                    # Mark all fetched tasks as done
                    for _ in range(transactions_count):
                        self.queue.task_done()
                        
                # if self.queue.empty():
                #     logger.info("transaction queue flushed")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)
