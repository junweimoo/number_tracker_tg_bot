import asyncio
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class Scheduler:
    """
    A simple task scheduler for running one-time and recurring jobs.
    """
    def __init__(self, executor=None):
        """
        Initializes the Scheduler.

        Args:
            executor: Optional executor for running jobs in a separate thread/process.
        """
        self.jobs = []
        self.recurring_jobs = []
        self.running = False
        self._worker_task = None
        self.executor = executor

    def register_job(self, func, run_time, context=None, is_recurring=False):
        """
        Registers a one-time job to be run at a specific time.

        Args:
            func: The async function to execute.
            run_time (datetime): A datetime object specifying when to run the job.
            context (dict, optional): Optional context dictionary to pass to the function.
            is_recurring (bool): Whether the task is recurring. Defaults to False.
        """
        self.jobs.append({
            'func': func,
            'run_time': run_time,
            'context': context
        })
        self.jobs.sort(key=lambda x: x['run_time'])
        if not is_recurring:
            logger.info(f"Registered one-time job {func.__name__} at {run_time}")

    def register_recurring_job(self, func, hour, minute, second, tz, context=None):
        """
        Registers a recurring job to be run daily at a specific time.
        
        Args:
            func: The async function to execute.
            hour (int): Hour (0-23).
            minute (int): Minute (0-59).
            second (int): Second (0-59).
            tz (timezone): Timezone object.
            context (dict, optional): Optional context dictionary to pass to the function.
        """
        self.recurring_jobs.append({
            'func': func,
            'hour': hour,
            'minute': minute,
            'second': second,
            'tz': tz,
            'context': context
        })
        logger.info(f"Registered recurring job {func.__name__} at {hour}:{minute}:{second} {tz}")
        
        # Schedule the first run
        self._schedule_next_run(self.recurring_jobs[-1])

    def _schedule_next_run(self, job_config):
        """
        Schedules the next execution of a recurring job.

        Args:
            job_config (dict): The configuration of the recurring job.
        """
        now = datetime.now(job_config['tz'])
        target_time = now.replace(
            hour=job_config['hour'], 
            minute=job_config['minute'], 
            second=job_config['second'], 
            microsecond=0
        )
        
        if target_time <= now:
            target_time += timedelta(days=1)

        target_time_naive = target_time.astimezone().replace(tzinfo=None)

        original_func = job_config['func']

        async def recurring_wrapper(ctx=None):
            try:
                if ctx:
                    await original_func(ctx)
                else:
                    await original_func()
            finally:
                self._schedule_next_run(job_config)
        
        # Register the wrapped job
        self.register_job(recurring_wrapper, target_time_naive, job_config['context'], True)

    def start_worker(self):
        """Starts the background worker task."""
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("Scheduler worker started.")

    async def stop_worker(self):
        """Stops the scheduler worker task."""
        self.running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            logger.info("Scheduler worker stopped.")


    async def _worker(self):
        """The main worker loop that checks for and executes due jobs."""
        self.running = True
        logger.info("Scheduler started.")
        while self.running:
            now = datetime.now()
            
            # Check for jobs that are due
            while self.jobs and self.jobs[0]['run_time'] <= now:
                job = self.jobs.pop(0)
                try:
                    logger.info(f"Executing job {job['func'].__name__}")
                    loop = asyncio.get_running_loop()
                    if self.executor:
                         if job['context']:
                            await loop.run_in_executor(self.executor, job['func'], job['context'])
                         else:
                            await loop.run_in_executor(self.executor, job['func'])
                    else:
                        if job['context']:
                            asyncio.create_task(job['func'](job['context']))
                        else:
                            asyncio.create_task(job['func']())
                except Exception as e:
                    logger.error(f"Error executing job {job['func'].__name__}: {e}", exc_info=True)

            await asyncio.sleep(1)
