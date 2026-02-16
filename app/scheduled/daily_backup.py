import logging

logger = logging.getLogger(__name__)

class DailyBackupTask:
    def __init__(self, admin_service, config):
        self.admin_service = admin_service
        self.config = config

    async def run_daily_backup(self):
        backup_path = self.config.backup_path
        logger.info(f"Running daily backup to {backup_path}")
        count = await self.admin_service.export_number_logs(backup_path)
        logger.info(f"Backup complete: {count} logs exported to {backup_path}")