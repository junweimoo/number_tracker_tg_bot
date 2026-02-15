import json
import os

class Config:
    def __init__(self, config_path='config.json'):
        self.config_path = config_path
        self.data = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return json.load(f)

    @property
    def message_regex(self):
        return self.data.get('regex', {}).get('message_regex')

    @property
    def reply_message(self):
        return self.data.get('replies', {}).get('reply_message')

    @property
    def developer_user_ids(self):
        return self.data.get('developer_user_ids', [])

    @property
    def tracked_chat_ids(self):
        return self.data.get('tracked_chat_ids', [])

    @property
    def hit_numbers(self):
        return self.data.get('hit_numbers', {})

    @property
    def forwarding_chat_ids(self):
        return self.data.get('forwarding_chat_ids', {})

    @property
    def daily_stats_chat_ids(self):
        return self.data.get('daily_stats_chat_ids', [])

    @property
    def timezone_gmt(self):
        return self.data.get('timezone_gmt', 0)

    @property
    def match_replies(self):
        return self.data.get('match_replies', {})

    @property
    def attendance_replies(self):
        return self.data.get('attendance_replies', [])