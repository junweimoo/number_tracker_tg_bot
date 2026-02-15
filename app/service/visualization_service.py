import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import io
import asyncio
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class VisualizationService:
    def __init__(self, db, config, repositories, bot=None):
        self.db = db
        self.config = config
        self.bot = bot
        self.stats_repo = repositories['stats']
        self.attendance_repo = repositories['attendance']
        self.match_log_repo = repositories['match_log']
        self.user_repo = repositories['user']
        self.number_log_repo = repositories['number_log']

    def set_bot(self, bot):
        self.bot = bot

    async def generate_number_count_visualization(self, chat_id, user_id=None, start_date=None):
        try:
            if start_date:
                data = await self.stats_repo.get_number_counts_since(chat_id, start_date, user_id)
                title_suffix = f"since {start_date}"
            else:
                data = await self.stats_repo.get_all_number_counts(chat_id, user_id)
                title_suffix = "All Time"

            if not data:
                return None

            # Unpack data
            numbers = [row[0] for row in data]
            counts = [row[1] for row in data]

            # Create figure
            # Matplotlib is not thread-safe and can be slow, but for now we run it in the loop.
            # Ideally this should be in an executor if it takes too long.
            color = '#90EE90' # Light green
            edge_color = '#228B22' # Forest green
            plt.figure(figsize=(12, 6))
            plt.bar(numbers, counts, color=color, edgecolor=edge_color, alpha=0.7)
            
            # Determine user name for title if specific user
            user_info = ""
            if user_id:
                user_name = await self.user_repo.get_user_name(user_id, chat_id)
                user_info = f" for {user_name}"

            plt.title(f"Number Frequency{user_info} ({title_suffix})")
            plt.xlabel("Number")
            plt.ylabel("Count")
            plt.grid(axis='y', linestyle='--', alpha=0.5)
            
            # Set x-axis limits and ticks
            plt.xlim(-1, 101)
            plt.xticks(range(0, 101, 5))  # Ticks every 5 numbers
            
            # Ensure y-axis has integer ticks
            plt.gca().yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

            # Save to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            return buf

        except Exception as e:
            logger.error(f"Error generating visualization: {e}", exc_info=True)
            return None

    async def generate_time_series_visualization(self, chat_id, user_id=None, hourly_buckets=True, buckets=48):
        try:
            # Set style
            plt.style.use('seaborn-v0_8-pastel')
            plt.figure(figsize=(12, 6))
            
            # Define color theme
            color = '#90EE90' # Light green
            edge_color = '#228B22' # Forest green

            tz_offset = self.config.timezone_gmt
            tz = timezone(timedelta(hours=tz_offset))
            now = datetime.now(tz)

            if hourly_buckets:
                # Past 'buckets' hours
                start_time = now - timedelta(hours=buckets)
                data = await self.number_log_repo.get_hourly_counts(chat_id, start_time, user_id)
                title_period = f"Past {buckets} Hours"
                xlabel = "Time (Hour)"
                date_fmt = mdates.DateFormatter('%H:%M', tz=tz)
            else:
                # Past 'buckets' days
                start_date = (now - timedelta(days=buckets)).date()
                data = await self.stats_repo.get_daily_counts(chat_id, start_date, user_id)
                title_period = f"Past {buckets} Days"
                xlabel = "Date"
                date_fmt = mdates.DateFormatter('%Y-%m-%d', tz=tz)

            if not data:
                return None

            # Unpack data
            timestamps = [row[0] for row in data]
            counts = [row[1] for row in data]

            # Plot
            plt.plot(timestamps, counts, marker='o', linestyle='-', color=edge_color, markerfacecolor=color, markersize=8, linewidth=2)
            plt.fill_between(timestamps, counts, color=color, alpha=0.3)

            # Formatting
            user_info = ""
            if user_id:
                user_name = await self.user_repo.get_user_name(user_id, chat_id)
                user_info = f" for {user_name}"

            plt.title(f"Activity Over Time{user_info} ({title_period})", fontsize=16, color='#333333')
            plt.xlabel(xlabel, fontsize=12)
            plt.ylabel("Numbers Logged", fontsize=12)
            
            # Grid
            plt.grid(True, linestyle='--', alpha=0.6)
            
            # X-axis date formatting
            ax = plt.gca()
            ax.xaxis.set_major_formatter(date_fmt)
            plt.gcf().autofmt_xdate() # Rotate dates
            
            # Ensure y-axis has integer ticks
            ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

            # Save to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
            buf.seek(0)
            plt.close()
            
            return buf

        except Exception as e:
            logger.error(f"Error generating time series visualization: {e}", exc_info=True)
            return None
