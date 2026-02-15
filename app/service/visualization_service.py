import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import matplotlib.colors as mcolors
import io
import asyncio
import networkx as nx
import numpy as np
from datetime import datetime, timedelta, timezone, date
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

logger = logging.getLogger(__name__)

class VisualizationService:
    """
    Service responsible for generating various data visualizations and charts.
    """
    def __init__(self, db, config, repositories, bot=None):
        """
        Initializes the VisualizationService.

        Args:
            db: The database connection.
            config: Configuration object.
            repositories (dict): Dictionary of repository instances.
            bot: Optional bot instance.
        """
        self.db = db
        self.config = config
        self.bot = bot
        self.stats_repo = repositories['stats']
        self.attendance_repo = repositories['attendance']
        self.match_log_repo = repositories['match_log']
        self.user_repo = repositories['user']
        self.number_log_repo = repositories['number_log']
        self.stats_view_service = None # Will be set later or injected

    def set_bot(self, bot):
        """
        Sets the bot instance.

        Args:
            bot: The bot instance.
        """
        self.bot = bot

    def set_stats_view_service(self, service):
        """
        Sets the stats view service.

        Args:
            service: The stats view service instance.
        """
        self.stats_view_service = service

    async def generate_number_count_visualization(self, chat_id, user_id=None, start_date=None, ax=None):
        """
        Generates a bar chart visualization of number frequencies.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of a specific user. Defaults to None.
            start_date (date, optional): The start date for filtering data. Defaults to None.
            ax (matplotlib.axes.Axes, optional): An existing axes object to plot on. Defaults to None.

        Returns:
            io.BytesIO or matplotlib.axes.Axes: A buffer containing the image or the axes object.
        """
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

            # Create figure if ax is not provided
            standalone = ax is None
            if standalone:
                fig, ax = plt.subplots(figsize=(12, 6))
            
            color = '#90EE90' # Light green
            edge_color = '#228B22' # Forest green
            ax.bar(numbers, counts, color=color, edgecolor=edge_color, alpha=0.7)
            
            # Determine user name for title if specific user
            user_info = ""
            if user_id:
                user_name = await self.user_repo.get_user_name(user_id, chat_id)
                user_info = f" for {user_name}"

            ax.set_title(f"Number Frequency{user_info} ({title_suffix})", fontsize=14, color='#333333')
            ax.set_xlabel("Number")
            ax.set_ylabel("Count")
            ax.grid(axis='y', linestyle='--', alpha=0.5)
            
            # Set x-axis limits and ticks
            ax.set_xlim(-1, 101)
            ax.set_xticks(range(0, 101, 10))  # Ticks every 10 numbers for profile view
            
            # Ensure y-axis has integer ticks
            ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

            if standalone:
                # Save to buffer
                buf = io.BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight')
                buf.seek(0)
                plt.close()
                return buf
            
            return ax

        except Exception as e:
            logger.error(f"Error generating visualization: {e}", exc_info=True)
            return None

    async def generate_number_count_visualization_grid(self, chat_id, user_id=None, start_date=None):
        """
        Generates a grid-based visualization of number frequencies (0-100).

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of a specific user. Defaults to None.
            start_date (date, optional): The start date for filtering data. Defaults to None.

        Returns:
            io.BytesIO: A buffer containing the generated image.
        """
        try:
            if start_date:
                data = await self.stats_repo.get_number_counts_since(chat_id, start_date, user_id)
                title_suffix = f"since {start_date}"
            else:
                data = await self.stats_repo.get_all_number_counts(chat_id, user_id)
                title_suffix = "All Time"

            if not data:
                return None

            # Map data to a dictionary for easy lookup
            counts_map = {row[0]: int(row[1]) for row in data}
            
            # Create figure
            fig, ax = plt.subplots(figsize=(12, 13))
            
            # Define colors for the green theme
            # 0: White, 1-10+: Shades of Green
            colors = ["#FFFFFF", "#E8F5E9", "#C8E6C9", "#A5D6A7", "#81C784", "#66BB6A", "#4CAF50", "#43A047", "#388E3C", "#2E7D32", "#1B5E20"]
            cmap = mcolors.LinearSegmentedColormap.from_list("custom_greens", colors, N=11)
            norm = mcolors.Normalize(vmin=0, vmax=10)
            
            # Draw the grid (11 rows, 10 columns for numbers 0-100)
            for i in range(101): # Numbers 0 to 100
                r = i // 10
                c = i % 10
                
                count = counts_map.get(i, 0)
                display_count = min(count, 10)
                face_color = cmap(norm(display_count))
                
                # Plotting from top-left: row 0 is at the top
                # y-coordinate: 10 - r
                rect = plt.Rectangle((c, 10 - r), 1, 1, facecolor=face_color, edgecolor='#E0E0E0', linewidth=0.5)
                ax.add_patch(rect)
                
                # Text color: dark for light background, light for dark background
                text_color = 'white' if display_count >= 6 else '#333333'
                
                # Number label
                ax.text(c + 0.5, 10 - r + 0.65, str(i), 
                        ha='center', va='center', fontsize=16, color=text_color, fontweight='bold')
                
                # Count label
                if count > 0:
                    ax.text(c + 0.5, 10 - r + 0.3, f"({count})", 
                            ha='center', va='center', fontsize=12, color=text_color)

            # Formatting
            user_info = ""
            if user_id:
                user_name = await self.user_repo.get_user_name(user_id, chat_id)
                user_info = f" for {user_name}"

            ax.set_title(f"Number Frequency Grid{user_info}\n({title_suffix})", fontsize=16, pad=20, color='#333333')
            
            ax.set_xlim(0, 10)
            ax.set_ylim(0, 11)
            ax.set_aspect('equal')
            ax.axis('off')

            # Save to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=120, facecolor='white')
            buf.seek(0)
            plt.close()
            
            return buf

        except Exception as e:
            logger.error(f"Error generating grid visualization: {e}", exc_info=True)
            return None

    async def generate_time_series_visualization(self, chat_id, user_id=None, hourly_buckets=True, buckets=48, ax=None):
        """
        Generates a time series visualization of activity.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of a specific user. Defaults to None.
            hourly_buckets (bool): Whether to use hourly buckets (True) or daily (False). Defaults to True.
            buckets (int): The number of buckets (hours or days) to display. Defaults to 48.
            ax (matplotlib.axes.Axes, optional): An existing axes object to plot on. Defaults to None.

        Returns:
            io.BytesIO or matplotlib.axes.Axes: A buffer containing the image or the axes object.
        """
        try:
            # Set style
            plt.style.use('seaborn-v0_8-pastel')
            
            standalone = ax is None
            if standalone:
                plt.figure(figsize=(12, 6))
                ax = plt.gca()
            
            # Define color theme
            color = '#90EE90' # Light green
            edge_color = '#228B22' # Forest green

            tz_offset = self.config.timezone_gmt
            tz = timezone(timedelta(hours=tz_offset))
            now = datetime.now(tz)

            if hourly_buckets:
                # Past 'buckets' hours
                start_time = now - timedelta(hours=buckets)
                # Round down to the start of the hour
                start_time = start_time.replace(minute=0, second=0, microsecond=0)
                data = await self.number_log_repo.get_hourly_counts(chat_id, start_time, user_id)
                
                # Fill gaps with 0
                data_dict = {row[0]: row[1] for row in data}
                full_data = []
                for i in range(buckets + 1):
                    ts = start_time + timedelta(hours=i)
                    full_data.append((ts, data_dict.get(ts, 0)))
                
                title_period = f"Past {buckets} Hours"
                xlabel = "Time (Hour)"
                date_fmt = mdates.DateFormatter('%H:%M', tz=tz)
            else:
                # Past 'buckets' days
                start_date = (now - timedelta(days=buckets)).date()
                data = await self.stats_repo.get_daily_counts(chat_id, start_date, user_id)
                
                # Fill gaps with 0
                data_dict = {row[0]: row[1] for row in data}
                full_data = []
                for i in range(buckets + 1):
                    d = start_date + timedelta(days=i)
                    # Convert date to datetime for fill_between compatibility
                    dt = datetime.combine(d, datetime.min.time()).replace(tzinfo=tz)
                    full_data.append((dt, data_dict.get(d, 0)))

                title_period = f"Past {buckets} Days"
                xlabel = "Date"
                date_fmt = mdates.DateFormatter('%Y-%m-%d', tz=tz)

            if not full_data:
                return None

            # Unpack data
            timestamps = [row[0] for row in full_data]
            counts = [float(row[1]) for row in full_data]

            # Plot
            ax.plot(timestamps, counts, marker='o', linestyle='-', color=edge_color, markerfacecolor=color, markersize=6, linewidth=2)
            ax.fill_between(timestamps, counts, color=color, alpha=0.3)

            # Formatting
            user_info = ""
            if user_id:
                user_name = await self.user_repo.get_user_name(user_id, chat_id)
                user_info = f" for {user_name}"

            ax.set_title(f"Activity Over Time{user_info} ({title_period})", fontsize=14, color='#333333')
            ax.set_xlabel(xlabel, fontsize=10)
            ax.set_ylabel("Numbers Logged", fontsize=10)
            
            # Grid
            ax.grid(True, linestyle='--', alpha=0.6)
            
            # X-axis date formatting
            ax.xaxis.set_major_formatter(date_fmt)
            # Rotate dates manually if not standalone
            for label in ax.get_xticklabels():
                label.set_rotation(30)
                label.set_horizontalalignment('right')
            
            # Ensure y-axis has integer ticks
            ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

            if standalone:
                # Save to buffer
                buf = io.BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                buf.seek(0)
                plt.close()
                return buf
            
            return ax

        except Exception as e:
            logger.error(f"Error generating time series visualization: {e}", exc_info=True)
            return None

    async def generate_match_graph_visualization(self, chat_id, user_id=None, start_date=None, ax=None):
        """
        Generates a network graph visualization of matches between users.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int, optional): The ID of a specific user to highlight. Defaults to None.
            start_date (date, optional): The start date for filtering data. Defaults to None.
            ax (matplotlib.axes.Axes, optional): An existing axes object to plot on. Defaults to None.

        Returns:
            io.BytesIO or matplotlib.axes.Axes: A buffer containing the image or the axes object.
        """
        try:
            # 1. Fetch data
            user_counts = await self.stats_repo.get_user_total_counts(chat_id)
            match_pairs = await self.match_log_repo.get_all_matched_pairs(chat_id, user_id)
            users = await self.user_repo.get_all_users_in_chat(chat_id)
            
            if not match_pairs:
                return None

            user_name_map = {row[0]: row[1] for row in users}
            user_count_map = {row[0]: int(row[1]) for row in user_counts}

            # 2. Build Graph
            G = nx.Graph()
            for u1, u2, count in match_pairs:
                if u1 not in user_name_map or u2 not in user_name_map:
                    continue
                G.add_edge(u1, u2, weight=float(count))

            if not G.nodes():
                return None

            # 3. Plotting Setup
            standalone = ax is None
            if standalone:
                plt.figure(figsize=(12, 10))
                ax = plt.gca()
            
            pos = nx.spring_layout(G, k=0.5, iterations=50)
            nodelist = list(G.nodes())
            
            # Node sizes based on total number counts
            node_sizes = [float(max(300, user_count_map.get(uid, 0) * 50)) for uid in nodelist]

            # Separate self-loops and other edges
            self_loops = [(u, v) for u, v in G.edges() if u == v]
            other_edges = [(u, v) for u, v in G.edges() if u != v]

            # Edge widths for regular matches
            other_weights = [G[u][v]['weight'] for u, v in other_edges]
            max_other_weight = max(other_weights) if other_weights else 1
            edge_widths = [(w / max_other_weight) * 5 + 1 for w in other_weights]

            # Node border widths for self-matches (to avoid obscured loops)
            self_weights = [G[u][v]['weight'] for u, v in self_loops]
            max_self_weight = max(self_weights) if self_weights else 1
            node_linewidths = []
            for uid in nodelist:
                weight = G.get_edge_data(uid, uid, default={'weight': 0})['weight']
                lw = 1.0 + (weight / max_self_weight * 5.0 if weight > 0 else 0)
                node_linewidths.append(lw)

            # Labels
            labels = {uid: user_name_map.get(uid, str(uid)) for uid in nodelist}

            # 4. Draw
            nx.draw_networkx_nodes(G, pos, nodelist=nodelist, node_size=node_sizes, 
                                   node_color='#90EE90', alpha=0.8, 
                                   edgecolors='#228B22', linewidths=node_linewidths, ax=ax)
            
            nx.draw_networkx_edges(G, pos, edgelist=other_edges, width=edge_widths, 
                                   edge_color='#228B22', alpha=0.5, ax=ax)
            
            nx.draw_networkx_labels(G, pos, labels=labels, font_size=10, 
                                    font_family='sans-serif', font_weight='bold', ax=ax)

            title = "Chat Match Graph"
            if user_id:
                user_name = user_name_map.get(user_id, "User")
                title = f"Match Graph for {user_name}"
            
            ax.set_title(title, fontsize=14, color='#333333')
            ax.axis('off')

            if standalone:
                # Save to buffer
                buf = io.BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight', dpi=120)
                buf.seek(0)
                plt.close()
                return buf
            
            return ax

        except Exception as e:
            logger.error(f"Error generating match graph visualization: {e}", exc_info=True)
            return None

    async def personal_stats_visualization(self, chat_id, user_id, first_name="User"):
        """
        Generates a comprehensive personal statistics visualization dashboard.

        Args:
            chat_id (int): The ID of the chat.
            user_id (int): The ID of the user.
            first_name (str): The first name of the user. Defaults to "User".

        Returns:
            io.BytesIO: A buffer containing the generated dashboard image.
        """
        try:
            # Create a large figure with GridSpec
            fig = plt.figure(figsize=(20, 12), facecolor='#F5F5F5')
            # 2 rows, 2 columns
            gs = GridSpec(2, 2, figure=fig, width_ratios=[1, 2], height_ratios=[1, 1])
            
            # --- Left Column: Text Stats ---
            ax_text = fig.add_subplot(gs[:, 0])
            ax_text.axis('off')
            
            # Fetch data for text stats
            count = 0
            average = 0
            unique_count = 0
            result = await self.stats_repo.get_user_stats(user_id, chat_id)
            if result and result[0] is not None and result[0] > 0:
                count = result[0]
                total_sum = result[1]
                unique_count = result[2]
                average = round(total_sum / count, 4)

            hit_numbers = sorted([int(n) for n in self.config.hit_numbers.keys()])
            specific_counts_raw = await self.stats_repo.get_specific_number_counts(user_id, chat_id, hit_numbers)
            specific_counts_dict = {num: cnt for num, cnt in specific_counts_raw}
            counts_list = [f"{num} (Count: {specific_counts_dict.get(num, 0)})" for num in hit_numbers]
            counts_str = "\n".join(counts_list) if counts_list else "No numbers recorded yet."

            most_frequent_str = "N/A"
            most_frequent_results = await self.stats_repo.get_most_frequent_numbers(user_id, chat_id)
            if most_frequent_results:
                numbers = [str(row[0]) for row in most_frequent_results]
                freq_count = most_frequent_results[0][1]
                most_frequent_str = f"{', '.join(numbers)} (Count: {freq_count})"

            top_matches_str = "None"
            top_matches = await self.match_log_repo.get_top_matches(user_id, chat_id, limit=3)
            if top_matches:
                match_names = []
                for match_user_id, match_count in top_matches:
                    match_name = await self.user_repo.get_user_name(match_user_id, chat_id)
                    match_names.append(f"{match_name} ({match_count})")
                top_matches_str = "\n".join(match_names)

            streak_query = self.user_repo.get_fetch_streak_query()
            streak_result = await self.db.fetch_one(streak_query, (user_id, chat_id))
            current_streak = streak_result[0] if streak_result else 0

            achievements_str = await self.stats_view_service.get_user_achievements_emojis(user_id, chat_id)

            # Format the response using config
            profile_template = "\n".join(self.config.profile_text)
            stats_text = profile_template.format(
                name=f"{first_name}",
                count=f"{count}",
                average=f"{average}",
                unique_count=f"{unique_count}",
                counts=counts_str,
                most_frequent=most_frequent_str,
                top_matches=top_matches_str,
                streak=f"{current_streak}",
                achievements=achievements_str
            )
            
            # Add Title
            ax_text.text(0, 1, f"{first_name.upper()}",
                         transform=ax_text.transAxes, ha='left', va='top',
                         fontsize=24, fontweight='bold', color='#228B22')
            
            # Add Stats Content
            ax_text.text(0, 0.92, stats_text,
                         transform=ax_text.transAxes, ha='left', va='top', 
                         fontsize=16, linespacing=2.0, color='#333333')

            # --- Right Column: Visualizations ---
            
            # 1. Histogram (Top Right)
            ax_hist = fig.add_subplot(gs[0, 1])
            await self.generate_number_count_visualization(chat_id, user_id=user_id, ax=ax_hist)
            
            # 2. Time Series (Bottom Right)
            ax_time = fig.add_subplot(gs[1, 1])
            # 48 buckets of a day each = 48 days
            await self.generate_time_series_visualization(chat_id, user_id=user_id, hourly_buckets=False, buckets=48, ax=ax_time)
            
            # --- Rainbow Border ---
            # Create a rainbow gradient border using multiple rectangles
            border_width = 0.015
            ratio = 0.6
            n_colors = 100
            rainbow_cmap = plt.get_cmap('gist_rainbow')
            
            # Top border
            for i in range(n_colors):
                rect = Rectangle((i/n_colors, 1-border_width), 1/n_colors, border_width, 
                                 transform=fig.transFigure, color=rainbow_cmap(i/n_colors), zorder=5)
                fig.patches.append(rect)
            
            # Bottom border
            for i in range(n_colors):
                rect = Rectangle((i/n_colors, 0), 1/n_colors, border_width, 
                                 transform=fig.transFigure, color=rainbow_cmap(1 - i/n_colors), zorder=5)
                fig.patches.append(rect)
                
            # Left border
            for i in range(n_colors):
                rect = Rectangle((0, i/n_colors), border_width*ratio, 1/n_colors,
                                 transform=fig.transFigure, color=rainbow_cmap(1 - i/n_colors), zorder=5)
                fig.patches.append(rect)
                
            # Right border
            for i in range(n_colors):
                rect = Rectangle((1-border_width*ratio, i/n_colors), border_width*ratio, 1/n_colors,
                                 transform=fig.transFigure, color=rainbow_cmap(i/n_colors), zorder=5)
                fig.patches.append(rect)

            # Save to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=100, facecolor=fig.get_facecolor())
            buf.seek(0)
            plt.close()
            
            return buf

        except Exception as e:
            logger.error(f"Error generating personal stats visualization: {e}", exc_info=True)
            return None
