import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    """
    Manages the database schema, including table creation, indexing, and TimescaleDB hypertables.
    """
    def __init__(self, db):
        """
        Initializes the SchemaManager.

        Args:
            db: The database connection.
        """
        self.db = db

    def init_db(self):
        """Initializes the database tables, TimescaleDB extension, and Continuous Aggregates."""
        self.db._execute_query_sync("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        self.init_number_logs()
        self.init_match_logs()
        self.init_user_data()
        self.init_user_attendance()
        self.init_user_number_counts()
        self.init_user_daily_number_counts()
        self.init_match_counts()

    def clear_db(self):
        """Clears all data from the tables."""
        tables = [
            "number_logs",
            "match_logs",
            "user_data",
            "user_attendance",
            "user_number_counts",
            "user_daily_number_counts",
            "match_counts"
        ]
        for table in tables:
            try:
                self.db._execute_query_sync(f"TRUNCATE TABLE {table} CASCADE;")
                logger.info(f"Table '{table}' truncated.")
            except Exception as e:
                logger.error(f"Failed to truncate table '{table}': {e}")

    def init_number_logs(self):
        """Initializes the number_logs table and its associated TimescaleDB hypertable and aggregates."""
        # 1. Create the base table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS number_logs 
            ( 
             id        SERIAL, 
             chat_id   BIGINT      NOT NULL, 
             thread_id BIGINT, 
             user_id   BIGINT      NOT NULL, 
             user_name TEXT        NOT NULL, 
             ts        TIMESTAMPTZ NOT NULL, 
             number    INTEGER     NOT NULL, 
             PRIMARY KEY (id, ts)
            ); 
        """
        self.db._execute_query_sync(create_table_query)

        # 2. Convert to hypertable
        self.db._execute_query_sync("SELECT create_hypertable('number_logs', 'ts', if_not_exists => TRUE);")

        # 3. Create index for user_id and chat_id on raw table
        self.db._execute_query_sync("CREATE INDEX IF NOT EXISTS idx_logs_user_chat ON number_logs (user_id, chat_id);")

        logger.info("Table 'number_logs' and its index initialized.")

        # 4. Create Continuous Aggregate View
        create_view_query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS user_stats_daily
            WITH (timescaledb.continuous = true) AS
            SELECT 
                time_bucket(INTERVAL '1 day', ts) AS bucket,
                user_id,
                chat_id,
                count(*) as entry_count,
                sum(number) as total_sum
            FROM number_logs
            GROUP BY bucket, user_id, chat_id
            WITH NO DATA;
            """
        try:
            self.db._execute_query_sync(create_view_query)

            # 5. Create index on the Materialized View itself
            self.db._execute_query_sync("CREATE INDEX IF NOT EXISTS idx_view_user_chat ON user_stats_daily (user_id, chat_id);")

            # 6. Add a refresh policy
            self.db._execute_query_sync("""
                    SELECT add_continuous_aggregate_policy('user_stats_daily',
                        start_offset => INTERVAL '7 days',
                        end_offset => INTERVAL '1 hour',
                        schedule_interval => INTERVAL '1 hour',
                        if_not_exists => TRUE);
                    """)
            logger.info("Continuous aggregate 'user_stats_daily' and its index initialized.")
        except Exception as e:
            logger.error(f"Note on continuous aggregate creation: {e}")

    def init_match_logs(self):
        """Initializes the match_logs table and its associated TimescaleDB hypertable."""
        #1. Create the base table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS match_logs
            (
                 id           SERIAL,
                 chat_id      BIGINT       NOT NULL,
                 thread_id    BIGINT,
                 user_id_1    BIGINT       NOT NULL,
                 user_name_1  TEXT         NOT NULL,
                 user_id_2    BIGINT       NOT NULL,
                 user_name_2  TEXT         NOT NULL,
                 ts           TIMESTAMPTZ  NOT NULL,
                 match_type   TEXT         NOT NULL,
                 number_1     INTEGER      NOT NULL,
                 number_2     INTEGER      NOT NULL,
                 PRIMARY KEY (id, ts)
            );
        """
        self.db._execute_query_sync(create_table_query)

        # 2. Convert to hypertable
        self.db._execute_query_sync("SELECT create_hypertable('match_logs', 'ts', if_not_exists => TRUE);")

        # 3. Create index for user_id and chat_id on raw table
        self.db._execute_query_sync("""
           CREATE INDEX IF NOT EXISTS idx_match_logs_user_chat ON match_logs (user_id_1, user_id_2, chat_id);
        """)

        logger.info("Table 'match_logs' and its index initialized.")

    def init_user_data(self):
        """Initializes the user_data table for storing user profiles and achievements."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data 
        (
            id              SERIAL       PRIMARY KEY,
            chat_id         BIGINT       NOT NULL,
            thread_id       BIGINT,
            user_id         BIGINT       NOT NULL,
            user_name       TEXT         NOT NULL,
            user_handle     TEXT,
            numbers_bitmap  bit(128)     DEFAULT repeat('0', 128)::bit(128),
            last_login_date DATE         DEFAULT CURRENT_DATE,
            current_streak  INT          DEFAULT 0,
            achievements    TEXT,
            extend_info     TEXT,
            CONSTRAINT uq_user_chat UNIQUE (user_id, chat_id)
        );
        """
        self.db._execute_query_sync(create_table_query)

        self.db._execute_query_sync("""
            CREATE INDEX IF NOT EXISTS idx_user_data_user_chat ON user_data (user_id, chat_id);
        """)

        logger.info("Table 'user_data' and its index initialized.")

    def init_user_attendance(self):
        """Initializes the user_attendance table for tracking daily activity."""
        # Create user_attendance table for tracking daily activity
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_attendance (
            user_id     BIGINT NOT NULL,
            thread_id   BIGINT,
            chat_id     BIGINT NOT NULL,
            log_date    DATE   NOT NULL,
            PRIMARY KEY (user_id, chat_id, log_date)
        );
        """
        self.db._execute_query_sync(create_table_query)

        self.db._execute_query_sync("""
            CREATE INDEX IF NOT EXISTS idx_user_attendance_user_chat ON user_attendance (user_id, chat_id);
        """)

        logger.info("Table 'user_attendance' and its index initialized.")

    def init_user_number_counts(self):
        """Initializes the user_number_counts table for storing total counts of each number per user."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_number_counts (
            user_id     BIGINT NOT NULL,
            chat_id     BIGINT NOT NULL,
            number      INTEGER NOT NULL,
            count       BIGINT DEFAULT 1,
            PRIMARY KEY (user_id, chat_id, number)
        );
        """
        self.db._execute_query_sync(create_table_query)

        self.db._execute_query_sync("""
            CREATE INDEX IF NOT EXISTS idx_user_counts_user_chat ON user_number_counts (user_id, chat_id);
        """)
        logger.info("Table 'user_number_counts' initialized.")

    def init_user_daily_number_counts(self):
        """Initializes the user_daily_number_counts table for storing daily counts of each number per user."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_daily_number_counts (
            user_id     BIGINT NOT NULL,
            chat_id     BIGINT NOT NULL,
            log_date    DATE   NOT NULL,
            number      INTEGER NOT NULL,
            count       BIGINT DEFAULT 1,
            PRIMARY KEY (user_id, chat_id, log_date, number)
        );
        """
        self.db._execute_query_sync(create_table_query)

        self.db._execute_query_sync("""
            CREATE INDEX IF NOT EXISTS idx_user_daily_counts_user_chat_date ON user_daily_number_counts (user_id, chat_id, log_date);
        """)
        logger.info("Table 'user_daily_number_counts' initialized.")

    def init_match_counts(self):
        """Initializes the match_counts table for storing total match counts between user pairs."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS match_counts (
            id           SERIAL,
            chat_id      BIGINT       NOT NULL,
            thread_id    BIGINT,
            user_id_1    BIGINT       NOT NULL,
            user_id_2    BIGINT       NOT NULL,
            match_type   TEXT         NOT NULL,
            count        BIGINT       DEFAULT 0,
            PRIMARY KEY (chat_id, user_id_1, user_id_2, match_type)
        );
        """
        self.db._execute_query_sync(create_table_query)

        self.db._execute_query_sync("""
            CREATE INDEX IF NOT EXISTS idx_match_counts_users_chat ON match_counts (user_id_1, user_id_2, chat_id);
        """)
        logger.info("Table 'match_counts' and its index initialized.")
