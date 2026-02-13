import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    def __init__(self, db):
        self.db = db

    def init_db(self):
        """Initializes the database tables, TimescaleDB extension, and Continuous Aggregates."""
        self.db.execute_query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        self.init_number_logs()
        self.init_match_logs()
        self.init_user_data()
        self.init_user_attendance()
        self.init_user_number_counts()

    def init_number_logs(self):
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
        self.db.execute_query(create_table_query)

        # 2. Convert to hypertable
        self.db.execute_query("SELECT create_hypertable('number_logs', 'ts', if_not_exists => TRUE);")

        # 3. Create index for user_id and chat_id on raw table
        self.db.execute_query("CREATE INDEX IF NOT EXISTS idx_logs_user_chat ON number_logs (user_id, chat_id);")

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
            self.db.execute_query(create_view_query)

            # 5. Create index on the Materialized View itself
            self.db.execute_query("CREATE INDEX IF NOT EXISTS idx_view_user_chat ON user_stats_daily (user_id, chat_id);")

            # 6. Add a refresh policy
            self.db.execute_query("""
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
        self.db.execute_query(create_table_query)

        # 2. Convert to hypertable
        self.db.execute_query("SELECT create_hypertable('match_logs', 'ts', if_not_exists => TRUE);")

        # 3. Create index for user_id and chat_id on raw table
        self.db.execute_query("""
           CREATE INDEX IF NOT EXISTS idx_match_logs_user_chat ON match_logs (user_id_1, user_id_2, chat_id);
        """)

        logger.info("Table 'match_logs' and its index initialized.")

    def init_user_data(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data 
        (
            id             SERIAL       PRIMARY KEY,
            chat_id        BIGINT       NOT NULL,
            thread_id      BIGINT,
            user_id        BIGINT       NOT NULL,
            user_name      TEXT         NOT NULL,
            user_handle    TEXT,
            numbers_bitmap bit(128)     DEFAULT repeat('0', 128)::bit(128),
            extend_info    TEXT
        );
        """
        self.db.execute_query(create_table_query)

        self.db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_user_data_user_chat ON user_data (user_id, chat_id);
        """)

        logger.info("Table 'user_data' and its index initialized.")

    def init_user_attendance(self):
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
        self.db.execute_query(create_table_query)

        self.db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_user_attendance_user_chat ON user_attendance (user_id, chat_id);
        """)

        logger.info("Table 'user_attendance' and its index initialized.")

    def init_user_number_counts(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_number_counts (
            user_id     BIGINT NOT NULL,
            chat_id     BIGINT NOT NULL,
            number      INTEGER NOT NULL,
            count       BIGINT DEFAULT 1,
            PRIMARY KEY (user_id, chat_id, number)
        );
        """
        self.db.execute_query(create_table_query)
        logger.info("Table 'user_number_counts' initialized.")