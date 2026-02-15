import os
import logging
import psycopg2
from psycopg2 import pool
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class Database:
    """
    Core database class for managing PostgreSQL connections and executing queries.
    """
    def __init__(self):
        """
        Initializes the Database instance, setting up the connection pool and executor.
        """
        self.user = os.environ.get("POSTGRES_USER")
        self.password = os.environ.get("POSTGRES_PASSWORD")
        self.db_name = os.environ.get("POSTGRES_DB")
        self.host = os.environ.get("POSTGRES_HOST", "db")
        self.port = os.environ.get("POSTGRES_PORT", "5432")
        
        self.connection_pool = None
        self._initialize_pool()
        self.executor = ThreadPoolExecutor(max_workers=8)

    def _initialize_pool(self):
        """
        Initializes the PostgreSQL connection pool.
        """
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 20,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db_name
            )
            logger.info("Database connection pool created successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error while connecting to PostgreSQL", error)

    def get_connection(self):
        """
        Retrieves a connection from the pool.

        Returns:
            psycopg2.extensions.connection: A database connection.
        """
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            raise Exception("Connection pool is not initialized")

    def release_connection(self, connection):
        """
        Releases a connection back to the pool.

        Args:
            connection: The connection to release.
        """
        if self.connection_pool:
            self.connection_pool.putconn(connection)

    def close_all_connections(self):
        """
        Closes all connections in the pool.
        """
        if self.connection_pool:
            self.connection_pool.closeall()
            print("PostgreSQL connection pool is closed")

    def _execute_query_sync(self, query, params=None):
        """
        Synchronously executes a query.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            psycopg2.extensions.cursor: The cursor after execution.
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            connection.commit()
            return cursor
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error executing query", error)
            connection.rollback()
            raise error
        finally:
            cursor.close()
            self.release_connection(connection)

    async def execute_query(self, query, params=None):
        """
        Asynchronously executes a query.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            psycopg2.extensions.cursor: The cursor after execution.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._execute_query_sync, query, params)

    def _fetch_one_sync(self, query, params=None):
        """
        Synchronously fetches one row.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            tuple: The fetched row.
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            connection.commit()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error executing query", error)
            connection.rollback()
            raise error
        finally:
            cursor.close()
            self.release_connection(connection)

    async def fetch_one(self, query, params=None):
        """
        Asynchronously fetches one row.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            tuple: The fetched row.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._fetch_one_sync, query, params)

    def _fetch_all_sync(self, query, params=None):
        """
        Synchronously fetches all rows.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            list: A list of fetched rows.
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            connection.commit()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error executing query", error)
            connection.rollback()
            raise error
        finally:
            cursor.close()
            self.release_connection(connection)

    async def fetch_all(self, query, params=None):
        """
        Asynchronously fetches all rows.

        Args:
            query (str): The SQL query.
            params (tuple, optional): Query parameters.

        Returns:
            list: A list of fetched rows.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._fetch_all_sync, query, params)

    def _execute_transaction_sync(self, queries_with_params):
        """
        Synchronously executes multiple queries in a single atomic transaction.

        Args:
            queries_with_params (list): List of tuples (query, params).

        Returns:
            bool: True if successful.
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            for query, params in queries_with_params:
                cursor.execute(query, params)
            connection.commit()
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error executing transaction", error)
            connection.rollback()
            raise error
        finally:
            cursor.close()
            self.release_connection(connection)

    async def execute_transaction(self, queries_with_params):
        """
        Asynchronously executes multiple queries in a single atomic transaction.

        Args:
            queries_with_params (list): List of tuples (query, params).

        Returns:
            bool: True if successful.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._execute_transaction_sync, queries_with_params)
