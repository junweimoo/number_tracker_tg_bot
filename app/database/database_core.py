import os
import logging
import psycopg2
from psycopg2 import pool
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.user = os.environ.get("POSTGRES_USER")
        self.password = os.environ.get("POSTGRES_PASSWORD")
        self.db_name = os.environ.get("POSTGRES_DB")
        self.host = os.environ.get("POSTGRES_HOST", "db")
        self.port = os.environ.get("POSTGRES_PORT", "5432")
        
        self.connection_pool = None
        self._initialize_pool()
        self.executor = ThreadPoolExecutor(max_workers=8)

    def _initialize_pool(self):
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
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            raise Exception("Connection pool is not initialized")

    def release_connection(self, connection):
        if self.connection_pool:
            self.connection_pool.putconn(connection)

    def close_all_connections(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            print("PostgreSQL connection pool is closed")

    def _execute_query_sync(self, query, params=None):
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._execute_query_sync, query, params)

    def _fetch_one_sync(self, query, params=None):
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._fetch_one_sync, query, params)

    def _fetch_all_sync(self, query, params=None):
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._fetch_all_sync, query, params)

    def _execute_transaction_sync(self, queries_with_params):
        """
        Executes multiple queries in a single atomic transaction.
        queries_with_params: List of tuples (query, params)
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._execute_transaction_sync, queries_with_params)