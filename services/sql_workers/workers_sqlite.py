# services/sql_workers/workers_sqlite.py

from PyQt5.QtCore import QObject, pyqtSignal # pylint: disable=no-name-in-module
import sqlite3
import os
from datetime import datetime
from dateutil import parser as date_parser
import logging


class BaseLogToSQLiteWorker(QObject):
    """
    Base Worker class for parsing logs and inserting into SQLite.
    """
    progressSignal = pyqtSignal(int, int)                     # (current_line, total_lines)
    finishedSignal = pyqtSignal(str, float, float)            # (db_path, min_time_timestamp, max_time_timestamp)
    errorSignal = pyqtSignal(str)

    def __init__(self, file_path, db_path=None, table_name="logs"):
        super().__init__()
        self.file_path = file_path
        self.db_path = db_path or ":memory:"  # Use in-memory DB by default
        self.table_name = table_name
        self.conn = None
        self.min_time = None
        self.max_time = None
        self._is_cancelled = False
        self.logger = logging.getLogger('BaseLogToSQLiteWorker') # pylint: disable=no-member

    def setup_database(self, create_table_sql, create_indices_sql):
        """
        Sets up the SQLite database with necessary tables and indices.
        """
        self.conn = sqlite3.connect(self.db_path, timeout=60)
        self.conn.execute("PRAGMA journal_mode = WAL;")  # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA synchronous = NORMAL;")  # Balance between performance and safety
        self.conn.commit()
        c = self.conn.cursor()
        c.execute(create_table_sql)
        c.executescript(create_indices_sql)
        self.conn.commit()
        c.close()
        self.logger.debug("Database tables and indices created.")

    def batch_insert(self, insert_sql, data_batch):
        """
        Inserts a batch of data into the database within a transaction.
        """
        c = self.conn.cursor()
        try:
            c.execute("BEGIN TRANSACTION;")
            c.executemany(insert_sql, data_batch)
            self.conn.commit()
            self.logger.debug(f"Batch of {len(data_batch)} records inserted.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Batch insert failed: {e}")
            raise e
        finally:
            c.close()

    def cancel(self):
        """
        Sets the cancellation flag to True.
        """
        self._is_cancelled = True
        self.logger.info("Cancellation flag set.")

    def _check_cancelled(self):
        """
        Checks if the cancellation flag has been set.
        """
        return self._is_cancelled
