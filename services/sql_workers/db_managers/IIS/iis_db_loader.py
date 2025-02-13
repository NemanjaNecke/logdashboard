# services/sql_workers/db_managers/iis_db_loader.py

import sqlite3
import logging
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot # pylint: disable=no-name-in-module

class DatabaseLoaderSignals(QObject):
    """
    Defines the signals available from a running worker thread:

      - progress: emits the integer percentage of completion
      - finished: emits the loaded data (list of dicts)
      - error: emits an error string if something goes wrong
    """
    progress = pyqtSignal(int)
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

class DatabaseLoader(QRunnable):
    """
    Worker thread for loading data from a SQLite database with pagination support.
    """

    def __init__(self, db_path, table_name,
                 page_size=1000, current_page=1,
                 start_ts=None, end_ts=None, selected_columns=None,
                 filters=None):
        """
        :param db_path: Path to the SQLite database.
        :param table_name: Name of the table to read.
        :param page_size: Number of records to load per page. Default=1000.
        :param current_page: 1-based page number. Default=1.
        :param start_ts: Optional start timestamp filter (numeric). Default=None.
        :param end_ts: Optional end timestamp filter (numeric). Default=None.
        :param selected_columns: Columns to retrieve. Retrieves all if None.
        :param filters: Tuple of (where_clause, params) for additional SQL filtering.
        """
        super().__init__()
        self.db_path = db_path
        self.table_name = table_name
        self.page_size = page_size
        self.current_page = current_page
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.selected_columns = selected_columns
        self.filters = filters  # (where_clause, params)
        self.signals = DatabaseLoaderSignals()
        self.logger = logging.getLogger('DatabaseLoader') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member
        self._is_interrupted = False

    def run(self):
        try:
            self.logger.info(f"Connecting to database: {self.db_path}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Determine columns to select
            if self.selected_columns:
                # Validate selected columns against the table schema
                cursor.execute(f"PRAGMA table_info({self.table_name})")
                available_columns = [info[1] for info in cursor.fetchall()]
                invalid_columns = [col for col in self.selected_columns if col not in available_columns]
                if invalid_columns:
                    self.logger.error(f"Selected columns {invalid_columns} are not present in table '{self.table_name}'.")
                    raise ValueError(f"Selected columns {invalid_columns} are not present in table '{self.table_name}'.")
                columns = ", ".join(self.selected_columns)
            else:
                columns = "*"

            # Build the WHERE clause
            where_clauses = []
            params = []
            if self.start_ts is not None and self.end_ts is not None:
                where_clauses.append("combined_ts >= ? AND combined_ts <= ?")
                params.extend([self.start_ts, self.end_ts])
            elif self.start_ts is not None:
                where_clauses.append("combined_ts >= ?")
                params.append(self.start_ts)
            elif self.end_ts is not None:
                where_clauses.append("combined_ts <= ?")
                params.append(self.end_ts)

            # Incorporate additional filters
            if self.filters:
                additional_where, additional_params = self.filters
                if additional_where:
                    where_clauses.append(additional_where)
                    params.extend(additional_params)

            where_stmt = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            # Calculate offset
            offset = self.page_size * (self.current_page - 1)

            # Build the SELECT with LIMIT/OFFSET
            query_sql = f"SELECT {columns} FROM {self.table_name}{where_stmt} LIMIT {self.page_size} OFFSET {offset}"
            self.logger.debug(f"Running paginated query: {query_sql} with params={params}")

            cursor.execute(query_sql, params)

            # Fetch all rows
            rows = cursor.fetchall()

            # Get column names
            if self.selected_columns:
                column_names = self.selected_columns
            else:
                column_names = [description[0] for description in cursor.description]

            # Convert rows to list of dictionaries
            data = [dict(zip(column_names, row)) for row in rows]

            conn.close()

            self.logger.info(f"Fetched {len(data)} records from {self.table_name}.")
            self.signals.finished.emit(data)
        except Exception as e:
            self.logger.error(f"Error during paginated database loading: {e}")
            self.signals.error.emit(str(e))
