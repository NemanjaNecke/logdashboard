# services/sql_workers/db_managers/iis_stats_loader.py

import sqlite3
import logging
from collections import defaultdict
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot # pylint: disable=no-name-in-module
from services.sql_workers.db_managers.IIS.db_manager_iis import DatabaseManager

class StatsLoaderSignals(QObject):
    """
    Defines the signals available from the StatsLoader worker thread.
    
    Supported signals are:
    - progress: emits the number of rows processed and total rows
    - finished: emits the stats dictionary
    - error: emits an error string if something goes wrong
    """
    progress = pyqtSignal(int, int)  # (processed_rows, total_rows)
    finished = pyqtSignal(dict)      # stats dictionary
    error = pyqtSignal(str)          # error message

class StatsLoader(QRunnable):
    """
    Worker thread for generating field-level statistics and storing them
    in the 'stats_iis_logs' table using chunked processing.
    """
    def __init__(self, db_path, table_name="iis_logs", stats_table="stats_iis_logs"):
        super().__init__()
        self.db_path = db_path
        self.table_name = table_name
        self.stats_table = stats_table
        self.signals = StatsLoaderSignals()
        self.logger = logging.getLogger('StatsLoader') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)       # pylint: disable=no-member
        self.is_cancelled = False

    def cancel(self):
        """
        Sets the cancellation flag to True.
        The run method should periodically check this flag to stop processing.
        """
        self.is_cancelled = True
        self.logger.info("Cancellation requested for StatsLoader.")

    @pyqtSlot()
    def run(self):
        """
        Executes the statistics generation and storage.
        """
        try:
            self.logger.info(f"Starting StatsLoader for table '{self.table_name}'.")
            db_manager = DatabaseManager(self.db_path)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Retrieve columns from the table
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns_info = cursor.fetchall()
            columns = [info[1] for info in columns_info]
            self.logger.debug(f"Columns retrieved: {columns}")

            if not columns:
                error_msg = f"No columns found in table '{self.table_name}'."
                self.logger.error(error_msg)
                self.signals.error.emit(error_msg)
                cursor.close()
                conn.close()
                return

            # Initialize in-memory stats using defaultdict
            stats = {col: defaultdict(int) for col in columns}
            self.logger.debug("Initialized in-memory stats dictionaries.")

            # Count total rows for progress reporting
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total_rows = cursor.fetchone()[0]
            self.logger.debug(f"Total rows to process: {total_rows}")

            if total_rows == 0:
                self.logger.info("No rows found in table; skipping stats generation.")
                self.signals.finished.emit({})
                cursor.close()
                conn.close()
                return

            # Define chunk size
            chunk_size = 10000
            processed_rows = 0
            offset = 0

            while processed_rows < total_rows:
                if self.is_cancelled:
                    self.logger.info("StatsLoader cancelled by user.")
                    self.signals.error.emit("Statistics generation was cancelled.")
                    cursor.close()
                    conn.close()
                    return

                # Retrieve a chunk of rows
                cursor.execute(f"""
                    SELECT * FROM {self.table_name}
                    LIMIT {chunk_size} OFFSET {offset}
                """)
                rows = cursor.fetchall()
                if not rows:
                    break

                for row in rows:
                    for col_idx, col_name in enumerate(columns):
                        value = row[col_idx]
                        stats[col_name][str(value)] += 1

                processed_rows += len(rows)
                offset += len(rows)
                self.signals.progress.emit(processed_rows, total_rows)
                self.logger.debug(f"Processed {processed_rows}/{total_rows} rows.")

            # Convert defaultdicts to regular dicts
            final_stats = {col: dict(counts) for col, counts in stats.items()}
            self.logger.debug("Converted in-memory stats to regular dictionaries.")

            # Store the stats in the database
            db_manager.store_field_stats(final_stats, stats_table=self.stats_table)
            self.logger.info("Stored field statistics in the database.")

            # Emit finished signal with stats
            self.signals.finished.emit(final_stats)
            self.logger.info("StatsLoader completed successfully.")

            # Close connections
            cursor.close()
            conn.close()

        except sqlite3.Error as e:
            self.logger.error(f"SQLite error during stats generation: {e}")
            self.signals.error.emit(f"SQLite error during stats generation: {e}")
        except Exception as e:
            self.logger.exception("An unexpected error occurred in StatsLoader.")
            self.signals.error.emit(f"Unexpected error: {e}")
