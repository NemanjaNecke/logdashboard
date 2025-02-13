# services/sql_workers/workers_iis.py

import sqlite3
import logging
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot  # pylint: disable=no-name-in-module
import os
import time
from data.log_parsers.IIS.log_parsers_iis import parse_iis_log_generator  # Ensure correct import path
from services.sql_workers.db_managers.IIS.db_manager_iis import DatabaseManager  # Ensure correct import path

logger = logging.getLogger('IISLogToSQLiteWorker')  # pylint: disable=no-member
logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

class IISLogToSQLiteWorkerSignals(QObject):
    """
    Defines the signals available from the worker thread.

    Supported signals are:
    - finished: emits (db_path, min_time_ts, max_time_ts, file_size_mb)
    - error: emits a string error message
    - progress: emits (progress_pct, 100) where progress_pct is an integer percentage
    """
    finished = pyqtSignal(str, float, float, float)  # (db_path, min_time_ts, max_time_ts, file_size_mb)
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int)  # (progress_pct, 100)

class IISLogToSQLiteWorker(QRunnable):
    """
    Worker thread for parsing IIS log files and inserting them into SQLite.
    Utilizes a generator-based parser for efficient memory usage.
    """
    def __init__(self, filepath, db_path):
        super().__init__()
        self.filepath = filepath
        self.db_path = db_path
        self.signals = IISLogToSQLiteWorkerSignals()
        self.logger = logging.getLogger('IISLogToSQLiteWorker')  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member
        self.is_cancelled = False
        # **Corrected Initialization**
        self.db_manager = DatabaseManager(self.db_path)
        self.db_manager.init_iis_logs_table()
        self.db_manager.init_metadata()
        
    def cancel(self):
        """
        Sets the cancellation flag to True.
        The run method should periodically check this flag to stop processing.
        """
        self.is_cancelled = True
        self.logger.info("Cancellation requested for IISLogToSQLiteWorker.")

    @pyqtSlot()
    def run(self):
        """
        Parses one or more IIS log files and inserts their records into SQLite.
        Supports both a single file (string) or multiple files (list).
        """
        try:
            self.logger.info("Starting parsing...")
            total_file_size = 0
            overall_min_ts = None
            overall_max_ts = None
            processed_lines = 0
            batch_size = 100000
            batch_data = []

            # Support both a single file or a list of files.
            file_list = self.filepath if isinstance(self.filepath, list) else [self.filepath]

            for file in file_list:
                if not os.path.exists(file):
                    error_msg = f"Log file does not exist: {file}"
                    self.logger.error(error_msg)
                    self.signals.error.emit(error_msg)
                    return

                file_size_bytes = os.path.getsize(file)
                file_size_mb = file_size_bytes / (1024 * 1024)
                total_file_size += file_size_mb
                self.logger.debug(f"Processing file: {file} ({file_size_mb:.2f} MB)")

                with open(file, "r", encoding="utf-8", errors="replace") as f:
                    parser = parse_iis_log_generator(f)
                    last_emitted_pct = 0  # For progress reporting

                    for row_dict in parser:
                        if self.is_cancelled:
                            self.logger.info("Parsing cancelled by user.")
                            self.signals.error.emit("Parsing was cancelled by user.")
                            return

                        # Only process rows with a valid timestamp
                        if row_dict.get("combined_ts") is not None:
                            # Build the record to insert (same as before)
                            record = {
                                'date': row_dict.get('date', '-'),
                                'time': row_dict.get('time', '-'),
                                's_ip': row_dict.get('s_ip', '-'),
                                'cs_method': row_dict.get('cs_method', '-'),
                                'cs_uri_stem': row_dict.get('cs_uri_stem', '-'),
                                'cs_uri_query': row_dict.get('cs_uri_query', '-'),
                                's_port': row_dict.get('s_port', '-'),
                                'cs_username': row_dict.get('cs_username', '-'),
                                'c_ip': row_dict.get('c_ip', '-'),
                                'cs_User_Agent': row_dict.get('cs_User_Agent', '-'),
                                'cs_Referer': row_dict.get('cs_Referer', '-'),
                                'sc_status': row_dict.get('sc_status', '-'),
                                'sc_substatus': row_dict.get('sc_substatus', '-'),
                                'sc_win32_status': row_dict.get('sc_win32_status', '-'),
                                'time_taken': row_dict.get('time_taken', '-'),
                                'ns_client_ip': row_dict.get('ns_client_ip', '-'),
                                'combined_ts': row_dict.get('combined_ts', None),
                                'raw_line': row_dict.get('raw_line', '')
                            }
                            batch_data.append(record)

                            ts = row_dict.get("combined_ts")
                            if ts:
                                overall_min_ts = ts if overall_min_ts is None or ts < overall_min_ts else overall_min_ts
                                overall_max_ts = ts if overall_max_ts is None or ts > overall_max_ts else overall_max_ts

                        processed_lines += 1

                        # Report progress based on file position
                        current_pos = f.buffer.tell()  # byte position
                        progress_pct = int((current_pos / file_size_bytes) * 100)
                        progress_pct = min(progress_pct, 100)
                        if progress_pct > last_emitted_pct:
                            self.signals.progress.emit(progress_pct, 100)
                            last_emitted_pct = progress_pct

                        # Insert batch if we have enough records
                        if len(batch_data) >= batch_size:
                            self._insert_batch(batch_data)
                            batch_data.clear()
                            self.logger.debug(f"Processed {processed_lines} lines so far.")

                    # After finishing the file, insert any remaining records
                    if batch_data:
                        self._insert_batch(batch_data)
                        batch_data.clear()
                        self.signals.progress.emit(100, 100)
                        self.logger.debug(f"Finished processing {processed_lines} lines for file {file}.")

                    self.logger.info(f"Completed processing file: {file}")

            # Save total file size into metadata
            self.db_manager.insert_file_metadata('file_size', total_file_size)
            self.signals.finished.emit(self.db_path, overall_min_ts, overall_max_ts, total_file_size)
            self.logger.info("Parsing and insertion completed successfully.")

        except Exception as e:
            self.logger.exception("An unexpected error occurred during parsing.")
            self.signals.error.emit(str(e))

    def _insert_batch(self, batch_data):
        """
        Inserts a batch of parsed data into the SQLite database.
        Implements retry logic to handle potential database locks.
        """
        self.logger.debug(f"Inserting batch of {len(batch_data)} records into database.")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # Prepare insert statement
            insert_sql = """
                INSERT OR IGNORE INTO iis_logs (
                    date, time, s_ip, cs_method, cs_uri_stem, cs_uri_query,
                    s_port, cs_username, c_ip, cs_User_Agent, cs_Referer,
                    sc_status, sc_substatus, sc_win32_status, time_taken,
                    ns_client_ip, combined_ts, raw_line
                ) VALUES (
                    :date, :time, :s_ip, :cs_method, :cs_uri_stem, :cs_uri_query,
                    :s_port, :cs_username, :c_ip, :cs_User_Agent, :cs_Referer,
                    :sc_status, :sc_substatus, :sc_win32_status, :time_taken,
                    :ns_client_ip, :combined_ts, :raw_line
                )
            """
            attempts = 0
            max_attempts = 5
            while attempts < max_attempts:
                try:
                    cursor.executemany(insert_sql, batch_data)
                    conn.commit()
                    self.logger.debug(f"Inserted {cursor.rowcount} records into database.")
                    break  # Success
                except sqlite3.OperationalError as e:
                    if 'locked' in str(e).lower():
                        wait_time = (2 ** attempts) + 0.1 * attempts
                        self.logger.warning(f"Database locked. Retrying in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        attempts += 1
                    else:
                        self.logger.error(f"SQLite OperationalError during batch insert: {e}")
                        raise
            else:
                raise sqlite3.OperationalError("Failed to insert batch due to persistent database locks.")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error during batch insert: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
