# # services/sql_workers/db_managers/EVTX/db_manager_evtx.py

import sqlite3
import logging
import threading

class EVTXDatabaseManager:
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, db_path):
        with cls._lock:
            if db_path not in cls._instances:
                instance = super(EVTXDatabaseManager, cls).__new__(cls)
                instance._initialized = False
                cls._instances[db_path] = instance
        return cls._instances[db_path]

    def __init__(self, db_path):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        self.db_path = db_path
        self.logger = logging.getLogger('EVTXDBManager') #pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) #pylint: disable=no-member

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute("PRAGMA cache_size = -200000")
        self.init_evtx_table("evtx_logs")

    def init_evtx_table(self, table_name="evtx_logs"):
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    EventID TEXT,
                    Level TEXT,
                    Channel TEXT,
                    Computer TEXT,
                    ProviderName TEXT,
                    RecordNumber TEXT,
                    timestamp TEXT,
                    timestamp_epoch REAL,
                    EventData TEXT,
                    EventData_display TEXT,
                    raw_xml TEXT,
                    PRIMARY KEY(EventID, RecordNumber, timestamp_epoch)
                )
            """)
            
                    # New analytics table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS evtx_analytics (
                field TEXT,
                value TEXT,
                count INTEGER,
                PRIMARY KEY(field, value)
            )
        """)
            
            
            self.conn.commit()
            self.logger.info(f"EVTX table '{table_name}'  adn initialized.")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing EVTX table: {e}")

    def query_logs(self, query, params=()):
        """
        Helper to execute a SELECT query on the evtx_logs table.
        Returns the rows or an empty list if an error occurs.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            self.logger.debug(f"Executed query: {query} with params: {params}, returned {len(rows)} rows")
            return rows
        except sqlite3.Error as e:
            self.logger.error(f"Error executing query: {e}")
            return []

    def begin_transaction(self):
        try:
            self.conn.execute("BEGIN")
        except sqlite3.Error as e:
            self.logger.error(f"Error beginning transaction: {e}")

    def commit_transaction(self):
        try:
            self.conn.commit()
            self.logger.debug("Transaction committed successfully.")
        except sqlite3.Error as e:
            self.logger.error(f"Error committing transaction: {e}")

    def rollback_transaction(self):
        try:
            self.conn.rollback()
            self.logger.debug("Transaction rolled back.")
        except sqlite3.Error as e:
            self.logger.error(f"Error rolling back transaction: {e}")

    def insert_evtx_logs(self, logs, table_name="evtx_logs", commit=True):
        if not logs:
            return
        try:
            cursor = self.conn.cursor()
            insert_sql = f"""
                INSERT OR IGNORE INTO {table_name} (
                    EventID, Level, Channel, Computer, ProviderName, RecordNumber,
                    timestamp, timestamp_epoch, EventData, EventData_display, raw_xml
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            data_batch = [
                (
                    log.get("EventID"),
                    log.get("Level"),
                    log.get("Channel"),
                    log.get("Computer"),
                    log.get("ProviderName"),
                    log.get("RecordNumber"),
                    log.get("timestamp"),
                    log.get("timestamp_epoch"),
                    log.get("EventData"),
                    log.get("EventData_display"),
                    log.get("raw_xml")
                )
                for log in logs
            ]
            cursor.executemany(insert_sql, data_batch)
            if commit:
                self.conn.commit()
            self.logger.debug(f"Inserted {len(data_batch)} EVTX logs into '{table_name}'.")
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting EVTX logs: {e}")

    def get_all_timestamps(self, table_name="evtx_logs"):
        self.logger.debug(f"Retrieving all timestamps from table={table_name}")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT timestamp_epoch FROM {table_name} WHERE timestamp_epoch IS NOT NULL")
            rows = cursor.fetchall()
            timestamps = [float(row[0]) for row in rows if row[0] is not None]
            conn.close()
            self.logger.info(f"Retrieved {len(timestamps)} timestamps from '{table_name}'.")
            return timestamps
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_all_timestamps: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error in get_all_timestamps: {e}")
            return []

    def get_columns(self, table_name="evtx_logs"):
        self.logger.debug(f"Retrieving columns from table={table_name}")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            columns = [info[1] for info in columns_info]
            conn.close()
            self.logger.info(f"Retrieved columns from '{table_name}': {columns}")
            return columns
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_columns: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error in get_columns: {e}")
            return []

    def get_cursor(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            return cursor
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_cursor: {e}")
            return None
    def save_analytics(self, field_value_counts):
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM evtx_analytics")
            data = [(f, v, c) for f in field_value_counts for v,c in field_value_counts[f].items()]
            cursor.executemany("INSERT INTO evtx_analytics VALUES (?,?,?)", data)
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error saving analytics: {e}")

    def get_cached_analytics(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT field, value, count FROM evtx_analytics")
            results = cursor.fetchall()
            analytics = {}
            for field, value, count in results:
                analytics.setdefault(field, {})[value] = count
            return analytics
        except sqlite3.Error as e:
            self.logger.error(f"Error loading analytics: {e}")
            return None
