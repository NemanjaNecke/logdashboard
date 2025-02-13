# services/sql_workers/db_managers/db_manager_iis.py

import sqlite3
import logging
import os

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger('DBManagerIIS')  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member
          # Keep this only for existing functionalities
        
    def init_iis_logs_table(self, table_name="iis_logs"):
        """
        Initializes the IIS logs table with appropriate columns.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date TEXT,
                    time TEXT,
                    s_ip TEXT,
                    cs_method TEXT,
                    cs_uri_stem TEXT,
                    cs_uri_query TEXT,
                    s_port INTEGER,
                    cs_username TEXT,
                    c_ip TEXT,
                    cs_User_Agent TEXT,
                    cs_Referer TEXT,
                    sc_status INTEGER,
                    sc_substatus INTEGER,
                    sc_win32_status INTEGER,
                    time_taken INTEGER,
                    ns_client_ip TEXT,
                    combined_ts REAL,
                    raw_line TEXT
                )
            """)
            conn.commit()
            conn.close()
            self.init_stats_table("stats_iis_logs")
            self.logger.info(f"IIS logs table '{table_name}' initialized.")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing IIS logs table: {e}")
            
    def get_all_columns(self, table_name):
        """
        Retrieves all column names from the specified table.
        
        Args:
            table_name (str): Name of the table.
        
        Returns:
            list: List of column names as strings.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            conn.close()
            # Each row in columns_info is a tuple where the second element is the column name
            column_names = [info[1] for info in columns_info]
            self.logger.debug(f"Columns in '{table_name}': {column_names}")
            return column_names
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving columns for table '{table_name}': {e}")
            return []
    def insert_file_metadata(self, key, value):
        """
        Inserts or updates a metadata key-value pair.

        Args:
            key (str): Metadata key.
            value (Any): Metadata value.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO metadata (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, value))
            conn.commit()
            conn.close()
            self.logger.debug(f"Metadata '{key}' inserted/updated with value: {value}")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in insert_metadata: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in insert_metadata: {e}")
            raise
    def query_records(self, table_name, start_ts=None, end_ts=None, selected_columns=None, limit=None, offset=None):
        """
        Retrieves records from the specified table, optionally applying a time filter
        on 'combined_ts' and selecting specific columns with pagination support.
        
        Args:
            table_name (str): Name of the table to query.
            start_ts (float, optional): Start timestamp for filtering.
            end_ts (float, optional): End timestamp for filtering.
            selected_columns (list of str, optional): Columns to retrieve. Retrieves all if None.
            limit (int, optional): Maximum number of records to retrieve.
            offset (int, optional): Number of records to skip before starting to retrieve.
        
        Returns:
            list of dict: Retrieved records as a list of dictionaries.
        """
        self.logger.debug(f"Querying records from table={table_name}, start_ts={start_ts}, end_ts={end_ts}, selected_columns={selected_columns}, limit={limit}, offset={offset}")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Determine columns to select
            if selected_columns:
                # Validate selected columns against the table schema
                cursor.execute(f"PRAGMA table_info({table_name})")
                available_columns = [info[1] for info in cursor.fetchall()]
                invalid_columns = [col for col in selected_columns if col not in available_columns]
                if invalid_columns:
                    self.logger.error(f"Selected columns {invalid_columns} are not present in table '{table_name}'.")
                    raise ValueError(f"Selected columns {invalid_columns} are not present in table '{table_name}'.")
                columns = ", ".join(selected_columns)
            else:
                columns = "*"

            # Build the WHERE clause
            where_clauses = []
            params = []
            if start_ts is not None and end_ts is not None:
                where_clauses.append("combined_ts >= ? AND combined_ts <= ?")
                params.extend([start_ts, end_ts])
            elif start_ts is not None:
                where_clauses.append("combined_ts >= ?")
                params.append(start_ts)
            elif end_ts is not None:
                where_clauses.append("combined_ts <= ?")
                params.append(end_ts)

            where_stmt = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            # Build LIMIT and OFFSET
            limit_stmt = f" LIMIT {limit}" if limit is not None else ""
            offset_stmt = f" OFFSET {offset}" if offset is not None else ""

            # Complete SQL Query
            sql = f"SELECT {columns} FROM {table_name}{where_stmt}{limit_stmt}{offset_stmt}"
            self.logger.debug(f"SQL Query: {sql}, Params: {params}")

            cursor.execute(sql, params)

            # Fetch all rows
            rows = cursor.fetchall()

            # Get column names
            if selected_columns:
                column_names = selected_columns
            else:
                column_names = [description[0] for description in cursor.description]

            # Convert rows to list of dictionaries
            data = [dict(zip(column_names, row)) for row in rows]

            conn.close()

            self.logger.info(f"Fetched {len(data)} records from {table_name}.")
            return data
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in query_records: {e}")
            return []
        except ValueError as ve:
            self.logger.error(f"Value error in query_records: {ve}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error in query_records: {e}")
            return []

    def insert_iis_log(self, table_name, log_entry):
        """
        Inserts a single IIS log entry into the specified table.
        
        Args:
            table_name (str): Name of the table.
            log_entry (dict): Dictionary containing log fields.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Extract fields in the order of the table schema
            fields = ["date", "time", "s_ip", "cs_method", "cs_uri_stem", "cs_uri_query",
                      "s_port", "cs_username", "c_ip", "cs_User_Agent", "cs_Referer",
                      "sc_status", "sc_substatus", "sc_win32_status", "time_taken",
                      "ns_client_ip", "combined_ts", "raw_line"]

            # Convert numeric fields
            numeric_fields = ["s_port", "sc_status", "sc_substatus", "sc_win32_status", "time_taken", "combined_ts"]
            for field in numeric_fields:
                value = log_entry.get(field, '-')
                if value == '-':
                    log_entry[field] = None
                else:
                    try:
                        log_entry[field] = float(value) if field == "combined_ts" else int(value)
                    except ValueError:
                        log_entry[field] = None
                        self.logger.warning(f"Invalid value for {field}: {value}")

            values = [log_entry.get(field, '-') for field in fields]

            placeholders = ','.join(['?'] * len(fields))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
            cursor.execute(insert_sql, values)

            conn.commit()
            conn.close()
            self.logger.debug(f"Inserted log entry into '{table_name}': {log_entry['raw_line']}")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in insert_iis_log: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in insert_iis_log: {e}")

    # ---------------------------------------------------------------------
    # STATS TABLE METHODS
    # ---------------------------------------------------------------------
    def init_stats_table(self, stats_table="stats_iis_logs"):
        """
        Creates a table to store precomputed stats if it does not exist yet.
        The schema:
            stats_table (field TEXT, value TEXT, count INT)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {stats_table} (
                    field TEXT,
                    value TEXT,
                    count INTEGER,
                    PRIMARY KEY(field, value)
                )
            """)
            conn.commit()
            conn.close()
            self.logger.info(f"Stats table '{stats_table}' initialized.")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing stats table: {e}")

    def store_field_stats(self, stats, stats_table="stats_iis_logs"):
        """
        Stores the stats dictionary into 'stats_table'.
        stats is of the form: { "column1": {val1: count1, val2: count2}, "column2": {...} }
        Overwrites existing data if any.
        """
        try:
            self.logger.debug(f"Storing field stats into table={stats_table}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Clear old data
            cursor.execute(f"DELETE FROM {stats_table}")

            # Insert each column-value-count
            insert_sql = f"INSERT INTO {stats_table} (field, value, count) VALUES (?, ?, ?)"
            data_batch = []
            for field_name, val_counts in stats.items():
                for val, ct in val_counts.items():
                    data_batch.append((field_name, val, ct))

            cursor.executemany(insert_sql, data_batch)
            conn.commit()
            conn.close()
            self.logger.info(f"Stored {len(data_batch)} stats rows into {stats_table}")
        except sqlite3.Error as e:
            self.logger.error(f"Error storing field stats: {e}")

    def load_field_stats(self, stats_table="stats_iis_logs", fields=None):
        """
        Loads precomputed stats from 'stats_table' and returns them in the dict format:
        {
            "column1": { "valA": countA, "valB": countB, ... },
            "column2": ...
        }

        :param fields: Optional list of fields to load stats for. If None, loads all.
        :return: Stats dictionary or empty dict if table is empty or does not exist.
        """
        results = {}
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Make sure the table actually exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                           (stats_table,))
            row = cursor.fetchone()
            if not row:
                self.logger.warning(f"Stats table '{stats_table}' does not exist.")
                conn.close()
                return {}

            # If fields are specified, filter by those
            if fields:
                placeholders = ','.join('?' for _ in fields)
                query = f"SELECT field, value, count FROM {stats_table} WHERE field IN ({placeholders})"
                cursor.execute(query, fields)
            else:
                cursor.execute(f"SELECT field, value, count FROM {stats_table}")

            rows = cursor.fetchall()
            conn.close()

            for (field, val, ct) in rows:
                if field not in results:
                    results[field] = {}
                results[field][val] = ct

            self.logger.debug(f"Loaded field stats: {len(results)} fields with their values.")
            return results
        except sqlite3.Error as e:
            self.logger.error(f"Error loading field stats from {stats_table}: {e}")
            return {}
        
    def init_metadata(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value INTEGER
                )
            """)
            conn.commit()
            conn.close()
            self.logger.debug(f"Metadata created")
        except Exception as e:
            self.logger.error(" Couldn't initialize metadata")

    def load_metadata(self, key):
        """
        Loads a specific metadata value by key.
        """
        self.logger.debug(f"Loading metadata for key '{key}'.")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT value FROM metadata WHERE key = ?", (key,))
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error during metadata loading: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ---------------------------------------------------------------------
    # NEW METHOD: Retrieve All Timestamps
    # ---------------------------------------------------------------------
    def get_all_timestamps(self, table_name="iis_logs"):
        """
        Retrieves all 'combined_ts' timestamps from the specified table.

        :param table_name: Name of the table to query.
        :return: List of timestamps as floats.
        """
        self.logger.debug(f"Retrieving all timestamps from table={table_name}")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT combined_ts FROM {table_name} WHERE combined_ts IS NOT NULL")
            rows = cursor.fetchall()
            timestamps = [float(row[0]) for row in rows if row[0] is not None]
            conn.close()
            self.logger.info(f"Retrieved {len(timestamps)} timestamps from {table_name}")
            return timestamps
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_all_timestamps: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error in get_all_timestamps: {e}")
            return []
    # --------------------------------------------------------------------- #
    # New method to retrieve number of records in a table
    def get_total_records(self, table_name="iis_logs", start_ts=None, end_ts=None):
        """
        Retrieves the total number of records in the specified table, optionally filtered by timestamps.

        Args:
            table_name (str): Name of the table.
            start_ts (float, optional): Start timestamp for filtering.
            end_ts (float, optional): End timestamp for filtering.

        Returns:
            int: Total number of matching records.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            where_clauses = []
            params = []
            if start_ts is not None and end_ts is not None:
                where_clauses.append("combined_ts >= ? AND combined_ts <= ?")
                params.extend([start_ts, end_ts])
            elif start_ts is not None:
                where_clauses.append("combined_ts >= ?")
                params.append(start_ts)
            elif end_ts is not None:
                where_clauses.append("combined_ts <= ?")
                params.append(end_ts)

            where_stmt = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            count_sql = f"SELECT COUNT(*) FROM {table_name}{where_stmt}"
            cursor.execute(count_sql, params)
            total_records = cursor.fetchone()[0]

            conn.close()
            self.logger.debug(f"Total records in '{table_name}': {total_records}")
            return total_records
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_total_records: {e}")
            return 0
