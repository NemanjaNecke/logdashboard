# services/sql_workers/db_managers/db_helper.py

import os
import sqlite3
from datetime import datetime
import logging




class DatabaseHelper:
    """
    Provides utility functions to interact with stored databases.
    """
    def __init__(self, db_directory):
        self.db_directory = db_directory
        self.logger = logging.getLogger('DBHelper') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member

    def list_databases(self):
        """
        Lists all SQLite database files in the db_directory.
        Returns a list of dictionaries with database details.
        """
        databases = []
        try:
            for file in os.listdir(self.db_directory):
                if file.endswith('.db'):
                    db_path = os.path.join(self.db_directory, file)
                    db_info = {
                        'name': file,
                        'path': db_path,
                        'type': self.identify_db_type(file),
                        'created': self.get_creation_date(db_path)
                    }
                    databases.append(db_info)
            self.logger.debug(f"Found {len(databases)} databases in '{self.db_directory}'.")
            return databases
        except FileNotFoundError:
            self.logger.info("Created a db folder")
            os.makedirs('db')
    def identify_db_type(self, filename):
        """
        Identifies the database type based on the filename.
        Assumes naming convention: <type>_logs_<date>.db
        """
        filename_lower = filename.lower()
        if 'iis_logs' in filename_lower:
            return 'IIS'
        elif 'evtx_logs' in filename_lower:
            return 'EVTX'
        elif 'generic_logs' in filename_lower:
            return 'Generic'
        else:
            return 'Unknown'

    def get_creation_date(self, db_path):
        """
        Retrieves the creation date of the database file.
        """
        try:
            timestamp = os.path.getctime(db_path)
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.error(f"Error retrieving creation date for '{db_path}': {e}")
            return 'N/A'

    def export_database_to_csv(self, db_path, csv_path):
        """
        Exports the entire database to a CSV file without using f-strings.
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Retrieve all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            with open(csv_path, 'w', encoding='utf-8') as f:
                for table in tables:
                    table_name = table[0]
                    # Write table name
                    f.write("Table: {}\n".format(table_name))

                    # Get column names
                    cursor.execute("PRAGMA table_info({});".format(table_name))
                    columns = [info[1] for info in cursor.fetchall()]
                    f.write(",".join(columns) + "\n")

                    # Get rows from the table
                    cursor.execute("SELECT * FROM {};".format(table_name))
                    rows = cursor.fetchall()

                    for row in rows:
                        row_str = ",".join(
                            [
                                '"{}"'.format(str(item).replace('"', '""')) if ',' in str(item) else str(item)
                                for item in row
                            ]
                        )
                        f.write(row_str + "\n")
                    # Add a newline between tables
                    f.write("\n")

            cursor.close()
            conn.close()
            self.logger.info("Exported '{}' to CSV at '{}'.".format(db_path, csv_path))
            return True, ""
        except Exception as e:
            self.logger.error("Failed to export '{}' to CSV: {}".format(db_path, e))
            return False, str(e)