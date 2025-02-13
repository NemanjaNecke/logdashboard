from services.sql_workers.db_managers.DB_manager.db_helper import DatabaseHelper
import os
from datetime import datetime
import logging

class DBController:
    """
    Controller to manage database operations.
    """
    def __init__(self, db_directory):
        self.db_helper = DatabaseHelper(db_directory)
        self.logger = logging.getLogger('DBController')  # pylint: disable=no-member

    def list_databases(self):
        """
        Retrieves the list of databases.
        Augments each entry with file size and creation date.
        """
        databases = self.db_helper.list_databases()
        for db in databases: # type: ignore
            path = db.get('path')
            if path and os.path.exists(path):
                try:
                    # Get file size (in bytes) and creation time
                    db['size'] = os.path.getsize(path)
                    created_ts = os.path.getctime(path)
                    # Format creation date as a string
                    db['created'] = datetime.fromtimestamp(created_ts).strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    self.logger.error(f"Error retrieving file info for {path}: {e}")
                    db['size'] = "N/A"
                    db['created'] = "N/A"
        return databases

    def export_database(self, db_path, csv_path):
        """
        Exports a single database to a CSV file.
        """
        success, error = self.db_helper.export_database_to_csv(db_path, csv_path)
        if success:
            self.logger.info(f"Exported '{db_path}' to '{csv_path}'.")
        else:
            self.logger.error(f"Failed to export '{db_path}': {error}")
        return success, error