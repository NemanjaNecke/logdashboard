# services/sql_workers/db_managers/display_stats_loader.py
import logging
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot # pylint: disable=no-name-in-module

from services.sql_workers.db_managers.IIS.db_manager_iis import DatabaseManager

class DisplayStatsLoaderSignals(QObject):
    """
    Defines the signals available from a running worker thread:
    """
    progress = pyqtSignal(int, int)  # current, total
    finished = pyqtSignal(dict)      # stats dictionary
    error = pyqtSignal(str)          # error message

class DisplayStatsLoader(QRunnable):
    """
    Worker thread for loading statistics for specific fields from the database.
    If 'fields' is None, loads all fields from stats_iis_logs.
    """
    def __init__(self, db_path, fields=None, stats_table="stats_iis_logs"):
        super().__init__()
        self.db_path = db_path
        self.fields = fields  # None => load all fields
        self.stats_table = stats_table
        self.signals = DisplayStatsLoaderSignals()
        self.logger = logging.getLogger('DisplayStatsLoader') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member
        self.is_cancelled = False

    def run(self):
        """
        Executes the statistics loading process.
        """
        self.logger.info(f"DisplayStatsLoader started for fields: {self.fields}")
        try:
            db_manager = DatabaseManager(self.db_path)
            self.logger.debug("DatabaseManager initialized.")

            # If fields is None, load *all* stats from the table
            stats = db_manager.load_field_stats(stats_table=self.stats_table,
                                                fields=self.fields)
            if not stats:
                error_msg = "No statistics found for the specified fields (or table is empty)."
                self.logger.error(error_msg)
                self.signals.error.emit(error_msg)
                return

            # If fields=None, we can’t do a step-based progress on each field. We'll do one pass.
            if self.fields is None:
                # Arbitrary logic: we have 1 step total. Then done.
                self.signals.progress.emit(1, 1)
                self.signals.finished.emit(stats)
                self.logger.info("DisplayStatsLoader completed successfully with all fields.")
                return

            # If fields is not None, we handle the enumerated progress
            total_fields = len(self.fields)
            for idx, field in enumerate(self.fields, start=1):
                if self.is_cancelled:
                    self.logger.info("DisplayStatsLoader cancelled by user.")
                    self.signals.error.emit("Statistics loading was cancelled.")
                    return
                self.signals.progress.emit(idx, total_fields)
                self.logger.debug(f"Loaded stats for field '{field}' ({idx}/{total_fields}).")

            # After enumerating all fields, emit finished
            self.signals.finished.emit(stats)
            self.logger.info("DisplayStatsLoader completed successfully.")

        except Exception as e:
            self.logger.error(f"Error in DisplayStatsLoader: {e}")
            self.signals.error.emit(str(e))

    def cancel(self):
        """
        Cancels the statistics loading process.
        """
        self.is_cancelled = True
        self.logger.info("Cancellation flag set for DisplayStatsLoader.")
