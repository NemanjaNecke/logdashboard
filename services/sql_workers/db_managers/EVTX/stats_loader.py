# services/sql_workers/db_managers/EVTX/stats_loader.py
import logging
import sqlite3
from PyQt5.QtCore import QMetaObject, QObject, pyqtSignal, QRunnable, Qt, Q_ARG, pyqtSlot # pylint: disable=no-name-in-module

import sqlite3
import logging
from PyQt5.QtCore import QMetaObject, Qt, Q_ARG, QRunnable

class StatsLoader(QRunnable):
    """
    Loads statistics for the EVTX logs.
    Only allowed columns (for filtering) are processed.
    The result is sent to a callback with onStatsLoaded.
    """
    def __init__(self, db_manager, callback):
        super().__init__()
        self.db_manager = db_manager
        self.callback = callback  # Expecting a QObject with an onStatsLoaded slot
        self.logger = logging.getLogger("EVTXStatsLoader")
        self.logger.setLevel(logging.DEBUG)

    def run(self):
        try:
            # Allowed columns for EVTX statistics (omit unwanted columns)
            allowed_columns = [
                "EventID", "Level", "Channel", "Computer", "ProviderName",
                "timestamp", "EventData_display"
            ]
            # Check for cached analytics first.
            cached = self.db_manager.get_cached_analytics()
            if cached:
                # Filter the cached results to include only allowed columns.
                filtered = {col: vals for col, vals in cached.items() if col in allowed_columns}
                QMetaObject.invokeMethod(
                    self.callback,
                    "onStatsLoaded",
                    Qt.QueuedConnection,
                    Q_ARG(object, filtered)
                )
                return

            field_value_counts = {}
            # Use the persistent connection from the db_manager
            conn = self.db_manager.conn
            cursor = conn.cursor()

            for col in allowed_columns:
                try:
                    query = f"SELECT {col}, COUNT(*) FROM evtx_logs GROUP BY {col}"
                    cursor.execute(query)
                    field_value_counts[col] = {str(val): cnt for val, cnt in cursor.fetchall()}
                except Exception as e:
                    self.logger.error(f"Error loading stats for column '{col}': {e}")

            # Save the analytics to cache
            self.db_manager.save_analytics(field_value_counts)
            QMetaObject.invokeMethod(
                self.callback,
                "onStatsLoaded",
                Qt.QueuedConnection,
                Q_ARG(object, field_value_counts)
            )
        except Exception as e:
            self.logger.error(f"StatsLoader error: {e}")


class FieldStatsLoaderSignals(QObject):
    """
    Signals for the FieldStatsLoader:
      - progress: emits an integer (0..100)
      - finished: emits the final dict {field: {value: count, ...}, ...}
      - error: emits an error string
    """
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)


class FieldStatsLoader(QRunnable):
    """
    Loads stats for each column in a separate thread and emits progress signals.
    """

    def __init__(self, db_manager, columns):
        super().__init__()
        self.db_manager = db_manager
        self.columns = columns
        self.signals = FieldStatsLoaderSignals()
        self.logger = logging.getLogger('FieldStatsLoader')  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

    @pyqtSlot()
    def run(self):
        field_value_counts = {}
        total_cols = len(self.columns)

        try:
            # Check cached analytics first ----------------------------------
            cached = self.db_manager.get_cached_analytics()
            if cached:
                # Filter cached analytics using self.columns (allowed columns)
                filtered = {col: vals for col, vals in cached.items() if col in self.columns}
                self.signals.finished.emit(filtered)
                return
            # --------------------------------------------------------------
            conn = sqlite3.connect(self.db_manager.db_path)
            cursor = conn.cursor()

            try:
                for i, col in enumerate(self.columns, start=1):
                    if col in ["raw_xml", "timestamp_epoch"]:
                        progress_percent = int(i / total_cols * 100)
                        self.signals.progress.emit(progress_percent)
                        continue

                    try:
                        results = self.db_manager.query_logs(f"SELECT {col}, COUNT(*) FROM evtx_logs GROUP BY {col}")
                        col_stats = {}
                        for val, cnt in results:
                            col_stats[str(val)] = cnt
                        field_value_counts[col] = col_stats
                    except Exception as e:
                        self.logger.error(f"Error loading stats for column {col}: {e}")
                    progress_percent = int(i / total_cols * 100)
                    self.signals.progress.emit(progress_percent)
            except Exception as e:
                self.logger.error(f"StatsLoader error: {e}")
                self.signals.error.emit(str(e))
                return

            conn.close()
        except Exception as e:
            self.logger.error(f"StatsLoader error: {e}")
            self.signals.error.emit(str(e))
            return

        self.signals.finished.emit(field_value_counts)