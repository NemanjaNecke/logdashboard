# services/controllers/iis_controller.py

from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot # pylint: disable=no-name-in-module
from services.sql_workers.db_managers.IIS.workers_iis import IISLogToSQLiteWorker
from PyQt5.QtCore import QThreadPool # pylint: disable=no-name-in-module
import logging

class IISController(QObject):
    """
    Controller that manages the parsing of IIS logs.
    """
    parseFinished = pyqtSignal(str, float, float, float)  # (db_path, min_time_ts, max_time_ts, file_size_mb)
    parseError = pyqtSignal(str)
    progressUpdate = pyqtSignal(int, int)  # (processed_lines, total_lines)

    def __init__(self, filepath, db_path):
        super().__init__()
        self.logger = logging.getLogger('IISController') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member
        self.filepath = filepath
        self.db_path = db_path
        self.threadpool = QThreadPool.globalInstance()

        # Initialize the log parser worker
        self.worker = IISLogToSQLiteWorker(filepath, db_path=db_path)
        # No need to move to thread as QRunnable handles it

        # Connect signals and slots
        self.worker.signals.finished.connect(self.onParseFinished)
        self.worker.signals.error.connect(self.onParseError)
        self.worker.signals.progress.connect(self.onProgressUpdate)

        self.isParsing = False
        self.active_workers = []  # List to keep references to active workers

    def startParsing(self):
        """
        Starts the parsing process in a separate thread.
        """
        if not self.isParsing:
            self.isParsing = True
            self.logger.info("Starting parsing thread.")
            self.threadpool.start(self.worker)
            self.active_workers.append(self.worker)  # Keep reference
            self.logger.debug("Parsing thread started.")

    def cancelParsing(self):
        """
        Cancels the ongoing parsing process.
        """
        if self.isParsing:
            self.worker.cancel()
            self.logger.info("Parsing cancellation requested.")
            # The worker will emit an error signal upon cancellation

    @pyqtSlot(str, float, float, float)
    def onParseFinished(self, db_path, min_ts, max_ts, file_size_mb):
        """
        Handles the completion of the parsing process.
        """
        self.isParsing = False
        self.logger.info("Parsing finished successfully.")
        self.parseFinished.emit(db_path, min_ts, max_ts, file_size_mb)
        self.active_workers.remove(self.worker)  # Remove reference

    @pyqtSlot(str)
    def onParseError(self, error_msg):
        """
        Handles errors that occur during parsing.
        """
        self.isParsing = False
        self.logger.error(f"Parsing error: {error_msg}")
        self.parseError.emit(error_msg)
        if self.worker in self.active_workers:
            self.active_workers.remove(self.worker)  # Remove reference

    @pyqtSlot(int, int)
    def onProgressUpdate(self, current, total):
        """
        Handles progress updates from the parsing worker.
        """
        self.progressUpdate.emit(current, total)
        if total:
            percentage = int((current / total) * 100)
            self.logger.debug(f"Parsing progress: {percentage}%")
        else:
            self.logger.debug(f"Parsing progress: Processed {current} lines.")
