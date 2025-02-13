# worker_generic.py

import logging
import os
from datetime import datetime
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot

from data.log_parsers.GENERIC.log_parsers_generic import parse_generic_log, parse_multiple_logs
from services.sql_workers.db_managers.GENERIC.db_manager_generic import GenericDBManager

class GenericLogToSQLiteWorkerSignals(QObject):
    """
    Defines signals for our parsing worker:
      - finished(db_path, min_ts, max_ts, file_size_mb)
      - error(str)
      - progress(current, total)
    """
    finished = pyqtSignal(str, float, float, float)
    error    = pyqtSignal(str)
    progress = pyqtSignal(int, int)


class GenericLogToSQLiteWorker(QRunnable):
    """
    Worker that can handle one **OR** multiple files. 
    If you pass multiple file_paths, they get combined into a single DB 
    with 'source_file' marking lines.
    """

    def __init__(self, file_paths, db_path):
        super().__init__()
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        self.file_paths = file_paths  # list of file paths
        self.db_path = db_path

        self.signals = GenericLogToSQLiteWorkerSignals()
        self.is_cancelled = False
        self.logger = logging.getLogger("GenericLogWorker")
        self.logger.setLevel(logging.DEBUG)

    def cancel(self):
        self.is_cancelled = True

    @pyqtSlot()
    def run(self):
        """
        Executed in the worker thread. We parse the files, store them in DB, etc.
        """
        try:
            # 1) Combine sizes
            total_bytes = 0
            for fp in self.file_paths:
                if os.path.exists(fp):
                    total_bytes += os.path.getsize(fp)
            file_size_mb = total_bytes / (1024*1024)

            db_manager = GenericDBManager(self.db_path)
            db_manager.init_tables()

            # 2) We can parse them all at once with parse_multiple_logs
            all_rows, min_dt, max_dt, transactions = parse_multiple_logs(self.file_paths)

            total = len(all_rows)
            if total == 0 and not transactions:
                self.logger.warning("No data found in logs.")
                self.signals.finished.emit(self.db_path, 0.0, 0.0, file_size_mb)
                return

            # 3) Insert raw lines in batch
            BATCH_SIZE = 500
            batch = []
            for i, record in enumerate(all_rows, start=1):
                if self.is_cancelled:
                    self.logger.info("Parsing canceled by user.")
                    self.signals.error.emit("Parsing canceled by user.")
                    return
                batch.append(record)
                if len(batch) >= BATCH_SIZE:
                    db_manager.insert_logs_batch(batch)
                    batch.clear()
                    self.signals.progress.emit(i, total)

            # leftover
            if batch:
                db_manager.insert_logs_batch(batch)
                self.signals.progress.emit(total, total)

            # 4) Insert aggregator data
            tx_list = list(transactions.items())
            tx_total = len(tx_list)
            for j, (txid, txdata) in enumerate(tx_list, start=1):
                if self.is_cancelled:
                    self.logger.info("Canceled by user (aggregator stage).")
                    self.signals.error.emit("Canceled by user.")
                    return
                db_manager.upsert_transaction(txdata)
                self.signals.progress.emit(j, tx_total)

            # store metadata
            db_manager.store_metadata("file_size_mb", file_size_mb)


            # convert min_dt, max_dt to floats (if they are datetime objects)
            min_ts = min_dt.timestamp() if isinstance(min_dt, datetime) else (min_dt if min_dt else 0.0)
            max_ts = max_dt.timestamp() if isinstance(max_dt, datetime) else (max_dt if max_dt else 0.0)

            self.signals.progress.emit(100, 100)
            self.signals.finished.emit(self.db_path, min_ts, max_ts, file_size_mb)

        except Exception as ex:
            self.logger.exception(f"Error in GenericLogToSQLiteWorker: {ex}")
            self.signals.error.emit(str(ex))
