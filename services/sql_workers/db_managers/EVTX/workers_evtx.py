# services/sql_workers/db_managers/EVTX/workers_evtx.py

import os
import logging
import sqlite3
from evtx import PyEvtxParser
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot
from evtx import PyEvtxParser  # type: ignore

from services.sql_workers.db_managers.EVTX.db_manager_evtx import EVTXDatabaseManager
from data.log_parsers.EVTX.log_parsers_evtx import parse_evtx_record_xml


class TimestampLoaderSignals(QObject):
    """
    Signals for the timestamp loader.
      - finished: emits the list of timestamps once loaded
    """
    finished = pyqtSignal(object)


class TimestampLoader(QRunnable):
    """
    Loads timestamps from DB in a background thread.
    """

    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.signals = TimestampLoaderSignals()

    @pyqtSlot()
    def run(self):
        timestamps = self.db_manager.get_all_timestamps("evtx_logs")
        # Emit the result via finished
        self.signals.finished.emit(timestamps)


class EVTXInsertWorkerSignals(QObject):
    """
    Signals for EVTXInsertWorker:
      - progress: integer
      - finished: string
      - error: string
    """
    progress = pyqtSignal(int)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)


class EVTXInsertWorker(QRunnable):
    """
    Parses a .evtx file, inserts logs into DB in background,
    with partial progress updates.
    """

    def __init__(self, evtx_file_path, db_path, table_name="evtx_logs"):
        super().__init__()
        self.evtx_file_path = evtx_file_path
        self.db_path = db_path
        self.table_name = table_name
        self.signals = EVTXInsertWorkerSignals()
        self.is_interrupted = False
        self.logger = logging.getLogger("EVTXInsertWorker")
        self.logger.setLevel(logging.DEBUG)

    @pyqtSlot()
    def run(self):
        if not os.path.exists(self.evtx_file_path):
            self.signals.error.emit(f"File not found: {self.evtx_file_path}")
            return

        db_manager = EVTXDatabaseManager(self.db_path)
        db_manager.begin_transaction()

        try:
            
            parser = PyEvtxParser(self.evtx_file_path)

            # Try to get total records for progress
            try:
                total_records_estimated = parser.get_number_of_records()
            except AttributeError:
                total_records_estimated = None

            batch_size = 5000
            batch = []
            processed = 0

            for record in parser.records():
                if self.is_interrupted:
                    self.logger.info("Parsing canceled by user.")
                    self.signals.error.emit("Parsing canceled by user.")
                    db_manager.rollback_transaction()
                    return

                try:
                    xml = record["data"]
                    event = parse_evtx_record_xml(xml)
                    if event:
                        # Remove raw_xml if you don't want it
                        if "raw_xml" in event:
                            del event["raw_xml"]
                        batch.append(event)
                        processed += 1
                except Exception as e:
                    self.logger.error(f"Error parsing record {processed+1}: {e}")
                    continue

                if len(batch) >= batch_size:
                    db_manager.insert_evtx_logs(batch, self.table_name, commit=False)
                    batch.clear()

                    # Progress
                    if total_records_estimated:
                        pct = int(processed / total_records_estimated * 100)
                        self.signals.progress.emit(pct)
                    else:
                        self.signals.progress.emit(processed)

            # leftover batch
            if batch:
                db_manager.insert_evtx_logs(batch, self.table_name, commit=False)

            db_manager.commit_transaction()
            self.signals.progress.emit(100)
            msg = f"EVTX inserted OK. Processed {processed} records."
            self.signals.finished.emit(msg)
            self.logger.info(msg)

        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            db_manager.rollback_transaction()
            self.signals.error.emit(str(e))

    def set_interrupted(self):
        self.is_interrupted = True