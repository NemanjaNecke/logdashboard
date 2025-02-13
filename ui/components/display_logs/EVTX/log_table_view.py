from services.sql_workers.db_managers.EVTX.db_manager_evtx import EVTXDatabaseManager
from PyQt5.QtCore import (
    QAbstractTableModel, Qt, QVariant, QRunnable, QThreadPool,
    QMetaObject, QModelIndex, pyqtSignal, Q_ARG, pyqtSlot
)
import sqlite3
import logging

class DataLoader(QRunnable):
    """
    Runs a SELECT query in the background, returns rows.
    """
    def __init__(self, db_path, query, params, callback):
        super().__init__()
        self.db_path = db_path
        self.query = query
        self.params = params
        self.callback = callback
        self.logger = logging.getLogger("EVTXTableDataLoader")

    def run(self):
        data_rows = []
        try:
            db_manager = EVTXDatabaseManager(self.db_path)
            if self.query:
                data_rows = db_manager.query_logs(self.query, self.params or ())
            else:
                default_sql = (
                    "SELECT EventID, Level, Channel, Computer, ProviderName, RecordNumber, "
                    "timestamp, EventData_display AS EventData "
                    "FROM evtx_logs"
                )
                data_rows = db_manager.query_logs(default_sql)
        except Exception as e:
            self.logger.error(f"DataLoader error: {e}")
        finally:
            QMetaObject.invokeMethod(
                self.callback,
                "onDataLoaded",
                Qt.QueuedConnection,
                Q_ARG(object, data_rows)
            )

class EVTXTableModel(QAbstractTableModel):
    """
    A table model that loads data asynchronously from the DB.
    The SELECT query is built dynamically from the selected columns.
    """
    dataLoadedSignal = pyqtSignal(int)  # number of rows

    def __init__(self, db_path, columns=None, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger("EVTXTableModel")
        self.db_path = db_path
        self.data_rows = []
        # Use full set of columns if not provided.
        if columns is None:
            self.columns = [
                "EventID", "Level", "Channel", "Computer", "ProviderName",
                "RecordNumber", "timestamp", "EventData"
            ]
        else:
            self.columns = columns
        self.page_size = 50000
        self.current_offset = 0
        self.base_params = ()
        self.base_query = self._buildBaseQuery()
        self.loadDataAsync()

    def _buildBaseQuery(self):
        # Map displayed column names to the actual SQL expressions.
        mapping = {
            "EventID": "EventID",
            "Level": "Level",
            "Channel": "Channel",
            "Computer": "Computer",
            "ProviderName": "ProviderName",
            "RecordNumber": "RecordNumber",
            "timestamp": "timestamp",
            "EventData": "EventData_display AS EventData"
        }
        cols = [mapping[col] for col in self.columns if col in mapping]
        query = "SELECT " + ", ".join(cols) + " FROM evtx_logs"
        return query

    def loadDataAsync(self, query=None, params=None):
        if query:
            self.base_query = query
            self.base_params = params or ()
        else:
            # Rebuild the base query using the current self.columns.
            self.base_query = self._buildBaseQuery()
        self.current_offset = 0
        self._loadPage()

    def _loadPage(self):
        paginated_query = f"{self.base_query} LIMIT ? OFFSET ?"
        params = (*self.base_params, self.page_size, self.current_offset)
        loader = DataLoader(self.db_path, paginated_query, params, self)
        QThreadPool.globalInstance().start(loader)

    def loadNextPage(self):
        self.current_offset += self.page_size
        self._loadPage()
        
    @pyqtSlot(object)
    def onDataLoaded(self, rows):
        if self.current_offset == 0:
            self.beginResetModel()
            self.data_rows = rows
            self.endResetModel()
        else:
            start_row = len(self.data_rows)
            end_row = start_row + len(rows) - 1
            self.beginInsertRows(QModelIndex(), start_row, end_row)
            self.data_rows.extend(rows)
            self.endInsertRows()
        self.dataLoadedSignal.emit(len(self.data_rows))

    def rowCount(self, parent=None):
        return len(self.data_rows)

    def columnCount(self, parent=None):
        return len(self.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        if role == Qt.DisplayRole:
            val = self.data_rows[index.row()][index.column()]
            return str(val) if val is not None else ""
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self.columns[section]
            else:
                return str(section + 1)
        return QVariant()
