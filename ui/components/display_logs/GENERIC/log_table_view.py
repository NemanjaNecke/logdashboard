import sqlite3
import logging
from PyQt5.QtCore import QAbstractTableModel, Qt, QVariant  # pylint: disable=no-name-in-module

class GenericTableModel(QAbstractTableModel):
    def __init__(self, db_path, columns, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger("TransTableModel")  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

        self.db_path = db_path
        self.columns = columns
        self.data_rows = []

    def loadData(self, query=None, params=None):
        self.beginResetModel()
        self.data_rows.clear()

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            if query:
                self.logger.debug(f"Executing query: {query} with {params}")
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
            else:
                # Corrected table name to 'generic_logs'
                sql = f"SELECT {', '.join(self.columns)} FROM generic_logs"
                cursor.execute(sql)
            self.data_rows = cursor.fetchall()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
        self.endResetModel()

    def rowCount(self, parent=None):
        return len(self.data_rows)

    def columnCount(self, parent=None):
        return len(self.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        if role == Qt.DisplayRole:
            val = self.data_rows[index.row()][index.column()]
            if val is None:
                return ""
            return str(val)
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self.columns[section]
            else:
                return section + 1
        return QVariant()