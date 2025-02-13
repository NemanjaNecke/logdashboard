# ui/components/display_logs/IIS/iis_log_table_model.py

import logging
from PyQt5.QtCore import QAbstractTableModel, Qt, QVariant, QModelIndex # pylint: disable=no-name-in-module

class IISLogTableModel(QAbstractTableModel):
    def __init__(self, data=None, columns=None, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger('IISLogTableModel') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member
        self._data = data or []
        self._columns = columns or []

    def rowCount(self, parent=QModelIndex()):
        return len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return len(self._columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        if role == Qt.DisplayRole:
            row = self._data[index.row()]
            column = self._columns[index.column()]
            value = row.get(column, "")
            # Special handling for 'combined_ts'
            if column == "combined_ts" and value:
                try:
                    from datetime import datetime, timezone
                    dt = datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
                    return dt
                except Exception as e:
                    self.logger.warning(f"Failed to convert combined_ts for row {index.row()}: {e}")
                    return "Invalid Timestamp"
            return str(value)
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return QVariant()
        if orientation == Qt.Horizontal:
            try:
                return self._columns[section].replace('_', ' ').title()
            except IndexError:
                return QVariant()
        else:
            return QVariant()

    def sort(self, column, order):
        """Sort table by given column number."""
        if not self._data:
            return
        column_name = self._columns[column]
        self.layoutAboutToBeChanged.emit()
        try:
            if column_name in ['sc_status', 'time_taken']:
                self._data.sort(key=lambda x: int(x.get(column_name, 0)), reverse=(order == Qt.DescendingOrder))
            else:
                self._data.sort(key=lambda x: x.get(column_name, ""), reverse=(order == Qt.DescendingOrder))
        except Exception as e:
            self.logger.error(f"Error sorting data by column {column_name}: {e}")
        self.layoutChanged.emit()

    def addData(self, new_data):
        """
        Adds new data to the model efficiently.
        """
        if not new_data:
            return
        start_row = len(self._data)
        end_row = start_row + len(new_data) - 1
        self.beginInsertRows(QModelIndex(), start_row, end_row)
        self._data.extend(new_data)
        self.endInsertRows()

    def clearData(self):
        """
        Clears all data from the model efficiently.
        """
        self.beginResetModel()
        self._data = []
        self.endResetModel()

    def set_data(self, data, columns):
        self.beginResetModel()
        self._data = data
        self._columns = columns
        self.endResetModel()