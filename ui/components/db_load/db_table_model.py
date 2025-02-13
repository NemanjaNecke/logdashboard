from PyQt5.QtCore import Qt, QAbstractTableModel, QVariant, QSortFilterProxyModel
import logging
from datetime import datetime

class DBTableModel(QAbstractTableModel):
    """
    Model to represent database information in a QTableView.
    Displays Name, Type, Created Date, Size (formatted), and Path.
    """
    def __init__(self, databases=None):
        super().__init__()
        # Define header columns
        self.header = ["Name", "Type", "Created", "Size", "Path"]
        # Expect each database entry to be a dictionary with keys:
        # 'name', 'type', 'created', 'size', and 'path'
        self.databases = databases or []
        self.logger = logging.getLogger('DBTableModel')
        self.logger.setLevel(logging.DEBUG)

    def rowCount(self, parent=None):
        return len(self.databases)

    def columnCount(self, parent=None):
        return len(self.header)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()
        
        db = self.databases[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == 0:
                return db.get("name", "")
            elif col == 1:
                return db.get("type", "")
            elif col == 2:
                # 'created' should be a formatted string (e.g., "2025-01-01 12:34:56")
                return db.get("created", "")
            elif col == 3:
                size = db.get("size")
                return self.format_size(size)
            elif col == 4:
                return db.get("path", "")
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.header[section]
        return QVariant()

    def format_size(self, size):
        """
        Formats a file size given in bytes into a human-readable string.
        """
        try:
            size = float(size)
        except (ValueError, TypeError):
            return ""
        if size < 1024:
            return f"{size:.0f} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.2f} KB"
        else:
            return f"{size / (1024 * 1024):.2f} MB"


class DBSortFilterProxyModel(QSortFilterProxyModel):
    """
    Proxy model to enable sorting and filtering of the DBTableModel.
    Customizes the sorting for numeric and date columns.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
    
    def lessThan(self, left, right):
        # Get data from the source model for the two items.
        left_data = self.sourceModel().data(left, Qt.DisplayRole)
        right_data = self.sourceModel().data(right, Qt.DisplayRole)
        column = left.column()

        # For the Size column (index 3), compare numeric values.
        if column == 3:
            try:
                left_size = float(self.sourceModel().databases[left.row()].get("size", 0))
                right_size = float(self.sourceModel().databases[right.row()].get("size", 0))
                return left_size < right_size
            except Exception:
                return left_data < right_data

        # For the Created column (index 2), compare datetime objects.
        elif column == 2:
            try:
                left_date = datetime.strptime(left_data, "%Y-%m-%d %H:%M:%S")
                right_date = datetime.strptime(right_data, "%Y-%m-%d %H:%M:%S")
                return left_date < right_date
            except Exception:
                return left_data < right_data

        # For all other columns, do a case-insensitive string comparison.
        return str(left_data).lower() < str(right_data).lower()