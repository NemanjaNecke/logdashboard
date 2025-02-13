# ui/components/display_logs/IIS/log_table_view.py

import logging
from PyQt5.QtCore import Qt # pylint: disable=no-name-in-module
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QTableView, QHeaderView, QMenu, QAction, QApplication # pylint: disable=no-name-in-module
from ui.components.display_logs.IIS.table.iis_log_table_model import IISLogTableModel 
from services.converters.IIS.delegate_status import StatusDelegate
from PyQt5.QtGui import QColor # pylint: disable=no-name-in-module

class LogTableView(QWidget):
    """
    A widget that displays IIS logs in a table with resizable columns and rows.
    It can show/hide rows based on external filters.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger('LogTableView')# pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)# pylint: disable=no-member

        # Layout
        layout = QVBoxLayout(self)
        self.setLayout(layout)

        # TableView + Model
        self.table_view = QTableView()
        self.model = IISLogTableModel()  # Use the custom model
        self.table_view.setModel(self.model)

        # Resizable columns and rows
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.verticalHeader().setSectionResizeMode(QHeaderView.Interactive)

        self.table_view.setSortingEnabled(True)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.SingleSelection)
        self.table_view.setEditTriggers(QTableView.NoEditTriggers)

        layout.addWidget(self.table_view)

        # StatusDelegate for 'sc_status' column
        self.status_delegate = StatusDelegate(self.table_view)

        # Connect row-click if needed
        self.table_view.clicked.connect(self.onRowClicked)

        # Keep track of hidden rows to allow resetting
        self.hidden_rows = set()

        # Initialize columns
        self.columns = []

    def setData(self, data, columns):
        """
        Sets the data for the table model.
        """
        self.logger.debug("LogTableView.setData called.")
        self.model.beginResetModel()
        self.model._data = data
        self.model._columns = columns
        self.model.endResetModel()
        self.columns = columns

        # Re-apply delegate for 'sc_status' if present
        if 'sc_status' in self.columns:
            sc_idx = self.columns.index('sc_status')
            self.table_view.setItemDelegateForColumn(sc_idx, self.status_delegate)
            self.logger.debug("StatusDelegate applied to 'sc_status' column after setting data.")

        self.logger.info(f"LogTableView populated with {len(data)} records.")

    def appendData(self, new_data):
        """
        Appends new data to the table model.
        """
        self.model.addData(new_data)
        self.logger.info(f"Appended {len(new_data)} new records to LogTableView.")

    def hideRow(self, row_idx):
        """
        Hides a specific row.
        
        :param row_idx: Index of the row to hide.
        """
        if row_idx not in self.hidden_rows:
            self.table_view.setRowHidden(row_idx, True)
            self.hidden_rows.add(row_idx)
            self.logger.debug(f"Hid row {row_idx}.")

    def showRow(self, row_idx):
        """
        Shows a specific row.
        
        :param row_idx: Index of the row to show.
        """
        if row_idx in self.hidden_rows:
            self.table_view.setRowHidden(row_idx, False)
            self.hidden_rows.discard(row_idx)
            self.logger.debug(f"Showed row {row_idx}.")

    def resetRowVisibility(self):
        """
        Resets all row visibility to show all rows.
        """
        for row_idx in list(self.hidden_rows):
            self.showRow(row_idx)
        self.hidden_rows.clear()
        self.logger.debug("Reset all row visibility.")

    def onRowClicked(self, index):
        """
        Handles clicks on table cells. Can be extended for additional functionality.
        """
        if index.isValid(): 
            row = index.row()
            col = index.column()
            # Use data() with DisplayRole to retrieve the text
            value = self.model.data(index, Qt.DisplayRole)
            self.logger.info(f"Clicked on row {row + 1}, column {self.columns[col]}: {value}")
            
    def contextMenuEvent(self, event):
        """
        Overridden context menu event to allow copying the cell value or the entire row.
        Right-click on a table cell shows a menu with “Copy Cell” and “Copy Row.”
        """
        index = self.table_view.indexAt(event.pos())
        if not index.isValid():
            return

        menu = QMenu(self)

        copyCellAction = QAction("Copy Cell", self)
        copyRowAction = QAction("Copy Row", self)
        menu.addAction(copyCellAction)
        menu.addAction(copyRowAction)

        copyCellAction.triggered.connect(lambda: self.copyCell(index))
        copyRowAction.triggered.connect(lambda: self.copyRow(index.row()))

        menu.exec_(event.globalPos())

    def copyCell(self, index):
        """
        Copies the content of the cell at the given index to the clipboard.
        """
        value = self.model.data(index, Qt.DisplayRole)
        QApplication.clipboard().setText(str(value))
        self.logger.info("Copied cell value to clipboard.")

    def copyRow(self, row):
        """
        Copies the entire row's data (joined by tabs) to the clipboard.
        """
        row_data = []
        for col in range(self.model.columnCount()):
            idx = self.model.index(row, col)
            row_data.append(str(self.model.data(idx, Qt.DisplayRole)))
        text = "\t".join(row_data)
        QApplication.clipboard().setText(text)
        self.logger.info("Copied row data to clipboard.")
