# ui/dock_advanced_search.py

from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDockWidget, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton,
    QLabel, QTreeWidget, QTreeWidgetItem, QMessageBox, QComboBox, QDialog
)
from PyQt5.QtCore import Qt, pyqtSignal, pyqtSlot, QThreadPool
import os
import logging


# Import log dock classes so we can locate them when a result is double-clicked.
from ui.components.display_logs.IIS.dock_iis import IISDock
from ui.components.display_logs.EVTX.dock_evtx import EVTXDock
from ui.components.display_logs.GENERIC.dock_generic import GenericDock


class AdvancedSearchDock(QDockWidget):
    """
    A dock widget for advanced search. This dock aggregates search results from all open
    log docks (IIS, EVTX, Generic) and displays them. Double-clicking a result will trigger
    the corresponding dock to highlight the match.
    """
    def __init__(self, parent=None):
        super().__init__("Advanced Search", parent)
        self.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.logger = logging.getLogger("AdvancedSearchDock")
        self.logger.setLevel(logging.DEBUG)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(5, 5, 5, 5)

        # --- Search controls ---
        controls_layout = QHBoxLayout()
        self.search_label = QLabel("Search:")
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Enter search text...")
        self.search_btn = QPushButton("Search")
        self.search_btn.clicked.connect(self.startSearch)
        controls_layout.addWidget(self.search_label)
        controls_layout.addWidget(self.search_box)
        controls_layout.addWidget(self.search_btn)
        layout.addLayout(controls_layout)

        # --- Advanced Search Button ---
        # This button opens a dedicated advanced search dialog
        self.adv_search_btn = QPushButton("Advanced Search...")
        self.adv_search_btn.clicked.connect(self.openSearchWindow)
        layout.addWidget(self.adv_search_btn)

        # --- Results display ---
        self.results_tree = QTreeWidget()
        self.results_tree.setHeaderLabels(["Source", "Result Info"])
        self.results_tree.itemDoubleClicked.connect(self.onResultDoubleClicked)
        layout.addWidget(self.results_tree)

        self.setWidget(container)

        self.workers = []
        self.threadpool = QThreadPool.globalInstance()
        self.logger.debug("AdvancedSearchDock initialized.")

    @pyqtSlot()
    def openSearchWindow(self):
        """
        Opens the Advanced Search dialog (your original one) to allow detailed search options.
        When the dialog is accepted, it will trigger the execution of an advanced search.
        """
        dialog = AdvancedSearchDialog(self)
        self.populateAdvancedSearchFields(dialog.field_combo)
        if dialog.exec_() == QDialog.Accepted:
            params = dialog.getSearchParameters()
            # Use the parameters from the dialog to run an advanced search.
            self.executeAdvancedSearch(params)

    def populateAdvancedSearchFields(self, field_combo: QComboBox):
        """
        Populate the field combo in the advanced search dialog with available fields across open log docks.
        """
        parent = self.parent()
        if not parent:
            return
        # Gather fields from all open log docks.
        fields = set()
        log_docks = parent.findChildren((IISDock, EVTXDock, GenericDock))
        for dock in log_docks:
            if hasattr(dock, 'model'):
                for col in range(dock.model.columnCount()):
                    field = dock.model.headerData(col, Qt.Horizontal)
                    if field and field not in {"ID", "Timestamp", "Timestamp Epoch"}:
                        fields.add(field)
        field_combo.clear()
        field_combo.addItem("All Fields")
        for f in sorted(fields):
            field_combo.addItem(f)

    @pyqtSlot()
    def startSearch(self):
        """
        Initiates a basic search based on the text entered in the search box.
        This method gathers all open log docks and starts a SearchWorker for each.
        """
        search_text = self.search_box.text().strip()
        if not search_text:
            QMessageBox.warning(self, "Search", "Please enter search text.")
            return

        # Clear previous results
        self.results_tree.clear()
        self.workers = []

        parent = self.parent()
        if not parent:
            QMessageBox.warning(self, "Search", "No parent window found.")
            return

        # Gather database paths and a source identifier from each open log dock.
        log_docks = parent.findChildren((IISDock, EVTXDock, GenericDock))
        if not log_docks:
            QMessageBox.warning(self, "Search", "No log docks are open.")
            return

        for dock in log_docks:
            db_path = getattr(dock, "db_path", None)
            source_id = None
            if hasattr(dock, "file_path"):
                if isinstance(dock.file_path, list):
                    source_id = "_".join(os.path.basename(fp) for fp in dock.file_path)
                else:
                    source_id = os.path.basename(dock.file_path)
            elif db_path:
                source_id = os.path.basename(db_path)
            if db_path and source_id:
                worker = SearchWorker(
                    db_path=db_path,
                    search_text=search_text,
                    search_field=None,  # search all fields
                    exact_match=False,
                    source_log=source_id
                )
                worker.result.connect(self.handleSearchResult)
                worker.error.connect(self.handleSearchError)
                worker.progress.connect(self.handleSearchProgress)
                worker.start()
                self.workers.append(worker)
        self.logger.info("Basic search started across all open docks.")

    def executeAdvancedSearch(self, params: dict):
        """
        Executes an advanced search using the parameters from the advanced search dialog.
        Parameters:
            params (dict): Contains 'field', 'text', and 'exact_match'.
        """
        search_text = params.get("text", "").strip()
        search_field = params.get("field")
        exact_match = params.get("exact_match", False)

        if not search_text:
            QMessageBox.warning(self, "Search", "Please enter search text.")
            return

        # Clear previous results
        self.results_tree.clear()
        self.workers = []

        parent = self.parent()
        if not parent:
            QMessageBox.warning(self, "Search", "No parent window found.")
            return

        log_docks = parent.findChildren((IISDock, EVTXDock, GenericDock))
        if not log_docks:
            QMessageBox.warning(self, "Search", "No log docks are open.")
            return

        for dock in log_docks:
            db_path = getattr(dock, "db_path", None)
            source_id = None
            if hasattr(dock, "file_path"):
                if isinstance(dock.file_path, list):
                    source_id = "_".join(os.path.basename(fp) for fp in dock.file_path)
                else:
                    source_id = os.path.basename(dock.file_path)
            elif db_path:
                source_id = os.path.basename(db_path)
            if db_path and source_id:
                worker = SearchWorker(
                    db_path=db_path,
                    search_text=search_text,
                    search_field=search_field if search_field != "All Fields" else None,
                    exact_match=exact_match,
                    source_log=source_id
                )
                worker.result.connect(self.handleSearchResult)
                worker.error.connect(self.handleSearchError)
                worker.progress.connect(self.handleSearchProgress)
                worker.start()
                self.workers.append(worker)
        self.logger.info("Advanced search started across all open docks.")

    @pyqtSlot(list, str)
    def handleSearchResult(self, results, source_log):
        """
        Aggregates search results from a SearchWorker.
        Each result is grouped under a top-level item for the corresponding source.
        """
        # Find or create a top-level item for this source.
        top_item = None
        for i in range(self.results_tree.topLevelItemCount()):
            item = self.results_tree.topLevelItem(i)
            if item.text(0) == source_log:
                top_item = item
                break
        if not top_item:
            top_item = QTreeWidgetItem([source_log])
            self.results_tree.addTopLevelItem(top_item)

        # For each result row, add a child item.
        for row in results:
            # Customize how you want to display the result.
            display_text = ", ".join(str(x) for x in row[:5])
            child = QTreeWidgetItem([source_log, display_text])
            # Save extra data in UserRole for later use.
            child.setData(0, Qt.UserRole, {"source": source_log, "row": row})
            top_item.addChild(child)

    @pyqtSlot(str)
    def handleSearchError(self, error_msg):
        QMessageBox.critical(self, "Search Error", error_msg)

    @pyqtSlot(int)
    def handleSearchProgress(self, percent):
        self.logger.debug(f"Advanced search progress: {percent}%")
        # You may update a progress bar here if desired.

    @pyqtSlot(QTreeWidgetItem, int)
    def onResultDoubleClicked(self, item, column):
        """
        When a result is double-clicked, locate the corresponding log dock
        (using the stored 'source' value) and invoke its search method.
        """
        data = item.data(0, Qt.UserRole)
        if not data:
            return
        source = data.get("source")
        result_row = data.get("row")
        if not source or result_row is None:
            return

        # Find open log docks
        main_win = self.parent()
        if not main_win:
            return

        # Search among IISDocks, EVTXDocks, and GenericDocks
        log_docks = main_win.findChildren((IISDock, EVTXDock, GenericDock))
        target_dock = None
        for dock in log_docks:
            dock_source = None
            if hasattr(dock, "file_path"):
                if isinstance(dock.file_path, list):
                    dock_source = "_".join(os.path.basename(fp) for fp in dock.file_path)
                else:
                    dock_source = os.path.basename(dock.file_path)
            elif hasattr(dock, "db_path"):
                dock_source = os.path.basename(dock.db_path)
            if dock_source == source:
                target_dock = dock
                break

        if target_dock:
            # Assuming each dock has a searchString method.
            if hasattr(target_dock, "searchString"):
                # Use the first column of the result row as a search term.
                search_term = str(result_row[0])
                target_dock.searchString(search_term)
                target_dock.raise_()
                target_dock.activateWindow()
            else:
                QMessageBox.information(self, "Result", f"Target dock for '{source}' found, but no search function available.")
        else:
            QMessageBox.warning(self, "Result", f"No open dock found for source '{source}'.")

    @pyqtSlot()
    def openSearchWindow(self):
        """
        Opens the advanced search dialog.
        This function is called when the user clicks the "Advanced Search..." button.
        """
        dialog = AdvancedSearchDialog(self)
        self.populateAdvancedSearchFields(dialog.field_combo)
        if dialog.exec_() == QDialog.Accepted:
            params = dialog.getSearchParameters()
            self.executeAdvancedSearch(params)
