# services/sql_workers/db_managers/GENERIC/generic_dock.py

import os
from datetime import timedelta
import sqlite3
import logging

from PyQt5.QtCore import Qt, pyqtSlot, QThreadPool, QModelIndex, QDateTime
from PyQt5.QtGui import QColor, QTextCharFormat, QTextCursor
from PyQt5.QtWidgets import (
    QDockWidget, QWidget, QSplitter, QVBoxLayout, QHBoxLayout, QLineEdit,
    QTreeWidget, QTreeWidgetItem, QPushButton, QProgressBar, QTabWidget,
    QLabel, QDateTimeEdit, QTableView, QMessageBox, QPlainTextEdit, QTextEdit
)

from ui.components.display_logs.GENERIC.analytics_gadget import AnalyticsGadget
from services.sql_workers.db_managers.GENERIC.workers_generic import GenericLogToSQLiteWorker
from services.sql_workers.db_managers.GENERIC.db_manager_generic import GenericDBManager
from ui.components.display_logs.GENERIC.log_table_view import GenericTableModel

# Custom Syntax Highlighter
from PyQt5.QtGui import QSyntaxHighlighter, QTextDocument

###############################################################################
# Custom Syntax Highlighter for logs
###############################################################################
class LogSyntaxHighlighter(QSyntaxHighlighter):
    """
    A simple syntax highlighter for log files.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        # Define log levels and their colors.
        self.log_levels = {
            "DEBUG": QColor("gray"),
            "INFO": QColor("blue"),
            "WARNING": QColor("orange"),
            "ERROR": QColor("red"),
            "CRITICAL": QColor("darkRed")
        }
        # Define simple matching rules.
        self.rules = []
        for level, color in self.log_levels.items():
            pattern = f"\\b{level}\\b"
            self.rules.append((Qt.MatchContains, pattern, color))
    
    def highlightBlock(self, text):
        for pattern_type, pattern, color in self.rules:
            if pattern_type == Qt.MatchContains:
                index = text.find(pattern)
                while index >= 0:
                    length = len(pattern)
                    fmt = QTextCharFormat()
                    fmt.setForeground(color)
                    self.setFormat(index, length, fmt)
                    index = text.find(pattern, index + length)

###############################################################################
# Main Dock Widget
###############################################################################
class GenericDock(QDockWidget):
    """
    A dockable viewer for transaction logs that:
      - can parse a .log/.txt file or load an existing DB,
      - display raw text in a text editor,
      - display data in a table (tabbed view),
      - support time‑based filtering,
      - include an analytics gadget that the user may hide or show, and
      - supports searching and highlighting.
    """
    def __init__(self, file_path="", db_path="", parent=None):
        if db_path:
            title = f"Trans DB: {os.path.basename(db_path)}"

        if isinstance(file_path, list):
            title = "Transaction Log: Multiple Files"
        else:
            title = f"Transaction Log: {os.path.basename(file_path)}"
        super().__init__(title, parent)
        self.setObjectName("TransDock")
        self.setAllowedAreas(Qt.AllDockWidgetAreas)

        self.logger = logging.getLogger("TransDock")
        self.logger.setLevel(logging.DEBUG)

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)

        # --- Search Bar Row ---
        search_layout = QHBoxLayout()
        search_label = QLabel("Search:")
        self.search_line = QLineEdit()
        self.search_line.setPlaceholderText("Enter search text...")
        self.search_btn = QPushButton("Search")
        self.search_btn.clicked.connect(self.onSearchClicked)
        self.clear_search_btn = QPushButton("Clear Search")
        self.clear_search_btn.clicked.connect(self.onClearSearch)
        # Toggle Analytics Button
        self.toggle_analytics_btn = QPushButton("Hide Analytics")
        self.toggle_analytics_btn.clicked.connect(self.onToggleAnalytics)
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_line)
        search_layout.addWidget(self.search_btn)
        search_layout.addWidget(self.clear_search_btn)
        search_layout.addWidget(self.toggle_analytics_btn)
        main_layout.addLayout(search_layout)

        # --- Main Horizontal Splitter ---
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)

        # Left: Analytics Gadget (initially visible)
        self.analytics_gadget = AnalyticsGadget()
        splitter.addWidget(self.analytics_gadget)
        self.analytics_gadget.setMinimumWidth(250)
        # Connect double-click on unique transactions in analytics.
        self.analytics_gadget.unique_tx_tree.itemDoubleClicked.connect(self.onAnalyticsTxDoubleClicked)

        # Right: Vertical Splitter for text viewer, table view, and tabbed tables.
        right_splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(right_splitter)
        splitter.setStretchFactor(1, 3)

        # Create a QTabWidget that contains the "Raw Text" and "Database Tables" tabs.
        self.view_tabs = QTabWidget()
        right_splitter.addWidget(self.view_tabs)

        # "Raw Text" tab: Text viewer with syntax highlighter.
        self.text_viewer = QPlainTextEdit()
        self.text_viewer.setReadOnly(True)
        self.syntax_highlighter = LogSyntaxHighlighter(self.text_viewer.document())
        self.view_tabs.addTab(self.text_viewer, "Raw Text")

        # "Database Tables" tab: a QTabWidget for each DB table.
        self.table_tabs = QTabWidget()
        self.view_tabs.addTab(self.table_tabs, "Database Tables")
        # Connect double-click on any table row to onTableDoubleClicked
        # (This connection is done when each table view is created.)

        # --- Search Results Pane ---
        # Shows search results (line number and text), similar to Notepad++.
        self.search_results = QTreeWidget()
        self.search_results.setHeaderLabels(["Line", "Text"])
        self.search_results.itemDoubleClicked.connect(self.onSearchResultClicked)
        main_layout.addWidget(self.search_results)

        # --- Time Filter UI ---
        time_filter_layout = QHBoxLayout()
        self.start_label = QLabel("Start:")
        self.start_dt = QDateTimeEdit(self)
        self.start_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.start_dt.setCalendarPopup(True)
        self.start_dt.setDateTime(QDateTime.currentDateTime().addSecs(-1800))
        self.end_label = QLabel("End:")
        self.end_dt = QDateTimeEdit(self)
        self.end_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.end_dt.setCalendarPopup(True)
        self.end_dt.setDateTime(QDateTime.currentDateTime().addSecs(1800))
        self.filter_btn = QPushButton("Apply Time Filter")
        self.filter_btn.clicked.connect(self.applyTimeFilter)
        time_filter_layout.addWidget(self.start_label)
        time_filter_layout.addWidget(self.start_dt)
        time_filter_layout.addWidget(self.end_label)
        time_filter_layout.addWidget(self.end_dt)
        time_filter_layout.addWidget(self.filter_btn)
        main_layout.addLayout(time_filter_layout)

        # --- Progress Bar and Cancel Button ---
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setVisible(False)
        self.cancel_btn.clicked.connect(self.cancelParsing)
        main_layout.addWidget(self.cancel_btn, alignment=Qt.AlignRight)

        # (The stats panel has been removed.)

        self.setWidget(main_widget)

        # State variables.
        self.file_path = file_path
        self.db_path = db_path
        self.db_manager = None
        # IMPORTANT: Ensure self.columns match your DB schema.
        self.columns = []  # Adjust if necessary.
        self.table_model = None
        self.insert_worker = None
        if isinstance(file_path, list):
            self.file_paths = file_path
        else:
            self.file_paths = [file_path]
        self.db_path = db_path

        if db_path:
            self.loadExistingDatabase(db_path)
        else:
            self.parseAndLoadFiles(self.file_paths)



    ###########################################################################
    # Analytics Toggle Method
    ###########################################################################
    def onToggleAnalytics(self):
        """Toggle the visibility of the analytics gadget."""
        if self.analytics_gadget.isVisible():
            self.analytics_gadget.hide()
            self.toggle_analytics_btn.setText("Show Analytics")
        else:
            self.analytics_gadget.show()
            self.toggle_analytics_btn.setText("Hide Analytics")

    ###########################################################################
    # Search and Highlight Methods using Extra Selections
    ###########################################################################
    def onSearchClicked(self):
        """Called when the search button is pressed."""
        search_text = self.search_line.text().strip()
        self.searchString(search_text)

    def onClearSearch(self):
        """Clears search highlights and search results."""
        self.text_viewer.setExtraSelections([])
        self.search_results.clear()
        self.search_line.clear()

    def searchString(self, text):
        """
        Searches for occurrences of 'text' (case-insensitive) in the text_viewer.
        Only the matching substring is highlighted with a light yellow background,
        preserving the original text (and syntax highlighting).
        Also populates the search results pane and scrolls to the first result.
        """
        # Clear previous extra selections.
        self.text_viewer.setExtraSelections([])

        if not text:
            self.search_results.clear()
            return

        selections = []
        doc = self.text_viewer.document()
        block = doc.firstBlock()
        while block.isValid():
            block_text = block.text()
            lower_block = block_text.lower()
            lower_search = text.lower()
            start = 0
            while True:
                idx = lower_block.find(lower_search, start)
                if idx == -1:
                    break
                # Use QTextEdit.ExtraSelection to create a selection.
                selection = QTextEdit.ExtraSelection()
                fmt = QTextCharFormat()
                fmt.setBackground(QColor("lightyellow"))
                selection.format = fmt
                cursor = QTextCursor(block)
                cursor.setPosition(block.position() + idx)
                cursor.setPosition(block.position() + idx + len(text), QTextCursor.KeepAnchor)
                selection.cursor = cursor
                selections.append(selection)
                start = idx + len(text)
            block = block.next()
        self.text_viewer.setExtraSelections(selections)
        self.populateSearchResults(text)
        # If at least one match is found, scroll to the first occurrence.
        if selections:
            first_cursor = selections[0].cursor
            self.text_viewer.setTextCursor(first_cursor)
            self.text_viewer.centerCursor()

    def populateSearchResults(self, search_text):
        """
        Splits the text_viewer content into lines and adds any line containing
        search_text (case-insensitive) to the search results pane.
        """
        self.search_results.clear()
        content = self.text_viewer.toPlainText()
        lines = content.splitlines()
        lower_search = search_text.lower()
        for i, line in enumerate(lines, start=1):
            if lower_search in line.lower():
                item = QTreeWidgetItem([str(i), line.strip()])
                self.search_results.addTopLevelItem(item)

    def onSearchResultClicked(self, item, column):
        """
        When a search result is double-clicked, scroll the text_viewer to that line.
        """
        try:
            line_number = int(item.text(0))
        except ValueError:
            return
        doc = self.text_viewer.document()
        block = doc.findBlockByNumber(line_number - 1)
        if block.isValid():
            cursor = QTextCursor(block)
            self.text_viewer.setTextCursor(cursor)
            self.text_viewer.centerCursor()

    ###########################################################################
    # Table and Analytics Double-Click Slots
    ###########################################################################
    def onTableDoubleClicked(self, index: QModelIndex):
        """
        When a table row is double-clicked, extract the transaction ID (assumed
        to be in column 0) and search/highlight it in the text_viewer.
        """
        if index.isValid():
            model = index.model()
            trans_id = model.data(model.index(index.row(), 0), Qt.DisplayRole)
            if trans_id:
                self.logger.debug(f"Table double-clicked: {trans_id}")
                self.searchString(str(trans_id))

    def onAnalyticsTxDoubleClicked(self, item, column):
        """
        When an item in the analytics gadget (unique transaction tree) is double-clicked,
        use its text (assumed to be the transaction ID) to search/highlight in the text_viewer.
        """
        tx_id = item.text(0)
        if tx_id:
            self.logger.debug(f"Analytics double-clicked: {tx_id}")
            self.searchString(tx_id)

    ###########################################################################
    # Parsing and Database Loading (existing code)
    ###########################################################################
    def parseAndLoadFiles(self, file_paths):
        self.logger.info(f"Parsing files: {file_paths}")
        # Set up your db_path as needed (for example, create a single DB name)
        db_dir = os.path.join(os.getcwd(), "db")
        os.makedirs(db_dir, exist_ok=True)
        # Create a DB name (here we use the first file’s base name plus a prefix)
        base_name = os.path.basename(file_paths[0])
        self.db_path = os.path.join(db_dir, f"trans_logs_{base_name}.db")
        self.logger.info(f"Database path set to: {self.db_path}")

        self.db_manager = GenericDBManager(self.db_path)
        self.db_manager.init_tables()
        self.analytics_gadget.db_manager = self.db_manager
        self.analytics_gadget.loadAnalytics()

        # Pass the list of files to the worker. The worker’s __init__
        # already wraps a single string in a list if necessary.
        self.insert_worker = GenericLogToSQLiteWorker(file_paths, self.db_path)
        self.insert_worker.signals.progress.connect(self.onProgress)
        self.insert_worker.signals.finished.connect(self.onInsertFinished)
        self.insert_worker.signals.error.connect(self.onInsertError)

        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self.cancel_btn.setVisible(True)

        QThreadPool.globalInstance().start(self.insert_worker)
        
    def loadExistingDatabase(self, db_path):
        """
        Load from an already existing database.
        """
        self.logger.info(f"Loading existing transaction DB: {db_path}")
        if not os.path.exists(db_path):
            QMessageBox.critical(self, "DB Not Found", f"Could not find {db_path}")
            return
        self.db_path = db_path
        self.db_manager = GenericDBManager(self.db_path)
        self.analytics_gadget.db_manager = self.db_manager
        self.analytics_gadget.loadAnalytics()

        self.populateTextPreview()
        self.populateTables()
        # Removed buildStats() since the stats pane is gone.
        self.passTimestamps()

    @pyqtSlot(int, int)
    def onProgress(self, current, total):
        percent = int((current / total) * 100) if total else 0
        self.progress_bar.setValue(percent)
        
    @pyqtSlot(str, float, float, float)
    def onInsertFinished(self, db_path, min_ts, max_ts, file_size_mb):
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.cancel_btn.setVisible(False)
        self.logger.info(f"Parse finished. DB: {db_path}")
        self.analytics_gadget.loadAnalytics()
        self.populateTextPreview()
        self.populateTables()
        self.passTimestamps()
        self.logger.info(f"File size: {file_size_mb:.2f} MB")

    @pyqtSlot(str)
    def onInsertError(self, err_msg):
        self.progress_bar.setVisible(False)
        self.cancel_btn.setVisible(False)
        QMessageBox.critical(self, "Parse Error", err_msg)

    def cancelParsing(self):
        if self.insert_worker:
            self.insert_worker.cancel()
        self.progress_bar.setVisible(False)
        self.cancel_btn.setVisible(False)
        self.logger.info("User canceled parse.")

    def passTimestamps(self):
        if not self.db_manager:
            return
        ts_list = self.db_manager.get_all_timestamps("generic_logs")
        timeline_dock = self.findTimelineDock()
        # self.logger.info(f"Timeline {timeline_dock} found and values {ts_list}")
        if timeline_dock:
            # Use the base name of the file so that each generic log file gets its own source name.
            if self.file_path:
                source_name = f"GenericLog: {os.path.basename(self.file_path[0])}"
            else:
                source_name = "GenericLog: Unknown"
            timeline_dock.addTimestamps(source_name, ts_list)

    def findTimelineDock(self):
        main_win = self.parent()
        if not main_win:
            return None
        from ui.components.timeline.dock_timeline_plotly import TimelineDock
        timeline = main_win.findChild(TimelineDock, "TimelineDock")
        return timeline

    def populateTextPreview(self):
        if isinstance(self.file_path, list):
            # Here we choose to load the first file as a preview
            preview_file = self.file_paths[0]
        else:
            preview_file = self.file_path

        if preview_file and os.path.exists(preview_file):
            try:
                with open(preview_file, "r", encoding="utf-8", errors="replace") as f:
                    self.text_viewer.setPlainText(f.read())
            except Exception as e:
                self.logger.error(f"Failed to load text: {e}")

    def populateTables(self):
        if not self.db_manager:
            return

        table_names = [
            "generic_logs",
            "generic_transactions",
            "generic_items",
            "generic_documents",
            "generic_tenders",
            "generic_promotions",
            "generic_msg_types",
            "generic_promo_items",
            "generic_loyalty_balances",
            "generic_loyalty_accounts",
            "generic_loyalty_members",
            "generic_loyalty_segments",
            "generic_loyalty_member_cards",
            "generic_loyalty_member_stores",
            "generic_item_taxes",
            "generic_promotion_messages",
            "generic_item_attributes"
        ]
        self.table_tabs.clear()
        for table in table_names:
            cols = self.db_manager.get_columns(table)
            if "id" in cols:
                cols.remove("id")
            # NOTE: Adjust column names if necessary to match your DB schema.
            model = GenericTableModel(self.db_path, cols)
            query = f"SELECT {', '.join(cols)} FROM {table}"
            model.loadData(query, ())
            table_view = QTableView()
            table_view.setModel(model)
            table_view.doubleClicked.connect(self.onTableDoubleClicked)
            self.table_tabs.addTab(table_view, table)

    @pyqtSlot()
    def applyTimeFilter(self):
        try:
            # Make sure self.columns is set appropriately for your DB.
            if not self.table_model:
                return
            start_dt = self.start_dt.dateTime().toPyDateTime()
            end_dt = self.end_dt.dateTime().toPyDateTime()
            start_epoch = start_dt.timestamp()
            end_epoch = end_dt.timestamp()

            query = f"""
                SELECT {', '.join(self.columns)}
                FROM generic_logs
                WHERE combined_ts BETWEEN ? AND ?
            """
            params = (start_epoch, end_epoch)
            self.table_model.loadData(query, params)
            self.analytics_gadget.loadAnalytics()
        except Exception as e:
            QMessageBox.critical(self, "Time Filter Error", str(e))

    @pyqtSlot(object)
    def onTimelineJump(self, target_datetime):
        self.logger.info(f"Timeline jump => {target_datetime}")
        window = timedelta(minutes=30)
        start_dt = target_datetime - window
        end_dt = target_datetime + window
        self.start_dt.setDateTime(QDateTime(start_dt))
        self.end_dt.setDateTime(QDateTime(end_dt))
        self.applyTimeFilter()
