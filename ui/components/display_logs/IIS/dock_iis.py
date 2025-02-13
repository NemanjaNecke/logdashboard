# ui/components/display_logs/IIS/dock_iis.py

import os
import logging
from datetime import datetime
from math import ceil
from PyQt5.QtCore import ( # pylint: disable=no-name-in-module
    Qt, pyqtSignal, pyqtSlot, QThreadPool,
    QDateTime
)
from PyQt5.QtWidgets import ( # pylint: disable=no-name-in-module
    QDockWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QProgressBar, QPushButton, QMessageBox,
    QSplitter, QFileDialog, QGroupBox, QSpinBox, QGridLayout,
    QDateTimeEdit, QDialog, QComboBox
)
from ui.components.display_logs.IIS.table.export import export

from services.controllers.IIS.iis_controller import IISController
from services.sql_workers.db_managers.IIS.db_manager_iis import DatabaseManager
from services.sql_workers.db_managers.IIS.iis_db_loader import DatabaseLoader
from services.sql_workers.db_managers.IIS.display_stats_loader import DisplayStatsLoader
from services.sql_workers.db_managers.IIS.iis_stats_loader import StatsLoader
from ui.components.display_logs.IIS.stats.stats_panel import StatsPanel
from ui.components.display_logs.IIS.table.log_table_view import LogTableView
from ui.components.display_logs.IIS.stats.all_stats_panel import AllStatsPanel
from ui.components.display_logs.IIS.table.column_selection_dialog import ColumnSelectionDialog
from ui.components.display_logs.IIS.search.search_dialog import SearchDialog

# For accessing TimelineDock
from ui.components.timeline.dock_timeline_plotly import TimelineDock

from services.converters.IIS.delegate_status import StatusDelegate  # Ensure correct import

class IISDock(QDockWidget):
    """
    A dockable window that displays IIS logs and relevant statistics.
    """
    timeRangeAvailable = pyqtSignal(object, object)  # (min_time, max_time)

    def __init__(self, file_path="", db_path=None, parent=None):
        # Determine display name based on file_path type.
        if isinstance(file_path, list):
            # Combine the base names (without extension) of all files
            combined_name = "_".join(os.path.splitext(os.path.basename(fp))[0] for fp in file_path)
            display_name = combined_name
        else:
            display_name = os.path.basename(file_path) if file_path else "None"

        super().__init__(f"IIS Log: {display_name}", parent)
        self.setObjectName("IISDock")
        self.setAllowedAreas(Qt.AllDockWidgetAreas)
        self.selected_columns = None  # For column selection dialog

        # -------------------------------------------------
        # Logger & ThreadPool
        # -------------------------------------------------
        self.logger = logging.getLogger("IISDock")  # pylint: disable=no-member
        self.logger.setLevel(logging.INFO)  # pylint: disable=no-member

        self.threadpool = QThreadPool()
        self.logger.debug(
            f"ThreadPool initialized with max {self.threadpool.maxThreadCount()} threads."
        )

        # -------------------------------------------------
        # Attributes
        # -------------------------------------------------
        self.start_parsing_button = QPushButton("Parse Log")
        if file_path:
            self.file_path = file_path
            self.start_parsing_button.setEnabled(True)
        else:
            self.start_parsing_button.setEnabled(False)
        # Determine the database path.
        if db_path:
            self.db_path = db_path
        else:
            if isinstance(file_path, list):
                self.db_path = os.path.join(os.getcwd(), "db", "iis_logs_combined.db")
            else:
                self.db_path = os.path.join(os.getcwd(), "db", f"iis_logs_{os.path.basename(file_path)}.db")
        self.current_filters = None
        self.db_file_name = None
        self.db_manager = DatabaseManager(self.db_path) if self.db_path else None
        self.all_cols = None
        self.controller = IISController(file_path, self.db_path) if file_path else None
        if self.controller:
            self.controller.parseFinished.connect(self.onParseFinished)
            self.controller.parseError.connect(self.onParseError)
            self.controller.progressUpdate.connect(self.onProgressUpdate)

        # Data / Pagination
        self.page_size = 50000
        self.current_page = 1
        self.log_data = []
        self.columns = []
        self.stats_data = {}
        self.total_pages = None
        self.total_records = None
        self.file_size_str = "Unkown size"
        # Show the total data size and number of pages
        self.data_size_label = QLabel("Data Size: 0 MB")
        self.num_pages_label = QLabel("Total Pages: 1")
        # Define the fields to display stats for
        self.display_fields = ["sc_status", "cs_uri_stem", "cs_method", "c_ip", "time_taken"]
        self.desired_order = [
            "time_taken", "sc_status", "cs_uri_stem", "cs_method",
            "c_ip", "s_ip", "s_port", "cs_host", "cs_referer", "cs_user_agent"
        ]
        # Time Filters
        self.start_ts = None
        self.end_ts = None

        # Active Stats Filters (for LogTableView)
        self.active_stats_filters = {}

        # Flags controlling stats logic
        self.stats_loaded = False
        self.is_display_stats_loading = False

        # -------------------------------------------------
        # Main UI Setup
        # -------------------------------------------------
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)

        # (A) Time Range Filter with Apply and Clear buttons + Stats Edit
        time_filter_group = QGroupBox("Time Range Filter")
        tf_layout = QGridLayout(time_filter_group)

        self.start_time_edit = QDateTimeEdit()
        self.start_time_edit.setCalendarPopup(True)
        self.start_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.start_time_edit.setDateTime(QDateTime.currentDateTime().addDays(-1))
        self.start_time_edit.setEnabled(False)
        # NEW: End Time
        self.end_time_edit = QDateTimeEdit()
        self.end_time_edit.setCalendarPopup(True)
        self.end_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.end_time_edit.setDateTime(QDateTime.currentDateTime())
        self.end_time_edit.setEnabled(False)
        # NEW BUTTON: Apply Time Filter
        self.apply_time_button = QPushButton("Apply Time Filter")
        self.apply_time_button.clicked.connect(self.applyTimeFilter)
        self.apply_time_button.setEnabled(False)
        # NEW BUTTON: Clear Time Filter
        self.clear_time_button = QPushButton("Clear Time Filter")
        self.clear_time_button.clicked.connect(self.clearTimeFilter)
        self.clear_time_button.setEnabled(False)
        controls_layout = QHBoxLayout()
        # Add a button for column selection
        self.select_columns_button = QPushButton("Select Columns")
        self.select_columns_button.clicked.connect(self.openColumnSelectionDialog)
        controls_layout.addWidget(self.select_columns_button)
        # NEW BUTTON: Show All Stats, Export, Apply Time Filter, Clear Time Filter
        self.show_all_stats_button = QPushButton("Show All Stats")
        self.show_all_stats_button.clicked.connect(self.onShowAllStatsClicked)
        self.show_all_stats_button.setEnabled(False)

        self.export_button = QPushButton("Export")
        tf_layout.addWidget(self.export_button, 0, 0)
        tf_layout.addWidget(self.show_all_stats_button, 0, 1)
        tf_layout.addWidget(QLabel("Start:"), 0, 2)
        tf_layout.addWidget(self.start_time_edit, 0, 2)
        tf_layout.addWidget(QLabel("End:"), 0, 3)
        tf_layout.addWidget(self.end_time_edit, 0, 3)
        tf_layout.addWidget(self.apply_time_button, 0, 4)
        tf_layout.addWidget(self.clear_time_button, 0, 5)
        tf_layout.addWidget(self.data_size_label, 1, 0, 1, 1)  # Adjust positions as needed
        tf_layout.addWidget(self.num_pages_label, 1, 3, 1, 1)
        main_layout.addWidget(time_filter_group)
        time_filter_group.setFixedHeight(80)
        # (B) Splitter with StatsPanel & LogTableView
        self.splitter = QSplitter(Qt.Horizontal)
        self.stats_panel = StatsPanel()
        self.splitter.addWidget(self.stats_panel)
        self.stats_panel.setMinimumWidth(250)

        self.log_table_view = LogTableView()
        self.splitter.addWidget(self.log_table_view)
        self.splitter.setStretchFactor(1, 3)

        main_layout.addWidget(self.splitter)
        main_layout.setStretchFactor(time_filter_group, 0)
        main_layout.setStretchFactor(self.splitter, 3)
        # (C) Bottom Controls
        self.open_db_button = QPushButton("Open DB")
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setEnabled(False)

        controls_layout.addWidget(self.open_db_button)
        controls_layout.addWidget(self.start_parsing_button)
        controls_layout.addWidget(self.cancel_button)

        # Add Search button here:
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.openSearchDialog)
        controls_layout.addWidget(self.search_button)
        # Create pagination controls
        self.prev_button = QPushButton("Prev Page")
        self.next_button = QPushButton("Next Page")
        self.page_spin = QSpinBox()
        self.page_spin.setRange(1, 999999)
        self.page_spin.setValue(1)
        # --- New: Page Size Dropdown ---
        self.page_size_combo = QComboBox()
        # Define options for page size (as strings)
        self.page_size_options = ["50000", "100000", "200000", "500000"]
        for opt in self.page_size_options:
            self.page_size_combo.addItem(opt)
        # Set default page size (should match self.page_size)
        self.page_size_combo.setCurrentText("50000")
        self.page_size_combo.currentIndexChanged.connect(self.onPageSizeChanged)
        # Optionally add a label for clarity:
        page_size_label = QLabel("Page Size:")

        # Add the new widgets to the layout
        controls_layout.addWidget(page_size_label)
        controls_layout.addWidget(self.page_size_combo)
        controls_layout.addWidget(self.prev_button)
        controls_layout.addWidget(self.next_button)
        controls_layout.addWidget(QLabel("Go to page:"))
        controls_layout.addWidget(self.page_spin)

        controls_layout.addStretch()

        self.status_label = QLabel("Status: Idle")
        controls_layout.addWidget(self.status_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        controls_layout.addWidget(self.progress_bar)

        main_layout.addLayout(controls_layout)
        self.setWidget(main_widget)
        
        # Connect signals
        self.open_db_button.clicked.connect(self.openDatabase)
        self.start_parsing_button.clicked.connect(self.startParsing)
        self.cancel_button.clicked.connect(self.cancelParsing)
        self.prev_button.clicked.connect(self.onPrevPage)
        self.next_button.clicked.connect(self.onNextPage)
        self.page_spin.valueChanged.connect(self.onPageSpin)
        self.stats_panel.refreshStatsRequested.connect(self.refreshFilters)
        self.export_button.clicked.connect(self.onExportClicked)
        # Connect StatsPanel to apply filters on LogTableView
        self.stats_panel.statsFilterApplied.connect(self.onStatsFilterFromPanel)

        # If we have only a DB (no file), load it
        if self.db_path and not file_path:
            self.loadDatabase(self.db_path)

        self.logger.debug("IISDock initialized.")

    # -------------------------------------------------
    # Page - size controls
    # -------------------------------------------------
    @pyqtSlot(int)
    def onPageSizeChanged(self, index):
        # Get the new page size from the combo box
        new_page_size = int(self.page_size_combo.currentText())
        if new_page_size != self.page_size:
            self.logger.info(f"Page size changed from {self.page_size} to {new_page_size}")
            self.page_size = new_page_size
            self.current_page = 1  # Optionally reset to the first page
            self.loadPage(self.db_path, page=self.current_page)
        
    
    # -------------------------------------------------
    # Search 
    # -------------------------------------------------
    @pyqtSlot()
    def openSearchDialog(self):
        """
        Opens the search dialog so the user can enter a search term and optionally select a column.
        If the user confirms the search, performSearch() is called.
        """
        # Ensure that a database is already open
        if not self.db_manager:
            self.logger.warning("No database available for search. Please open or parse a database first.")
            return

        # Get available columns from the database
        available_columns = self.db_manager.get_all_columns("iis_logs")
        dialog = SearchDialog(available_columns, self)
        if dialog.exec_() == QDialog.Accepted:
            search_term, search_column = dialog.getSearchCriteria()
            self.performSearch(search_term, search_column)
    
    def performSearch(self, search_term, search_column):
        """
        Constructs a SQL WHERE clause based on the search criteria and reloads the LogTableView
        with the search results.
        
        If a specific column is provided, the query will search only that column using the SQL LIKE operator.
        If no column is specified, the query will search all available columns.
        
        Args:
            search_term (str): The string to search for.
            search_column (str or None): The column name to search in, or None to search all columns.
        """
        if not search_term:
            QMessageBox.warning(self, "Empty Search", "Please enter a search term.")
            return
        
        # Build the WHERE clause for the search
        search_term_like = f"%{search_term}%"
        if search_column:
            # For known numeric columns, cast the column to TEXT
            numeric_columns = {"s_port", "sc_status", "sc_substatus", "sc_win32_status", "time_taken", "combined_ts"}
            if search_column in numeric_columns:
                where_clause = f"CAST({search_column} AS TEXT) LIKE ?"
            else:
                where_clause = f"{search_column} LIKE ?"
            params = [search_term_like]
        else:
            # Get all columns from the table
            columns = self.db_manager.get_all_columns("iis_logs")
            if not columns:
                QMessageBox.warning(self, "No Columns", "No columns found in the database to search.")
                return
            # For each column, if it is numeric, cast it to text before comparing.
            numeric_columns = {"s_port", "sc_status", "sc_substatus", "sc_win32_status", "time_taken", "combined_ts"}
            clause_parts = []
            for col in columns:
                if col in numeric_columns:
                    clause_parts.append(f"CAST({col} AS TEXT) LIKE ?")
                else:
                    clause_parts.append(f"{col} LIKE ?")
            where_clause = " OR ".join(clause_parts)
            where_clause = f"({where_clause})"
            params = [search_term_like] * len(columns)

        self.logger.info(f"Performing search for '{search_term}' in "
                         f"{'column ' + search_column if search_column else 'all columns'}")
        # Reset current filters and load the first page with the search filter
        self.current_filters = (where_clause, params)
        self.loadPage(self.db_path, page=1, filters=self.current_filters)    
        
    # -------------------------------------------------
    # Export current table to excel
    # -------------------------------------------------
    def onExportClicked(self):
        """
        Slot triggered by Export button. 
        Allows the user to pick a filename, 
        then exports the current table data 
        (with filters applied).
        """


        # Let user select a save path
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export to Excel",
            "",                          # default directory or filename
            "Excel Files (*.xlsx);;All Files (*)"
        )
        if not file_path:
            return  # User canceled or closed the dialog

        # We'll export only what's currently in the table view:
        # self.log_data (the currently loaded page) 
        # plus self.columns. If you want *all* filtered rows, 
        # you'll need to query them from the DB. 
        # But here's the simplest approach:
        current_data = self.log_data
        current_columns = self.columns

        try:
            export.export_to_excel(current_data, current_columns, file_path)
            QMessageBox.information(self, "Export Successful", f"Data exported to {file_path}")
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{e}")
    # -------------------------------------------------
    # TIME FILTER HANDLING
    # -------------------------------------------------
    def applyTimeFilter(self, start_dt=None, end_dt=None):
        if start_dt is None or end_dt is None:
            start_dt = self.start_time_edit.dateTime().toUTC().toPyDateTime()
            end_dt = self.end_time_edit.dateTime().toUTC().toPyDateTime()

        if start_dt > end_dt:
            QMessageBox.warning(self, "Invalid Time Range", "Start time must be before end time.")
            return

        self.start_ts = start_dt.timestamp()
        self.end_ts = end_dt.timestamp()

        self.logger.info(f"Applied time filter: {start_dt} - {end_dt}")
        if self.db_path:
            self.loadPage(self.db_path, page=1)
        else:
            self.logger.warning("No database path set; cannot apply time filter.")

    def clearTimeFilter(self):
        self.start_ts = None
        self.end_ts = None
        self.start_time_edit.setDateTime(QDateTime.currentDateTime().addDays(-1))
        self.end_time_edit.setDateTime(QDateTime.currentDateTime())

        self.logger.info("Cleared time filters.")
        if self.db_path:
            self.loadPage(self.db_path, page=1)

    # -------------------------------------------------
    # NEW: Show All Stats
    # -------------------------------------------------
    @pyqtSlot()
    def onShowAllStatsClicked(self):
        self.logger.info("User requested to show all stats.")
        if not self.db_path:
            QMessageBox.warning(self, "No Database", "Please open or parse a database first.")
            return
        self.startAllStatsLoader()

    def startAllStatsLoader(self):
        self.logger.info("Starting DisplayStatsLoader for ALL stats (fields=None).")
        self.all_stats_loader = DisplayStatsLoader(self.db_path, fields=None)
        self.all_stats_loader.signals.progress.connect(self.onAllStatsLoadProgress)
        self.all_stats_loader.signals.finished.connect(self.onAllStatsLoadFinished)
        self.all_stats_loader.signals.error.connect(self.onAllStatsLoadError)

        self.status_label.setText("Status: Loading *All* Statistics...")
        self.progress_bar.setValue(0)


        self.threadpool.start(self.all_stats_loader)

    @pyqtSlot(int, int)
    def onAllStatsLoadProgress(self, current, total):
        pct = int((current / total) * 100) if total else 0
        self.progress_bar.setValue(pct)
        self.status_label.setText(f"Status: Loading All Statistics... {pct}%")

    @pyqtSlot(dict)
    def onAllStatsLoadFinished(self, stats_dict):
        self.logger.info("AllStatsLoader completed successfully.")
        self.progress_bar.setValue(100)
        self.status_label.setText("Status: All Statistics Loaded.")

        self.start_parsing_button.setEnabled(True)
        self.open_db_button.setEnabled(True)

        dialog = AllStatsPanel(stats_dict, self)
        dialog.exec_()

    @pyqtSlot(str)
    def onAllStatsLoadError(self, err_msg):
        self.logger.error(f"AllStatsLoader Error: {err_msg}")
        QMessageBox.critical(self, "All Stats Loading Error", err_msg)
        self.status_label.setText("Status: Error")
        self.progress_bar.setValue(0)

        self.start_parsing_button.setEnabled(True)
        self.open_db_button.setEnabled(True)
    # -------------------------------------------------
    # THRESHOLD FILTERING
    # -------------------------------------------------
    @pyqtSlot()
    def onResetFilters(self):
        """
        Handles the reset filters action.
        """
        self.logger.info("User initiated filter reset.")
        self.stats_panel.resetFilters()

        # Clear active filters in IISDock
        self.active_stats_filters.clear()

        # Reload the first page without filters
        self.loadPage(self.db_path, page=1, filters=None)
    
    def applyThresholdFilters(self, thresholds):
        self.logger.debug(f"Applying threshold filters: {thresholds}")
        # Re-populate the stats panel with new thresholds using 'stats_data'
        self.stats_panel.populateStats(self.stats_data, self.active_stats_filters)
    
    def refreshFilters(self):
        self.logger.info(f"Starting refresh filter")
        self.startDisplayStatsLoader(self.db_path)

    # -------------------------------------------------
    # DATABASE LOADING LOGIC
    # -------------------------------------------------
    def loadDatabase(self, db_path):
        self.logger.info(f"Loading DB: {db_path}")
        self.db_path = db_path
        self.db_manager = DatabaseManager(self.db_path)

        try:
            self.file_size_mb = self.db_manager.load_metadata("file_size")
            self.logger.info(f"file_size_mb: {self.file_size_mb}")
        except Exception as e:
            self.logger.error(f"Didn't write file size to db: {e}")
            QMessageBox.critical(self, "DB Load Error", str(e))
        try:
            if self.file_size_mb is not None:
                self.file_size_str = f"{self.file_size_mb:.2f} MB"
            else:
                self.file_size_str = "Unknown Size"

            self.db_file_name = os.path.basename(self.db_path)
            self.setWindowTitle(f"IIS Log: | DB: {self.db_file_name} ({self.file_size_str})")
            self.logger.debug(f"Retrieved file size: {self.file_size_mb:.2f} MB")

            # Set data size label
            self.data_size_label.setText(f"Data Size: {self.file_size_str}")

            # Get total records and calculate total pages
            self.total_records = self.db_manager.get_total_records("iis_logs", self.start_ts, self.end_ts)
            self.logger.debug(f"Total records: {self.total_records}")
            self.total_pages = ceil(self.total_records / self.page_size) if self.page_size else 1
            self.num_pages_label.setText(f"Total Pages: {self.total_pages}")
            self.logger.debug(f"Total pages: {self.total_pages}")

            # Existing stats loading logic
            if not self.stats_loaded:
                stats = self.db_manager.load_field_stats("stats_iis_logs")
                if not stats:
                    self.logger.info("No existing stats found. Starting StatsLoader.")
                    self.startStatsLoader(db_path)
                else:
                    self.logger.info("Existing stats found. Loading specific stats for display.")
                    self.startDisplayStatsLoader(db_path)
                    self.stats_loaded = True
            else:
                self.loadPage(db_path, page=1)

        except Exception as e:
            self.logger.error(f"Error loading database: {e}")
            QMessageBox.critical(self, "DB Load Error", str(e))
        
    def fetch_and_pass_all_timestamps(self):
        """
        Retrieves ALL timestamps from DB (unfiltered)
        and adds them to the TimelineDock.
        """
        if not self.db_manager:
            self.logger.warning("DatabaseManager not initialized.")
            return

        self.logger.info("Fetching all timestamps from database (unfiltered).")
        all_timestamps = self.db_manager.get_all_timestamps("iis_logs")
        self.logger.info(f"Fetched {len(all_timestamps)} total timestamps from DB.")

        # Locate the TimelineDock
        main_win = self.parent()
        if not main_win:
            self.logger.warning("No parent main window found; cannot pass timestamps to TimelineDock.")
            return

        timeline_dock = main_win.findChild(TimelineDock, "TimelineDock")
        if not timeline_dock:
            self.logger.warning("TimelineDock not found in parent; cannot pass timestamps.")
            return

        # Add them to the timeline
        source_name = f"iis_{id(self)}"
        try:
            timeline_dock.addTimestamps(source_name, all_timestamps)
            self.logger.info(f"Passed {len(all_timestamps)} timestamps to TimelineDock with name='{source_name}'.")
        except Exception as e:
            self.logger.error(f"Failed to add all timestamps to TimelineDock: {e}")

    def startStatsLoader(self, db_path):
        self.status_label.setText("Status: Generating Statistics...")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(False)
        self.open_db_button.setEnabled(False)

        self.stats_loader = StatsLoader(db_path, "iis_logs")
        self.stats_loader.signals.progress.connect(self.onStatsProgress)
        self.stats_loader.signals.finished.connect(self.onStatsFinished)
        self.stats_loader.signals.error.connect(self.onStatsError)

        self.threadpool.start(self.stats_loader)

    def loadPage(self, db_path, page=1, selected_columns=None, filters=None):
        self.current_page = page
        self.status_label.setText(f"Status: Loading page {page}...")
        self.current_filters = filters
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(False)
        self.open_db_button.setEnabled(False)

        self.loader = DatabaseLoader(
            db_path=db_path,
            table_name="iis_logs",
            page_size=self.page_size,
            current_page=page,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
            selected_columns=selected_columns,
            filters=filters  # Pass filters here
        )
        self.loader.signals.progress.connect(self.onLoadProgress)
        self.loader.signals.finished.connect(self.onLoadFinished)
        self.loader.signals.error.connect(self.onLoadError)

        self.threadpool.start(self.loader)
        
    # Stats Filter from StatsPanel Loading
    def startDisplayStatsLoader(self, db_path):
        if self.is_display_stats_loading:
            self.logger.info("DisplayStatsLoader is already running.")
            return

        self.is_display_stats_loading = True
        self.status_label.setText("Status: Loading Specific Statistics...")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(False)
        self.open_db_button.setEnabled(False)

        # Use selected_columns if available; else use default display_fields
        display_fields = self.selected_columns if self.selected_columns else self.display_fields

        self.display_stats_loader = DisplayStatsLoader(db_path, fields=display_fields)
        self.display_stats_loader.signals.progress.connect(self.onDisplayStatsProgress)
        self.display_stats_loader.signals.finished.connect(self.onDisplayStatsFinished)
        self.display_stats_loader.signals.error.connect(self.onDisplayStatsError)

        self.threadpool.start(self.display_stats_loader)

    # -------------------------------------------------
    # STATS LOADER SIGNALS
    # -------------------------------------------------
    @pyqtSlot(int, int)
    def onStatsProgress(self, current, total):
        pct = int((current / total) * 100) if total else 0
        self.progress_bar.setValue(pct)
        self.status_label.setText(f"Status: Generating Statistics... {pct}%")

    @pyqtSlot(dict)
    def onStatsFinished(self, stats_dict):
        self.logger.info("StatsLoader completed successfully.")
        self.stats_data = stats_dict
        self.stats_panel.populateStats(stats_dict)
        self.progress_bar.setValue(100)
        self.status_label.setText("Status: Statistics Ready.")

        self.stats_loaded = True

        # Start loading display stats
        self.startDisplayStatsLoader(self.db_path)
        

    @pyqtSlot(str)
    def onStatsError(self, err_msg):
        self.logger.error(f"StatsLoader Error: {err_msg}")
        QMessageBox.critical(self, "Statistics Loading Error", err_msg)
        self.status_label.setText("Status: Error")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(True)
        self.open_db_button.setEnabled(True)

    # -------------------------------------------------
    # DISPLAY STATS LOADER SIGNALS
    # -------------------------------------------------
    @pyqtSlot(int, int)
    def onDisplayStatsProgress(self, current, total):
        pct = int((current / total) * 100) if total else 0
        self.progress_bar.setValue(pct)
        self.status_label.setText(f"Status: Loading Specific Statistics... {pct}%")

    @pyqtSlot(dict)
    def onDisplayStatsFinished(self, stats_dict):
        self.logger.info("DisplayStatsLoader completed successfully.")
        self.stats_data = stats_dict
        self.stats_panel.populateStats(stats_dict)
        self.progress_bar.setValue(100)
        self.status_label.setText("Status: Specific Statistics Loaded.")
        # Pass all timestamps to the timeline
        self.logger.info("Passing all timestamps to TimelineDock.")
        self.fetch_and_pass_all_timestamps()
        self.loadPage(self.db_path, page=1)
        self.is_display_stats_loading = False


    @pyqtSlot(str)
    def onDisplayStatsError(self, err_msg):
        self.logger.error(f"DisplayStatsLoader Error: {err_msg}")
        QMessageBox.critical(self, "Statistics Loading Error",
                             f"An error occurred while loading specific statistics:\n{err_msg}")
        self.status_label.setText("Status: Error")
        self.progress_bar.setValue(0)

        self.is_display_stats_loading = False
        self.start_parsing_button.setEnabled(True)
        self.open_db_button.setEnabled(True)

    # -------------------------------------------------
    # DATABASE LOADER SIGNALS (Pagination)
    # -------------------------------------------------
    @pyqtSlot()
    def openColumnSelectionDialog(self):
        if not self.db_manager:
            QMessageBox.warning(self, "No Database", "Please open or parse a database first.")
            return

        available_columns = self.db_manager.get_all_columns("iis_logs")
        # Exclude 'id' if present
        available_columns = [col for col in available_columns if col != 'id']

        # Fetch currently displayed columns
        current_columns = self.log_table_view.columns if hasattr(self.log_table_view, 'columns') else available_columns

        dialog = ColumnSelectionDialog(available_columns, selected_columns=current_columns, parent=self)
        if dialog.exec_() == QDialog.Accepted:
            selected_columns = dialog.get_selected_columns()
            if not selected_columns:
                QMessageBox.warning(self, "No Columns Selected", "At least one column must be selected.")
                return
            self.logger.info(f"User selected columns: {selected_columns}")
            self.selected_columns = selected_columns  # Store selected columns
            self.loadPage(self.db_path, page=1, selected_columns=self.selected_columns)


    @pyqtSlot(int)
    def onLoadProgress(self, progress):
        self.progress_bar.setValue(progress)
        self.status_label.setText(f"Status: Loading Page... {progress}%")

    @pyqtSlot(list)
    def onLoadFinished(self, data):
        self.log_data = data
        self.populateTable()
        row_count = len(data)

        self.progress_bar.setValue(100)
        self.status_label.setText(
            f"Displaying page {self.current_page} with {row_count} rows out of {self.total_records} total."
        )

        self.start_parsing_button.setEnabled(False)
        self.open_db_button.setEnabled(False)

        # Update total records and pages based on current filters
        if self.current_filters:
            final_where, params = self.current_filters
        else:
            final_where, params = None, []

        try:
            self.total_records = self.db_manager.get_total_records(
                "iis_logs",
                self.start_ts,
                self.end_ts,

            )
        except TypeError:
            # Adjust based on the actual signature of get_total_records
            self.total_records = self.db_manager.get_total_records(
                "iis_logs",
                self.start_ts,
                self.end_ts
            )

        self.logger.debug(f"Total records after filters: {self.total_records}")
        total_pages = ceil(self.total_records / self.page_size) if self.page_size else 1
        self.num_pages_label.setText(f"Total Pages: {total_pages}")

        if self.current_page <= 1:
            self.prev_button.setEnabled(False)
        else:
            self.prev_button.setEnabled(True)

        if self.current_page >= total_pages:
            self.next_button.setEnabled(False)
        else:
            self.next_button.setEnabled(True)

        self.page_spin.blockSignals(True)
        self.page_spin.setRange(1, total_pages)  # Set the range based on total pages
        self.page_spin.setValue(self.current_page)
        self.page_spin.blockSignals(False)
        self.clear_time_button.setEnabled(True)
        self.apply_time_button.setEnabled(True)
        self.start_time_edit.setEnabled(True)
        self.end_time_edit.setEnabled(True)
        self.show_all_stats_button.setEnabled(True)

    @pyqtSlot(str)
    def onLoadError(self, err_msg):
        self.logger.error(f"DatabaseLoader Error: {err_msg}")
        QMessageBox.critical(self, "Database Load Error", err_msg)
        self.status_label.setText("Status: Error")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(True)
        self.open_db_button.setEnabled(True)

    # -------------------------------------------------
    # PARSING LOG SIGNALS
    # -------------------------------------------------
    def startParsing(self, filepath=None):
        if not self.controller:
            QMessageBox.critical(self, "Parse Error", "No IISController set.")
            return
        if not filepath:
            filepath = self.controller.filepath
        if not filepath:
            QMessageBox.critical(self, "Parse Error", "No log file path specified.")
            return

        self.logger.info(f"Initiating parsing for {filepath}")
        self.status_label.setText("Status: Parsing Started")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.open_db_button.setEnabled(False)

        self.log_data.clear()
        self.columns.clear()

        self.controller.startParsing()

    def cancelParsing(self):
        if self.controller:
            self.controller.cancelParsing()
            self.status_label.setText("Status: Cancelling...")
            self.cancel_button.setEnabled(False)
            self.open_db_button.setEnabled(True)

    @pyqtSlot(int, int)
    def onProgressUpdate(self, current, total):
        if total > 0:
            percentage = current
            self.progress_bar.setValue(percentage)
            self.status_label.setText(f"Status: Parsing... {percentage}%")
            self.logger.debug(f"Parsing progress: {percentage}%")
        else:
            self.progress_bar.setValue(0)
            self.status_label.setText("Status: Parsing...")
            self.logger.debug("Parsing progress: Processed lines without total.")

    @pyqtSlot(str, float, float, float)
    def onParseFinished(self, db_path, min_ts, max_ts, file_size_mb):
        self.progress_bar.setValue(100)
        min_str = datetime.fromtimestamp(min_ts).strftime("%Y-%m-%d %H:%M:%S") if min_ts else "N/A"
        max_str = datetime.fromtimestamp(max_ts).strftime("%Y-%m-%d %H:%M:%S") if max_ts else "N/A"
        self.status_label.setText(f"Status: Parsing Completed. Range: {min_str} - {max_str}")

        self.file_size_str = f"{file_size_mb:.2f} MB"

        # Determine title based on whether file_path is a list or a single file.
        if isinstance(self.file_path, list):
            # Combine the base names (without extension) for all files
            combined_name = "_".join(os.path.splitext(os.path.basename(fp))[0] for fp in self.file_path)
            title = f"IIS Log: {combined_name}"
        else:
            title = f"IIS Log: {os.path.basename(self.file_path)}"

        self.setWindowTitle(f"{title} ({self.file_size_str})")

        self.loadDatabase(db_path)

        self.start_parsing_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.open_db_button.setEnabled(True)
    @pyqtSlot(str)
    def onParseError(self, err_msg):
        self.logger.error(f"Parsing Error: {err_msg}")
        QMessageBox.critical(self, "Parsing Error", err_msg)
        self.status_label.setText("Status: Error")
        self.progress_bar.setValue(0)
        self.start_parsing_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.open_db_button.setEnabled(True)

    # -------------------------------------------------
    # PAGINATION CONTROLS
    # -------------------------------------------------
    def onPrevPage(self):
        if self.current_page > 1:
            self.loadPage(self.db_path, self.current_page - 1)

    def onNextPage(self):
        self.loadPage(self.db_path, self.current_page + 1)

    def onPageSpin(self, page_num):
        self.loadPage(self.db_path, page_num)

    # -------------------------------------------------
    # TABLE POPULATION
    # -------------------------------------------------

    def populateTable(self):
        if self.log_data:
            self.all_cols = set(self.log_data[0].keys())
        else:
            self.all_cols = []

        if self.selected_columns:
            self.columns = self.selected_columns
            # Reorder columns based on desired_order
            ordered_columns = [col for col in self.desired_order if col in self.columns]
            unordered_columns = list(self.all_cols - set(ordered_columns))
            self.columns = ordered_columns + unordered_columns
        else:
            self.columns = list(self.all_cols)
            # Reorder columns based on desired_order
            ordered_columns = [col for col in self.desired_order if col in self.columns]
            unordered_columns = list(set(self.columns) - set(ordered_columns))
            self.columns = ordered_columns + unordered_columns

        self.log_table_view.setData(self.log_data, self.columns) # Ensure set_data method exists
        self.log_table_view.model.layoutChanged.emit()
        

    # -------------------------------------------------
    # STATS PANEL FILTERING
    # -------------------------------------------------
    @pyqtSlot(str, object, str)
    def onStatsFilterFromPanel(self, field, value, operator):
        """
        Handles filter signals emitted from the StatsPanel and updates the active filter state
        cumulatively so that multiple filters for the same field are preserved.

        When the user clicks an item in the stats panel:
        - For a categorical field:
            • If the item is unchecked, it means the user wants to exclude that value,
                so a ('neq', value) condition is added for that field.
            • If the item is checked, it means the user wants to include that value,
                so any existing ('neq', value) condition is removed.
        - For numerical (threshold) fields, similar logic applies based on the operator.
        
        This method retrieves the current active filter list for the given field (if any), then:
        - If the operator is 'neq' (i.e. the user is turning off the filter for that value),
            it adds the condition to the list if not already present.
        - If the operator is 'eq' (i.e. the user is turning the filter back on for that value),
            it removes any condition for that value.
        
        After updating the internal active filter state (stored in self.active_stats_filters),
        the method calls applyFiltersToLogTableView() so that the LogTableView is reloaded using 
        the new combined filter conditions.

        Args:
            field (str): The column name that the filter applies to.
            value (object): The value associated with the filter.
            operator (str): The filtering operator. Typically:
                            - 'neq' indicates the value should be excluded (i.e. filter turned off)
                            - 'eq'  indicates the value should be included (i.e. filter turned on)
        """
        # Retrieve any existing filter conditions for this field.
        current_conditions = self.active_stats_filters.get(field, [])

        # If operator is 'neq', add the condition if it's not already in the list.
        if operator == 'neq':
            if (operator, value) not in current_conditions:
                current_conditions.append((operator, value))
        # If operator is 'eq', remove any condition for this value.
        elif operator == 'eq':
            current_conditions = [cond for cond in current_conditions if cond[1] != value]

        # Update the filter state for this field.
        if current_conditions:
            self.active_stats_filters[field] = current_conditions
        else:
            if field in self.active_stats_filters:
                del self.active_stats_filters[field]

        self.logger.info(
            f"Stats panel filter applied: {field} {operator} {value}, active filters now: {self.active_stats_filters.get(field)}"
        )
        try:
            self.applyFiltersToLogTableView()
        except Exception as e:
            self.logger.error(f"Exception when applying stats filter: {e}")
            QMessageBox.warning(self, "Filter Error", str(e))

    def applyFiltersToLogTableView(self):
        self.logger.debug("Applying active stats filters to LogTableView.")

        # Reconstruct the filters to pass to the database loader
        where_clauses = []
        params = []

        for field, conditions in self.active_stats_filters.items():
            field_clauses = []
            for op, val in conditions:
                if op == 'eq':
                    field_clauses.append(f"{field} = ?")
                    params.append(val)
                elif op == 'neq':
                    field_clauses.append(f"{field} != ?")
                    params.append(val)
                elif op == 'gt':
                    field_clauses.append(f"{field} > ?")
                    params.append(val)
                elif op == 'lt':
                    field_clauses.append(f"{field} < ?")
                    params.append(val)
            if field_clauses:
                where_clauses.append("(" + " AND ".join(field_clauses) + ")")

        # Combine all where clauses with AND
        final_where = " AND ".join(where_clauses) if where_clauses else None

        self.logger.debug(f"Applying SQL filters: {final_where} with params {params}")

        # Reload the first page with new filters
        self.loadPage(self.db_path, page=1, filters=(final_where, params))



    # -------------------------------------------------
    # TIMELINE CLICK => FILTER LOGS
    # -------------------------------------------------
    @pyqtSlot(object)
    def onTimelineJump(self, target_datetime):
        """
        Called by TimelineDock when user clicks a point in the timeline.
        We apply a ±15 minute filter around that timestamp and reload.
        """
        self.logger.info(f"Received timeline jump signal with datetime: {target_datetime}")
        QMessageBox.information(self, "Timeline Clicked", f"Clicked Time: {target_datetime}")

        target_ts = target_datetime.timestamp()
        start_ts = target_ts - 900  # 15 minutes
        end_ts = target_ts + 900

        self.logger.info(
            f"Filtering logs for time range: {datetime.fromtimestamp(start_ts)} "
            f"- {datetime.fromtimestamp(end_ts)}"
        )

        if self.db_path:
            # Reuse applyTimeFilter to set start_ts / end_ts and reload
            self.applyTimeFilter(
                start_dt=datetime.fromtimestamp(start_ts),
                end_dt=datetime.fromtimestamp(end_ts)
            )
        else:
            self.logger.warning("No database path set; cannot apply timeline filter.")

    # -------------------------------------------------
    # Overridden closeEvent
    # -------------------------------------------------
    def closeEvent(self, event):
        if self.controller and self.controller.isParsing:
            ans = QMessageBox.question(
                self, "Close", "Parsing is in progress. Do you want to cancel and close?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if ans == QMessageBox.Yes:
                self.controller.cancelParsing()
                self._removeTimestampsFromTimeline()
                event.accept()
            else:
                event.ignore()
        else:
            self._removeTimestampsFromTimeline()
            # Clear large datasets
            self.log_data = []
            self.columns = []
            self.stats_data = {}

            event.accept()
    
    def _removeTimestampsFromTimeline(self):
        main_win = self.parent()
        if not main_win:
            return
        timeline_dock = main_win.findChild(TimelineDock, "TimelineDock")
        if not timeline_dock:
            return
        source_name = f"iis_{id(self)}"
        timeline_dock.removeTimestamps(source_name)

    # -------------------------------------------------
    # LOAD DB (Open DB dialog)
    # -------------------------------------------------
    @pyqtSlot()
    def openDatabase(self):
        try:
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Open IIS Database", os.getcwd(),
                "SQLite DB Files (*.db *.sqlite);;All Files (*)"
            )
            if file_path:
                self.logger.info(f"Opening IIS Database: {file_path}")
                if self.controller and self.controller.isParsing:
                    QMessageBox.warning(
                        self, "Parsing in Progress",
                        "Please wait until parsing is finished or cancel it before opening a new database."
                    )
                    return

                self.db_path = file_path
                self.db_manager = DatabaseManager(self.db_path)
                try:
                    self.file_size_mb = self.db_manager.load_metadata("file_size")
                    self.logger.info(f"file_size_mb: {self.file_size_mb}")
                except Exception as e:
                    self.logger.error(f"Didn't write file size to db: {e}")
                    QMessageBox.critical(self, "DB Load Error", str(e))
                try:
                    if self.file_size_mb is not None:
                        self.file_size_str = f"{self.file_size_mb:.2f} MB"
                    else:
                        self.file_size_str = "Unknown Size"

                    self.db_file_name = os.path.basename(self.db_path)
                    self.setWindowTitle(f"IIS Log: | DB: {self.db_file_name} ({self.file_size_str})")
                    self.logger.debug(f"Retrieved file size: {self.file_size_mb:.2f} MB")

                    # Set data size label
                    self.data_size_label.setText(f"Data Size: {self.file_size_str}")

                    # Get total records and calculate total pages
                    self.total_records = self.db_manager.get_total_records("iis_logs", self.start_ts, self.end_ts)
                    
                    self.logger.debug(f"Total records: {self.total_records}")
                    self.total_pages = ceil(self.total_records / self.page_size) if self.page_size else 1
                    self.num_pages_label.setText(f"Total Pages: {self.total_pages}")
                    self.logger.debug(f"Total pages: {self.total_pages}")
                except Exception as e:
                    self.logger.info(f"Didn't get metada {e}")
                    QMessageBox.warning(text=f"{e}")
                stats = self.db_manager.load_field_stats("stats_iis_logs")
                if not stats:
                    self.logger.info("Stats missing. Initiate StatsLoader.")
                    self.status_label.setText("Status: Generating Statistics...")
                    self.progress_bar.setValue(0)
                    self.start_parsing_button.setEnabled(False)
                    self.open_db_button.setEnabled(False)

                    self.stats_loader = StatsLoader(self.db_path, "iis_logs")
                    self.stats_loader.signals.progress.connect(self.onStatsProgress)
                    self.stats_loader.signals.finished.connect(self.onStatsFinished)
                    self.stats_loader.signals.error.connect(self.onStatsError)
                    self.threadpool.start(self.stats_loader)
                else:
                    self.logger.info("Stats already exist. Proceeding to load specific stats.")
                    self.startDisplayStatsLoader(self.db_path)
                    self.stats_loaded = True

                self.logger.info("Started DatabaseLoader and StatsLoader if needed.")
        except Exception as e:
            self.logger.error(f"Failed to open database: {e}")
            QMessageBox.critical(
                self, "Database Error",
                f"Failed to open database:\n{e}"
            )
