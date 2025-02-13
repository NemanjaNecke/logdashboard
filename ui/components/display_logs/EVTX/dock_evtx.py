from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDockWidget, QSplitter, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QSizePolicy, QSpacerItem, QHeaderView,
    QTreeWidget, QTreeWidgetItem, QMessageBox, QProgressBar, QPushButton, QTableView, QDateTimeEdit, QLabel,
    QDialog, QDialogButtonBox, QListWidget, QListWidgetItem, QFileDialog
)
from PyQt5.QtCore import (  # pylint: disable=no-name-in-module
    Qt, pyqtSignal, pyqtSlot, QThreadPool, QModelIndex, QDateTime, QTimer
)
import logging
import os
import json
import sqlite3
from datetime import timedelta

import pandas as pd

from services.sql_workers.db_managers.EVTX.workers_evtx import EVTXInsertWorker, TimestampLoader
from services.sql_workers.db_managers.EVTX.stats_loader import FieldStatsLoader, StatsLoader
from services.sql_workers.db_managers.EVTX.db_manager_evtx import EVTXDatabaseManager

# Table model & details dialog & stats panel
from ui.components.display_logs.EVTX.log_table_view import EVTXTableModel
from ui.components.display_logs.EVTX.event_details_dialog import EventDetailsDialog
from ui.components.display_logs.EVTX.stats_panel import StatsPanel
# TimelineDock (optional)
from ui.components.timeline.dock_timeline_plotly import TimelineDock


###############################################################################
# ColumnSelectionDialog: Allow the user to select columns.
###############################################################################
class ColumnSelectionDialog(QDialog):
    def __init__(self, all_columns, selected_columns=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Columns")
        self.resize(300, 400)
        self.all_columns = all_columns
        # Use the full list as default available choices if none provided.
        if selected_columns is None:
            self.selected_columns = all_columns[:]
        else:
            self.selected_columns = selected_columns[:]

        layout = QVBoxLayout(self)

        self.list_widget = QListWidget(self)
        # For each column add an item with a checkbox.
        for col in self.all_columns:
            item = QListWidgetItem(col)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            if col in self.selected_columns:
                item.setCheckState(Qt.Checked)
            else:
                item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)
        layout.addWidget(self.list_widget)

        # OK and Cancel buttons.
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def getSelectedColumns(self):
        selected = []
        for index in range(self.list_widget.count()):
            item = self.list_widget.item(index)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected


###############################################################################
# EVTXDock
###############################################################################
class EVTXDock(QDockWidget):
    """
    A dockable window for EVTX logs that shows the stats panel above the table.
    The layout is arranged such that the stats panel takes about 1/3 of the vertical space
    (fixed height) and the table view takes the rest. In the table view, the last column is set to stretch.
    """
    def __init__(self, file_path="", db_path="", parent=None):
        # Set the title based on available inputs.
        if db_path:
            title = f"EVTX Database: {os.path.basename(db_path)}"
        elif file_path:
            title = f"EVTX Log: {os.path.basename(file_path)}"
        else:
            title = "EVTX Log"
        super().__init__(title, parent)
        self.setObjectName("EVTXDock")
        self.setAllowedAreas(Qt.AllDockWidgetAreas)
        self.data_rows = []
        self.logger = logging.getLogger("EVTXDock")
        self.logger.setLevel(logging.DEBUG)

        # Create the main container and a vertical layout.
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)
        main_layout.setSpacing(10)

        # 1. Statistics Panel (Top)
        stats_container = QWidget()
        stats_layout = QVBoxLayout(stats_container)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        stats_layout.setSpacing(5)
        self.stats_panel = QTreeWidget()
        self.stats_helper = StatsPanel(self.stats_panel, chunk_size=50)
        self.stats_panel.setHeaderLabels(["Field / Value", "Count"])
        self.stats_panel.setColumnCount(2)
        self.stats_panel.setColumnWidth(0, 150)
        stats_container.setFixedHeight(150)
        stats_layout.addWidget(self.stats_panel)
        main_layout.addWidget(stats_container)

        # 2. Table View (Middle/Bottom)
        self.table_view = QTableView()
        self.table_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addWidget(self.table_view, stretch=1)

        # 3. Options Panel (Bottom)
        options_container = QWidget()
        options_layout = QHBoxLayout(options_container)
        options_layout.setContentsMargins(0, 0, 0, 0)
        options_layout.setSpacing(10)

        self.row_count_label = QLabel("Showing 0 rows")
        options_layout.addWidget(self.row_count_label)

        self.search_label = QLabel("Search:")
        self.search_lineedit = QLineEdit(self)
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.onSearchClicked)
        options_layout.addWidget(self.search_label)
        options_layout.addWidget(self.search_lineedit)
        options_layout.addWidget(self.search_button)

        self.start_time_label = QLabel("Start Time:")
        self.start_time_edit = QDateTimeEdit(self)
        self.start_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.start_time_edit.setCalendarPopup(True)

        self.end_time_label = QLabel("End Time:")
        self.end_time_edit = QDateTimeEdit(self)
        self.end_time_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.end_time_edit.setCalendarPopup(True)

        self.apply_time_filter_button = QPushButton("Apply Time Filter")
        self.apply_time_filter_button.clicked.connect(self.applyTimeFilter)
        options_layout.addWidget(self.start_time_label)
        options_layout.addWidget(self.start_time_edit)
        options_layout.addWidget(self.end_time_label)
        options_layout.addWidget(self.end_time_edit)
        options_layout.addWidget(self.apply_time_filter_button)

        self.clear_btn = QPushButton("Clear Filters")
        self.clear_btn.clicked.connect(self.clearFilters)
        options_layout.addWidget(self.clear_btn)

        # Export button.
        self.export_btn = QPushButton("Export")
        self.export_btn.clicked.connect(self.exportToExcel)
        options_layout.addWidget(self.export_btn)

        # New: Select Columns button (always uses full set of columns)
        # We store the full set in self.all_columns so that it never shrinks.
        self.all_columns = [
            "EventID", "Level", "Channel", "Computer", "ProviderName",
            "RecordNumber", "timestamp", "EventData"
        ]
        # Initially, display all columns.
        self.columns = self.all_columns[:]  
        self.select_columns_btn = QPushButton("Select Columns")
        self.select_columns_btn.clicked.connect(self.selectColumns)
        options_layout.addWidget(self.select_columns_btn)

        options_layout.addItem(QSpacerItem(20, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))

        self.load_more_btn = QPushButton("Load More")
        self.load_more_btn.clicked.connect(self.loadNextPage)
        options_layout.addWidget(self.load_more_btn)

        main_layout.addWidget(options_container)

        # 4. Shared Progress Bar & Cancel Button (Bottom)
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancelParsing)
        self.cancel_button.setVisible(False)
        main_layout.addWidget(self.cancel_button, alignment=Qt.AlignRight)

        self.setWidget(main_widget)

        # Data and DB initialization
        self.db_manager = None
        self.file_path = file_path or ""
        self.db_path = db_path or ""
        self.filters = {}

        if self.db_path:
            self.loadEVTXDatabase(self.db_path)
        elif self.file_path:
            self.loadEVTXLog(self.file_path)
        else:
            self.logger.warning("EVTXDock: no file_path or db_path provided.")

        # Connect signals
        self.stats_panel.itemClicked.connect(self.onStatsItemClicked)
        self.table_view.clicked.connect(self.onTableCellClicked)

    # ----------------------------
    # Column Selection for Table View
    # ----------------------------
    @pyqtSlot()
    def selectColumns(self):
        """
        Opens a column selection dialog that allows the user to choose which columns
        to display in the table view. The dialog always shows the full list of columns.
        """
        dlg = ColumnSelectionDialog(self.all_columns, self.columns, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            selected = dlg.getSelectedColumns()
            if selected:
                self.logger.info(f"Table columns updated to: {selected}")
                self.columns = selected  # Update the current display columns
                self.populateTable()
            else:
                QMessageBox.warning(self, "No Columns Selected", "At least one column must be selected.")

    # ----------------------------
    # Export and other methods below remain similar.
    # ----------------------------
    def loadNextPage(self):
        """Exposed method to trigger pagination"""
        self.table_model.loadNextPage()

    def loadEVTXLog(self, filepath):
        self.logger.info(f"Opening EVTX Log for parsing: {filepath}")
        self.db_path = os.path.join(os.getcwd(), "db", f"evtx_logs_{os.path.basename(filepath)}.db")
        self.logger.debug(f"Will store data in DB: {self.db_path}")
        self.db_manager = EVTXDatabaseManager(self.db_path)
        self.insert_worker = EVTXInsertWorker(filepath, self.db_path)
        self.insert_worker.signals.progress.connect(self.onInsertProgress)
        self.insert_worker.signals.finished.connect(self.onInsertFinished)
        self.insert_worker.signals.error.connect(self.onInsertError)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self.cancel_button.setVisible(True)
        QThreadPool.globalInstance().start(self.insert_worker)

    @pyqtSlot(int)
    def onInsertProgress(self, progress):
        self.logger.debug(f"Insertion progress: {progress}%")
        self.progress_bar.setValue(progress)

    @pyqtSlot(str)
    def onInsertFinished(self, message):
        self.logger.info(message)
        self.progress_bar.setValue(100)
        self.progress_bar.setVisible(False)
        self.cancel_button.setVisible(False)
        self.loadTimestampsAsync()
        self.buildFieldStatsAsync(recalc=True)
        self.populateTable()

    @pyqtSlot(str)
    def onInsertError(self, error_msg):
        self.logger.error(f"Error inserting EVTX logs: {error_msg}")
        self.progress_bar.setVisible(False)
        self.cancel_button.setVisible(False)
        QMessageBox.critical(self, "EVTX Insertion Error", error_msg)

    def cancelParsing(self):
        if hasattr(self, "insert_worker") and self.insert_worker:
            self.insert_worker.set_interrupted()
            self.logger.info("Parsing canceled by user.")
            self.progress_bar.setVisible(False)
            self.cancel_button.setVisible(False)
            QMessageBox.information(self, "Parsing Canceled", "EVTX parsing was canceled.")

    def loadEVTXDatabase(self, db_path):
        self.logger.info(f"Loading existing EVTX Database: {db_path}")
        if not os.path.exists(db_path):
            self.logger.error("Database file does not exist!")
            QMessageBox.critical(self, "Database Not Found", f"No DB file:\n{db_path}")
            return
        self.db_manager = EVTXDatabaseManager(db_path)
        self.loadTimestampsAsync()
        self.buildFieldStatsAsync(recalc=False)
        self.populateTable()

    def loadTimestampsAsync(self):
        if not self.db_manager:
            return
        loader = TimestampLoader(self.db_manager)
        loader.signals.finished.connect(self.onTimestampsLoaded)
        QThreadPool.globalInstance().start(loader)

    @pyqtSlot(object)
    def onTimestampsLoaded(self, timestamps):
        self.logger.debug(f"Loaded {len(timestamps)} timestamps.")
        timeline_dock = self.findTimelineDock()
        if timeline_dock:
            source_name = f"evtx_{id(self)}"
            timeline_dock.addTimestamps(source_name, timestamps)
            self.logger.debug(f"TimelineDock updated with {len(timestamps)} timestamps.")
        else:
            self.logger.warning("No TimelineDock found, skipping timestamps add.")

    def findTimelineDock(self):
        mw = self.parent()
        if not mw:
            return None
        dock = mw.findChild(TimelineDock, "TimelineDock")
        return dock

    def onTimelineJump(self, target_datetime):
        try:
            start_time_py = target_datetime - timedelta(hours=0.15)
            end_time_py = target_datetime + timedelta(hours=0.15)
            start_qdt = QDateTime(
                start_time_py.year, start_time_py.month, start_time_py.day,
                start_time_py.hour, start_time_py.minute, start_time_py.second
            )
            end_qdt = QDateTime(
                end_time_py.year, end_time_py.month, end_time_py.day,
                end_time_py.hour, end_time_py.minute, end_time_py.second
            )
            self.start_time_edit.setDateTime(start_qdt)
            self.end_time_edit.setDateTime(end_qdt)
            self.applyTimeFilter()
        except Exception as e:
            self.logger.error(f"Timeline jump error: {e}")
            QMessageBox.critical(self, "Timeline Error", str(e))

    def getTotalRowCount(self) -> int:
        try:
            conn = sqlite3.connect(self.db_manager.db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM evtx_logs")
            row = cur.fetchone()
            conn.close()
            if row and row[0]:
                return int(row[0])
        except Exception as e:
            self.logger.error(f"Error getting total row count: {e}")
        return 0

    @pyqtSlot(int)
    def onTableDataLoaded(self, row_count: int):
        total = self.getTotalRowCount()
        self.row_count_label.setText(f"Showing {row_count} / {total} rows")

    def populateTable(self):
        try:
            # Create a new table model with the current selected columns.
            self.table_model = EVTXTableModel(self.db_manager.db_path, self.columns)
            self.table_view.setModel(self.table_model)
            self.table_model.dataLoadedSignal.connect(self.onTableDataLoaded)
            header = self.table_view.horizontalHeader()
            last_index = header.count() - 1
            header.setSectionResizeMode(last_index, QHeaderView.Stretch)
        except Exception as e:
            self.logger.error(f"Error populating table: {e}")
            QMessageBox.critical(self, "Table Error", str(e))

    def buildFieldStatsAsync(self, recalc=False):
        if not self.db_manager:
            return
        self.logger.info("Starting async field stats loading.")
        stats_columns = [
            "EventID", "Level", "Channel", "Computer", "ProviderName",
            "timestamp", "EventData_display"
        ]
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        if recalc:
            loader = StatsLoader(self.db_manager, self)
            QThreadPool.globalInstance().start(loader)
        else:
            self.field_stats_loader = FieldStatsLoader(self.db_manager, stats_columns)
            self.field_stats_loader.signals.progress.connect(self.onStatsProgress)
            self.field_stats_loader.signals.finished.connect(self.onStatsFinished)
            self.field_stats_loader.signals.error.connect(self.onStatsError)
            QThreadPool.globalInstance().start(self.field_stats_loader)

    @pyqtSlot(int)
    def onStatsProgress(self, percent):
        self.logger.debug(f"Stats progress: {percent}%")
        self.progress_bar.setValue(percent)

    @pyqtSlot(object)
    def onStatsFinished(self, field_value_counts):
        self.logger.info("Stats loaded from DB (async).")
        self.progress_bar.hide()
        self.stats_helper.setStats(field_value_counts)

    @pyqtSlot(str)
    def onStatsError(self, error_msg):
        self.logger.error(f"Stats error: {error_msg}")
        self.progress_bar.setVisible(False)
        QMessageBox.critical(self, "Stats Error", error_msg)

    @pyqtSlot(object)
    def onStatsLoaded(self, field_value_counts):
        self.logger.info("Recalculated stats loaded (via StatsLoader).")
        self.progress_bar.hide()
        self.stats_helper.setStats(field_value_counts)

    def applyTimeFilter(self, start_time=None, end_time=None):
        try:
            if not start_time:
                start_time = self.start_time_edit.dateTime().toPyDateTime()
            if not end_time:
                end_time = self.end_time_edit.dateTime().toPyDateTime()
            start_ts = start_time.timestamp()
            end_ts = end_time.timestamp()
            query = """
                SELECT EventID, Level, Channel, Computer, ProviderName, RecordNumber,
                       timestamp, EventData_display AS EventData
                FROM evtx_logs
                WHERE timestamp_epoch BETWEEN ? AND ?
            """
            params = (start_ts, end_ts)
            self.table_model.loadDataAsync(query, params)
            self.logger.info(f"Time filter applied: {start_time} - {end_time}")
        except Exception as e:
            self.logger.error(f"Time filter error: {e}")
            QMessageBox.critical(self, "Filter Error", str(e))
            
    def clearFilters(self):
        self.search_lineedit.clear()
        self.start_time_edit.clear()
        self.end_time_edit.clear()
        default_query = """
            SELECT EventID, Level, Channel, Computer, ProviderName, RecordNumber,
                   timestamp, EventData_display AS EventData
            FROM evtx_logs
        """
        self.table_model.loadDataAsync(default_query)
        self.logger.info("Cleared stats filter; loaded all rows.")
        
    def onSearchClicked(self):
        search_text = self.search_lineedit.text().strip()
        if not search_text:
            self.logger.info("Search is empty. Reloading all data...")
            query = f"SELECT {', '.join(self.columns)} FROM evtx_logs"
            self.table_model.loadDataAsync(query)
            return
        self.logger.info(f"Performing search for: {search_text}")
        searchable_cols = [
            "EventID", "Level", "Channel", "Computer", 
            "ProviderName", "RecordNumber", "timestamp", "EventData_display"
        ]
        or_clauses = []
        for col in searchable_cols:
            or_clauses.append(f"{col} LIKE ?")
        where_clause = " OR ".join(or_clauses)
        query = f"""
            SELECT EventID, Level, Channel, Computer, ProviderName, 
                   RecordNumber, timestamp, EventData_display AS EventData
            FROM evtx_logs
            WHERE {where_clause}
        """
        like_str = f"%{search_text}%"
        params = tuple(like_str for _ in searchable_cols)
        self.table_model.loadDataAsync(query, params)

    @pyqtSlot()
    def exportToExcel(self):
        save_path, _ = QFileDialog.getSaveFileName(self, "Save Excel File", os.getcwd(), "Excel Files (*.xlsx);;All Files (*)")
        if not save_path:
            return
        # For export, use the full list of available columns.
        dlg = ColumnSelectionDialog(self.all_columns, self.all_columns, parent=self)
        if dlg.exec_() != QDialog.Accepted:
            return
        selected_columns = dlg.getSelectedColumns()
        if not selected_columns:
            QMessageBox.warning(self, "No Columns Selected", "Please select at least one column for export.")
            return
        data_rows = self.table_model.data_rows
        if not data_rows:
            QMessageBox.information(self, "No Data", "There is no data to export.")
            return
        export_data = []
        # Get indices from self.all_columns so that export order always comes from the full list.
        col_indices = [self.all_columns.index(col) for col in selected_columns if col in self.all_columns]
        for row in data_rows:
            row_dict = {}
            for idx in col_indices:
                col_name = self.all_columns[idx]
                row_dict[col_name] = row[idx]
            export_data.append(row_dict)
        try:
            df = pd.DataFrame(export_data)
            df.to_excel(save_path, index=False)
            QMessageBox.information(self, "Export Successful", f"Data exported to {save_path}")
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{e}")

    @pyqtSlot(QTreeWidgetItem, int)
    def onStatsItemClicked(self, item, column):
        data = item.data(0, Qt.UserRole)
        if data:
            field, val = data
            self.logger.info(f"Filtering table: {field} = {val}")
            self.applyFilter(field, val)
        else:
            self.logger.info("Clearing stats-based filter.")
            self.clearFilter()

    def applyFilter(self, field, value):
        try:
            query = f"""
                SELECT EventID, Level, Channel, Computer, ProviderName, RecordNumber,
                       timestamp, EventData_display AS EventData
                FROM evtx_logs
                WHERE {field} = ?
            """
            self.table_model.loadDataAsync(query, (value,))
        except Exception as e:
            self.logger.error(f"Filter error: {e}")
            QMessageBox.critical(self, "Filter Error", str(e))

    def clearFilter(self):
        try:
            query = """SELECT EventID, Level, Channel, Computer, ProviderName, RecordNumber,
                              timestamp, EventData_display AS EventData
                       FROM evtx_logs"""
            self.table_model.loadDataAsync(query)
            self.logger.info("Cleared stats filter; loaded all rows.")
        except Exception as e:
            self.logger.error(f"Error clearing filter: {e}")
            QMessageBox.critical(self, "Filter Error", str(e))

    @pyqtSlot(QModelIndex)
    def onTableCellClicked(self, index):
        if not index.isValid():
            return
        row = index.row()
        event_dict = {}
        for col_idx, col_name in enumerate(self.columns):
            idx = self.table_model.index(row, col_idx)
            val = self.table_model.data(idx, Qt.DisplayRole)
            event_dict[col_name] = str(val)
        self.showEventDetails(event_dict)

    def showEventDetails(self, event):
        data_raw = event.get("EventData", "")
        try:
            parsed = json.loads(data_raw)
            event["EventData"] = json.dumps(parsed, indent=4)
        except (json.JSONDecodeError, TypeError):
            pass
        dlg = EventDetailsDialog(event, self)
        dlg.exec_()
