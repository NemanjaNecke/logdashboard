# ui/main_window.py

import logging
import os
import shutil
import tempfile
import qdarkstyle
import pandas as pd
from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QMainWindow, QAction, QFileDialog, QMessageBox, QApplication, QDockWidget, QProgressDialog, QWidget
)
from PyQt5.QtGui import QIcon  # pylint: disable=no-name-in-module
from PyQt5.QtCore import Qt, pyqtSlot, QThreadPool, QDateTime   # pylint: disable=no-name-in-module
# Import your other docks
from ui.components.timeline.dock_timeline_plotly import TimelineDock
from ui.components.search.dock_search import AdvancedSearchDock
from ui.components.display_logs.IIS.dock_iis import IISDock
from ui.components.display_logs.EVTX.dock_evtx import EVTXDock
from ui.components.display_logs.GENERIC.dock_generic import GenericDock
from services.logging.dock_log import LogDock
from ui.components.db_load.dock_db_manager import DBManagerDock
from ui.components.charts.dock_3d import ThreeDDock
from services.analyze.IIS.iis_analyze_worker import AnalyzerWorker
from ui.components.display_logs.IIS.stats.analyze_dialog import AnalysisDialog  # Import AnalysisDialog
from ui.components.display_logs.IIS.stats.all_stats_panel import AllStatsPanel  # Import AllStatsPanel
from ui.components.display_logs.IIS.table.sheet_selection_dialog import SheetSelectionDialog  # Import SheetSelectionDialog
from ui.components.display_logs.IIS.failed_request.failed_request_trace_dialog import FrebViewerDock

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        # Window Title and Size
        self.setWindowTitle("Log Dashboard")
        self.resize(1600, 1900)

        # Initialize centralized logger
        self.logger = logging.getLogger('MainWindow')
        self.logger.setLevel(logging.DEBUG)

        # Set docking behavior to allow nesting, tabbing, etc.
        self.setDockOptions(
            QMainWindow.AllowNestedDocks |
            QMainWindow.AllowTabbedDocks |
            QMainWindow.AnimatedDocks
        )

        # Apply QDarkStyle for a modern dark theme
        QApplication.instance().setStyleSheet(qdarkstyle.load_stylesheet_pyqt5()) # type: ignore

        # Thread pool
        self.threadpool = QThreadPool.globalInstance()
        self.logger.debug(f"ThreadPool initialized with max {self.threadpool.maxThreadCount()} threads.")

        # Create / add your docks
        self.create_docks()
        self.createMenuBar()

        # Worker references
        self.worker = None
        self.progress_dialog = None

    def create_docks(self):
        """
        Create the Timeline in the top area (dashboard feel).
        Logs (IIS/EVTX) will be added on demand via openIISDock/openEVTXDock in bottom area.
        """
        # Timeline at the TOP
        self.timeline_dock = TimelineDock(self)
        self.timeline_dock.setObjectName("TimelineDock")
        self.make_dock_fully_floatable(self.timeline_dock)
        self.addDockWidget(Qt.TopDockWidgetArea, self.timeline_dock)
        self.timeline_dock.visibilityChanged.connect(self.onTimelineDockVisibilityChanged)
        self.timeline_dock.jumpToTimeSignal.connect(self.onJumpToTime)

        # Optionally create other docks (log_dock, db_manager_dock) and hide by default:
        self.log_dock = LogDock(self)
        self.log_dock.setObjectName("LogDock")
        self.make_dock_fully_floatable(self.log_dock)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
        self.log_dock.hide()

        self.db_manager_dock = DBManagerDock(self)
        self.db_manager_dock.setObjectName("DBManagerDock")
        self.make_dock_fully_floatable(self.db_manager_dock)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.db_manager_dock)
        self.db_manager_dock.hide()
        

        # If you wanted to *always* create IIS/EVTX ahead of time, do it here.
        # But typically you'll create them only when a user opens a file.
        # For reference in openIISDock/openEVTXDock:
        self.iis_dock = None
        self.evtx_dock = None
        self.search_dock = None
        
    def make_dock_fully_floatable(self, dock_widget: QDockWidget):
        """
        Helper to ensure the dock can float, move, close, and be placed in any area.
        """
        dock_widget.setFeatures(
            QDockWidget.DockWidgetMovable |
            QDockWidget.DockWidgetFloatable |
            QDockWidget.DockWidgetClosable
        )
        dock_widget.setAllowedAreas(Qt.AllDockWidgetAreas)

    def createMenuBar(self):
        menubar = self.menuBar()

        ####################
        # File Menu
        ####################
        file_menu = menubar.addMenu("File")

        # Open Database Action
        open_db_action = QAction(QIcon.fromTheme("folder"), "Open Database...", self)
        open_db_action.triggered.connect(self.openDatabaseDialog)
        file_menu.addAction(open_db_action)

        # Open Log File Action
        open_log_action = QAction(QIcon.fromTheme("document-open"), "Open Log File...", self)
        open_log_action.triggered.connect(self.openLogFileDialog)
        file_menu.addAction(open_log_action)

        # Exit Action
        exit_action = QAction(QIcon.fromTheme("application-exit"), "Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        ####################
        # Search Menu
        ####################
        search_menu = menubar.addMenu("Search")

        # Advanced Search Action
        advanced_search_action = QAction(QIcon.fromTheme("edit-find"), "Advanced Search...", self)
        advanced_search_action.triggered.connect(self.openAdvancedSearch)
        search_menu.addAction(advanced_search_action)

        ####################
        # View Menu
        ####################
        # In the View menu, create a toggle action and store it:
        view_menu = menubar.addMenu("View")
        self.toggle_timeline_action = QAction(
            QIcon.fromTheme("view-refresh"), "Toggle Timeline Dock", self, 
            checkable=True, checked=True
        ) # type: ignore
        self.toggle_timeline_action.triggered.connect(
            lambda checked: self.toggleDock(self.timeline_dock, checked)
        )
        view_menu.addAction(self.toggle_timeline_action)

        ####################
        # IIS Menu
        ####################
        iis_menu = menubar.addMenu("IIS")

        # Open IIS Log Action
        open_iis_log_action = QAction(QIcon.fromTheme("folder"), "Open IIS Log...", self)
        open_iis_log_action.triggered.connect(self.openIISLog)
        iis_menu.addAction(open_iis_log_action)

        # Open Stored Log (Database) Action under IIS
        open_stored_log_action = QAction(QIcon.fromTheme("document-open"), "Open Stored Log...", self)
        open_stored_log_action.triggered.connect(self.openDatabaseDialog)
        iis_menu.addAction(open_stored_log_action)

        # Analyze IIS Logs Action
        analyze_iis_action = QAction(QIcon.fromTheme("view-statistics"), "Analyze IIS Logs...", self)
        analyze_iis_action.triggered.connect(self.openAnalysisDialog)
        iis_menu.addAction(analyze_iis_action)
        
        # NEW: Compare Reports Action under IIS
        compare_reports_action = QAction(QIcon.fromTheme("view-compare"), "Compare Reports...", self)
        compare_reports_action.triggered.connect(self.openCompareReports)
        iis_menu.addAction(compare_reports_action)
        
        freb_trace_action = QAction("View FREB Trace...", self)
        freb_trace_action.triggered.connect(self.openFrebTrace)
        iis_menu.addAction(freb_trace_action)

        ####################
        # EVTX Menu
        ####################
        evtx_menu = menubar.addMenu("EVTX")

        # Open EVTX Log Action
        open_evtx_log_action = QAction(QIcon.fromTheme("folder"), "Open EVTX Log...", self)
        open_evtx_log_action.triggered.connect(self.openEVTXLog)
        evtx_menu.addAction(open_evtx_log_action)

        # Open Stored EVTX Database Action under EVTX
        open_stored_evtx_action = QAction(QIcon.fromTheme("document-open"), "Open Stored EVTX Log...", self)
        open_stored_evtx_action.triggered.connect(self.openEVTXDatabaseDialog)
        evtx_menu.addAction(open_stored_evtx_action)

        ####################
        # GENERIC Menu
        ####################
        generic_menu = menubar.addMenu("GENERIC")

        # Open Generic Log Action
        open_generic_log_action = QAction(QIcon.fromTheme("folder"), "Open Generic Log...", self)
        open_generic_log_action.triggered.connect(self.openGenericLog)
        generic_menu.addAction(open_generic_log_action)

        # Open Stored Generic Database Action under GENERIC
        open_stored_generic_action = QAction(QIcon.fromTheme("document-open"), "Open Stored Generic Log...", self)
        open_stored_generic_action.triggered.connect(self.openGenericDatabaseDialog)  # This method will be defined below
        generic_menu.addAction(open_stored_generic_action)

        ####################
        # Help Menu
        ####################
        help_menu = menubar.addMenu("Help")

        # Show Log Dock Action
        show_log_dock_action = QAction(QIcon.fromTheme("view-log"), "Show Log Dock", self)
        show_log_dock_action.triggered.connect(lambda: self.toggleDock(self.log_dock, True))
        help_menu.addAction(show_log_dock_action)

        # Show DB Manager Action
        show_db_manager_action = QAction(QIcon.fromTheme("utilities-system-monitor"), "Show DB Manager", self)
        show_db_manager_action.triggered.connect(lambda: self.toggleDock(self.db_manager_dock, True))
        help_menu.addAction(show_db_manager_action)

    ####################
    # NEW: Compare Reports Method
    ####################
    @pyqtSlot()
    def openCompareReports(self):
        """
        Opens the AllStatsPanel in comparison mode without loading statistics.
        """
        self.logger.info("Opening AllStatsPanel for Report Comparison.")
        dialog = AllStatsPanel(stats_dict=None, parent=self)  # Pass None to skip loading statistics
        dialog.exec_()
        
    @pyqtSlot()
    def openAnalysisDialog(self):
        """
        Opens the AnalysisDialog for selecting analysis mode and files.
        """
        dialog = AnalysisDialog(self)
        dialog.analysis_selected.connect(self.start_analysis)
        dialog.exec_()

    @pyqtSlot(list, str, dict)
    def start_analysis(self, file_paths, mode, analysis_params):
        """
        Starts the IIS log analysis using AnalyzerWorker.

        Args:
            file_paths (list of str): Selected log file paths.
            mode (str): Analysis mode - 'single', 'cluster', or 'multiple'.
        """
        self.logger.info(f"Starting analysis with mode '{mode}' and files: {file_paths}")
        
        # Create a temporary directory to store the initial Excel report
        temp_dir = tempfile.mkdtemp()
        temp_excel_path = os.path.join(temp_dir, "temp_analysis.xlsx")

        # Initialize and start the worker
        # Pass analysis_params to the AnalyzerWorker so it can pass them to iis_analyze
        self.worker = AnalyzerWorker(file_paths, mode, temp_excel_path, temp_dir, analysis_params=analysis_params)
        self.worker.signals.finished.connect(lambda path, temp_path, temp_d: self.onAnalysisFinished(path, temp_path, temp_d))
        self.worker.signals.error.connect(self.onAnalysisError)
        self.worker.signals.progress.connect(self.update_progress)  # Connect progress signal
        self.threadpool.start(self.worker)

        # Show progress dialog
        self.progress_dialog = QProgressDialog("Analyzing logs...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowTitle("Analysis in Progress")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.canceled.connect(self.cancel_analysis)  # Connect to cancellation handler
        self.progress_dialog.show()

        self.logger.info(f"Started analysis with mode '{mode}'. Temporary Output: '{temp_excel_path}'")

    @pyqtSlot(str, str, str)
    def onAnalysisFinished(self, excel_path, temp_excel_path, temp_dir):
        """
        Called when the analysis is finished successfully.

        Args:
            excel_path (str): Path to the generated Excel report.
            temp_excel_path (str): Path to the temporary Excel report.
            temp_dir (str): Path to the temporary directory.
        """
        self.logger.info(f"IIS log analysis completed. Output file: {excel_path}")
        self.progress_dialog.close() # type: ignore

        # Load the Excel file to list available sheets using a context manager
        try:
            with pd.ExcelFile(excel_path) as excel_file:
                sheet_names = excel_file.sheet_names
                self.logger.debug(f"Available sheets: {sheet_names}")
        except Exception as e:
            self.logger.error(f"Failed to read Excel report: {e}")
            QMessageBox.critical(self, "Error", f"Failed to read Excel report:\n{e}")
            self.worker = None  # Release the worker reference
            return

        if not sheet_names:
            QMessageBox.warning(self, "No Sheets Found", "The analysis report contains no sheets.")
            self.worker = None  # Release the worker reference
            return

        # Show sheet selection dialog
        sheet_selection_dialog = SheetSelectionDialog(sheet_names, self)
        if sheet_selection_dialog.exec_():
            selected_sheets = sheet_selection_dialog.get_selected_sheets()
            self.logger.debug(f"Selected sheets: {selected_sheets}")

            if not selected_sheets:
                QMessageBox.warning(self, "No Sheets Selected", "No sheets were selected for saving.")
                self.worker = None  # Release the worker reference
                return

            # Prompt user to choose final save path
            options = QFileDialog.Options()
            options |= QFileDialog.DontUseNativeDialog
            final_save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Customized Analysis Report",
                os.path.join(os.getcwd(), "customized_analysis.xlsx"),
                "Excel Files (*.xlsx);;All Files (*)",
                options=options
            )
            if not final_save_path:
                self.logger.warning("User canceled the final save file dialog.")
                self.worker = None  # Release the worker reference
                return

            # Write selected sheets to the final Excel file
            try:
                with pd.ExcelWriter(final_save_path, engine='xlsxwriter') as writer:
                    for sheet in selected_sheets:
                        df = pd.read_excel(excel_path, sheet_name=sheet)
                        df.to_excel(writer, sheet_name=sheet, index=False)
                self.logger.info(f"Customized analysis report saved to '{final_save_path}'")
                QMessageBox.information(self, "Analysis Complete", f"Customized analysis report has been saved to:\n{final_save_path}")
            except Exception as e:
                self.logger.error(f"Failed to save customized report: {e}")
                QMessageBox.critical(self, "Error", f"Failed to save customized report:\n{e}")
        else:
            self.logger.info("Sheet selection dialog was canceled.")

        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
            self.logger.debug(f"Temporary directory '{temp_dir}' has been removed.")
        except Exception as e:
            self.logger.warning(f"Failed to remove temporary directory '{temp_dir}': {e}")
            # Inform the user and provide an option to delete manually
            reply = QMessageBox.question(
                self,
                "Temporary Files Not Deleted",
                f"Failed to remove temporary directory:\n{temp_dir}\nWould you like to delete it manually?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                try:
                    shutil.rmtree(temp_dir)
                    self.logger.info(f"Temporary directory '{temp_dir}' deleted by the user.")
                except Exception as ex:
                    self.logger.error(f"User failed to delete temporary directory '{temp_dir}': {ex}")
                    QMessageBox.critical(self, "Deletion Failed", f"Failed to delete temporary directory:\n{temp_dir}\nPlease delete it manually.")
            else:
                self.logger.info(f"User chose not to delete temporary directory '{temp_dir}'.")
                QMessageBox.information(self, "Temporary Files Remaining", f"Temporary directory remains at:\n{temp_dir}")

        # Release the worker reference
        self.worker = None
    @pyqtSlot(bool)
    def onTimelineDockVisibilityChanged(self, visible):
        # Update the toggle action checked state based on the dock’s visibility.
        if hasattr(self, 'toggle_timeline_action'):
            self.toggle_timeline_action.setChecked(visible)
            
    @pyqtSlot(str)
    def onAnalysisError(self, error_msg):
        """
        Called when an error occurs during analysis.
        """
        self.logger.error(f"IIS log analysis error: {error_msg}")
        self.progress_dialog.close() # type: ignore
        QMessageBox.critical(self, "Analysis Error", f"An error occurred during analysis:\n{error_msg}")
        self.worker = None  # Release the worker reference

    @pyqtSlot(str)
    def update_progress(self, message):
        """
        Updates the progress dialog with a new message.

        Args:
            message (str): The progress message to display.
        """
        if self.progress_dialog:
            self.progress_dialog.setLabelText(message)

    def cancel_analysis(self):
        """
        Handles the cancellation of the analysis.
        """
        if self.worker is not None:
            self.worker.set_interrupted()  # Signal the worker to stop
            self.logger.info("Analysis canceled by the user.")
            self.progress_dialog.close() # type: ignore
            self.worker = None

    def openLogFileDialog(self):
        """
        Opens a file dialog to select a log file for parsing.
        """
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Open Log File", os.getcwd(), "Log Files (*.log *.txt);;All Files (*)")
            if file_path:
                self.openIISDock(file_path)
        except Exception as e:
            self.logger.error(f"Failed to open log file: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open log file:\n{e}")

    def openDatabaseDialog(self):
        """
        Opens a file dialog to select a SQLite database to display.
        """
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Open Stored Database", os.getcwd(), "SQLite DB Files (*.db *.sqlite);;All Files (*)")
            if file_path:
                self.openIISDockWithDatabase(file_path)
        except Exception as e:
            self.logger.error(f"Failed to open database: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open database:\n{e}")

    def openEVTXDatabaseDialog(self):
        """
        Opens a file dialog to select a stored EVTX database to display.
        """
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Open Stored EVTX Database", os.getcwd(), "SQLite DB Files (*.db *.sqlite);;All Files (*)")
            if file_path:
                self.openEVTXDockWithDatabase(file_path)
        except Exception as e:
            self.logger.error(f"Failed to open EVTX database: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open EVTX database:\n{e}")
                
    def openEVTXDockWithDatabase(self, db_path):
        """
        Initializes the EVTXDock with the provided database for display.
        """
        self.logger.info(f"Opening EVTX Database for Display: {db_path}")
        dock = EVTXDock(db_path=db_path, parent=self)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)
        
    def openIISLog(self):
        """
        Opens one or more IIS log files and displays them in the IISDock.
        """
        try:
            # Use getOpenFileNames to allow multi‑file selection
            file_paths, _ = QFileDialog.getOpenFileNames(
                self,
                "Open IIS Log(s)",
                os.getcwd(),
                "IIS Logs (*.log *.txt);;All Files (*)"
            )
            if file_paths:
                self.openIISDock(file_paths)
        except Exception as e:
            self.logger.error(f"Failed to open IIS log(s): {e}")
            QMessageBox.critical(self, "Error", f"Failed to open IIS log(s):\n{e}")


    def openIISDock(self, file_paths):
        """
        Opens one or more IIS log(s) in a new IISDock and places it in the bottom area.
        If multiple files are selected, they will be parsed as one combined log.
        """
        # Determine the title based on the number of files
        if isinstance(file_paths, list) and len(file_paths) > 1:
            title = "IIS Logs: Combined"
        else:
            # if only one file or if file_paths is a single string
            single_file = file_paths[0] if isinstance(file_paths, list) else file_paths
            title = f"IIS Log: {os.path.basename(single_file)}"

        self.logger.info(f"Opening {title} for parsing.")
        
        # Pass the file_paths (list or single string) to IISDock.
        dock = IISDock(file_path=file_paths, parent=self)
        self.make_dock_fully_floatable(dock)
        dock.setWindowTitle(title)
        dock.setObjectName("IISDock-Dynamic")
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)
        self.iis_dock = dock

    def openIISDockWithDatabase(self, db_path):
        """
        Initializes the IISDock with the provided database for display.

        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.logger.info(f"Opening IIS Database for Display: {db_path}")
        dock = IISDock(db_path=db_path, parent=self)  # Use keyword argument for db_path
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def openEVTXLog(self):
        """
        Opens an EVTX log file and displays it in the EVTXDock.
        """
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Open EVTX Log", os.getcwd(), "EVTX Files (*.evtx *.xml);;All Files (*)")
            if file_path:
                self.openEVTXDock(file_path)
        except Exception as e:
            self.logger.error(f"Failed to open EVTX log: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open EVTX log:\n{e}")

    def openEVTXDock(self, file_path):
        """
        Opens an EVTX log in a new EVTXDock and places it in the bottom area.
        If the IIS dock also exists, it splits them side by side horizontally.
        """
        self.logger.info(f"Opening EVTX Log for Parsing: {file_path}")
        dock = EVTXDock(file_path=file_path, parent=self)
        self.make_dock_fully_floatable(dock)
        dock.setObjectName("EVTXDock-Dynamic")

        # Add to the bottom
        self.addDockWidget(Qt.BottomDockWidgetArea, dock)
        self.evtx_dock = dock

        # If IIS dock also exists (and isn't floating), split horizontally
        if self.iis_dock and not self.iis_dock.isFloating():
            self.splitDockWidget(self.iis_dock, self.evtx_dock, Qt.Horizontal)

    @pyqtSlot()
    def openGenericLog(self):
        """
        Opens a Generic log file and displays it in the GenericDock.
        """
        try:
            file_paths, _ = QFileDialog.getOpenFileNames(
                self,
                "Open Generic Log",
                os.getcwd(),
                "Log/Text/XML Files (*.log *.txt *.xml, *.log.*);;All Files (*)"
            )
            if file_paths:
                self.openGenericDock(file_paths)
        except Exception as e:
            self.logger.error(f"Failed to open Generic log: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open Generic log:\n{e}")

    def openGenericDock(self, file_path):
        """
        Initializes the GenericDock with the provided log file for parsing.

        Args:
            file_path (str): Path to the Generic log file.
        """
        try:
            self.logger.info(f"Opening Generic Log for Parsing: {file_path}")
            dock = GenericDock(file_path, parent=self)  # Use keyword argument for file_path
            self.addDockWidget(Qt.RightDockWidgetArea, dock)
        except Exception as e:
            self.logger.error(f"Open Generic dock function error: {e}")

    def open3DDock(self):
        """
        Opens the 3D Charts Dock.
        """
        self.logger.info("Opening 3D Dock")
        dock = ThreeDDock(self)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    def openAdvancedSearch(self):
        """
        Opens the Advanced Search window.
        """
        self.advanced_search_dock = AdvancedSearchDock(self)
        self.addDockWidget(Qt.RightDockWidgetArea, self.advanced_search_dock)
        
    def openFrebTrace(self):
        # Let the user select a FREB XML file.
        xml_file, _ = QFileDialog.getOpenFileName(
            self, "Select FREB XML File", "", "XML Files (*.xml)"
        )
        if not xml_file:
            return

        # Determine the XSL file path (assumed to be in the same folder as the XML file).
        xsl_file = os.path.join(os.path.dirname(xml_file), "freb.xsl")
        if not os.path.exists(xsl_file):
            QMessageBox.critical(self, "Error", f"XSL file not found:\n{xsl_file}")
            return

        # Create an instance of the FrebViewerDock and add it to the main window.
        dock = FrebViewerDock(xml_file, xsl_file, parent=self)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

    @pyqtSlot(object)
    def onJumpToTime(self, target_datetime):
        """
        The timeline triggers jump => pass to all open docks that implement scrollToTime().

        Args:
            target_datetime (datetime): The target datetime to jump to.
        """
        self.logger.info(f"Timeline jumped to {target_datetime}")

        all_docks = self.findChildren(IISDock) \
                  + self.findChildren(EVTXDock) \
                  + self.findChildren(GenericDock)
        for dock in all_docks:
            dock.onTimelineJump(target_datetime)

    @pyqtSlot(str)
    def onSearchString(self, search_text):
        """
        The search dock triggers a search => pass to all log docks.

        Args:
            search_text (str): The search string entered by the user.
        """
        if not search_text:
            return
        self.logger.info(f"Search triggered: {search_text}")

        all_docks = self.findChildren(IISDock) \
                  + self.findChildren(EVTXDock) \
                  + self.findChildren(GenericDock)

        for dock in all_docks:
            dock.searchString(search_text) # type: ignore

    ####################
    # DOCK TOGGLE FUNCTION
    ####################
    def toggleDock(self, dock: QDockWidget, visible: bool):
        """
        Toggle the visibility of a dock widget.

        Args:
            dock (QDockWidget): The dock widget to toggle.
            visible (bool): Whether to show or hide the dock.
        """
        dock.setVisible(visible)

    ####################
    # Added Missing Method
    ####################
    @pyqtSlot()
    def openGenericDatabaseDialog(self):
        """
        Opens a file dialog to select a stored Generic log database to display.
        """
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Open Stored Generic Database", os.getcwd(), "SQLite DB Files (*.db *.sqlite);;All Files (*)")
            if file_path:
                self.openGenericDockWithDatabase(file_path)
        except Exception as e:
            self.logger.error(f"Failed to open Generic database: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open Generic database:\n{e}")

    def openGenericDockWithDatabase(self, db_path):
        """
        Initializes the GenericDock with the provided database for display.

        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.logger.info(f"Opening Generic Database for Display: {db_path}")
        dock = GenericDock(db_path, parent=self)  # Use keyword argument for db_path
        self.addDockWidget(Qt.RightDockWidgetArea, dock)
