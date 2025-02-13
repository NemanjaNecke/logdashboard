# ui/components/display_logs/IIS/all_stats_panel.py

import logging
import pandas as pd
import os
from PyQt5.QtCore import Qt, pyqtSlot  # pylint: disable=no-name-in-module
from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDialog,
    QVBoxLayout,
    QTreeWidget,
    QTreeWidgetItem,
    QPushButton,
    QTabWidget,
    QWidget,
    QTableView,
    QLabel,
    QFileDialog,
    QMessageBox,
    QTextEdit,
    QHeaderView,
    QComboBox,
    QHBoxLayout
)
from PyQt5.QtGui import QStandardItemModel, QStandardItem  # pylint: disable=no-name-in-module

from services.analyze.IIS.iis_analyze_worker import AnalyzerWorker  # Reuse the worker
from ui.components.display_logs.IIS.table.sheet_selection_dialog import SheetSelectionDialog  # Import the SheetSelectionDialog
from ui.components.display_logs.IIS.stats.compare_stats_dialog import CompareStatsDialog  # Import CompareStatsDialog
from services.sql_workers.db_managers.IIS.db_manager_iis import DatabaseManager  # Import DatabaseManager
from services.sql_workers.db_managers.IIS.iis_stats_loader import StatsLoader  # Import StatsLoader


class AllStatsPanel(QDialog):
    """
    Dialog that displays all IIS log statistics in a tree structure,
    loading child items lazily upon expansion to keep the UI responsive
    for large data sets. Additionally, it can display analysis reports
    as both tabular data and textual reports. Supports loading up to two
    reports and comparing them.
    """

    MAX_REPORTS = 2  # Maximum number of reports that can be loaded

    def __init__(self, stats_dict=None, parent=None):
        """
        Initializes the AllStatsPanel.

        :param stats_dict: Optional dictionary containing statistics to display.
        :param parent: Parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("All IIS Statistics")
        self.resize(1200, 800)  # Increased size for better visibility
        self.report1_combo = QComboBox()
        self.report2_combo = QComboBox()
        self.excel_files = []

        # Initialize logger
        self.logger = logging.getLogger('AllStatsPanel')  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

        # Main layout
        layout = QVBoxLayout(self)

        # Tab Widget to hold Tree and Analysis Reports
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # Conditionally add Statistics Tree Tab if stats_dict is provided
        if stats_dict is not None:
            # Tab 1: Statistics Tree
            self.tree_tab = QWidget()
            self.tree_layout = QVBoxLayout(self.tree_tab)
            self.tree_widget = QTreeWidget()
            self.tree_widget.setColumnCount(3)
            self.tree_widget.setHeaderLabels(["Field", "Value", "Count"])
            self.tree_layout.addWidget(self.tree_widget)
            self.tab_widget.addTab(self.tree_tab, "Statistics Tree")
        else:
            self.logger.debug("stats_dict not provided; Statistics Tree tab will not be added.")

        # Tab 2: Analysis Report
        self.report_tab = QWidget()
        self.report_layout = QVBoxLayout(self.report_tab)
        self.report_label = QLabel("No Analysis Report Loaded.")
        self.report_label.setAlignment(Qt.AlignCenter)
        self.report_layout.addWidget(self.report_label)

        # Buttons for loading reports and comparing
        button_layout = QHBoxLayout()
        self.load_report_button = QPushButton("Load Analysis Report")
        self.load_report_button.clicked.connect(self.load_analysis_report)
        button_layout.addWidget(self.load_report_button)

        self.compare_reports_button = QPushButton("Compare Reports")
        self.compare_reports_button.clicked.connect(self.compare_reports)
        self.compare_reports_button.setEnabled(False)  # Disabled until two reports are loaded
        button_layout.addWidget(self.compare_reports_button)

        self.report_layout.addLayout(button_layout)

        # Initialize a QTabWidget to hold multiple sheet views
        self.analysis_tab_widget = QTabWidget()
        self.report_layout.addWidget(self.analysis_tab_widget)

        self.tab_widget.addTab(self.report_tab, "Analysis Report")

        # Initialize field cache for lazy loading
        self.field_cache = {}  # field -> list of (value, count)

        # Keep track of loaded reports
        self.loaded_reports = []  # List of dicts with keys: 'name', 'type', 'data'

        # Populate the statistics tree if stats_dict is provided
        if stats_dict is not None:
            self.buildTopLevelItems(stats_dict)

        # Connect expansion signal to lazy-load child items if Statistics Tree is present
        if stats_dict is not None:
            self.tree_widget.itemExpanded.connect(self.onItemExpanded)

        self.logger.debug("AllStatsPanel initialized.")

    def buildTopLevelItems(self, stats_dict):
        """
        Creates one top-level item per field, with a placeholder child for lazy expansion.
        """
        if not stats_dict:
            self.logger.debug("No stats found to display in AllStatsPanel.")
            return

        for field, value_counts in stats_dict.items():
            # Sort by highest count descending
            sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
            self.field_cache[field] = sorted_values

            # Create top-level item
            field_item = QTreeWidgetItem([field, "", ""])
            self.tree_widget.addTopLevelItem(field_item)

            # Add a dummy child so it shows as expandable
            if sorted_values:
                placeholder = QTreeWidgetItem(["[Loading...]", "", ""])
                field_item.addChild(placeholder)
            else:
                # No child data, do not add placeholder
                pass

            # Initially, we keep each field collapsed
            self.tree_widget.collapseItem(field_item)

    def onItemExpanded(self, item):
        """
        Called when a top-level item is expanded.
        Loads the real child items.
        """
        # If the item has a single child named "[Loading...]" -> we haven't loaded yet
        if item.childCount() == 1 and item.child(0).text(0) == "[Loading...]":
            # Remove the placeholder
            item.removeChild(item.child(0))

            field = item.text(0)
            values = self.field_cache.get(field, [])
            self.logger.debug(f"Lazy loading {len(values)} child items for field '{field}'.")

            # Load all child items at once for simplicity
            for value, count in values:
                child_item = QTreeWidgetItem(["", str(value), str(count)])
                item.addChild(child_item)

    def load_analysis_report(self):
        """
        Allows the user to select an Excel report and displays its contents.
        Supports loading up to two Excel files, aggregating their sheets.
        """
        try:
            if len(self.excel_files) >= self.MAX_REPORTS:
                QMessageBox.warning(self, "Maximum Reports Loaded",
                                    f"You can only load up to {self.MAX_REPORTS} analysis reports.")
                return

            # Select Excel file
            excel_path, _ = QFileDialog.getOpenFileName(
                self, "Select Analysis Excel File", os.getcwd(),
                "Excel Files (*.xlsx);;All Files (*)"
            )
            if not excel_path:
                return  # User canceled

            if excel_path in self.excel_files:
                QMessageBox.warning(self, "Duplicate Report", "The selected report has already been loaded.")
                return
        
            if len(self.excel_files) > 1:
                self.compare_reports_button.setEnabled(True)



            self.excel_files.append(excel_path)
            self.logger.info(f"Selected Excel report: {excel_path}")
            base_file_name = os.path.basename(excel_path)
            self.report_label.setText(f"Loaded Excel Report: {base_file_name}")

            # Load Excel file using pandas
            with pd.ExcelFile(excel_path) as xl:
                sheet_names = xl.sheet_names
                self.logger.debug(f"Available sheets: {sheet_names}")

            if not sheet_names:
                self.logger.warning("Selected Excel file contains no sheets.")
                QMessageBox.warning(self, "No Sheets Found", "The selected Excel file contains no sheets.")
                return

            # Prompt user to select sheets to load
            sheet_selection_dialog = SheetSelectionDialog(sheet_names, self)
            if sheet_selection_dialog.exec_():
                selected_sheets = sheet_selection_dialog.get_selected_sheets()
                self.logger.debug(f"Selected sheets to load: {selected_sheets}")

                if not selected_sheets:
                    QMessageBox.warning(self, "No Sheets Selected", "No sheets were selected for loading.")
                    return

                # Load and display each selected sheet
                for sheet in selected_sheets:
                    try:
                        df = pd.read_excel(excel_path, sheet_name=sheet)
                        self.logger.debug(f"Loaded sheet '{sheet}' with {len(df)} rows and {len(df.columns)} columns.")

                        # Determine if the sheet is textual or tabular
                        # Assuming sheets named 'Report' and 'AdvancedReport' are textual
                        if sheet.lower() in ["report", "advancedreport"]:
                            # Concatenate all cell values into a single text block
                            # Handle multiple columns by joining their text
                            report_text = "\n".join(
                                df.astype(str).fillna('').apply(' '.join, axis=1).tolist()
                            )

                            # Create a QTextEdit to display the report
                            text_edit = QTextEdit()
                            text_edit.setReadOnly(True)
                            text_edit.setText(report_text)

                            # Create unique report name by combining file name and sheet name
                            report_name = f"{os.path.splitext(base_file_name)[0]} - {sheet}"

                            # Add the text edit to a new tab
                            self.analysis_tab_widget.addTab(text_edit, report_name)
                            self.logger.debug(f"Added textual sheet '{sheet}' as '{report_name}' to Analysis Report tabs.")

                            # Store the loaded report
                            self.loaded_reports.append({
                                'name': report_name,
                                'type': 'textual',
                                'data': report_text
                            })
                        else:
                            # For other sheets, use QTableView
                            model = self.pandasModel(df)

                            # Create QTableView and set model
                            table_view = QTableView()
                            table_view.setModel(model)
                            table_view.setWordWrap(True)  # Enable word wrap
                            table_view.setTextElideMode(Qt.ElideNone)  # Prevent text eliding
                            table_view.horizontalHeader().setStretchLastSection(True)  # Stretch last column
                            table_view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)  # Auto resize rows
                            table_view.setAlternatingRowColors(True)
                            table_view.setSortingEnabled(True)  # Allow sorting
                            table_view.resizeColumnsToContents()  # Adjust column widths

                            # Create unique report name by combining file name and sheet name
                            report_name = f"{os.path.splitext(base_file_name)[0]} - {sheet}"

                            # Add the table view to a new tab
                            self.analysis_tab_widget.addTab(table_view, report_name)
                            self.logger.debug(f"Added tabular sheet '{sheet}' as '{report_name}' to Analysis Report tabs.")

                            # Store the loaded report
                            self.loaded_reports.append({
                                'name': report_name,
                                'type': 'tabular',
                                'data': df
                            })

                    except Exception as e:
                        self.logger.error(f"Failed to load sheet '{sheet}': {e}")
                        QMessageBox.critical(self, "Load Error", f"Failed to load sheet '{sheet}':\n{e}")
                if len(self.excel_files) > 1:
                    self.compare_reports_button.setEnabled(True)
                self.logger.info("All selected sheets have been loaded into Analysis Report.")
                QMessageBox.information(self, "Load Complete", "Selected sheets have been loaded successfully.")

            else:
                self.logger.info("Sheet selection dialog was canceled.")
                self.report_label.setText("No Analysis Report Loaded.")
                return
        except Exception as e:
            self.logger.error(f"Failed to load analysis report: {e}")
            QMessageBox.critical(self, "Load Error", f"Failed to load analysis report:\n{e}")

    def pandasModel(self, df):
        """
        Converts a pandas DataFrame to a QStandardItemModel for display in QTableView.
        Enables word wrapping and aligns text for better readability.
        """
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(df.columns.tolist())

        for row in df.itertuples(index=False):
            items = [QStandardItem(str(field)) for field in row]
            for item in items:
                item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                # Enable word wrap by not restricting the item flags
            model.appendRow(items)

        return model

    def compare_reports(self):
        """
        Initiates the comparison of two selected tabular reports.
        """
        # Filter loaded reports to include only tabular ones
        tabular_reports = [r for r in self.loaded_reports if r['type'] == 'tabular']

        if len(tabular_reports) < 2:
            QMessageBox.warning(self, "Insufficient Reports", "Please load two tabular reports to compare.")
            return

        # Retrieve the two reports
        report1 = tabular_reports[0]
        report2 = tabular_reports[1]
        # Create a dialog to select two reports to compare
        compare_dialog = QDialog(self)
        compare_dialog.setWindowTitle("Select Reports to Compare")
        compare_dialog.resize(400, 150)

        dialog_layout = QVBoxLayout(compare_dialog)

        # Dropdowns for selecting reports
        dropdown_layout = QHBoxLayout()

        dropdown_layout.addWidget(QLabel("Report 1:"))

        for report in tabular_reports:
            self.report1_combo.addItem(report['name'])
        dropdown_layout.addWidget(self.report1_combo)

        dropdown_layout.addWidget(QLabel("Report 2:"))

        for report in tabular_reports:
            self.report2_combo.addItem(report['name'])
        dropdown_layout.addWidget(self.report2_combo)

        dialog_layout.addLayout(dropdown_layout)

        # Compare button
        button_layout = QHBoxLayout()
        compare_button = QPushButton("Compare")
        compare_button.clicked.connect(lambda: self.execute_comparison(compare_dialog))
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(compare_dialog.reject)
        button_layout.addStretch()
        button_layout.addWidget(compare_button)
        button_layout.addWidget(cancel_button)

        dialog_layout.addLayout(button_layout)

        if compare_dialog.exec_() == QDialog.Accepted:
            report1_name = self.report1_combo.currentText()
            report2_name = self.report2_combo.currentText()

            if report1_name == report2_name:
                QMessageBox.warning(self, "Invalid Selection", "Please select two different reports to compare.")
                return

            # Retrieve the report data
            report1 = next(r for r in tabular_reports if r['name'] == report1_name)
            report2 = next(r for r in tabular_reports if r['name'] == report2_name)

            # Open the comparison dialog
            comparison_dialog = CompareStatsDialog(report1, report2, self)
            comparison_dialog.exec_()

    def execute_comparison(self, compare_dialog):
        """
        Executes the comparison after user selection.

        :param compare_dialog: The dialog where reports are selected.
        """
        report1_name = self.report1_combo.currentText()
        report2_name = self.report2_combo.currentText()

        # Retrieve the report data
        report1 = next(r for r in self.loaded_reports if r['name'] == report1_name)
        report2 = next(r for r in self.loaded_reports if r['name'] == report2_name)


        # Open the comparison dialog
        comparison_dialog = CompareStatsDialog(report1, report2, self)
        comparison_dialog.exec_()

    ####################
    # ADDITIONAL METHODS IF NEEDED
    ####################
    # If there are other methods related to loading reports or comparing, ensure they remain unchanged.

