from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDockWidget, QWidget, QVBoxLayout, QPushButton, QMessageBox,
    QFileDialog, QTableView, QHBoxLayout, QAbstractItemView
)
from PyQt5.QtCore import Qt, pyqtSlot  # pylint: disable=no-name-in-module
import os
import logging

# Import helper and other necessary docks
from services.controllers.DB_manager.db_controller import DBController
from ui.components.display_logs.IIS.dock_iis import IISDock
from ui.components.display_logs.EVTX.dock_evtx import EVTXDock
from ui.components.display_logs.GENERIC.dock_generic import GenericDock
from ui.components.db_load.db_table_model import DBTableModel, DBSortFilterProxyModel


class DBManagerDock(QDockWidget):
    """
    Dock widget to manage stored databases.
    """
    def __init__(self, parent=None):
        super().__init__("DB Manager", parent)
        self.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.setObjectName("DBManagerDock")  # Ensure unique object name
        self.logger = logging.getLogger('DBManagerDock')  # pylint: disable=no-member
        container = QWidget()
        main_layout = QVBoxLayout()
        container.setLayout(main_layout)

        # Initialize DBController
        self.db_controller = DBController(os.path.join(os.getcwd(), 'db'))

        # Table to display databases
        self.table_view = QTableView()
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        # Change to allow multiple row selection:
        self.table_view.setSelectionMode(QAbstractItemView.MultiSelection)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.verticalHeader().setVisible(False)
        main_layout.addWidget(self.table_view)

        # Buttons
        button_layout = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.delete_btn = QPushButton("Delete Selected")
        self.export_btn = QPushButton("Export Selected to CSV")
        button_layout.addWidget(self.refresh_btn)
        button_layout.addWidget(self.delete_btn)
        button_layout.addWidget(self.export_btn)
        main_layout.addLayout(button_layout)

        self.setWidget(container)

        # Connect signals
        self.refresh_btn.clicked.connect(self.load_databases)
        self.delete_btn.clicked.connect(self.delete_selected_database)
        self.export_btn.clicked.connect(self.export_selected_database)

        # Load databases initially
        self.load_databases()

    def load_databases(self):
        """
        Loads the list of databases into the table view.
        """
        databases = self.db_controller.list_databases()
        # Create the source model
        self.model = DBTableModel(databases)
        # Wrap it in a sort/filter proxy model
        self.proxy_model = DBSortFilterProxyModel()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)
        # Optionally hide the 'Path' column (adjust index if needed)
        self.table_view.setColumnHidden(4, True)
        # Resize columns for better visibility
        self.table_view.resizeColumnsToContents()
        self.table_view.horizontalHeader().setStretchLastSection(True)

    @pyqtSlot()
    def open_selected_database(self):
        """
        Opens the selected database in its respective viewer.
        """
        selected_indexes = self.table_view.selectionModel().selectedRows()
        if not selected_indexes:
            QMessageBox.warning(self, "Open Database", "No database selected to open.")
            return

        # For simplicity, open the first selected database
        selected_row = selected_indexes[0].row()
        db = self.model.databases[selected_row]
        db_path = db['path']
        db_type = db['type']

        if not os.path.exists(db_path):
            QMessageBox.critical(self, "Error", f"Database file not found: {db_path}")
            return

        parent = self.parent()
        if not parent:
            QMessageBox.critical(self, "Error", "Parent window not found.")
            return

        # Check if the dock is already open
        existing_docks = parent.findChildren((IISDock, EVTXDock, GenericDock))
        for dock in existing_docks:
            if getattr(dock, 'db_path', None) == db_path:
                dock.raise_()
                dock.activateWindow()
                QMessageBox.information(self, "Info", f"Database '{db_path}' is already open.")
                return

        # Determine which dock to open based on db_type
        if db_type.lower() == 'iis':
            dock = IISDock(db_path, parent)
            parent.addDockWidget(Qt.RightDockWidgetArea, dock)
            dock.show()
            self.logger.info(f"Opened IIS database: {db_path}")
        elif db_type.lower() == 'evtx':
            dock = EVTXDock(db_path, parent)
            parent.addDockWidget(Qt.RightDockWidgetArea, dock)
            dock.show()
            self.logger.info(f"Opened EVTX database: {db_path}")
        elif db_type.lower() == 'generic':
            dock = GenericDock(db_path, parent)
            parent.addDockWidget(Qt.RightDockWidgetArea, dock)
            dock.show()
            self.logger.info(f"Opened Generic database: {db_path}")
        else:
            QMessageBox.warning(self, "Unknown Type", f"Unknown database type: {db_type}")
            self.logger.error(f"Attempted to open unknown database type: {db_path}")

    @pyqtSlot()
    def delete_selected_database(self):
        """
        Deletes the selected databases after confirmation.
        """
        selected_indexes = self.table_view.selectionModel().selectedRows()
        if not selected_indexes:
            QMessageBox.warning(self, "Delete Database", "No database selected to delete.")
            return

        # Get the list of databases to delete
        # Use the proxy's mapping to source model indices
        dbs_to_delete = []
        for proxy_index in selected_indexes:
            source_index = self.proxy_model.mapToSource(proxy_index)
            db = self.model.databases[source_index.row()]
            dbs_to_delete.append(db)

        db_names = [db.get("name", "Unknown") for db in dbs_to_delete]
        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            "Are you sure you want to delete the following databases?\n" + "\n".join(db_names),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply == QMessageBox.No:
            return

        for db in dbs_to_delete:
            db_path = db.get('path')
            db_name = db.get('name')
            try:
                os.remove(db_path)
                self.logger.info(f"Deleted database: {db_path}")

                # If the database is currently open in a dock, close it
                parent = self.parent()
                if parent:
                    open_docks = parent.findChildren((IISDock, EVTXDock, GenericDock))
                    for dock in open_docks:
                        if getattr(dock, 'db_path', None) == db_path:
                            dock.close()
                            parent.removeDockWidget(dock)
                            self.logger.info(f"Closed dock for deleted database: {db_path}")
            except Exception as e:
                QMessageBox.critical(self, "Delete Error", f"Failed to delete database '{db_name}':\n{e}")
        QMessageBox.information(self, "Delete Database", "Selected databases have been deleted.")
        self.load_databases()

    @pyqtSlot()
    def export_selected_database(self):
        """
        Exports the selected database to a CSV file.
        If more than one database is selected, only the first is exported.
        """
        selected_indexes = self.table_view.selectionModel().selectedRows()
        if not selected_indexes:
            QMessageBox.warning(self, "Export Database", "No database selected to export.")
            return

        # For simplicity, export only the first selected database.
        proxy_index = selected_indexes[0]
        source_index = self.proxy_model.mapToSource(proxy_index)
        db = self.model.databases[source_index.row()]
        db_path = db['path']
        db_name = db['name']

        if not os.path.exists(db_path):
            QMessageBox.critical(self, "Error", f"Database file not found: {db_path}")
            return

        # Choose CSV save location
        csv_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export to CSV",
            os.path.join(os.getcwd(), f"{db_name}.csv"),
            "CSV Files (*.csv)"
        )
        if not csv_path:
            return  # User cancelled

        success, error = self.db_controller.export_database(db_path, csv_path)
        if success:
            self.logger.info(f"Exported '{db_path}' to CSV at '{csv_path}'.")
            QMessageBox.information(self, "Export Database", f"Exported '{db_name}' to CSV at:\n{csv_path}")
        else:
            self.logger.error(f"Failed to export '{db_path}' to CSV: {error}")
            QMessageBox.critical(self, "Export Error", f"Failed to export '{db_name}' to CSV:\n{error}")
